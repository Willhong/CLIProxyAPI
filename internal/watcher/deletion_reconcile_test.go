package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func deletionFixture(t *testing.T) (*Watcher, string, chan AuthUpdate) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "token.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude","email":"test@example.com"}`), 0600); err != nil {
		t.Fatal(err)
	}
	w := &Watcher{authDir: dir, configPath: filepath.Join(dir, "config.yaml")}
	w.SetConfig(&config.Config{AuthDir: dir})
	queue := make(chan AuthUpdate, 16)
	w.SetAuthUpdateQueue(queue)
	t.Cleanup(w.stopDispatch)
	w.addOrUpdateClient(path)
	synctest.Wait()
	select {
	case u := <-queue:
		if u.Action != AuthUpdateActionAdd {
			t.Fatalf("initial update: %+v", u)
		}
	default:
		t.Fatal("initial auth not dispatched")
	}
	return w, path, queue
}

func assertDeletionReconciled(t *testing.T, w *Watcher, path string, queue chan AuthUpdate) {
	t.Helper()
	synctest.Wait()
	w.clientsMutex.RLock()
	known := len(w.currentAuths) + len(w.fileAuthsByPath) + len(w.lastAuthHashes)
	w.clientsMutex.RUnlock()
	if known != 0 {
		t.Fatalf("stale auth retained after final deletion: %s (%d cache entries)", path, known)
	}
	select {
	case u := <-queue:
		if u.Action != AuthUpdateActionDelete || u.Revision() == 0 {
			t.Fatalf("expected stamped delete, got %+v", u)
		}
	default:
		t.Fatal("final deletion was not dispatched")
	}
	select {
	case u := <-queue:
		t.Fatalf("unexpected extra update: %+v", u)
	default:
	}
}

func TestDeletionReconcileCoalescesTrailingChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, queue := deletionFixture(t)
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		time.Sleep(500 * time.Millisecond)
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		time.Sleep(600 * time.Millisecond)
		synctest.Wait()
		w.clientsMutex.RLock()
		n := len(w.currentAuths)
		w.clientsMutex.RUnlock()
		if n != 1 {
			t.Fatal("superseded timer removed auth before trailing edge")
		}
		w.authRescanMu.Lock()
		pending := len(w.authRechecks)
		w.authRescanMu.Unlock()
		if pending != 1 {
			t.Fatalf("expected one coalesced check, got %d", pending)
		}
		time.Sleep(500 * time.Millisecond)
		assertDeletionReconciled(t, w, path, queue)
		w.authRescanMu.Lock()
		pending = len(w.authRechecks)
		w.authRescanMu.Unlock()
		if pending != 0 {
			t.Fatalf("completed check retained timer: %d", pending)
		}
	})
}

func TestDeletionReconcileStopCancelsPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, queue := deletionFixture(t)
		fw, err := fsnotify.NewWatcher()
		if err != nil {
			t.Fatal(err)
		}
		w.watcher = fw
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
		if err := w.Stop(); err != nil {
			t.Fatal(err)
		}
		// A queued event after Stop must not revive a timer either.
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		time.Sleep(2 * time.Second)
		synctest.Wait()
		w.authRescanMu.Lock()
		pending := len(w.authRechecks)
		w.authRescanMu.Unlock()
		if pending != 0 {
			t.Fatalf("Stop retained checks: %d", pending)
		}
		w.clientsMutex.RLock()
		n := len(w.currentAuths)
		w.clientsMutex.RUnlock()
		if n != 1 {
			t.Fatal("callback mutated auth after Stop")
		}
		select {
		case u := <-queue:
			t.Fatalf("callback dispatched after Stop: %+v", u)
		default:
		}
	})
}

func TestDeletionReconcileKnownWithoutHash(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, queue := deletionFixture(t)
		w.clientsMutex.Lock()
		delete(w.lastAuthHashes, w.normalizeAuthPath(path))
		w.clientsMutex.Unlock()
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
		time.Sleep(2 * time.Second)
		assertDeletionReconciled(t, w, path, queue)
	})
}

func TestDeletionReconcileReadErrorDoesNotDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, queue := deletionFixture(t)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
		// EISDIR, unlike ENOENT, is not evidence that credentials were removed.
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		w.clientsMutex.RLock()
		n := len(w.currentAuths)
		w.clientsMutex.RUnlock()
		if n != 1 {
			t.Fatal("read error revoked auth")
		}
		select {
		case u := <-queue:
			t.Fatalf("read error emitted update: %+v", u)
		default:
		}
		w.authRescanMu.Lock()
		pending := len(w.authRechecks)
		w.authRescanMu.Unlock()
		if pending != 0 {
			t.Fatalf("read error started unbounded retry: %d", pending)
		}
	})
}

func TestDeletionReconcileFinalRemoveWithinDebounce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, queue := deletionFixture(t)
		// Atomic self-save replaces the inode without changing the credentials.
		replacement := path + ".tmp"
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(replacement, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacement, path); err != nil {
			t.Fatal(err)
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		// No third filesystem event: the trailing reconciliation must delete it.
		time.Sleep(2 * time.Second)
		assertDeletionReconciled(t, w, path, queue)
	})
}

func TestDeletionReconcileMissingWriteCreate(t *testing.T) {
	for _, op := range []fsnotify.Op{fsnotify.Write, fsnotify.Create} {
		t.Run(op.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, path, queue := deletionFixture(t)
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				w.handleEvent(fsnotify.Event{Name: path, Op: op})
				time.Sleep(2 * time.Second)
				assertDeletionReconciled(t, w, path, queue)
			})
		})
	}
}

func TestDeletionReconcileAtomicGapDoesNotRevoke(t *testing.T) {
	for _, op := range []fsnotify.Op{fsnotify.Write, fsnotify.Create, fsnotify.Remove, fsnotify.Rename} {
		t.Run(op.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, path, queue := deletionFixture(t)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				go func() {
					time.Sleep(60 * time.Millisecond)
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Error(err)
					}
				}()
				w.handleEvent(fsnotify.Event{Name: path, Op: op})
				time.Sleep(2 * time.Second)
				synctest.Wait()
				w.clientsMutex.RLock()
				n := len(w.currentAuths)
				w.clientsMutex.RUnlock()
				if n != 1 {
					t.Fatalf("atomic replacement revoked auth: %d", n)
				}
				select {
				case u := <-queue:
					t.Fatalf("atomic replacement emitted update: %+v", u)
				default:
				}
			})
		})
	}
}
