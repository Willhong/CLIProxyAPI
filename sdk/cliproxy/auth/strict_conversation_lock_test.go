package auth

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	bolt "go.etcd.io/bbolt"
)

func TestStrictConversationConcurrentPickLockCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	m := strictChildManager(path)
	entered, release := make(chan struct{}), make(chan struct{})
	firstDone := make(chan error, 1)
	ctx := WithConversationCaller(context.Background(), "local-lock-test")
	opts := func(id string) cliproxyexecutor.Options {
		return cliproxyexecutor.Options{Headers: http.Header{ConversationIDHeader: {id}}}
	}
	go func() {
		_, _, _, err := m.strictPick(ctx, opts("first"), []string{"gemini"}, "", nil, func(cliproxyexecutor.Options) (*Auth, ProviderExecutor, string, error) {
			close(entered)
			<-release
			return &Auth{ID: "child-a", Provider: "gemini"}, schedulerTestExecutor{}, "gemini", nil
		})
		firstDone <- err
	}()
	<-entered // First strictPick owns an open DB and a write transaction.
	canceled, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	var picks atomic.Int32
	go func() {
		_, _, _, err := m.strictPick(canceled, opts("second"), []string{"gemini"}, "", nil, func(cliproxyexecutor.Options) (*Auth, ProviderExecutor, string, error) {
			picks.Add(1)
			return &Auth{ID: "child-b", Provider: "gemini"}, schedulerTestExecutor{}, "gemini", nil
		})
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("second error=%v", err)
		}
	case <-time.After(300 * time.Millisecond):
		close(release)
		<-firstDone
		<-secondDone
		t.Fatal("concurrent canceled pick waited for the first transaction")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if picks.Load() != 0 {
		t.Fatalf("canceled selector ran %d times", picks.Load())
	}
	// Same-process contention must not poison subsequent requests or pin changes.
	if _, _, err := m.pickNext(ctx, "gemini", "", opts("first"), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.pickNext(ctx, "gemini", "", opts("third"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestStrictConversationExternalLockRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	held, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	timer := time.AfterFunc(30*time.Millisecond, func() { _ = held.Close() })
	defer timer.Stop()
	db, err := openConversationBindings(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStrictConversationAlreadyCanceledDoesNotOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := openConversationBindings(ctx, path)
	if db != nil {
		db.Close()
		t.Fatal("opened DB for canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

// Exercise the real strictPick path while an independent DB handle owns flock.
// Release the handle even on the old implementation so a failing test never
// leaves a 30-second waiter or a goroutine using a deleted temporary directory.
func TestStrictConversationExternalLockCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	held, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := strictChildManager(path)
	ctx, cancel := context.WithTimeout(WithConversationCaller(context.Background(), "lock-test"), 40*time.Millisecond)
	defer cancel()
	var picks atomic.Int32
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, _, _, err := m.strictPick(ctx, cliproxyexecutor.Options{Headers: http.Header{ConversationIDHeader: {"canceled"}}}, []string{"gemini"}, "", nil, func(cliproxyexecutor.Options) (*Auth, ProviderExecutor, string, error) {
			picks.Add(1)
			return &Auth{ID: "child-a", Provider: "gemini"}, schedulerTestExecutor{}, "gemini", nil
		})
		done <- err
	}()
	var result error
	select {
	case result = <-done:
		t.Logf("strictPick returned after %s while external flock remained held: %v", time.Since(start), result)
	case <-time.After(300 * time.Millisecond):
		stack := make([]byte, 1<<20)
		stack = stack[:runtime.Stack(stack, true)]
		t.Logf("canceled request still waiting at %s; bbolt.flock in stacks=%v", time.Since(start), strings.Contains(string(stack), "bbolt.flock"))
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}
		held = nil
		result = <-done
		t.Errorf("strictPick did not honor cancellation while external DB handle held the lock; after release err=%v picks=%d", result, picks.Load())
	}
	if held != nil {
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(result, context.DeadlineExceeded) {
		t.Errorf("error=%v, want deadline exceeded", result)
	}
	if picks.Load() != 0 {
		t.Errorf("canceled request ran selector %d times", picks.Load())
	}
	// No asynchronous opener may acquire/retain flock after cancellation.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("DB lock lingered after request returned: %v", err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(conversationBucket).ForEach(func(_, _ []byte) error { t.Error("canceled request persisted a binding"); return nil })
	}); err != nil {
		t.Fatal(err)
	}
}
