package watcher

import "time"

// scheduleAuthRecheckLocked coalesces noisy events into a single trailing check.
// Callers hold authRescanMu. Replacing the timer (rather than resetting it) also
// invalidates callbacks which have already fired but are waiting for the lock.
func (w *Watcher) scheduleAuthRecheckLocked(path string, delay time.Duration) {
	if w.stopped.Load() {
		return
	}
	normalized := w.normalizeAuthPath(path)
	if w.authRechecks == nil {
		w.authRechecks = make(map[string]*time.Timer)
	}
	if previous := w.authRechecks[normalized]; previous != nil {
		previous.Stop()
	}
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		w.authRescanMu.Lock()
		defer w.authRescanMu.Unlock()
		if w.authRechecks[normalized] != timer {
			return
		}
		delete(w.authRechecks, normalized)
		if w.stopped.Load() {
			return
		}
		// Read once: reconcile current contents or a confirmed ENOENT, not the
		// original event's operation. Do not retry indefinitely on filesystem errors.
		w.readAuthFileLocked(path, true)
	})
	w.authRechecks[normalized] = timer
}

func (w *Watcher) stopAuthRechecks() {
	w.authRescanMu.Lock()
	defer w.authRescanMu.Unlock()
	for path, timer := range w.authRechecks {
		timer.Stop()
		delete(w.authRechecks, path)
	}
}
