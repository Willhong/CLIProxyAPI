package cachestats

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func fixedStore(now time.Time) *Store {
	s := NewStore()
	s.now = func() time.Time { return now }
	return s
}

func record(auth, model string, at time.Time, detail coreusage.Detail) coreusage.Record {
	return coreusage.Record{AuthID: auth, AuthIndex: auth + "-idx", Provider: "codex", Model: model, RequestedAt: at, Detail: detail}
}

func TestHitRateUsesCanonicalBreakdown(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 30, 0, 0, time.UTC)
	s := fixedStore(now)
	breakdown := coreusage.NewSubsetTokenBreakdown(1000, 900, 0, 50, 0, 1050)
	if !breakdown.Valid() {
		t.Fatalf("fixture breakdown invalid: %+v", breakdown)
	}
	s.Add(record("a.json", "gpt-x", now, coreusage.Detail{InputTokens: 1000, CachedTokens: 900, TokenBreakdown: breakdown}))
	s.Add(record("a.json", "gpt-x", now, coreusage.Detail{InputTokens: 1000, CachedTokens: 0}))

	snap := s.Snapshot(time.Hour)
	if snap.Total.Requests != 2 || snap.Total.InputTokens != 2000 || snap.Total.CacheReadTokens != 900 {
		t.Fatalf("unexpected totals: %+v", snap.Total)
	}
	if snap.Total.HitRate == nil || *snap.Total.HitRate != 0.45 {
		t.Fatalf("hit rate = %v, want 0.45", snap.Total.HitRate)
	}
	if len(snap.Credentials) != 1 || snap.Credentials[0].AuthIndex != "a.json-idx" || len(snap.Credentials[0].Models) != 1 {
		t.Fatalf("unexpected credentials: %+v", snap.Credentials)
	}
}

func TestLegacyDetailTreatsInputAsInclusive(t *testing.T) {
	c := countsFromDetail(coreusage.Detail{InputTokens: 100, CachedTokens: 40, CacheCreationTokens: 5})
	if c.InputTokens != 100 || c.CacheReadTokens != 40 || c.CacheWriteTokens != 5 {
		t.Fatalf("unexpected counts: %+v", c)
	}
	if c = countsFromDetail(coreusage.Detail{InputTokens: 10, CacheReadTokens: 30}); c.InputTokens != 30 {
		t.Fatalf("cache read must not exceed input: %+v", c)
	}
}

func TestWindowAndNoInput(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 30, 0, 0, time.UTC)
	s := fixedStore(now)
	s.Add(record("a.json", "m", now.Add(-3*time.Hour), coreusage.Detail{InputTokens: 100, CachedTokens: 100}))
	s.Add(coreusage.Record{Model: "m", RequestedAt: now})
	failed := record("b.json", "m", now, coreusage.Detail{})
	failed.Failed = true
	s.Add(failed)

	snap := s.Snapshot(time.Hour)
	if snap.Total.Requests != 1 || snap.Total.Failed != 1 || snap.Total.HitRate != nil {
		t.Fatalf("1h window: %+v", snap.Total)
	}
	if snap = s.Snapshot(24 * time.Hour); snap.Total.Requests != 2 || len(snap.Credentials) != 2 {
		t.Fatalf("24h window: %+v", snap)
	}
}

func TestPersistenceRoundTripAndRetention(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 30, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "cache-stats.json")
	s := fixedStore(now)
	if err := s.SetPath(path); err != nil {
		t.Fatal(err)
	}
	s.Add(record("a.json", "m", now, coreusage.Detail{InputTokens: 100, CachedTokens: 80}))
	s.Add(record("a.json", "m", now.Add(-9*24*time.Hour), coreusage.Detail{InputTokens: 100}))
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("stat %v mode %v", err, info)
	}

	loaded := fixedStore(now)
	if err = loaded.SetPath(path); err != nil {
		t.Fatal(err)
	}
	snap := loaded.Snapshot(Retention)
	if snap.Total.Requests != 1 || snap.Total.CacheReadTokens != 80 {
		t.Fatalf("reloaded totals: %+v", snap.Total)
	}
	if err = NewStore().SetPath("relative.json"); err == nil {
		t.Fatal("relative path must be rejected")
	}
}
