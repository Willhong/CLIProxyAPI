package management

import (
	"testing"
	"time"
)

func TestParseCacheStatsWindow(t *testing.T) {
	cases := map[string]time.Duration{"": 24 * time.Hour, "1h": time.Hour, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour}
	for raw, want := range cases {
		if got, ok := parseCacheStatsWindow(raw); !ok || got != want {
			t.Fatalf("%q: got %v %v, want %v", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"0h", "-1h", "9d", "abc", "xd"} {
		if _, ok := parseCacheStatsWindow(raw); ok {
			t.Fatalf("%q must be rejected", raw)
		}
	}
}
