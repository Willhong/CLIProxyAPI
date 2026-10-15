package auth

import (
	"context"
	"crypto/sha256"
	"net/http"
	"path/filepath"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	bolt "go.etcd.io/bbolt"
)

func strictTwoProviderManager(t *testing.T, path string) *Manager {
	t.Helper()
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	for _, provider := range []string{"gemini", "claude"} {
		m.executors[provider] = schedulerTestExecutor{}
	}
	for id, provider := range map[string]string{"g1": "gemini", "g2": "gemini", "c1": "claude", "c2": "claude"} {
		if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: provider}); err != nil {
			t.Fatal(err)
		}
	}
	m.runtimeConfig.Store(&internalconfig.Config{Routing: internalconfig.RoutingConfig{StrictConversationAffinity: true, ConversationBindingsFile: path}})
	return m
}

// One conversation may use several providers (Hermes main model, aux tasks, MoA);
// each provider keeps its own durable account.
func TestStrictConversationBindsOneAccountPerProvider(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	ctx := WithConversationCaller(context.Background(), "caller-a")
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Cliproxy-Conversation-Id": []string{"thread"}}}
	m := strictTwoProviderManager(t, path)

	gemini, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil || gemini.Provider != "gemini" {
		t.Fatalf("first provider %v, %v", gemini, err)
	}
	claude, _, err := m.pickNext(ctx, "claude", "", opts, nil)
	if err != nil || claude.Provider != "claude" {
		t.Fatalf("second provider in the same conversation %v, %v", claude, err)
	}

	restarted := strictTwoProviderManager(t, path)
	for i := 0; i < 3; i++ {
		again, _, err := restarted.pickNext(ctx, "claude", "", opts, nil)
		if err != nil || again.ID != claude.ID {
			t.Fatalf("claude binding moved: %v, %v", again, err)
		}
		again, _, err = restarted.pickNext(ctx, "gemini", "", opts, nil)
		if err != nil || again.ID != gemini.ID {
			t.Fatalf("gemini binding moved: %v, %v", again, err)
		}
	}
	if _, _, err = restarted.pickNext(ctx, "claude", "", opts, map[string]struct{}{claude.ID: {}}); err == nil {
		t.Fatal("failover selected another claude account")
	}
}

// Bindings written before per-provider keys keep their account for that provider
// and no longer block other providers.
func TestStrictConversationKeepsLegacyBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	ctx := WithConversationCaller(context.Background(), "caller-a")
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Cliproxy-Conversation-Id": []string{"thread"}}}
	m := strictTwoProviderManager(t, path)
	id, err := strictConversationID(opts)
	if err != nil {
		t.Fatal(err)
	}
	legacy := sha256.Sum256([]byte(strictCaller(ctx) + "\x00" + id))
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *bolt.Tx) error { return tx.Bucket(conversationBucket).Put(legacy[:], []byte("g2")) }); err != nil {
		t.Fatal(err)
	}
	db.Close()

	for i := 0; i < 2; i++ {
		got, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
		if err != nil || got.ID != "g2" {
			t.Fatalf("legacy gemini binding %v, %v", got, err)
		}
	}
	if got, _, err := m.pickNext(ctx, "claude", "", opts, nil); err != nil || got.Provider != "claude" {
		t.Fatalf("legacy binding blocked claude: %v, %v", got, err)
	}
}
