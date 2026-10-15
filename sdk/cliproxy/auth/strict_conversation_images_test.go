package auth

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	bolt "go.etcd.io/bbolt"
)

// Image generation and edit requests are one-shot and carry no conversation, so they
// may pick any account without a binding. Every other endpoint still requires one.
func TestStrictConversationAllowsOneShotImageRequests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	m := strictTwoProviderManager(t, path)
	caller := WithConversationCaller(context.Background(), "caller-a")

	for _, endpoint := range []string{"POST /v1/images/generations", "POST /v1/images/edits"} {
		ctx := logging.WithEndpoint(caller, endpoint)
		prepared, err := m.prepareStrictConversation(ctx, cliproxyexecutor.Options{})
		if err != nil {
			t.Fatalf("%s prepare: %v", endpoint, err)
		}
		if got, _, err := m.pickNext(ctx, "gemini", "", prepared, nil); err != nil || got == nil {
			t.Fatalf("%s pick: %v, %v", endpoint, got, err)
		}
	}

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_ = db.View(func(tx *bolt.Tx) error {
		if n := tx.Bucket(conversationBucket).Stats().KeyN; n != 0 {
			t.Fatalf("one-shot image requests wrote %d bindings", n)
		}
		return nil
	})

	for _, endpoint := range []string{"POST /v1/chat/completions", "POST /v1/responses", ""} {
		ctx := logging.WithEndpoint(caller, endpoint)
		if _, err := m.prepareStrictConversation(ctx, cliproxyexecutor.Options{}); err == nil {
			t.Fatalf("%q accepted a request without a conversation", endpoint)
		}
		if _, _, err := m.pickNext(ctx, "gemini", "", cliproxyexecutor.Options{}, nil); err == nil {
			t.Fatalf("%q picked without a conversation", endpoint)
		}
	}
}
