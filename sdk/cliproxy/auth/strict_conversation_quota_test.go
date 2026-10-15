package auth

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// When the bound account has used up its quota, the conversation moves to another
// account of the same provider and stays there; other failures still fail closed.
func TestStrictConversationMovesOffExhaustedAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	ctx := WithConversationCaller(context.Background(), "caller-a")
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Cliproxy-Conversation-Id": []string{"thread"}}}
	m := strictTwoProviderManager(t, path)

	first, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	exhausted := first.Clone()
	until := time.Now().Add(time.Hour)
	exhausted.Unavailable = true
	exhausted.NextRetryAfter = until
	exhausted.Quota = QuotaState{Exceeded: true, Reason: "credential_quota", NextRecoverAt: until}
	if _, err = m.Update(context.Background(), exhausted); err != nil {
		t.Fatal(err)
	}

	moved, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil || moved.ID == first.ID || moved.Provider != "gemini" {
		t.Fatalf("exhausted account kept: %v, %v", moved, err)
	}
	restarted := strictTwoProviderManager(t, path)
	if again, _, err := restarted.pickNext(ctx, "gemini", "", opts, nil); err != nil || again.ID != moved.ID {
		t.Fatalf("move was not durable: %v, %v", again, err)
	}

	if _, _, err = m.pickNext(ctx, "gemini", "", opts, map[string]struct{}{moved.ID: {}}); err == nil {
		t.Fatal("a non-quota failure moved the conversation")
	}
}
