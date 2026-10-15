package auth

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	config "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestStrictConversationRejectsUnsupportedHomeBeforeDispatch(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(&config.Config{Home: config.HomeConfig{Enabled: true}, Routing: config.RoutingConfig{StrictConversationAffinity: true}})
	opts := executor.Options{Headers: http.Header{http.CanonicalHeaderKey(ConversationIDHeader): {"bb:thread"}}}
	ctx := WithConversationCaller(context.Background(), "caller")
	for _, kind := range []string{"execute", "count", "stream"} {
		t.Run(kind, func(t *testing.T) {
			var err error
			switch kind {
			case "execute":
				_, err = m.Execute(ctx, []string{"codex"}, executor.Request{Model: "gpt-test"}, opts)
			case "count":
				_, err = m.ExecuteCount(ctx, []string{"codex"}, executor.Request{Model: "gpt-test"}, opts)
			case "stream":
				_, err = m.ExecuteStream(ctx, []string{"codex"}, executor.Request{Model: "gpt-test"}, opts)
			}
			var routingErr *Error
			if !errors.As(err, &routingErr) || routingErr.Code != "strict_routing_unsupported" {
				t.Fatalf("unsupported Home dispatch was not rejected by strict policy: %v", err)
			}
		})
	}
}

func TestStrictConversationStorageAndIdentityFaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	m := strictChildManager(path)
	ctx := WithConversationCaller(context.Background(), "caller")
	for _, ids := range [][]string{nil, {"a", "b"}, {""}, {" leading"}, {"a,b"}, {"a\nb"}} {
		opts := executor.Options{Headers: http.Header{http.CanonicalHeaderKey(ConversationIDHeader): ids}}
		if _, _, err := m.pickNext(ctx, "gemini", "", opts, nil); err == nil {
			t.Fatalf("invalid identity accepted: %q", ids)
		}
	}
	opts := executor.Options{Headers: http.Header{http.CanonicalHeaderKey(ConversationIDHeader): {"bb:stable"}}}
	first, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.Remove(ctx, first.ID)
	if _, _, err := m.pickNext(ctx, "gemini", "", opts, nil); err == nil {
		t.Fatal("removed credential silently reassigned")
	}
	if err := InitializeConversationBindings(path); err == nil {
		t.Fatal("initializer overwrote existing DB")
	}
	if err := os.WriteFile(path, []byte("invalid database"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.pickNext(ctx, "gemini", "", opts, nil); err == nil {
		t.Fatal("corrupt storage accepted")
	}
}
