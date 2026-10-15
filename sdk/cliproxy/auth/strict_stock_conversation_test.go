package auth

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// These names were captured from unmodified Codex/Claude CLIs and Hermes ACP.
func TestStrictStockConversationHeaders(t *testing.T) {
	cases := []struct{ name, header, id, prefix string }{
		{"codex", "session-id", "01a13960-9ca3-7632-ae0c-41d43ffbe8f8", "codex:"},
		{"codex-underscore", "session_id", "01a13960-9ca3-7632-ae0c-41d43ffbe8f8", "codex:"},
		{"claude", "x-claude-code-session-id", "29abeef4-6c51-461e-86a9-f023be4fad2f", "claude:"},
		{"hermes", "X-Hermes-Session-ID", "0bdd12e3-e6b1-4597-bb29-1ccc9ee07480", "hermes:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bindings.db")
			if err := InitializeConversationBindings(path); err != nil {
				t.Fatal(err)
			}
			opts := executor.Options{Headers: http.Header{tc.header: {tc.id}}}
			id, err := strictConversationID(opts)
			if err != nil || id != tc.prefix+tc.id {
				t.Fatalf("native identity=%q err=%v", id, err)
			}
			ctx := WithConversationCaller(context.Background(), "caller")
			m := strictChildManager(path)
			first, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
			if err != nil {
				t.Fatal(err)
			}
			opts.Headers.Set("X-Client-Request-Id", "different-request")
			resumed, _, err := strictChildManager(path).pickNext(ctx, "gemini", "", opts, nil)
			if err != nil || resumed.ID != first.ID {
				t.Fatalf("native resume changed account: %v %v", resumed, err)
			}
			if _, _, err = m.pickNext(ctx, "gemini", "", opts, map[string]struct{}{first.ID: {}}); err == nil {
				t.Fatal("native identity failed over")
			}
			prepared, err := m.prepareStrictConversation(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			again, err := strictConversationID(prepared)
			if err != nil || again != id {
				t.Fatalf("prepared identity drifted: %q %v", again, err)
			}
			if tc.name == "hermes" && prepared.Headers.Get("X-Hermes-Session-ID") != "" {
				t.Fatal("private Hermes routing header leaked")
			}
		})
	}
}

func TestStrictStockConversationRejectsAmbiguousIdentity(t *testing.T) {
	for _, headers := range []http.Header{
		{"Session-Id": {"a", "b"}},
		{"Session-Id": {"a"}, "session-id": {"b"}},
		{"Session-Id": {"a"}, "Session_id": {"b"}},
		{"Session-Id": {"a"}, "X-Claude-Code-Session-Id": {"b"}},
		{"X-Hermes-Session-ID": {" a"}},
		{"X-Request-ID": {"not-a-conversation"}},
	} {
		if id, err := strictConversationID(executor.Options{Headers: headers}); err == nil {
			t.Fatalf("ambiguous/unsupported identity accepted: %q from %v", id, headers)
		}
	}
}

func TestStrictStockNewConversationCanChooseAnotherAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	m := strictChildManager(path)
	ctx := WithConversationCaller(context.Background(), "caller")
	opts := executor.Options{Headers: http.Header{"Session-Id": {"first-conversation"}}}
	first, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts.Headers.Set("Session-Id", "new-conversation")
	second, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatal("new conversation was forced onto first conversation binding")
	}
	opts.Headers.Set("Session-Id", "first-conversation")
	resumed, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil || resumed.ID != first.ID {
		t.Fatal("resumed conversation lost binding")
	}
}
