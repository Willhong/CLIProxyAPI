package auth

import (
	"context"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func fallbackManager(t *testing.T, provider, path string) (*Manager, context.Context, cliproxyexecutor.Options) {
	t.Helper()
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.executors[provider] = schedulerTestExecutor{}
	m.runtimeConfig.Store(&internalconfig.Config{Routing: internalconfig.RoutingConfig{StrictConversationAffinity: true, ConversationBindingsFile: path}})
	for _, a := range []*Auth{
		{ID: "subscription", Provider: provider, Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth}},
		{ID: "paid", Provider: provider, Attributes: map[string]string{AttributeAuthKind: AuthKindAPIKey, "subscription_fallback_auth_id": "subscription", "subscription_fallback_enabled": "true", "priority": "100"}},
	} {
		registry.GetGlobalRegistry().RegisterClient(a.ID, provider, []*registry.ModelInfo{{ID: "fallback-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	return m, WithConversationCaller(context.Background(), "synthetic-caller"), cliproxyexecutor.Options{Headers: http.Header{ConversationIDHeader: []string{"synthetic-thread"}}}
}

func exhaustFallback(t *testing.T, m *Manager, id string) {
	t.Helper()
	a, _ := m.GetByID(id)
	ctx := logging.WithFreshResponseHeadersHolder(context.Background())
	message := `{"error":{"type":"usage_limit_reached"}}`
	if a.Provider == "claude" {
		logging.SetResponseHeaders(ctx, http.Header{"Anthropic-Ratelimit-Unified-5h-Status": {"rejected"}})
	}
	retry := time.Hour
	m.MarkResult(ctx, Result{AuthID: id, Provider: a.Provider, Model: "fallback-model", Error: &Error{HTTPStatus: 429, Message: message}, RetryAfter: &retry, CredentialScope: true})
}

func TestSubscriptionFallbackSelectionRestartRecovery(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bindings.db")
			if err := InitializeConversationBindings(path); err != nil {
				t.Fatal(err)
			}
			m, ctx, opts := fallbackManager(t, provider, path)
			pick := func(want string) {
				t.Helper()
				a, _, err := m.pickNext(ctx, provider, "fallback-model", opts, nil)
				if err != nil || a.ID != want {
					t.Fatalf("want %s: %v %v", want, a, err)
				}
			}
			pick("subscription")
			exhaustFallback(t, m, "subscription")
			pick("paid")
			m, ctx, opts = fallbackManager(t, provider, path)
			exhaustFallback(t, m, "subscription")
			pick("paid")
			a, _ := m.GetByID("subscription")
			clearAuthStateOnSuccess(a, time.Now())
			for _, state := range a.ModelStates {
				resetModelState(state, time.Now())
			}
			m.mu.Lock()
			m.auths[a.ID] = a
			m.mu.Unlock()
			m.scheduler.upsertAuth(a)
			pick("subscription")
			var wg sync.WaitGroup
			for i := 0; i < 6; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					a, _, err := m.pickNext(ctx, provider, "fallback-model", opts, nil)
					if err != nil || a.ID != "subscription" {
						t.Errorf("concurrent selection: %v %v", a, err)
					}
				}()
			}
			wg.Wait()
		})
	}
}

func TestSubscriptionFallbackDenialsAndOrdinarySelection(t *testing.T) {
	cases := []string{"disabled-toggle", "missing-parent", "disabled-parent", "wrong-provider", "401", "403", "500", "network", "rate-limit", "unknown"}
	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bindings.db")
			if err := InitializeConversationBindings(path); err != nil {
				t.Fatal(err)
			}
			m, ctx, opts := fallbackManager(t, "claude", path)
			exhaustFallback(t, m, "subscription")
			a, _ := m.GetByID("subscription")
			paid, _ := m.GetByID("paid")
			switch tc {
			case "disabled-toggle":
				paid.Attributes["subscription_fallback_enabled"] = "false"
			case "missing-parent":
				paid.Attributes["subscription_fallback_auth_id"] = "missing"
			case "disabled-parent":
				a.Disabled = true
			case "wrong-provider":
				paid.Provider = "codex"
			case "401":
				a.LastError = &Error{HTTPStatus: 401}
			case "403":
				a.LastError = &Error{HTTPStatus: 403}
			case "500":
				a.LastError = &Error{HTTPStatus: 500}
			case "network":
				a.LastError = &Error{Message: "network"}
			case "rate-limit":
				a.Quota.Reason = "quota"
				delete(a.Metadata, subscriptionProofKey)
			case "unknown":
				a.Quota = QuotaState{}
				delete(a.Metadata, subscriptionProofKey)
			}
			m.mu.Lock()
			m.auths[a.ID] = a
			m.mu.Unlock()
			m.scheduler.upsertAuth(a)
			m.Update(ctx, paid)
			if selected, _, err := m.pickNext(ctx, "claude", "fallback-model", opts, nil); err == nil && selected.ID == "paid" {
				t.Fatal("unauthorized paid selection")
			}
			m.runtimeConfig.Store(&internalconfig.Config{})
			for _, legacy := range []bool{false, true} {
				var selected *Auth
				var err error
				if legacy {
					selected, _, err = m.pickNextLegacy(ctx, "claude", "fallback-model", opts, nil)
				} else {
					selected, _, err = m.pickNext(ctx, "claude", "fallback-model", opts, nil)
				}
				if err == nil && selected.ID == "paid" {
					t.Fatal("linked API entered ordinary scheduler")
				}
			}
			if _, err := m.Register(ctx, &Auth{ID: "ordinary", Provider: "claude", Attributes: map[string]string{AttributeAuthKind: AuthKindAPIKey}}); err != nil {
				t.Fatal(err)
			}
			if selected, _, err := m.pickNext(ctx, "claude", "", cliproxyexecutor.Options{}, map[string]struct{}{"subscription": {}}); err != nil || selected.ID != "ordinary" {
				t.Fatalf("unlinked API broken: %v %v", selected, err)
			}
		})
	}
}

func TestSubscriptionFallbackAlternativePrecedenceAndMixed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	InitializeConversationBindings(path)
	m, ctx, opts := fallbackManager(t, "claude", path)
	first, _, err := m.pickNext(ctx, "claude", "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	exhaustFallback(t, m, first.ID)
	m.Register(ctx, &Auth{ID: "alternative", Provider: "claude", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth}})
	m.executors["codex"] = schedulerTestExecutor{}
	m.Register(ctx, &Auth{ID: "other-provider", Provider: "codex", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth, "priority": "999"}})
	a, _, p, err := m.pickNextMixed(ctx, []string{"codex", "claude"}, "", opts, nil)
	if err != nil || a.ID != "alternative" || p != "claude" {
		t.Fatalf("alternative/provider precedence: %v %s %v", a, p, err)
	}
	exhaustFallback(t, m, "alternative")
	// The new bound subscription has no linked key: fail closed rather than spend
	// through the key belonging to the previous subscription.
	if a, _, _, err = m.pickNextMixed(ctx, []string{"claude", "codex"}, "", opts, nil); err == nil && a.ID == "paid" {
		t.Fatal("used another subscription's linked API")
	}
}

func TestSubscriptionFallbackExecuteAndStreamBoundaries(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, mode := range []string{"execute", "bootstrap", "emitted"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bindings.db")
			InitializeConversationBindings(path)
			m, ctx, opts := fallbackManager(t, "codex", path)
			m.SetRetryConfig(0, 0, 0)
			retry := time.Hour
			quotaErr := streamQuotaError{customStatusError: customStatusError{code: 429, msg: `{"error":{"type":"usage_limit_reached"}}`, retryAfter: &retry}, credentialScoped: true}
			var attempts []string
			m.RegisterExecutor(&fallbackEntryExecutor{customStreamMockExecutor: customStreamMockExecutor{identifier: "codex", streamFn: func(_ context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
				attempts = append(attempts, a.ID)
				ch := make(chan cliproxyexecutor.StreamChunk, 2)
				if a.ID == "subscription" {
					if mode == "emitted" {
						ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: output\n\n")}
					}
					ch <- cliproxyexecutor.StreamChunk{Err: quotaErr}
				} else {
					ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: paid\n\n")}
				}
				close(ch)
				return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
			}}, executeFn: func(a *Auth) (cliproxyexecutor.Response, error) {
				attempts = append(attempts, a.ID)
				if a.ID == "subscription" {
					return cliproxyexecutor.Response{}, quotaErr
				}
				return cliproxyexecutor.Response{Payload: []byte("paid")}, nil
			}})
			if mode == "execute" {
				r, err := m.Execute(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "fallback-model"}, opts)
				if err != nil || string(r.Payload) != "paid" {
					t.Fatalf("execute: %s %v attempts=%v", r.Payload, err, attempts)
				}
			} else {
				r, err := m.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "fallback-model"}, opts)
				if err != nil {
					t.Fatalf("stream: %v attempts=%v", err, attempts)
				}
				for range r.Chunks {
				}
			}
			want := 2
			if mode == "emitted" {
				want = 1
			}
			if len(attempts) != want {
				t.Fatalf("attempts=%v want=%d", attempts, want)
			}
		})
	}
}

type fallbackEntryExecutor struct {
	customStreamMockExecutor
	executeFn func(*Auth) (cliproxyexecutor.Response, error)
}

func (e *fallbackEntryExecutor) Execute(_ context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return e.executeFn(a)
}
