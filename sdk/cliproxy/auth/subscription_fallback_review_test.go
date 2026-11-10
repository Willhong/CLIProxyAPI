package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func reviewFallback(t *testing.T, provider string) (*Manager, context.Context, cliproxyexecutor.Options) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	return fallbackManager(t, provider, path)
}

func reviewQuota(m *Manager, provider string, explicit, credential bool) {
	ctx := logging.WithFreshResponseHeadersHolder(context.Background())
	message := `{"error":{"type":"rate_limit_error"}}`
	if provider == "claude" {
		h := http.Header{"Anthropic-Ratelimit-Unified-Status": {"rejected"}}
		if explicit {
			h.Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
		}
		logging.SetResponseHeaders(ctx, h)
	} else if explicit {
		message = `{"error":{"type":"usage_limit_reached"}}`
	}
	retry := time.Hour
	m.MarkResult(ctx, Result{AuthID: "subscription", Provider: provider, Model: "fallback-model", Error: &Error{HTTPStatus: 429, Message: message}, RetryAfter: &retry, CredentialScope: credential})
}

func TestSubscriptionReviewOrdinaryBinding(t *testing.T) {
	m, ctx, opts := reviewFallback(t, "claude")
	m.mu.Lock()
	delete(m.auths, "paid")
	m.mu.Unlock()
	ordinary := &Auth{ID: "ordinary-bound", Provider: "claude", Attributes: map[string]string{AttributeAuthKind: AuthKindAPIKey, "priority": "100"}}
	registry.GetGlobalRegistry().RegisterClient(ordinary.ID, "claude", []*registry.ModelInfo{{ID: "fallback-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(ordinary.ID) })
	m.Register(ctx, ordinary)
	a, _, err := m.pickNext(ctx, "claude", "fallback-model", opts, nil)
	if err != nil || a.ID != ordinary.ID {
		t.Fatalf("ordinary API binding changed: auth=%v err=%v", a, err)
	}
	// Existing API binding stays pinned even when OAuth later has higher priority.
	subscription, _ := m.GetByID("subscription")
	subscription.Attributes["priority"] = "999"
	m.Update(ctx, subscription)
	a, _, err = m.pickNext(ctx, "claude", "fallback-model", opts, nil)
	if err != nil || a.ID != ordinary.ID {
		t.Fatalf("existing API binding lost: %v %v", a, err)
	}
}

func TestSubscriptionReviewExplicitEvidence(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, provider := range []string{"claude", "codex"} {
		for _, credential := range []bool{false, true} {
			t.Run(provider+map[bool]string{true: "/credential", false: "/model"}[credential], func(t *testing.T) {
				m, ctx, opts := reviewFallback(t, provider)
				reviewQuota(m, provider, false, credential)
				a, _, err := m.pickNext(ctx, provider, "fallback-model", opts, nil)
				if err == nil && a.ID == "paid" {
					t.Error("ambiguous quota authorized spending")
				}
				reviewQuota(m, provider, true, credential)
				a, _, err = m.pickNext(ctx, provider, "fallback-model", opts, nil)
				if err != nil || a.ID != "paid" {
					t.Errorf("explicit quota did not authorize fallback: auth=%v err=%v", a, err)
				}
			})
		}
	}
}

func TestSubscriptionReviewEligibleSecondKey(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, blocked := range []string{"cooldown", "401", "tried"} {
		t.Run(blocked, func(t *testing.T) {
			m, ctx, opts := reviewFallback(t, "codex")
			reviewQuota(m, "codex", true, true)
			second := &Auth{ID: "paid-second", Provider: "codex", Attributes: map[string]string{AttributeAuthKind: AuthKindAPIKey, "subscription_fallback_auth_id": "subscription", "subscription_fallback_enabled": "true"}}
			registry.GetGlobalRegistry().RegisterClient(second.ID, "codex", []*registry.ModelInfo{{ID: "fallback-model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(second.ID) })
			m.Register(ctx, second)
			tried := map[string]struct{}{}
			if blocked == "tried" {
				tried["paid"] = struct{}{}
			} else {
				status := 429
				if blocked == "401" {
					status = 401
				}
				retry := time.Hour
				m.MarkResult(ctx, Result{AuthID: "paid", Provider: "codex", Model: "fallback-model", Error: &Error{HTTPStatus: status}, RetryAfter: &retry})
			}
			a, _, err := m.pickNext(ctx, "codex", "fallback-model", opts, tried)
			if err != nil || a.ID != second.ID {
				t.Fatalf("healthy second key skipped: auth=%v err=%v", a, err)
			}
		})
	}
}

func TestSubscriptionReviewProofInvalidation(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, provider := range []string{"claude", "codex"} {
		for _, event := range []string{"success", "401", "403", "500", "network", "generic429", "stale-headers", "expiry", "stale-version", "stale-generation", "restored", "other-model"} {
			t.Run(provider+"/"+event, func(t *testing.T) {
				m, ctx, _ := reviewFallback(t, provider)
				reviewQuota(m, provider, true, false)
				before, _ := m.GetByID("subscription")
				if !subscriptionExhausted(before, "fallback-model", time.Now()) {
					t.Fatal("missing initial proof")
				}
				result := Result{AuthID: "subscription", Provider: provider, Model: "fallback-model", Error: &Error{HTTPStatus: 429, Message: `{"error":{"type":"rate_limit_error"}}`}}
				switch event {
				case "success":
					result.Success = true
					result.Error = nil
				case "401":
					result.Error.HTTPStatus = 401
				case "403":
					result.Error.HTTPStatus = 403
				case "500":
					result.Error.HTTPStatus = 500
				case "network":
					result.Error.HTTPStatus = 0
				case "stale-headers":
					// Passive quota snapshots must never replace current response evidence.
					before.Quota.Signals = map[string]string{"Anthropic-Ratelimit-Unified-5h-Status": "rejected"}
					m.Update(ctx, before)
				case "expiry":
					proof := before.Metadata[subscriptionProofKey].(subscriptionProof)
					proof.Until = time.Now().Add(-time.Second)
					before.Metadata[subscriptionProofKey] = proof
					if subscriptionExhausted(before, "fallback-model", time.Now()) {
						t.Fatal("expired proof accepted")
					}
					return
				case "other-model":
					if subscriptionExhausted(before, "unrelated-model", time.Now()) {
						t.Fatal("model proof leaked")
					}
					return
				case "restored":
					raw, err := json.Marshal(before)
					if err != nil {
						t.Fatal(err)
					}
					var restored Auth
					if err = json.Unmarshal(raw, &restored); err != nil {
						t.Fatal(err)
					}
					if subscriptionExhausted(&restored, "fallback-model", time.Now()) {
						t.Fatal("JSON resurrected proof")
					}
					return
				case "stale-version":
					result.CredentialVersion = before.CredentialVersion
					before.CredentialVersion += 2
					m.mu.Lock()
					m.auths[before.ID] = before
					m.mu.Unlock()
					reviewQuota(m, provider, false, false)
					result.Error.Message = `{"error":{"type":"usage_limit_reached"}}`
				case "stale-generation":
					result.Options.Metadata = map[string]any{subscriptionGenerationKey: before.Generation - 1}
					result.Error.Message = `{"error":{"type":"usage_limit_reached"}}`
				}
				retry := time.Hour
				result.RetryAfter = &retry
				m.MarkResult(logging.WithFreshResponseHeadersHolder(ctx), result)
				after, _ := m.GetByID("subscription")
				if subscriptionExhausted(after, "fallback-model", time.Now()) {
					t.Fatalf("%s retained/authorized proof", event)
				}
			})
		}
	}
}

func TestSubscriptionReviewFreshOtherParentAndMixed(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, ctx, opts := reviewFallback(t, "claude")
	first, _, err := m.pickNext(ctx, "claude", "fallback-model", opts, nil)
	if err != nil || first.ID != "subscription" {
		t.Fatal(first, err)
	}
	reviewQuota(m, "claude", true, false)
	// A fresh conversation can use a different exhausted parent's healthy key,
	// while the existing conversation must never steal it.
	parent := &Auth{ID: "alternative-parent", Provider: "claude", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth}}
	paid := &Auth{ID: "alternative-paid", Provider: "claude", Attributes: map[string]string{AttributeAuthKind: AuthKindAPIKey, "subscription_fallback_auth_id": parent.ID, "subscription_fallback_enabled": "true"}}
	for _, a := range []*Auth{parent, paid} {
		registry.GetGlobalRegistry().RegisterClient(a.ID, a.Provider, []*registry.ModelInfo{{ID: "fallback-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
		m.Register(ctx, a)
	}
	exhaustFallback(t, m, parent.ID)
	retry := time.Hour
	m.MarkResult(ctx, Result{AuthID: "paid", Provider: "claude", Model: "fallback-model", Error: &Error{HTTPStatus: 401}, RetryAfter: &retry})
	if a, _, err := m.pickNext(ctx, "claude", "fallback-model", opts, nil); err == nil && a.ID == paid.ID {
		t.Fatal("existing binding stole unrelated key")
	}
	other := &Auth{ID: "irrelevant-codex", Provider: "codex", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth}}
	m.executors["codex"] = schedulerTestExecutor{}
	m.Register(ctx, other) // Does not support the requested model.
	opts.Headers = http.Header{ConversationIDHeader: {"fresh-thread"}}
	a, _, _, err := m.pickNextMixed(ctx, []string{"codex", "claude"}, "fallback-model", opts, nil)
	if err != nil || a.ID != paid.ID {
		t.Fatalf("fresh eligible parent/mixed: %v %v", a, err)
	}
}

func TestSubscriptionReviewClaudeHeaderGate(t *testing.T) {
	result := Result{Error: &Error{HTTPStatus: 429}}
	for _, value := range []string{"", "allowed", "unknown", "REJECTED", "rejected, allowed"} {
		h := http.Header{"Anthropic-Ratelimit-Unified-Status": {"rejected"}, "Anthropic-Ratelimit-Unified-5h-Status": {value}}
		if explicitSubscriptionRejection("claude", result, h) {
			t.Fatalf("accepted ambiguous shared status %q", value)
		}
	}
	h := http.Header{"Anthropic-Ratelimit-Unified-5h-Status": {"rejected", "allowed"}}
	if explicitSubscriptionRejection("claude", result, h) {
		t.Fatal("accepted duplicate conflicting header")
	}
	h = http.Header{"Anthropic-Ratelimit-Unified-7d-Status": {"rejected"}}
	if !explicitSubscriptionRejection("claude", result, h) {
		t.Fatal("rejected explicit 7d proof")
	}
}
