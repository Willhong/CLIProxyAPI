package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func reloadQuota(m *Manager, a *Auth, generation uint64) {
	ctx := logging.WithFreshResponseHeadersHolder(context.Background())
	if a.Provider == "claude" {
		logging.SetResponseHeaders(ctx, http.Header{"Anthropic-Ratelimit-Unified-5h-Status": {"rejected"}})
	}
	retry := time.Hour
	m.MarkResult(ctx, Result{AuthID: a.ID, Provider: a.Provider, Model: "fallback-model", CredentialVersion: a.CredentialVersion, RegistrationEpoch: a.RegistrationEpoch, Options: cliproxyexecutor.Options{Metadata: map[string]any{subscriptionGenerationKey: generation}}, Error: &Error{HTTPStatus: 429, Message: `{"error":{"type":"usage_limit_reached"}}`}, RetryAfter: &retry, CredentialScope: true})
}

// Native OAuth files contain credentials, not the manager's response-derived proof.
func TestSubscriptionWatcherReloadPreservesProof(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, provider := range []string{"claude", "codex"} {
		for _, timing := range []string{"before-result", "after-result"} {
			t.Run(provider+"/"+timing, func(t *testing.T) {
				m, ctx, opts := reviewFallback(t, provider)
				original, _ := m.GetByID("subscription")
				original.Metadata = map[string]any{"access_token": "synthetic-token", "account_id": "synthetic-account"}
				snapshot, err := m.Update(ctx, original)
				if err != nil {
					t.Fatal(err)
				}
				reload := snapshot.Clone()
				reload.Generation, reload.RegistrationEpoch, reload.CredentialVersion = 0, 0, 0
				if timing == "before-result" {
					if _, err = m.Update(ctx, reload); err != nil {
						t.Fatal(err)
					}
				}
				reloadQuota(m, snapshot, snapshot.Generation)
				before, _ := m.GetByID("subscription")
				if !subscriptionExhausted(before, "fallback-model", time.Now()) {
					t.Fatal("missing proof before reload")
				}
				if _, err = m.Update(ctx, reload); err != nil {
					t.Fatal(err)
				}
				after, _ := m.GetByID("subscription")
				if !subscriptionExhausted(after, "fallback-model", time.Now()) {
					t.Fatal("normal Manager.Update lost response proof on unchanged OAuth reload")
				}
				opts.Headers = http.Header{ConversationIDHeader: {"fresh-after-reload"}}
				picked, _, err := m.pickNext(ctx, provider, "fallback-model", opts, nil)
				if err != nil || picked.ID != "paid" {
					t.Fatalf("fresh conversation did not use paid fallback: %v %v", picked, err)
				}
			})
		}
	}
}

func TestSubscriptionReloadConcurrentInvalidation(t *testing.T) {
	withQuotaCooldownEnabled(t)
	synctest.Test(t, func(t *testing.T) {
		m, ctx, _ := reviewFallback(t, "codex")
		initial, _ := m.GetByID("subscription")
		initial.Metadata = map[string]any{"access_token": "synthetic"}
		snapshot, err := m.Update(ctx, initial)
		if err != nil {
			t.Fatal(err)
		}
		reloadQuota(m, snapshot, snapshot.Generation)
		reload, _ := m.GetByID(snapshot.ID)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		m.mu.Lock()
		m.store = &callbackAuthStore{save: func(*Auth) error { close(entered); <-release; return nil }}
		m.mu.Unlock()
		go func() { _, errUpdate := m.Update(ctx, reload); done <- errUpdate }()
		<-entered
		// Match runtime-state updates performed while persistLocked releases m.mu.
		m.mu.Lock()
		current := m.auths[snapshot.ID]
		delete(current.Metadata, subscriptionProofKey)
		current.Generation++
		m.mu.Unlock()
		close(release)
		if err = <-done; err != nil {
			t.Fatal(err)
		}
		after, _ := m.GetByID(snapshot.ID)
		if subscriptionExhausted(after, "fallback-model", time.Now()) {
			t.Fatal("reload resurrected proof invalidated during Save")
		}
	})
}

func TestSubscriptionReloadDoesNotResurrectProof(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, event := range []string{"token", "account", "proxy", "disabled", "reregister", "restart", "ambiguous", "stale-generation", "stale-version", "stale-epoch"} {
		t.Run(event, func(t *testing.T) {
			m, ctx, _ := reviewFallback(t, "codex")
			snapshot, _ := m.GetByID("subscription")
			reloadQuota(m, snapshot, snapshot.Generation)
			stale, _ := m.GetByID("subscription")
			if !subscriptionExhausted(stale, "fallback-model", time.Now()) {
				t.Fatal("missing proof")
			}
			switch event {
			case "token":
				stale.Metadata["access_token"] = "changed"
			case "account":
				stale.Metadata["account_id"] = "changed"
			case "proxy":
				stale.ProxyURL = "http://changed.invalid"
			case "disabled":
				stale.Disabled = true
			case "reregister":
				if _, err := m.Register(ctx, stale); err != nil {
					t.Fatal(err)
				}
			case "restart":
				raw, _ := json.Marshal(stale)
				var restored Auth
				if err := json.Unmarshal(raw, &restored); err != nil {
					t.Fatal(err)
				}
				fresh := NewManager(nil, nil, nil)
				if _, err := fresh.Register(ctx, &restored); err != nil {
					t.Fatal(err)
				}
				m = fresh
			case "ambiguous":
				m.MarkResult(ctx, Result{AuthID: stale.ID, Provider: stale.Provider, Model: "fallback-model", CredentialVersion: stale.CredentialVersion, RegistrationEpoch: stale.RegistrationEpoch, Error: &Error{HTTPStatus: 429, Message: `{"error":{"type":"ordinary"}}`}})
			case "stale-generation", "stale-version", "stale-epoch":
				m.MarkResult(ctx, Result{AuthID: stale.ID, Provider: stale.Provider, Model: "fallback-model", Success: true})
				if event == "stale-version" {
					stale.CredentialVersion = 0
				}
				if event == "stale-epoch" {
					stale.RegistrationEpoch = 0
				}
				generation := snapshot.Generation
				if event != "stale-generation" {
					current, _ := m.GetByID(stale.ID)
					generation = current.Generation
				}
				if event == "stale-version" {
					current, _ := m.GetByID(stale.ID)
					current.Metadata["access_token"] = "replacement"
					m.Update(ctx, current)
				}
				if event == "stale-epoch" {
					m.Register(ctx, stale.Clone())
				}
				reloadQuota(m, stale, generation)
			}
			if event == "token" || event == "account" || event == "proxy" || event == "disabled" || event == "ambiguous" {
				if _, err := m.Update(ctx, stale); err != nil {
					t.Fatal(err)
				}
			}
			after, _ := m.GetByID("subscription")
			if subscriptionExhausted(after, "fallback-model", time.Now()) {
				t.Fatal("invalid event retained/resurrected proof")
			}
		})
	}
}
