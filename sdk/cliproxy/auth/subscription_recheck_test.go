package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type subscriptionRecheckExecutor struct {
	schedulerTestExecutor
	run func(context.Context, *Auth) (cliproxyexecutor.Response, error)
}

func (e subscriptionRecheckExecutor) Identifier() string { return "claude" }
func (e subscriptionRecheckExecutor) Execute(ctx context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return e.run(ctx, a)
}

func (e subscriptionRecheckExecutor) ExecuteStream(ctx context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	resp, err := e.run(ctx, a)
	if err != nil {
		return nil, err
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: resp.Payload}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func TestSubscriptionFallbackRechecksRestoredCooldown(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, outcome := range []string{"explicit-quota", "recovered", "ambiguous-quota", "unauthorized", "stream-explicit-quota", "other-subscription"} {
		t.Run(outcome, func(t *testing.T) {
			m, ctx, opts := reviewFallback(t, "claude")
			m.SetRetryConfig(0, 0, 0)
			reviewQuota(m, "claude", true, true)
			original, _ := m.GetByID("subscription")
			raw, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var restored Auth
			if err = json.Unmarshal(raw, &restored); err != nil {
				t.Fatal(err)
			}
			if _, err = m.Register(ctx, &restored); err != nil {
				t.Fatal(err)
			}
			if outcome == "other-subscription" {
				other := restored.Clone()
				other.ID, other.Index = "subscription-z", ""
				registry.GetGlobalRegistry().RegisterClient(other.ID, "claude", []*registry.ModelInfo{{ID: "fallback-model"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(other.ID) })
				if _, err = m.Register(ctx, other); err != nil {
					t.Fatal(err)
				}
			}
			var attempts []string
			m.RegisterExecutor(subscriptionRecheckExecutor{run: func(execCtx context.Context, a *Auth) (cliproxyexecutor.Response, error) {
				attempts = append(attempts, a.ID)
				if a.ID == "paid" {
					return cliproxyexecutor.Response{Payload: []byte("paid")}, nil
				}
				if outcome == "recovered" {
					return cliproxyexecutor.Response{Payload: []byte("subscription")}, nil
				}
				status := 429
				if outcome == "unauthorized" {
					status = 401
				}
				if outcome == "explicit-quota" || outcome == "stream-explicit-quota" || outcome == "other-subscription" {
					h := http.Header{}
					h.Set("Anthropic-Ratelimit-Unified-7d-Status", "rejected")
					h.Set("Anthropic-Ratelimit-Unified-7d-Reset", time.Now().Add(time.Hour).Format(time.RFC3339))
					logging.SetResponseHeaders(execCtx, h)
				}
				retry := time.Hour
				return cliproxyexecutor.Response{}, streamQuotaError{customStatusError: customStatusError{code: status, msg: `{"error":{"type":"rate_limit_error"}}`, retryAfter: &retry}, credentialScoped: true}
			}})
			var resp cliproxyexecutor.Response
			if outcome == "stream-explicit-quota" {
				var stream *cliproxyexecutor.StreamResult
				stream, err = m.ExecuteStream(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: "fallback-model"}, opts)
				if err == nil {
					for chunk := range stream.Chunks {
						if chunk.Err != nil {
							err = chunk.Err
						}
						resp.Payload = append(resp.Payload, chunk.Payload...)
					}
				}
			} else {
				resp, err = m.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: "fallback-model"}, opts)
			}
			wantAttempts := []string{"subscription"}
			switch outcome {
			case "explicit-quota", "stream-explicit-quota", "other-subscription":
				if outcome == "other-subscription" {
					wantAttempts = append(wantAttempts, "subscription-z")
				}
				wantAttempts = append(wantAttempts, "paid")
				if err != nil || string(resp.Payload) != "paid" {
					t.Errorf("fresh rejection must allow paid: payload=%q err=%v", resp.Payload, err)
				}
			case "recovered":
				if err != nil || string(resp.Payload) != "subscription" {
					t.Errorf("recovery must stay subscribed: payload=%q err=%v", resp.Payload, err)
				}
			default:
				if err == nil {
					t.Error("non-explicit failure must not allow paid")
				}
				// A new request must not repeatedly bypass the subscription cooldown.
				_, _ = m.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: "fallback-model"}, opts)
			}
			if !reflect.DeepEqual(attempts, wantAttempts) {
				t.Fatalf("upstream attempts=%v, want %v", attempts, wantAttempts)
			}
		})
	}
}

func TestSubscriptionRecheckConcurrentAndReloadBound(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, ctx, opts := reviewFallback(t, "claude")
	reviewQuota(m, "claude", false, true)
	a, _ := m.GetByID("subscription")
	var wg sync.WaitGroup
	picks := make(chan string, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			picked, _, err := m.pickNext(ctx, "claude", "fallback-model", opts, nil)
			if err == nil {
				picks <- picked.ID
			}
		}()
	}
	wg.Wait()
	close(picks)
	var count int
	for id := range picks {
		count++
		if id != "subscription" {
			t.Errorf("untrusted state selected %s", id)
		}
	}
	if count != 1 {
		t.Fatalf("concurrent revalidation selected %d times, want1", count)
	}
	// The watcher snapshot predates admission and has no in-memory admission timer.
	if _, err := m.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	if picked, _, err := m.pickNext(ctx, "claude", "fallback-model", opts, nil); err == nil {
		t.Fatalf("reload reopened quota revalidation: %v", picked)
	}
}

func TestSubscriptionFallbackCountTokensPreservesGenerationProof(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, ctx, opts := reviewFallback(t, "claude")
	reviewQuota(m, "claude", true, true)
	m.MarkResult(ctx, Result{AuthID: "subscription", Provider: "claude", Model: "fallback-model", Success: true, SkipQuotaObservation: true})
	a, _, err := m.pickNext(ctx, "claude", "fallback-model", opts, nil)
	if err != nil || a.ID != "paid" {
		t.Fatalf("count-tokens discarded generation quota evidence: auth=%v err=%v", a, err)
	}
}
