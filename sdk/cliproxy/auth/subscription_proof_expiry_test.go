package auth

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestSubscriptionProofRawDeadline(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, provider := range []string{"claude", "codex"} {
		for _, credential := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/credential=%t", provider, credential), func(t *testing.T) {
				a := &Auth{Provider: provider, Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth}, CredentialVersion: 2, RegistrationEpoch: 3}
				jitter := 34 * time.Second
				a.Quota = QuotaState{Exceeded: true, NextRecoverAt: now.Add(jitter)}
				a.ModelStates = map[string]*ModelState{"model": {Unavailable: true, NextRetryAfter: now.Add(jitter), Quota: a.Quota}}
				h := http.Header{}
				message := fmt.Sprintf(`{"error":{"type":"usage_limit_reached","resets_at":%d}}`, now.Add(4*time.Second).Unix())
				if provider == "claude" {
					h.Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
					h.Set("Anthropic-Ratelimit-Unified-5h-Reset", fmt.Sprint(now.Add(4*time.Second).Unix()))
					h.Set("Retry-After", "4")
				}
				recordSubscriptionProof(a, Result{Error: &Error{HTTPStatus: 429, Message: message}, RetryAfter: &jitter, CredentialScope: credential}, "model", h, now)
				proof, ok := a.Metadata[subscriptionProofKey].(subscriptionProof)
				if !ok || !proof.Until.Equal(now.Add(4*time.Second)) {
					t.Errorf("proof=%+v want raw deadline t+4", proof)
				}
				if !subscriptionExhausted(a, "model", now.Add(3*time.Second)) {
					t.Error("valid raw proof denied before reset")
				}
				paid := 0
				for _, at := range []time.Time{now.Add(4 * time.Second), now.Add(5 * time.Second)} {
					if subscriptionExhausted(a, "model", at) {
						paid++
					}
				}
				if paid != 0 {
					t.Errorf("paid=%d want paid=0 at/after raw reset while cooldown=t+34", paid)
				}
				if !a.ModelStates["model"].Quota.NextRecoverAt.Equal(now.Add(jitter)) {
					t.Fatal("scheduling deadline changed")
				}
			})
		}
	}
}

func TestSubscriptionProofDeadlineInputs(t *testing.T) {
	now := time.Unix(1800000000, 0)
	reset := fmt.Sprint(now.Add(4 * time.Second).Unix())
	later := fmt.Sprint(now.Add(12 * time.Second).Unix())
	past := fmt.Sprint(now.Add(-time.Second).Unix())
	tests := []struct {
		name, provider, body string
		headers              http.Header
		retry                time.Duration
		want                 time.Duration
	}{
		{name: "claude/raw-retry-before-fuzz", provider: "claude", headers: http.Header{"Retry-After": {"4"}}, retry: 34 * time.Second, want: 4 * time.Second},
		{name: "claude/http-date", provider: "claude", headers: http.Header{"Retry-After": {now.Add(4 * time.Second).UTC().Format(http.TimeFormat)}}, want: 4 * time.Second},
		{name: "claude/latest-rejected-window", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {reset}, "Anthropic-Ratelimit-Unified-7d-Status": {"rejected"}, "Anthropic-Ratelimit-Unified-7d-Reset": {later}}, want: 12 * time.Second},
		{name: "claude/allowed-window-is-not-authority", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {reset}, "Anthropic-Ratelimit-Unified-7d-Status": {"allowed"}, "Anthropic-Ratelimit-Unified-7d-Reset": {later}}, want: 4 * time.Second},
		{name: "claude/latest-raw-retry", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {reset}, "Retry-After": {"12"}}, want: 12 * time.Second},
		{name: "claude/valid-window-with-other-malformed", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {reset}, "Anthropic-Ratelimit-Unified-7d-Status": {"rejected"}, "Anthropic-Ratelimit-Unified-7d-Reset": {"bad"}}, want: 4 * time.Second},
		{name: "claude/rfc3339", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {now.Add(4 * time.Second).Format(time.RFC3339)}}, want: 4 * time.Second},
		{name: "claude/malformed", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {"bad"}}, retry: 34 * time.Second},
		{name: "claude/past", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {past}}, retry: 34 * time.Second},
		{name: "claude/duplicate", provider: "claude", headers: http.Header{"Anthropic-Ratelimit-Unified-5h-Reset": {reset, later}}, retry: 34 * time.Second},
		{name: "claude/malformed-retry", provider: "claude", headers: http.Header{"Retry-After": {"bad"}}, retry: 34 * time.Second},
		{name: "claude/zero-retry", provider: "claude", headers: http.Header{"Retry-After": {"0"}}, retry: 34 * time.Second},
		{name: "claude/overflow-retry", provider: "claude", headers: http.Header{"Retry-After": {"9223372036854775807"}}, retry: 34 * time.Second},
		{name: "claude/no-deadline-bounded", provider: "claude", retry: 34 * time.Second, want: time.Second},
		{name: "codex/absolute-before-duration", provider: "codex", body: `"resets_at":` + reset, retry: 34 * time.Second, want: 4 * time.Second},
		{name: "codex/absolute-precedence", provider: "codex", body: `"resets_at":` + reset + `,"resets_in_seconds":12`, want: 4 * time.Second},
		{name: "codex/relative", provider: "codex", body: `"resets_in_seconds":4`, retry: 34 * time.Second, want: 4 * time.Second},
		{name: "codex/malformed-absolute", provider: "codex", body: `"resets_at":"bad","resets_in_seconds":12`, retry: 34 * time.Second},
		{name: "codex/past-absolute", provider: "codex", body: `"resets_at":` + past + `,"resets_in_seconds":12`, retry: 34 * time.Second},
		{name: "codex/fraction", provider: "codex", body: `"resets_in_seconds":4.5`, retry: 34 * time.Second},
		{name: "codex/null", provider: "codex", body: `"resets_at":null`, retry: 34 * time.Second},
		{name: "codex/zero", provider: "codex", body: `"resets_in_seconds":0`, retry: 34 * time.Second},
		{name: "codex/overflow", provider: "codex", body: `"resets_in_seconds":9223372036854775807`, retry: 34 * time.Second},
		{name: "codex/explicit-duration-compatible", provider: "codex", retry: time.Hour, want: time.Hour},
		{name: "codex/no-deadline-bounded", provider: "codex", want: time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := &Auth{Provider: tc.provider, Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth}, Quota: QuotaState{Exceeded: true, NextRecoverAt: now.Add(time.Hour)}}
			h := tc.headers.Clone()
			if h == nil {
				h = http.Header{}
			}
			h.Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
			message := `{"error":{"type":"usage_limit_reached"`
			if tc.body != "" {
				message += "," + tc.body
			}
			message += `}}`
			result := Result{Error: &Error{HTTPStatus: 429, Message: message}}
			if tc.retry != 0 {
				result.RetryAfter = &tc.retry
			}
			recordSubscriptionProof(a, result, "model", h, now)
			proof, ok := a.Metadata[subscriptionProofKey].(subscriptionProof)
			if tc.want == 0 {
				if ok {
					t.Fatalf("invalid/past raw deadline used scheduler authority: %+v", proof)
				}
				return
			}
			if !ok || !proof.Until.Equal(now.Add(tc.want)) {
				t.Fatalf("proof=%+v present=%t want=%v", proof, ok, now.Add(tc.want))
			}
			if subscriptionExhausted(a, "model", now.Add(tc.want)) {
				t.Fatal("proof remained valid at raw expiry")
			}
		})
	}
}

func TestSubscriptionProofExpiredDeniesPaidSelection(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			m, ctx, opts := reviewFallback(t, provider)
			// Record the response at t; selection runs at t+5 without sleeping.
			now := time.Now().Add(-5 * time.Second)
			jitter := 34 * time.Second
			h := http.Header{}
			h.Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
			h.Set("Anthropic-Ratelimit-Unified-5h-Reset", fmt.Sprint(now.Add(4*time.Second).Unix()))
			message := fmt.Sprintf(`{"error":{"type":"usage_limit_reached","resets_at":%d}}`, now.Add(4*time.Second).Unix())
			m.mu.Lock()
			a := m.auths["subscription"]
			a.Unavailable = true
			a.NextRetryAfter = now.Add(jitter)
			a.Quota = QuotaState{Exceeded: true, NextRecoverAt: now.Add(jitter)}
			recordSubscriptionProof(a, Result{Error: &Error{HTTPStatus: 429, Message: message}, RetryAfter: &jitter, CredentialScope: true}, "fallback-model", h, now)
			snapshot := a.Clone()
			m.mu.Unlock()
			m.scheduler.upsertAuth(snapshot)
			paid := 0
			for _, legacy := range []bool{false, true} {
				var selected *Auth
				var err error
				if legacy {
					selected, _, err = m.pickNextLegacy(ctx, provider, "fallback-model", opts, nil)
				} else {
					selected, _, err = m.pickNext(ctx, provider, "fallback-model", opts, nil)
				}
				if err == nil && selected.ID == "paid" {
					paid++
				}
			}
			scoped, _ := m.subscriptionOptions(cliproxyexecutor.Options{}, []string{provider}, "fallback-model", "subscription", nil)
			scope, _ := scoped.Metadata[subscriptionSelectionMetadataKey].(subscriptionSelectionScope)
			if paid != 0 || scope.fallbackID != "" {
				t.Fatalf("paid=%d fallbackID=%q want paid=0 at t+5, cooldown=t+34", paid, scope.fallbackID)
			}
		})
	}
}
