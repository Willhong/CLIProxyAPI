package auth

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const subscriptionSelectionMetadataKey = "subscription-fallback-selection"

type subscriptionSelectionScope struct {
	provider          string
	fallbackID        string
	subscriptionsOnly bool
}

func isSubscriptionFallback(a *Auth) bool {
	if a == nil {
		return false
	}
	_, linked := a.Attributes["subscription_fallback_auth_id"]
	_, marked := a.Attributes["subscription_fallback_enabled"]
	return linked || marked
}

// Spending proof is private, response-derived, and independent of cooling scope.
// JSON-restored credentials require a fresh upstream rejection before spending.
const subscriptionProofKey = "subscription-fallback-proof"
const subscriptionGenerationKey = "subscription-fallback-generation"

type subscriptionProofExecutor struct{ ProviderExecutor }

func proofOptions(a *Auth, opts cliproxyexecutor.Options) cliproxyexecutor.Options {
	opts.EnsureMetadata()
	opts.Metadata[subscriptionGenerationKey] = a.Generation
	return opts
}
func (e subscriptionProofExecutor) Execute(ctx context.Context, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return e.ProviderExecutor.Execute(ctx, a, req, proofOptions(a, opts))
}
func (e subscriptionProofExecutor) ExecuteStream(ctx context.Context, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return e.ProviderExecutor.ExecuteStream(ctx, a, req, proofOptions(a, opts))
}

// sameSubscriptionReloadIdentity compares credential/configuration identity only.
// Quota snapshots and private response evidence are not credential replacements.
func sameSubscriptionReloadIdentity(a, b *Auth) bool {
	if a == nil || b == nil || a.AuthKind() != AuthKindOAuth || b.AuthKind() != AuthKindOAuth || a.ID != b.ID || a.Provider != b.Provider || a.Disabled != b.Disabled || a.Prefix != b.Prefix || a.ProxyURL != b.ProxyURL || !reflect.DeepEqual(a.Attributes, b.Attributes) || CredentialsChanged(a, b) {
		return false
	}
	for _, key := range []string{"type", "account_id", "account_uuid", "organization_id", "org_id", "email", "project_id", "base_url", "proxy_url"} {
		if !reflect.DeepEqual(a.Metadata[key], b.Metadata[key]) {
			return false
		}
	}
	return true
}

// Only evidence already owned by the current manager may survive a reload.
// Caller snapshots (including a stale typed proof) never supply that evidence.
func retainSubscriptionProof(target, current *Auth, unchanged bool) {
	target.Metadata = maps.Clone(target.Metadata)
	delete(target.Metadata, subscriptionProofKey)
	target.subscriptionRecheckAfter = time.Time{}
	if !unchanged || target.CredentialVersion != current.CredentialVersion || target.RegistrationEpoch != current.RegistrationEpoch || target.Disabled || target.Status == StatusDisabled {
		return
	}
	target.subscriptionRecheckAfter = current.subscriptionRecheckAfter
	if proof, ok := current.Metadata[subscriptionProofKey].(subscriptionProof); ok && proof.Version == current.CredentialVersion && proof.Epoch == current.RegistrationEpoch {
		if target.Metadata == nil {
			target.Metadata = make(map[string]any)
		}
		target.Metadata[subscriptionProofKey] = proof
	}
}

type subscriptionProof struct {
	Model          string
	Until          time.Time
	Version, Epoch uint64
}

func explicitSubscriptionRejection(provider string, result Result, headers http.Header) bool {
	if result.Success || result.SkipQuotaObservation || result.Error == nil || result.Error.HTTPStatus != 429 {
		return false
	}
	switch canonicalSchedulingProvider(provider) {
	case "claude":
		for _, window := range []string{"5h", "7d"} {
			values := headers.Values("Anthropic-Ratelimit-Unified-" + window + "-Status")
			if len(values) == 1 && strings.TrimSpace(values[0]) == "rejected" {
				return true
			}
		}
	case "codex":
		var body struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		return json.Unmarshal([]byte(result.Error.Message), &body) == nil && body.Error.Type == "usage_limit_reached"
	}
	return false
}

func recordSubscriptionProof(a *Auth, result Result, model string, headers http.Header, now time.Time) {
	if a.AuthKind() != AuthKindOAuth || result.SkipQuotaObservation {
		return
	}
	a.Metadata = maps.Clone(a.Metadata)
	if a.Metadata == nil {
		a.Metadata = make(map[string]any)
	}
	delete(a.Metadata, subscriptionProofKey)
	if generation, ok := result.Options.Metadata[subscriptionGenerationKey].(uint64); ok && generation != a.Generation && (a.subscriptionReloadGeneration == 0 || generation < a.subscriptionReloadGeneration || generation > a.Generation) {
		return
	}
	if !explicitSubscriptionRejection(a.Provider, result, headers) {
		return
	}
	until := subscriptionSpendingDeadline(a.Provider, result, headers, now)
	if !until.After(now) {
		return
	}
	if result.CredentialScope {
		model = ""
	}
	a.Metadata[subscriptionProofKey] = subscriptionProof{Model: model, Until: until, Version: a.CredentialVersion, Epoch: a.RegistrationEpoch}
}

func subscriptionExhausted(a *Auth, model string, now time.Time) bool {
	if a == nil || a.Disabled || a.Status == StatusDisabled || a.AuthKind() != AuthKindOAuth {
		return false
	}
	if a.LastError != nil && a.LastError.HTTPStatus != 429 {
		return false
	}
	proof, ok := a.Metadata[subscriptionProofKey].(subscriptionProof)
	return ok && proof.Version == a.CredentialVersion && proof.Epoch == a.RegistrationEpoch && proof.Until.After(now) && (proof.Model == "" || proof.Model == canonicalModelKey(model))
}

// subscriptionOptions constructs a private selector capability. It is never
// derived from client metadata and only authorizes a single validated paid key.
// binding remains the subscription ID, including while the paid key is in use.
func (m *Manager) subscriptionOptions(opts cliproxyexecutor.Options, providers []string, model, bound string, tried map[string]struct{}) (cliproxyexecutor.Options, string) {
	opts.Metadata = maps.Clone(opts.Metadata)
	opts.EnsureMetadata()
	delete(opts.Metadata, subscriptionSelectionMetadataKey)
	m.mu.RLock()
	defer m.mu.RUnlock()
	scope := subscriptionSelectionScope{}
	if parent := m.auths[bound]; parent != nil && parent.AuthKind() != AuthKindOAuth {
		return opts, ""
	}
	configured := false
	for _, api := range m.auths {
		if !isSubscriptionFallback(api) || api.Attributes["subscription_fallback_enabled"] != "true" {
			continue
		}
		for _, provider := range providers {
			if canonicalSchedulingProvider(api.Provider) == canonicalSchedulingProvider(provider) {
				configured = true
			}
		}
	}
	if !configured {
		return opts, ""
	}
	if parent := m.auths[bound]; parent != nil {
		scope.provider = canonicalSchedulingProvider(parent.Provider)
	}
	candidates := make([]*Auth, 0)
	for _, a := range m.auths {
		if a == nil || a.Disabled || a.Status == StatusDisabled || a.AuthKind() != AuthKindOAuth || isSubscriptionFallback(a) {
			continue
		}
		provider := canonicalSchedulingProvider(a.Provider)
		if provider != "claude" && provider != "codex" {
			continue
		}
		if scope.provider != "" && scope.provider != provider {
			continue
		}
		allowed := false
		for _, p := range providers {
			if canonicalSchedulingProvider(p) == provider {
				allowed = true
			}
		}
		if !allowed || !m.authSupportsRouteModel(registry.GetGlobalRegistry(), a, model) {
			continue
		}
		candidates = append(candidates, a)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	// With no binding, let normal subscription selection establish the provider.
	// Exhaustion is evaluated within the provider that would serve the fallback.
	now := time.Now()
	for _, parent := range candidates {
		if bound != "" && parent.ID != bound {
			continue
		}
		allExhausted := true
		for _, a := range candidates {
			if canonicalSchedulingProvider(a.Provider) == canonicalSchedulingProvider(parent.Provider) && !subscriptionExhausted(a, m.selectionModelKeyForAuth(a, model), now) {
				allExhausted = false
			}
		}
		if allExhausted {
			keys := make([]string, 0)
			for _, api := range m.auths {
				if api == nil || api.Disabled || api.Status == StatusDisabled || api.AuthKind() != AuthKindAPIKey || !isSubscriptionFallback(api) {
					continue
				}
				if api.Attributes["subscription_fallback_auth_id"] != parent.ID || api.Attributes["subscription_fallback_enabled"] != "true" || canonicalSchedulingProvider(api.Provider) != canonicalSchedulingProvider(parent.Provider) {
					continue
				}
				if !m.authSupportsRouteModel(registry.GetGlobalRegistry(), api, model) {
					continue
				}
				if _, attempted := tried[api.ID]; attempted {
					continue
				}
				if api.LastError != nil && (api.LastError.HTTPStatus == 401 || api.LastError.HTTPStatus == 403) {
					continue
				}
				if blocked, _, _ := isAuthBlockedForModel(api, m.selectionModelKeyForAuth(api, model), now); blocked {
					continue
				}
				keys = append(keys, api.ID)
			}
			sort.Strings(keys)
			if len(keys) == 0 {
				continue
			}
			scope.provider = canonicalSchedulingProvider(parent.Provider)
			scope.fallbackID = keys[0]
			opts.Metadata[subscriptionSelectionMetadataKey] = scope
			opts.Metadata[cliproxyexecutor.PinnedAuthMetadataKey] = scope.fallbackID
			return opts, parent.ID
		}
	}
	// Keep unrelated API-only installations working. Subscription-first applies
	// when there is an eligible subscription or a bound subscription to protect.
	if len(candidates) > 0 {
		scope.subscriptionsOnly = true
	}
	opts.Metadata[subscriptionSelectionMetadataKey] = scope
	return opts, ""
}
