package auth

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
)

// A restored/invalidated proof must not authorize spending. It also must not
// strand a linked subscription behind its old cooldown until the quota reset.
// Revalidate with the subscription itself; only a fresh explicit response can
// subsequently select paid fallback. Never clear or fabricate shared quota state.
func (m *Manager) recheckSubscriptionQuota(ctx context.Context, opts cliproxyexecutor.Options, providers []string, model, bound string, tried map[string]struct{}, pickErr error) (*Auth, ProviderExecutor) {
	var cooldown *modelCooldownError
	if !errors.As(pickErr, &cooldown) || ctx.Err() != nil || m.hasPluginScheduler() {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if parent := m.auths[bound]; parent != nil && parent.AuthKind() != AuthKindOAuth {
		return nil, nil
	}
	now := time.Now()
	eligibility := authSelectionEligibilityForRequest(ctx, opts)
	ids := make([]string, 0, len(m.auths))
	for id := range m.auths {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		a := m.auths[id]
		if a == nil || a.Disabled || a.Status == StatusDisabled || a.AuthKind() != AuthKindOAuth || !eligibility.allows(a) {
			continue
		}
		if _, used := tried[id]; used {
			continue
		}
		provider := canonicalSchedulingProvider(a.Provider)
		allowed := false
		for _, p := range providers {
			if canonicalSchedulingProvider(p) == provider {
				allowed = true
			}
		}
		checkModel := m.selectionModelKeyForAuth(a, model)
		if !allowed || !m.authSupportsRouteModel(registry.GetGlobalRegistry(), a, model) || subscriptionExhausted(a, checkModel, now) || a.subscriptionRecheckAfter.After(now) {
			continue
		}
		if a.LastError != nil && a.LastError.HTTPStatus != 429 {
			continue
		}
		blocked, reason, _ := isAuthBlockedForModel(a, checkModel, now)
		if !blocked || reason != blockReasonCooldown {
			continue
		}
		linked := false
		for _, key := range m.auths {
			if key != nil && !key.Disabled && key.Status != StatusDisabled && key.AuthKind() == AuthKindAPIKey && canonicalSchedulingProvider(key.Provider) == provider && key.Attributes["subscription_fallback_enabled"] == "true" && m.authSupportsRouteModel(registry.GetGlobalRegistry(), key, model) {
				parent := m.auths[key.Attributes["subscription_fallback_auth_id"]]
				if parent == nil || parent.Disabled || parent.Status == StatusDisabled || parent.AuthKind() != AuthKindOAuth || canonicalSchedulingProvider(parent.Provider) != provider || !m.authSupportsRouteModel(registry.GetGlobalRegistry(), parent, model) {
					continue
				}
				linked = true
				break
			}
		}
		if !linked {
			continue
		}
		executor, ok := m.executorLocked(provider)
		if !ok {
			continue
		}
		a.subscriptionRecheckAfter = now.Add(30 * time.Second)
		log.WithFields(log.Fields{"provider": provider, "model": model}).Info("rechecking subscription quota before authorizing API fallback")
		probe := a.Clone()
		// Execution-model filtering also checks cooldown. Lift it only on this
		// one request's snapshot, never on the manager's shared credential.
		clearCooldownStateForAuth(probe, now)
		probe.Generation = a.Generation
		return probe, executor
	}
	return nil, nil
}
