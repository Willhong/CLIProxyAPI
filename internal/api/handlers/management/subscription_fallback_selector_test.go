package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Synthetic fixtures only. These tests call the credential resolver, never a provider.
func selectorFixture(t *testing.T) (*Handler, string) {
	t.Helper()
	cfg := &config.Config{ClaudeKey: []config.ClaudeKey{
		{APIKey: "selector-fake-A", SubscriptionFallback: &config.SubscriptionFallbackConfig{AuthID: "selector-oauth"}},
		{APIKey: "selector-fake-B", SubscriptionFallback: &config.SubscriptionFallbackConfig{AuthID: "selector-oauth"}},
	}}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: "selector-oauth", Provider: "claude", Metadata: map[string]any{"type": "claude", "access_token": "selector-fake-oauth"},
	}); err != nil {
		t.Fatal(err)
	}
	auths := selectorSynthesize(t, cfg)
	for _, a := range auths {
		if _, err := manager.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	h := &Handler{cfg: cfg, authManager: manager}
	return h, h.claudeKeysWithAuthIndex()[0].AuthIndex
}

func selectorSynthesize(t *testing.T, cfg *config.Config) []*coreauth.Auth {
	t.Helper()
	auths, err := synthesizer.NewConfigSynthesizer().Synthesize(&synthesizer.SynthesisContext{
		Config: cfg, Now: time.Unix(0, 0), IDGenerator: synthesizer.NewStableIDGenerator(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return auths
}

func selectorRegisterCurrent(t *testing.T, h *Handler) {
	t.Helper()
	for _, auth := range selectorSynthesize(t, h.cfg) {
		if _, err := h.authManager.Register(context.Background(), auth); err != nil {
			t.Fatal(err)
		}
	}
}

func selectorBody(t *testing.T, provider, index string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"provider": provider, "auth_index": index})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestSubscriptionFallbackSelectorCurrentIdentity(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		for _, field := range []string{"key", "base", "headers", "proxy", "prefix", "link"} {
			t.Run(provider+"/"+field, func(t *testing.T) {
				h := fallbackReadTestHandler(t, provider)
				oldBody := fallbackReadTestBody(t, h, provider)
				newLink := &config.SubscriptionFallbackConfig{AuthID: "new-subscription"}
				if _, err := h.authManager.Register(context.Background(), &coreauth.Auth{ID: newLink.AuthID, Provider: provider, Metadata: map[string]any{"access_token": "new-synthetic-oauth"}}); err != nil {
					t.Fatal(err)
				}
				if provider == "claude" {
					e := &h.cfg.ClaudeKey[0]
					switch field {
					case "key":
						e.APIKey = "replacement-synthetic-key"
					case "base":
						e.BaseURL = "https://replacement.invalid"
					case "headers":
						e.Headers = map[string]string{"X-Selector": "synthetic-private-header"}
					case "proxy":
						e.ProxyURL = "http://selector.invalid:8080"
					case "prefix":
						e.Prefix = "changed"
					case "link":
						e.SubscriptionFallback = newLink
					}
				} else {
					e := &h.cfg.CodexKey[0]
					switch field {
					case "key":
						e.APIKey = "replacement-synthetic-key"
					case "base":
						e.BaseURL = "https://replacement.invalid"
					case "headers":
						e.Headers = map[string]string{"X-Selector": "synthetic-private-header"}
					case "proxy":
						e.ProxyURL = "http://selector.invalid:8080"
					case "prefix":
						e.Prefix = "changed"
					case "link":
						e.SubscriptionFallback = newLink
					}
				}
				var current *coreauth.Auth
				for _, auth := range selectorSynthesize(t, h.cfg) {
					if auth.Provider == provider {
						current = auth
						break
					}
				}
				body := selectorBody(t, provider, current.EnsureIndex())
				for _, staleBody := range []string{oldBody, body} {
					if ok, _, code := selectorResolve(t, h, staleBody); ok || code != 400 {
						t.Fatal("stale runtime accepted")
					}
					for _, workspaces := range []bool{false, true} {
						if code, _ := fallbackReadTestCall(t, h, staleBody, workspaces, selectorNoNetwork{}); code != 400 {
							t.Fatal("stale route target accepted")
						}
					}
				}
				selectorRegisterCurrent(t, h)
				if ok, key, _ := selectorResolve(t, h, body); !ok || key != current.Attributes["api_key"] {
					t.Fatal("refreshed runtime rejected")
				}
			})
		}
	}
}

func TestSubscriptionFallbackSelectorAmbiguous(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		for _, field := range []string{"identical", "headers", "proxy", "prefix", "link"} {
			t.Run(provider+"/"+field, func(t *testing.T) {
				h := fallbackReadTestHandler(t, provider)
				body := fallbackReadTestBody(t, h, provider)
				if provider == "claude" {
					e := h.cfg.ClaudeKey[0]
					switch field {
					case "headers":
						e.Headers = map[string]string{"X-Other": "private-header"}
					case "proxy":
						e.ProxyURL = "http://other.invalid"
					case "prefix":
						e.Prefix = "other"
					case "link":
						e.SubscriptionFallback = &config.SubscriptionFallbackConfig{AuthID: "other"}
					}
					h.cfg.ClaudeKey = append(h.cfg.ClaudeKey, e)
				} else {
					e := h.cfg.CodexKey[0]
					switch field {
					case "headers":
						e.Headers = map[string]string{"X-Other": "private-header"}
					case "proxy":
						e.ProxyURL = "http://other.invalid"
					case "prefix":
						e.Prefix = "other"
					case "link":
						e.SubscriptionFallback = &config.SubscriptionFallbackConfig{AuthID: "other"}
					}
					h.cfg.CodexKey = append(h.cfg.CodexKey, e)
				}
				selectorRegisterCurrent(t, h)
				for _, workspaces := range []bool{false, true} {
					if code, _ := fallbackReadTestCall(t, h, body, workspaces, selectorNoNetwork{}); code != 400 {
						t.Fatal("ambiguous target accepted")
					}
				}
			})
		}
	}
}

func TestSubscriptionFallbackSelectorStrictContract(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		h := fallbackReadTestHandler(t, provider)
		var selected subscriptionFallbackReadRequest
		if err := json.Unmarshal([]byte(fallbackReadTestBody(t, h, provider)), &selected); err != nil {
			t.Fatal(err)
		}
		oauth, _ := h.authManager.GetByID("subscription")
		other := "claude"
		if provider == other {
			other = "codex"
		}
		bodies := []string{
			`{"provider":"` + provider + `"}`,
			selectorBody(t, provider, ""), selectorBody(t, provider, " "),
			selectorBody(t, provider, " "+selected.AuthIndex), selectorBody(t, provider, selected.AuthIndex+" "),
			selectorBody(t, provider, "unknown-index"), selectorBody(t, provider, oauth.EnsureIndex()),
			selectorBody(t, other, selected.AuthIndex),
			`{"provider":"` + provider + `","auth_index":"` + selected.AuthIndex + `","config_index":0}`,
			`{"provider":"` + provider + `","auth_index":12}`,
			`{"provider":"` + provider + `","auth_index":null}`,
			selectorBody(t, provider, selected.AuthIndex) + ` {}`,
			selectorBody(t, provider, strings.Repeat("x", 4096)),
		}
		for i, body := range bodies {
			for _, workspaces := range []bool{false, true} {
				if code, _ := fallbackReadTestCall(t, h, body, workspaces, selectorNoNetwork{}); code != 400 {
					t.Fatalf("provider=%s case=%d workspaces=%v code=%d", provider, i, workspaces, code)
				}
			}
		}
	}
}

func TestSubscriptionFallbackSelectorRuntimeIdentityTampering(t *testing.T) {
	for _, field := range []string{"api_key", "base_url", "subscription_fallback_auth_id", "header:extra", "proxy", "prefix", "provider", "index", "header-removal"} {
		t.Run(field, func(t *testing.T) {
			h := fallbackReadTestHandler(t, "claude")
			body := fallbackReadTestBody(t, h, "claude")
			current := selectorSynthesize(t, h.cfg)[0]
			live, _ := h.authManager.GetByID(current.ID)
			switch field {
			case "proxy":
				live.ProxyURL = "http://tampered.invalid"
			case "prefix":
				live.Prefix = "tampered"
			case "provider":
				live.Provider = "codex"
			case "index":
				live.Index = "explicit-runtime-index"
			case "header-removal":
				for key := range live.Attributes {
					if strings.HasPrefix(key, "header:") {
						delete(live.Attributes, key)
					}
				}
			default:
				live.Attributes[field] = "synthetic-tampered"
			}
			if _, err := h.authManager.Register(context.Background(), live); err != nil {
				t.Fatal(err)
			}
			for _, workspaces := range []bool{false, true} {
				if code, _ := fallbackReadTestCall(t, h, body, workspaces, selectorNoNetwork{}); code != 400 {
					t.Fatal("inconsistent runtime accepted")
				}
			}
		})
	}
}

func TestSubscriptionFallbackSelectorReorderBothRoutes(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		h := fallbackReadTestHandler(t, provider)
		body := fallbackReadTestBody(t, h, provider)
		if provider == "claude" {
			other := h.cfg.ClaudeKey[0]
			other.APIKey = "other-synthetic-key"
			h.cfg.ClaudeKey = append([]config.ClaudeKey{other}, h.cfg.ClaudeKey...)
		} else {
			other := h.cfg.CodexKey[0]
			other.APIKey = "other-synthetic-key"
			h.cfg.CodexKey = append([]config.CodexKey{other}, h.cfg.CodexKey...)
		}
		for _, workspaces := range []bool{false, true} {
			calls := 0
			transport := fallbackReadTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Header.Get("x-api-key") != fallbackReadTestKey {
					t.Fatal("reorder selected another credential")
				}
				return fallbackReadTestResponse(200, `{"data":[],"has_more":false}`), nil
			})
			if code, _ := fallbackReadTestCall(t, h, body, workspaces, transport); code != 200 {
				t.Fatal("reordered target rejected")
			}
			if provider == "claude" && calls == 0 {
				t.Fatal("expected mocked read")
			}
			if provider == "codex" && calls != 0 {
				t.Fatal("Codex contacted provider")
			}
		}
	}
}

func TestSubscriptionFallbackSelectorMissingRuntime(t *testing.T) {
	for _, variant := range []string{"manager", "config", "membership"} {
		h := fallbackReadTestHandler(t, "claude")
		body := fallbackReadTestBody(t, h, "claude")
		switch variant {
		case "manager":
			h.authManager = nil
		case "config":
			h.cfg = nil
		case "membership":
			manager := coreauth.NewManager(nil, nil, nil)
			oauth, _ := h.authManager.GetByID("subscription")
			if _, err := manager.Register(context.Background(), oauth); err != nil {
				t.Fatal(err)
			}
			h.authManager = manager
		}
		for _, workspaces := range []bool{false, true} {
			if code, _ := fallbackReadTestCall(t, h, body, workspaces, selectorNoNetwork{}); code != 400 {
				t.Fatalf("%s accepted", variant)
			}
		}
	}
}

type selectorNoNetwork struct{}

func (selectorNoNetwork) RoundTrip(*http.Request) (*http.Response, error) {
	panic("selector regression must not perform network requests")
}

func selectorResolve(t *testing.T, h *Handler, body string) (bool, string, int) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	_, credential, ok := h.subscriptionFallbackReadCredential(c, selectorNoNetwork{})
	for _, secret := range []string{"selector-fake-A", "selector-fake-B", "selector-fake-oauth"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("resolver response exposed synthetic secret")
		}
	}
	return ok, credential.key, w.Code
}

// A cached UI offset must not silently retarget from A to B.
func TestSubscriptionFallbackSelectorRejectsReorderedLegacyOffset(t *testing.T) {
	h, _ := selectorFixture(t)
	h.cfg.ClaudeKey[0], h.cfg.ClaudeKey[1] = h.cfg.ClaudeKey[1], h.cfg.ClaudeKey[0]
	ok, key, code := selectorResolve(t, h, `{"provider":"claude","config_index":0}`)
	if ok || code != http.StatusBadRequest {
		t.Fatalf("legacy offset must fail closed: accepted=%v status=%d selected_other_credential=%v", ok, code, key == "selector-fake-B")
	}
}

// Red on the old contract: the v8 exposed identity must survive config reorder,
// including when runtime config_index still points at the pre-reorder position.
func TestSubscriptionFallbackSelectorAuthIndexSurvivesReorder(t *testing.T) {
	h, index := selectorFixture(t)
	if index == "" {
		t.Fatal("fixture did not expose an auth index")
	}
	h.cfg.ClaudeKey[0], h.cfg.ClaudeKey[1] = h.cfg.ClaudeKey[1], h.cfg.ClaudeKey[0]
	body, err := json.Marshal(map[string]string{"provider": "claude", "auth_index": index})
	if err != nil {
		t.Fatal(err)
	}
	ok, key, code := selectorResolve(t, h, string(body))
	if !ok || key != "selector-fake-A" {
		t.Fatalf("stable selector did not resolve original credential: accepted=%v status=%d correct_target=%v", ok, code, key == "selector-fake-A")
	}
}

// Red on the old handler: a replaced config entry is usable even while the runtime
// manager contains only the previous credential. The replacement must not be read.
func TestSubscriptionFallbackSelectorRejectsStaleRuntimeLegacyOffset(t *testing.T) {
	h, _ := selectorFixture(t)
	h.cfg.ClaudeKey[0].APIKey = "selector-fake-replacement"
	ok, key, code := selectorResolve(t, h, `{"provider":"claude","config_index":0}`)
	if ok || code != http.StatusBadRequest {
		t.Fatalf("stale runtime must fail closed: accepted=%v status=%d selected_replacement=%v", ok, code, key == "selector-fake-replacement")
	}
}

// Green documentation test: EnsureIndex alone is not the complete config identity.
// Header/proxy/prefix changes retain the exposed index but change synthesized ID.
func TestSubscriptionFallbackSelectorIdentityLayers(t *testing.T) {
	for _, field := range []string{"headers", "proxy", "prefix"} {
		t.Run(field, func(t *testing.T) {
			h, _ := selectorFixture(t)
			before := selectorSynthesize(t, h.cfg)[0]
			switch field {
			case "headers":
				h.cfg.ClaudeKey[0].Headers = map[string]string{"X-Selector": "synthetic"}
			case "proxy":
				h.cfg.ClaudeKey[0].ProxyURL = "http://selector.invalid:8080"
			case "prefix":
				h.cfg.ClaudeKey[0].Prefix = "selector"
			}
			after := selectorSynthesize(t, h.cfg)[0]
			if before.ID == after.ID || before.EnsureIndex() != after.EnsureIndex() {
				t.Fatal("unexpected identity mapping; update remediation before applying")
			}
		})
	}
}
