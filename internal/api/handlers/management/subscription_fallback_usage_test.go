package management

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

const fallbackReadTestKey = "synthetic-secret-not-a-real-key"

type fallbackReadTestTransport func(*http.Request) (*http.Response, error)

func (f fallbackReadTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fallbackReadTestResponse(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func fallbackReadTestHandler(t *testing.T, provider string) *Handler {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	_, err := manager.Register(context.Background(), &coreauth.Auth{ID: "subscription", Provider: provider, Metadata: map[string]any{"type": provider, "access_token": "synthetic-oauth"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: &config.Config{
		ClaudeKey: []config.ClaudeKey{{APIKey: fallbackReadTestKey, BaseURL: "https://attacker.invalid", Headers: map[string]string{"x-api-key": "wrong", "Authorization": "wrong", "Host": "attacker.invalid"}, SubscriptionFallback: &config.SubscriptionFallbackConfig{AuthID: "subscription"}}},
		CodexKey:  []config.CodexKey{{APIKey: fallbackReadTestKey, SubscriptionFallback: &config.SubscriptionFallbackConfig{AuthID: "subscription"}}},
	}, authManager: manager}
	for _, auth := range selectorSynthesize(t, h.cfg) {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

func fallbackReadTestBody(t *testing.T, h *Handler, provider string) string {
	t.Helper()
	for _, auth := range h.authManager.List() {
		if auth.Provider == provider && auth.AuthKind() == "apikey" {
			body, err := json.Marshal(map[string]string{"provider": provider, "auth_index": auth.EnsureIndex()})
			if err != nil {
				t.Fatal(err)
			}
			return string(body)
		}
	}
	t.Fatal("fixture API key missing")
	return ""
}

func fallbackReadTestCall(t *testing.T, h *Handler, body string, workspaces bool, transport http.RoundTripper) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v8/management/credentials/subscription-fallback/usage", strings.NewReader(body))
	if workspaces {
		h.subscriptionFallbackWorkspaces(c, transport)
	} else {
		h.subscriptionFallbackUsage(c, transport, time.Date(2026, 11, 3, 1, 2, 3, 0, time.FixedZone("offset", 9*3600)))
	}
	if strings.Contains(w.Body.String(), fallbackReadTestKey) || strings.Contains(w.Body.String(), "synthetic-oauth") {
		t.Fatal("secret exposed")
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code == http.StatusOK {
		var request subscriptionFallbackReadRequest
		if err := json.Unmarshal([]byte(body), &request); err != nil {
			t.Fatal(err)
		}
		if result["provider"] != request.Provider || result["auth_index"] != request.AuthIndex {
			t.Fatal("response selector differs from request")
		}
		if _, exists := result["config_index"]; exists {
			t.Fatal("legacy selector exposed")
		}
	}
	return w.Code, result
}

func TestSubscriptionFallbackReadUsageComplete(t *testing.T) {
	h := fallbackReadTestHandler(t, "claude")
	calls := 0
	transport := fallbackReadTestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Scheme != "https" || r.URL.Host != "api.anthropic.com" || r.Method != http.MethodGet || r.Host != "api.anthropic.com" {
			t.Fatalf("unsafe destination: %s", r.URL)
		}
		if r.Header.Get("x-api-key") != fallbackReadTestKey || r.Header.Get("Authorization") != "" || r.Header.Get("anthropic-version") != "2023-06-01" {
			t.Fatal("unsafe headers")
		}
		q := r.URL.Query()
		if q.Get("starting_at") != "2026-11-01T00:00:00Z" || q.Get("ending_at") != "2026-11-02T16:02:03Z" {
			t.Fatalf("incorrect UTC interval: %v", q)
		}
		if strings.HasSuffix(r.URL.Path, "messages") {
			if q.Get("page") == "" {
				return fallbackReadTestResponse(200, `{"data":[{"results":[{"input_tokens":9007199254740993,"output_tokens":2,"cache_read_input_tokens":3}]}],"has_more":true,"next_page":"second"}`), nil
			}
			if q.Get("page") != "second" {
				t.Fatal("wrong cursor")
			}
			return fallbackReadTestResponse(200, `{"data":[{"results":[{"input_tokens":7,"output_tokens":8,"cache_creation_input_tokens":4}]}],"has_more":false}`), nil
		}
		if q.Get("page") != "" {
			t.Fatal("usage cursor contaminated cost")
		}
		return fallbackReadTestResponse(200, `{"data":[{"results":[{"amount":"123.456789","currency":"USD"},{"amount":"0.003211","currency":"usd"}]}],"has_more":false}`), nil
	})
	code, response := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), false, transport)
	if code != 200 || calls != 3 {
		t.Fatalf("code=%d calls=%d response=%v", code, calls, response)
	}
	usage := response["usage"].(map[string]any)
	// Marshal the typed helper separately below to verify integers beyond float64 precision.
	if usage["status"] != "available" || usage["value"].(map[string]any)["output_tokens"] != float64(10) {
		t.Fatal(response)
	}
	cost := response["cost"].(map[string]any)
	if cost["status"] != "available" || cost["value"].(map[string]any)["amount"] != "1.2346" {
		t.Fatal(response)
	}
	if response["credit_balance"] != nil || response["credit_balance_status"] != "unsupported" {
		t.Fatal(response)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/", nil)
	result := (subscriptionFallbackReadCredential{key: fallbackReadTestKey, transport: transport}).analytics(c, "/v1/organizations/usage_report/messages", mapValuesForFallbackTest(), false)
	values := result.Value.(map[string]json.Number)
	if values["input_tokens"].String() != "9007199254741000" {
		t.Fatal(values)
	}
}

func mapValuesForFallbackTest() map[string][]string {
	return map[string][]string{"starting_at": {"2026-11-01T00:00:00Z"}, "ending_at": {"2026-11-02T16:02:03Z"}}
}

func TestSubscriptionFallbackReadValidation(t *testing.T) {
	for _, body := range []string{`{}`, `{"provider":"Claude","config_index":0}`, `{"provider":"other","config_index":0}`, `{"provider":"claude","config_index":-1}`, `{"provider":"claude","config_index":999}`, `{"provider":"claude","config_index":0.5}`, `{"provider":"claude","config_index":0,"url":"https://attacker.invalid"}`, `{"provider":"claude","config_index":0} {}`} {
		t.Run(body, func(t *testing.T) {
			h := fallbackReadTestHandler(t, "claude")
			code, _ := fallbackReadTestCall(t, h, body, false, fallbackReadTestTransport(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected upstream"); return nil, nil }))
			if code != 400 {
				t.Fatalf("code=%d", code)
			}
		})
	}
	for _, variant := range []string{"missing", "unlinked", "cross-provider", "api-key", "blank-key"} {
		t.Run(variant, func(t *testing.T) {
			h := fallbackReadTestHandler(t, "claude")
			switch variant {
			case "missing":
				h.cfg.ClaudeKey[0].SubscriptionFallback.AuthID = "missing"
			case "unlinked":
				h.cfg.ClaudeKey[0].SubscriptionFallback = nil
			case "cross-provider":
				h = fallbackReadTestHandler(t, "codex")
			case "api-key":
				_, err := h.authManager.Register(context.Background(), &coreauth.Auth{ID: "api-key", Provider: "claude", Attributes: map[string]string{"api_key": "synthetic"}})
				if err != nil {
					t.Fatal(err)
				}
				h.cfg.ClaudeKey[0].SubscriptionFallback.AuthID = "api-key"
			case "blank-key":
				h.cfg.ClaudeKey[0].APIKey = ""
			}
			selectorRegisterCurrent(t, h)
			code, _ := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), true, fallbackReadTestTransport(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected upstream"); return nil, nil }))
			if code != 400 {
				t.Fatalf("code=%d", code)
			}
		})
	}
}

func TestSubscriptionFallbackReadCodexUnsupported(t *testing.T) {
	for _, workspaces := range []bool{false, true} {
		h := fallbackReadTestHandler(t, "codex")
		code, response := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "codex"), workspaces, fallbackReadTestTransport(func(*http.Request) (*http.Response, error) {
			t.Fatal("Codex must not contact guessed API")
			return nil, nil
		}))
		field := "usage"
		if workspaces {
			field = "workspaces"
		}
		if code != 200 || response[field].(map[string]any)["status"] != "unsupported" {
			t.Fatal(response)
		}
	}
}

func TestSubscriptionFallbackReadIndependentStatusAndSecrecy(t *testing.T) {
	for _, tc := range []struct {
		code   int
		status string
	}{{401, "permission_required"}, {403, "permission_required"}, {404, "unsupported"}, {405, "unsupported"}, {429, "error"}, {500, "error"}, {302, "error"}} {
		t.Run(http.StatusText(tc.code), func(t *testing.T) {
			h := fallbackReadTestHandler(t, "claude")
			calls := 0
			transport := fallbackReadTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if strings.HasSuffix(r.URL.Path, "messages") {
					resp := fallbackReadTestResponse(tc.code, fallbackReadTestKey)
					resp.Header.Set("Location", "https://attacker.invalid/"+fallbackReadTestKey)
					return resp, nil
				}
				return fallbackReadTestResponse(200, `{"data":[],"has_more":false}`), nil
			})
			code, response := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), false, transport)
			if code != 200 || calls != 2 || response["usage"].(map[string]any)["status"] != tc.status || response["cost"].(map[string]any)["status"] != "available" {
				t.Fatal(response, calls)
			}
			if response["usage"].(map[string]any)["value"] != nil {
				t.Fatal("incomplete value exposed")
			}
		})
	}
	h := fallbackReadTestHandler(t, "claude")
	_, response := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), false, fallbackReadTestTransport(func(*http.Request) (*http.Response, error) { return nil, errors.New(fallbackReadTestKey) }))
	if response["usage"].(map[string]any)["status"] != "error" {
		t.Fatal(response)
	}
}

func TestSubscriptionFallbackReadIncompleteAnalytics(t *testing.T) {
	cases := []string{
		`{"data":[],"has_more":true}`,
		`{"data":[],"has_more":true,"next_page":"repeat"}`,
		`{"data":[]}`,
		`{"data":null,"has_more":false}`,
		`{"data":[{}],"has_more":false}`,
		`{"data":[{"results":[{"input_tokens":-1,"output_tokens":2}]}],"has_more":false}`,
		`{"data":[{"results":[{"input_tokens":1.5,"output_tokens":2}]}],"has_more":false}`,
		`{"data":[{"results":[{"amount":"1/2","currency":"USD"}]}],"has_more":false}`,
		`{"data":[{"results":[{"amount":"1","currency":"EUR"}]}],"has_more":false}`,
		`not-json-` + fallbackReadTestKey,
		strings.Repeat("x", subscriptionFallbackMaxPageBytes+1),
	}
	for i, body := range cases {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			h := fallbackReadTestHandler(t, "claude")
			_, response := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), false, fallbackReadTestTransport(func(*http.Request) (*http.Response, error) { return fallbackReadTestResponse(200, body), nil }))
			for _, field := range []string{"usage", "cost"} {
				result := response[field].(map[string]any)
				if result["status"] != "error" || result["value"] != nil {
					t.Fatal(response)
				}
			}
		})
	}
}

func TestSubscriptionFallbackReadPaginationLimit(t *testing.T) {
	h := fallbackReadTestHandler(t, "claude")
	calls := 0
	_, response := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), false, fallbackReadTestTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return fallbackReadTestResponse(200, `{"data":[],"has_more":true,"next_page":"`+time.Unix(int64(calls), 0).Format(time.RFC3339)+`"}`), nil
	}))
	if calls != 2*subscriptionFallbackMaxPages || response["usage"].(map[string]any)["value"] != nil || response["cost"].(map[string]any)["status"] != "error" {
		t.Fatal(response, calls)
	}
}

func TestSubscriptionFallbackReadWorkspaces(t *testing.T) {
	h := fallbackReadTestHandler(t, "claude")
	calls := 0
	_, response := fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), true, fallbackReadTestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "api.anthropic.com" || r.URL.Path != "/v1/organizations/workspaces" || r.URL.Query().Get("include_default") != "true" || r.URL.Query().Get("limit") != "100" {
			t.Fatal(r.URL)
		}
		if calls == 1 {
			return fallbackReadTestResponse(200, `{"data":[{"id":"wrk_default","name":"Default","secret":"`+fallbackReadTestKey+`"}],"has_more":true,"last_id":"wrk_default"}`), nil
		}
		if r.URL.Query().Get("after_id") != "wrk_default" {
			t.Fatal(r.URL)
		}
		return fallbackReadTestResponse(200, `{"data":[{"id":"wrk_second","name":"Second","other":"ignored"}],"has_more":false}`), nil
	}))
	result := response["workspaces"].(map[string]any)
	if calls != 2 || result["status"] != "available" {
		t.Fatal(response)
	}
	entries := result["value"].([]any)
	if len(entries) != 2 || len(entries[0].(map[string]any)) != 2 {
		t.Fatal(response)
	}
	for _, body := range []string{`{"data":[{"id":"x","name":"X"}],"has_more":true}`, `{"data":[],"has_more":true,"last_id":"x"}`, `{"data":[{"id":"x","name":"X"}],"has_more":true,"last_id":"x"}`, `{"data":[{"id":"x","name":"` + fallbackReadTestKey + `"}],"has_more":false}`} {
		_, response = fallbackReadTestCall(t, h, fallbackReadTestBody(t, h, "claude"), true, fallbackReadTestTransport(func(*http.Request) (*http.Response, error) { return fallbackReadTestResponse(200, body), nil }))
		result = response["workspaces"].(map[string]any)
		if result["status"] != "error" || result["value"] != nil {
			t.Fatal(response)
		}
	}
}

func TestSubscriptionFallbackReadExactDecimal(t *testing.T) {
	for _, tc := range []struct{ cents, want string }{{"0", "0.00"}, {"1", "0.01"}, {"10", "0.10"}, {"123.456789", "1.23456789"}, {"900719925474099312345.123456789", "9007199254740993123.45123456789"}} {
		value, err := subscriptionFallbackDecimal(tc.cents)
		if err != nil {
			t.Fatal(err)
		}
		value.Quo(value, big.NewRat(100, 1))
		if got := subscriptionFallbackExactDecimal(value); got != tc.want {
			t.Fatalf("got=%s want=%s", got, tc.want)
		}
	}
	for _, s := range []string{"", "-1", "1e3", "1/2", "NaN", ".1", "1.", "1..2"} {
		if _, err := subscriptionFallbackDecimal(s); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
