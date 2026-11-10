package management

import (
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

const subscriptionFallbackMaxPages = 100
const subscriptionFallbackMaxPageBytes = 4 << 20

// These read-only endpoints deliberately do not use configured base URLs or headers.
// Only the existing credential/global proxy policy is inherited.
type subscriptionFallbackReadRequest struct {
	Provider  string `json:"provider"`
	AuthIndex string `json:"auth_index"`
}

type subscriptionFallbackReadResult struct {
	Status string `json:"status"`
	Value  any    `json:"value"`
	Reason string `json:"reason,omitempty"`
}

type subscriptionFallbackReadCredential struct {
	key       string
	transport http.RoundTripper
}

func fallbackReadUnavailable(status, reason string) subscriptionFallbackReadResult {
	return subscriptionFallbackReadResult{Status: status, Reason: reason}
}

// PostSubscriptionFallbackUsage reads official month-to-date organization analytics.
// Codex has no verified public read API in this feature and never makes a request.
func (h *Handler) PostSubscriptionFallbackUsage(c *gin.Context) {
	h.subscriptionFallbackUsage(c, nil, time.Now())
}

// PostSubscriptionFallbackWorkspaces discovers only workspace IDs and names.
func (h *Handler) PostSubscriptionFallbackWorkspaces(c *gin.Context) {
	h.subscriptionFallbackWorkspaces(c, nil)
}

func (h *Handler) subscriptionFallbackReadCredential(c *gin.Context, transport http.RoundTripper) (subscriptionFallbackReadRequest, subscriptionFallbackReadCredential, bool) {
	var body subscriptionFallbackReadRequest
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 4096))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&body)
	var extra any
	if err != nil || decoder.Decode(&extra) != io.EOF || (body.Provider != "claude" && body.Provider != "codex") || body.AuthIndex == "" || body.AuthIndex != strings.TrimSpace(body.AuthIndex) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return body, subscriptionFallbackReadCredential{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	invalid := func() (subscriptionFallbackReadRequest, subscriptionFallbackReadCredential, bool) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "linked subscription credential not found"})
		return body, subscriptionFallbackReadCredential{}, false
	}
	if h.cfg == nil || h.authManager == nil {
		return invalid()
	}
	// Synthesize the entire snapshot so duplicate ID counters match the watcher.
	auths, err := synthesizer.NewConfigSynthesizer().Synthesize(&synthesizer.SynthesisContext{
		Config: h.cfg, Now: time.Now(), IDGenerator: synthesizer.NewStableIDGenerator(),
	})
	if err != nil {
		return invalid()
	}
	var candidate *coreauth.Auth
	for _, auth := range auths {
		if auth != nil && auth.Provider == body.Provider && auth.AuthKind() == "apikey" && strings.TrimSpace(auth.Attributes["api_key"]) != "" && auth.EnsureIndex() == body.AuthIndex {
			if candidate != nil {
				return invalid()
			}
			candidate = auth
		}
	}
	if candidate == nil {
		return invalid()
	}
	live, found := h.authManager.GetByID(candidate.ID)
	if !found || live == nil || live.EnsureIndex() != body.AuthIndex || !fallbackSameCredentialIdentity(live, candidate) {
		return invalid()
	}
	// Only a fresh candidate's offset can address this same locked config snapshot.
	index, err := strconv.Atoi(candidate.Attributes["config_index"])
	if err != nil || index < 0 {
		return invalid()
	}
	var key, proxy string
	var link *config.SubscriptionFallbackConfig
	if h.cfg != nil {
		if body.Provider == "claude" && index < len(h.cfg.ClaudeKey) {
			entry := h.cfg.ClaudeKey[index]
			key, proxy, link = entry.APIKey, entry.ProxyURL, entry.SubscriptionFallback
		} else if body.Provider == "codex" && index < len(h.cfg.CodexKey) {
			entry := h.cfg.CodexKey[index]
			key, proxy, link = entry.APIKey, entry.ProxyURL, entry.SubscriptionFallback
		}
	}
	valid := false
	key = strings.TrimSpace(key)
	if key != "" && key == candidate.Attributes["api_key"] && key == live.Attributes["api_key"] && link != nil && strings.TrimSpace(link.AuthID) != "" {
		if auth, ok := h.authManager.GetByID(link.AuthID); ok && auth != nil && auth.Provider == body.Provider && auth.AuthKind() == "oauth" {
			valid = true
		}
	}
	if !valid {
		return invalid()
	}
	if transport == nil {
		transport = h.apiCallTransport(&coreauth.Auth{ProxyURL: proxy}, "")
	}
	return body, subscriptionFallbackReadCredential{key: key, transport: transport}, true
}

// Compare identity rather than runtime config_index, which may lag a reorder.
func fallbackSameCredentialIdentity(live, current *coreauth.Auth) bool {
	if live == nil || current == nil || live.ID != current.ID || live.Provider != current.Provider || live.AuthKind() != "apikey" || current.AuthKind() != "apikey" || live.ProxyURL != current.ProxyURL || live.Prefix != current.Prefix {
		return false
	}
	for _, field := range []string{"api_key", "base_url", "subscription_fallback_auth_id"} {
		if live.Attributes[field] != current.Attributes[field] {
			return false
		}
	}
	for key, value := range live.Attributes {
		if strings.HasPrefix(key, "header:") {
			if currentValue, exists := current.Attributes[key]; !exists || currentValue != value {
				return false
			}
		}
	}
	for key, value := range current.Attributes {
		if strings.HasPrefix(key, "header:") {
			if liveValue, exists := live.Attributes[key]; !exists || liveValue != value {
				return false
			}
		}
	}
	return true
}

func (h *Handler) subscriptionFallbackUsage(c *gin.Context, transport http.RoundTripper, now time.Time) {
	body, credential, ok := h.subscriptionFallbackReadCredential(c, transport)
	if !ok {
		return
	}
	now = now.UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	unsupported := fallbackReadUnavailable("unsupported", "No verified official API is available for this provider.")
	response := gin.H{
		"provider": body.Provider, "auth_index": body.AuthIndex,
		"starting_at": start.Format(time.RFC3339), "ending_at": now.Format(time.RFC3339),
		"usage": unsupported, "cost": unsupported,
		"credit_balance":        nil,
		"credit_balance_status": "unsupported",
		"credit_balance_reason": "No documented public credit-balance API is available.",
	}
	if body.Provider == "claude" {
		query := url.Values{"starting_at": {start.Format(time.RFC3339)}, "ending_at": {now.Format(time.RFC3339)}, "bucket_width": {"1d"}, "limit": {"31"}}
		response["usage"] = credential.analytics(c, "/v1/organizations/usage_report/messages", query, false)
		response["cost"] = credential.analytics(c, "/v1/organizations/cost_report", query, true)
	}
	c.JSON(http.StatusOK, response)
}

type subscriptionFallbackPage struct {
	Data     json.RawMessage `json:"data"`
	HasMore  *bool           `json:"has_more"`
	NextPage *string         `json:"next_page"`
	LastID   *string         `json:"last_id"`
}

// getPage never exposes upstream error text, headers or response bodies.
func (credential subscriptionFallbackReadCredential) getPage(c *gin.Context, path string, query url.Values) (subscriptionFallbackPage, subscriptionFallbackReadResult) {
	var page subscriptionFallbackPage
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, "https://api.anthropic.com"+path+"?"+query.Encode(), nil)
	if err != nil {
		return page, fallbackReadUnavailable("error", "Could not build the upstream request.")
	}
	req.Header.Set("x-api-key", credential.key)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Transport: credential.transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return page, fallbackReadUnavailable("error", "Upstream request failed.")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return page, fallbackReadUnavailable("permission_required", "Organization read permissions are required.")
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		return page, fallbackReadUnavailable("unsupported", "The official endpoint is unavailable for this credential.")
	}
	if resp.StatusCode != http.StatusOK {
		return page, fallbackReadUnavailable("error", "Upstream request failed.")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, subscriptionFallbackMaxPageBytes+1))
	if err != nil || len(raw) > subscriptionFallbackMaxPageBytes || json.Unmarshal(raw, &page) != nil || page.HasMore == nil || len(page.Data) == 0 || string(page.Data) == "null" {
		return page, fallbackReadUnavailable("error", "Invalid or incomplete upstream response.")
	}
	return page, subscriptionFallbackReadResult{Status: "available"}
}

var subscriptionFallbackTokenFields = []string{"input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"}

func (credential subscriptionFallbackReadCredential) analytics(c *gin.Context, path string, query url.Values, cost bool) subscriptionFallbackReadResult {
	// Each report owns its pagination cursor.
	query, _ = url.ParseQuery(query.Encode())
	totals := map[string]*big.Int{}
	for _, field := range subscriptionFallbackTokenFields {
		totals[field] = new(big.Int)
	}
	cents := new(big.Rat)
	seen := map[string]bool{}
	invalid := func() subscriptionFallbackReadResult {
		return fallbackReadUnavailable("error", "Invalid or incomplete upstream analytics.")
	}
	for count := 0; count < subscriptionFallbackMaxPages; count++ {
		page, result := credential.getPage(c, path, query)
		if result.Status != "available" {
			return result
		}
		var buckets []struct {
			Results []map[string]json.RawMessage `json:"results"`
		}
		if json.Unmarshal(page.Data, &buckets) != nil {
			return invalid()
		}
		for _, bucket := range buckets {
			if bucket.Results == nil {
				return invalid()
			}
			for _, row := range bucket.Results {
				if cost {
					var amount, currency string
					if json.Unmarshal(row["amount"], &amount) != nil || json.Unmarshal(row["currency"], &currency) != nil || !strings.EqualFold(currency, "usd") {
						return invalid()
					}
					value, err := subscriptionFallbackDecimal(amount)
					if err != nil {
						return invalid()
					}
					cents.Add(cents, value)
				} else {
					for _, field := range subscriptionFallbackTokenFields {
						// Optional cache counts are absent on some legacy reports.
						raw, exists := row[field]
						if !exists && strings.HasPrefix(field, "cache_") {
							continue
						}
						var number json.Number
						if !exists || json.Unmarshal(raw, &number) != nil {
							return invalid()
						}
						value, good := new(big.Int).SetString(string(number), 10)
						if !good || value.Sign() < 0 {
							return invalid()
						}
						totals[field].Add(totals[field], value)
					}
				}
			}
		}
		if !*page.HasMore {
			if cost {
				usd := new(big.Rat).Quo(cents, big.NewRat(100, 1))
				return subscriptionFallbackReadResult{Status: "available", Value: gin.H{"currency": "USD", "amount": subscriptionFallbackExactDecimal(usd)}}
			}
			values := map[string]json.Number{}
			for field, value := range totals {
				values[field] = json.Number(value.String())
			}
			return subscriptionFallbackReadResult{Status: "available", Value: values}
		}
		if page.NextPage == nil || *page.NextPage == "" || len(*page.NextPage) > 4096 || seen[*page.NextPage] {
			return invalid()
		}
		seen[*page.NextPage] = true
		query.Set("page", *page.NextPage)
	}
	return invalid()
}

// Decimal strings are cents, not dollars. Avoid binary floating-point rounding.
func subscriptionFallbackDecimal(s string) (*big.Rat, error) {
	if len(s) == 0 || len(s) > 256 {
		return nil, errors.New("invalid decimal")
	}
	dots, digits := 0, 0
	for _, ch := range s {
		if ch == '.' {
			dots++
			continue
		}
		if ch < '0' || ch > '9' {
			return nil, errors.New("invalid decimal")
		}
		digits++
	}
	if dots > 1 || digits == 0 || strings.HasPrefix(s, ".") || strings.HasSuffix(s, ".") {
		return nil, errors.New("invalid decimal")
	}
	value, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, errors.New("invalid decimal")
	}
	return value, nil
}

func subscriptionFallbackExactDecimal(value *big.Rat) string {
	// All inputs have at most 255 decimal places; division by 100 adds two.
	text := value.FloatString(258)
	text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	if !strings.Contains(text, ".") {
		text += ".00"
	} else if len(strings.SplitN(text, ".", 2)[1]) == 1 {
		text += "0"
	}
	return text
}

func (h *Handler) subscriptionFallbackWorkspaces(c *gin.Context, transport http.RoundTripper) {
	body, credential, ok := h.subscriptionFallbackReadCredential(c, transport)
	if !ok {
		return
	}
	result := fallbackReadUnavailable("unsupported", "No verified official workspace API is available for this provider.")
	if body.Provider == "claude" {
		result = credential.workspaces(c)
	}
	c.JSON(http.StatusOK, gin.H{"provider": body.Provider, "auth_index": body.AuthIndex, "workspaces": result})
}

func (credential subscriptionFallbackReadCredential) workspaces(c *gin.Context) subscriptionFallbackReadResult {
	type workspace struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	values := []workspace{}
	query := url.Values{"include_default": {"true"}, "limit": {"100"}}
	seen := map[string]bool{}
	invalid := func() subscriptionFallbackReadResult {
		return fallbackReadUnavailable("error", "Invalid or incomplete upstream workspace list.")
	}
	for count := 0; count < subscriptionFallbackMaxPages; count++ {
		page, result := credential.getPage(c, "/v1/organizations/workspaces", query)
		if result.Status != "available" {
			return result
		}
		var batch []workspace
		if json.Unmarshal(page.Data, &batch) != nil {
			return invalid()
		}
		for _, entry := range batch {
			if entry.ID == "" || strings.Contains(entry.ID, credential.key) || strings.Contains(entry.Name, credential.key) {
				return invalid()
			}
			values = append(values, entry)
		}
		if !*page.HasMore {
			return subscriptionFallbackReadResult{Status: "available", Value: values}
		}
		if len(batch) == 0 || page.LastID == nil || *page.LastID != batch[len(batch)-1].ID || len(*page.LastID) > 4096 || seen[*page.LastID] {
			return invalid()
		}
		seen[*page.LastID] = true
		query.Set("after_id", *page.LastID)
	}
	return invalid()
}
