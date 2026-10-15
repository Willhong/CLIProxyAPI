package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Both upstream accounts are synthetic local servers; these tests exercise the real
// HTTP handler, auth manager, Gemini executor, and persisted binding database.
func TestStrictConversationRealHandlerFakeUpstream(t *testing.T) {
	for _, header := range []string{coreauth.ConversationIDHeader, "Session-Id", "X-Claude-Code-Session-Id", "X-Hermes-Session-ID"} {
		t.Run(header, func(t *testing.T) { testStrictHTTPRouting(t, header) })
	}
}

func testStrictHTTPRouting(t *testing.T, identityHeader string) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := coreauth.InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	hits := map[string]int{}
	status := http.StatusOK
	upstream := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			hits[id]++
			currentStatus := status
			mu.Unlock()
			if r.Header.Get(coreauth.ConversationIDHeader) != "" {
				t.Error("private conversation header reached upstream")
			}
			if currentStatus != http.StatusOK {
				w.WriteHeader(currentStatus)
				_, _ = fmt.Fprint(w, `{"error":{"message":"synthetic upstream failure"}}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hello"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2},"responseId":"fake"}`))
		}))
	}
	first := upstream("a")
	defer first.Close()
	second := upstream("b")
	defer second.Close()
	cfg := &config.Config{}
	cfg.Routing.StrictConversationAffinity = true
	cfg.Routing.ConversationBindingsFile = path
	const model = "strict-fake-gemini-model"
	makeRouter := func() *gin.Engine {
		m := coreauth.NewManager(nil, nil, nil)
		m.SetConfig(cfg)
		m.RegisterExecutor(runtimeexecutor.NewGeminiExecutor(cfg))
		for id, url := range map[string]string{"strict-fake-a": first.URL, "strict-fake-b": second.URL} {
			if _, err := m.Register(context.Background(), &coreauth.Auth{ID: id, Provider: "gemini", Status: coreauth.StatusActive, ProxyURL: "direct", Attributes: map[string]string{"api_key": "synthetic-token", "base_url": url}}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(id, "gemini", []*registry.ModelInfo{{ID: model}})
		}
		router := gin.New()
		router.Use(func(c *gin.Context) { c.Set("userApiKey", "synthetic-caller"); c.Next() })
		router.POST("/v1/responses", NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, m)).Responses)
		return router
	}
	defer registry.GetGlobalRegistry().UnregisterClient("strict-fake-a")
	defer registry.GetGlobalRegistry().UnregisterClient("strict-fake-b")
	router := makeRouter()
	call := func(id string, stream bool, duplicate bool) *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{"model":%q,"input":"hello","stream":%t}`, model, stream)
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if id != "" {
			req.Header.Set(identityHeader, id)
		}
		if duplicate {
			req.Header.Add(identityHeader, "different-thread")
		}
		result := httptest.NewRecorder()
		router.ServeHTTP(result, req)
		return result
	}
	counts := func() (int, int) { mu.Lock(); defer mu.Unlock(); return hits["a"], hits["b"] }
	assertNoDispatch := func(label string, beforeA, beforeB int) {
		t.Helper()
		a, b := counts()
		if a != beforeA || b != beforeB {
			t.Fatalf("%s dispatched another account: before=(%d,%d) after=(%d,%d)", label, beforeA, beforeB, a, b)
		}
	}
	if got := call("", false, false); got.Code < 400 {
		t.Fatalf("missing ID accepted: %d %s", got.Code, got.Body)
	}
	if got := call("thread-1", false, true); got.Code < 400 {
		t.Fatalf("duplicate ID accepted: %d %s", got.Code, got.Body)
	}
	assertNoDispatch("missing/duplicate ID", 0, 0)
	for _, stream := range []bool{false, true} {
		got := call("thread-1", stream, false)
		if got.Code != 200 {
			t.Fatalf("stream=%t status=%d body=%s", stream, got.Code, got.Body)
		}
		if got.Body.Len() == 0 {
			t.Fatalf("stream=%t returned empty response", stream)
		}
	}
	a, b := counts()
	if (a == 0) == (b == 0) {
		t.Fatalf("same conversation rotated upstreams: a=%d b=%d", a, b)
	}
	// A new manager has no in-memory affinity. Its selection must recover the same
	// account from the shared database, including after a failed upstream attempt.
	router = makeRouter()
	if got := call("thread-1", false, false); got.Code != 200 {
		t.Fatalf("new manager status=%d body=%s", got.Code, got.Body)
	}
	afterA, afterB := counts()
	if (a > 0 && (afterA != a+1 || afterB != 0)) || (b > 0 && (afterB != b+1 || afterA != 0)) {
		t.Fatalf("manager restart switched accounts: before=(%d,%d) after=(%d,%d)", a, b, afterA, afterB)
	}
	// 429 runs last: it moves the conversation to the other account, which the
	// fail-closed checks for the other statuses assume has not happened yet.
	for _, failure := range []int{401, 403, 500, 429} {
		t.Run(fmt.Sprintf("upstream_%d", failure), func(t *testing.T) {
			mu.Lock()
			status = failure
			mu.Unlock()
			router = makeRouter() // Fresh runtime state; keep the persisted binding.
			beforeA, beforeB := counts()
			got := call("thread-1", false, false)
			if got.Code == 200 || !strings.Contains(got.Body.String(), "synthetic upstream failure") {
				t.Fatalf("upstream %d masked: status=%d body=%s", failure, got.Code, got.Body)
			}
			nextA, nextB := counts()
			if failure == http.StatusTooManyRequests {
				// Quota exhaustion moves the conversation: both accounts were tried once.
				if nextA != beforeA+1 || nextB != beforeB+1 {
					t.Fatalf("429 did not move to the other account: before=(%d,%d) after=(%d,%d)", beforeA, beforeB, nextA, nextB)
				}
				return
			}
			if nextA+nextB != beforeA+beforeB+1 || (beforeA > 0 && nextB != beforeB) || (beforeB > 0 && nextA != beforeA) {
				t.Fatalf("failure %d retried/fell back: before=(%d,%d) after=(%d,%d)", failure, beforeA, beforeB, nextA, nextB)
			}
			router = makeRouter()
			beforeA, beforeB = counts()
			streamed := call("thread-1", true, false)
			if streamed.Code == 200 && !strings.Contains(streamed.Body.String(), "error") {
				t.Fatalf("SSE upstream %d masked: status=%d body=%s", failure, streamed.Code, streamed.Body)
			}
			nextA, nextB = counts()
			if nextA+nextB > beforeA+beforeB+1 || (beforeA > 0 && nextB != beforeB) || (beforeB > 0 && nextA != beforeA) {
				t.Fatalf("SSE failure %d retried/fell back: before=(%d,%d) after=(%d,%d)", failure, beforeA, beforeB, nextA, nextB)
			}
		})
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	beforeA, beforeB := counts()
	if got := call("thread-1", false, false); got.Code < 400 {
		t.Fatalf("lost binding database accepted: %d %s", got.Code, got.Body)
	}
	assertNoDispatch("missing database", beforeA, beforeB)
	t.Logf("synthetic upstream HTTP/SSE, restart, failures: a=%d b=%d", beforeA, beforeB)
}
