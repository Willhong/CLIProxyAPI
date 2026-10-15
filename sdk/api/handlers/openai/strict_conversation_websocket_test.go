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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

// The accounts below are synthetic local WebSocket servers. Routing uses the
// production Responses handler, manager, CodexAutoExecutor, and Bolt database.
func TestStrictConversationRealWebsocketRouting(t *testing.T) {
	for _, header := range []string{coreauth.ConversationIDHeader, "Session-Id"} {
		t.Run(header, func(t *testing.T) { testStrictWebsocketRouting(t, header) })
	}
}

func testStrictWebsocketRouting(t *testing.T, identityHeader string) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := coreauth.InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	hits := map[string]int{}
	bootstrapStatus := 0
	upstream := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			hits[id]++
			failure := bootstrapStatus
			mu.Unlock()
			if r.Header.Get(coreauth.ConversationIDHeader) != "" {
				t.Error("private conversation ID leaked upstream")
			}
			if failure != 0 {
				w.WriteHeader(failure)
				_, _ = fmt.Fprint(w, `{"error":{"message":"synthetic websocket bootstrap failure"}}`)
				return
			}
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, payload, err := conn.ReadMessage()
			if err != nil {
				t.Errorf("synthetic account %s read: %v", id, err)
				return
			}
			if gjson.GetBytes(payload, "type").String() != "response.create" {
				t.Errorf("unexpected upstream frame: %s", payload)
				return
			}
			for _, event := range []string{
				`{"type":"response.created","response":{"id":"synthetic","output":[]}}`,
				fmt.Sprintf(`{"type":"response.completed","response":{"id":"synthetic","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}]}}`, id),
			} {
				if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
					t.Errorf("synthetic account %s write: %v", id, err)
					return
				}
			}
			_, _, _ = conn.ReadMessage()
		}))
	}
	first := upstream("a")
	defer first.Close()
	second := upstream("b")
	defer second.Close()
	cfg := &config.Config{}
	cfg.Routing.StrictConversationAffinity = true
	cfg.Routing.ConversationBindingsFile = path
	const model = "strict-fake-codex-ws"
	makeServer := func() *httptest.Server {
		manager := coreauth.NewManager(nil, nil, nil)
		manager.SetConfig(cfg)
		manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
		for id, url := range map[string]string{"strict-ws-a": first.URL, "strict-ws-b": second.URL} {
			if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"api_key": "synthetic-token", "base_url": url, "websockets": "true"}}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}})
		}
		router := gin.New()
		router.Use(func(c *gin.Context) { c.Set("userApiKey", "synthetic-caller"); c.Next() })
		router.GET("/v1/responses", NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager)).ResponsesWebsocket)
		return httptest.NewServer(router)
	}
	defer registry.GetGlobalRegistry().UnregisterClient("strict-ws-a")
	defer registry.GetGlobalRegistry().UnregisterClient("strict-ws-b")
	server := makeServer()
	defer server.Close()
	counts := func() (int, int) { mu.Lock(); defer mu.Unlock(); return hits["a"], hits["b"] }
	call := func(id, nativeSession string, duplicate bool) (string, error) {
		headers := http.Header{}
		if id != "" {
			headers.Set(identityHeader, id)
		}
		if duplicate {
			headers.Add(identityHeader, "another")
		}
		if identityHeader == coreauth.ConversationIDHeader && id != "" {
			headers.Set("Session_id", nativeSession)
		} else {
			headers.Set("X-Client-Request-Id", nativeSession)
		}
		conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", headers)
		if err != nil {
			return "", fmt.Errorf("websocket dial: %w (response=%v)", err, response)
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
			return "", err
		}
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return "", err
			}
			switch gjson.GetBytes(payload, "type").String() {
			case "error":
				return "error: " + string(payload), nil
			case "response.completed":
				return gjson.GetBytes(payload, "response.output.0.content.0.text").String(), nil
			}
		}
	}
	for _, tc := range []struct {
		id, session string
		duplicate   bool
	}{{"", "native-missing", false}, {"thread-ws", "native-duplicate", true}} {
		result, err := call(tc.id, tc.session, tc.duplicate)
		if err != nil || !strings.Contains(result, `"status":400`) || !strings.Contains(result, "conversation_id_required") {
			t.Fatalf("invalid ID response: %s err=%v", result, err)
		}
	}
	if a, b := counts(); a+b != 0 {
		t.Fatalf("invalid ID reached upstreams: a=%d b=%d", a, b)
	}
	var bound string
	for _, session := range []string{"native-session-1", "native-session-2"} {
		result, err := call("thread-ws", session, false)
		if err != nil || (result != "a" && result != "b") {
			t.Fatalf("native session %s: result=%s err=%v", session, result, err)
		}
		if bound != "" && result != bound {
			t.Fatalf("changed accounts across socket reconnect: %s -> %s", bound, result)
		}
		bound = result
	}
	server.Close()
	server = makeServer()
	result, err := call("thread-ws", "native-session-after-manager-restart", false)
	if err != nil || result != bound {
		t.Fatalf("new manager changed account: bound=%s result=%s err=%v", bound, result, err)
	}
	// 429 runs last: it moves the conversation to the other account, which the
	// fail-closed checks for the other statuses assume has not happened yet.
	for _, failure := range []int{401, 403, 500, 429} {
		t.Run(fmt.Sprintf("bootstrap_%d", failure), func(t *testing.T) {
			mu.Lock()
			bootstrapStatus = failure
			mu.Unlock()
			server.Close()
			server = makeServer() // Fresh runtime state; keep the persisted binding.
			beforeA, beforeB := counts()
			result, err := call("thread-ws", fmt.Sprintf("native-failure-%d", failure), false)
			if result == "a" || result == "b" || (result == "" && err == nil) {
				t.Fatalf("bootstrap %d unexpectedly accepted: result=%s err=%v", failure, result, err)
			}
			if err != nil {
				t.Fatalf("strict bootstrap %d must send an error frame: %v", failure, err)
			} else if !strings.Contains(result, "synthetic websocket bootstrap failure") {
				t.Fatalf("bootstrap %d wrong error frame: %s", failure, result)
			}
			a, b := counts()
			if failure == http.StatusTooManyRequests {
				// Quota exhaustion moves the conversation: both accounts were tried once.
				if a != beforeA+1 || b != beforeB+1 {
					t.Fatalf("bootstrap 429 did not move to the other account: before=(%d,%d) after=(%d,%d)", beforeA, beforeB, a, b)
				}
				return
			}
			if a+b > beforeA+beforeB+1 || (bound == "a" && b != beforeB) || (bound == "b" && a != beforeA) {
				t.Fatalf("bootstrap %d retried/fell back: before=(%d,%d) after=(%d,%d)", failure, beforeA, beforeB, a, b)
			}
		})
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	beforeA, beforeB := counts()
	result, err = call("thread-ws", "native-storage-lost", false)
	if err == nil && !strings.Contains(result, "conversation_storage_unavailable") {
		t.Fatalf("storage loss unexpectedly accepted: %s", result)
	}
	if err != nil {
		t.Fatalf("strict storage loss must send an error frame: %v", err)
	}
	lostA, lostB := counts()
	if lostA != beforeA || lostB != beforeB {
		t.Fatalf("storage loss reached upstream: before=(%d,%d) after=(%d,%d)", beforeA, beforeB, lostA, lostB)
	}
	a, b := counts()
	t.Logf("synthetic websocket hits after reconnect, manager restart, bootstrap failures: a=%d b=%d", a, b)
}
