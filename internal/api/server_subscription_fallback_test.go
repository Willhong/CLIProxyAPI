package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/handlers/management"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestSubscriptionFallbackManagementRoutesRequireAuthorization(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{RemoteManagement: config.RemoteManagement{SecretKey: "configured-synthetic-hash"}, CodexKey: []config.CodexKey{{APIKey: "synthetic-route-key", SubscriptionFallback: &config.SubscriptionFallbackConfig{AuthID: "route-oauth"}}}}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: "route-oauth", Provider: "codex", Metadata: map[string]any{"access_token": "synthetic-route-oauth"}}); err != nil {
		t.Fatal(err)
	}
	auths, err := synthesizer.NewConfigSynthesizer().Synthesize(&synthesizer.SynthesisContext{Config: cfg, Now: time.Unix(0, 0), IDGenerator: synthesizer.NewStableIDGenerator()})
	if err != nil || len(auths) != 1 {
		t.Fatal("invalid synthetic route fixture")
	}
	selected := auths[0]
	if _, err := manager.Register(context.Background(), selected); err != nil {
		t.Fatal(err)
	}
	h := management.NewHandler(cfg, "", manager)
	h.SetLocalPassword("synthetic-management-password")
	s := &Server{cfg: cfg, engine: gin.New(), mgmt: h}
	s.managementRoutesEnabled.Store(true)
	s.registerManagementV8Routes()
	for _, path := range []string{"usage", "workspaces"} {
		for _, authorized := range []bool{false, true} {
			req := httptest.NewRequest(http.MethodPost, "/v8/management/credentials/subscription-fallback/"+path, strings.NewReader(`{"provider":"claude","auth_index":"synthetic-unknown-index"}`))
			req.RemoteAddr = "127.0.0.1:1234"
			req.Header.Set("Content-Type", "application/json")
			want := http.StatusUnauthorized
			if authorized {
				req.Header.Set("Authorization", "Bearer synthetic-management-password")
				want = http.StatusBadRequest
			}
			res := httptest.NewRecorder()
			s.engine.ServeHTTP(res, req)
			if res.Code != want {
				t.Fatalf("%s authorized=%v: got %d want %d: %s", path, authorized, res.Code, want, res.Body.String())
			}
		}
		for _, legacy := range []bool{false, true} {
			body := `{"provider":"codex","auth_index":"` + selected.EnsureIndex() + `"}`
			want := http.StatusOK
			if legacy {
				body = `{"provider":"codex","config_index":0}`
				want = http.StatusBadRequest
			}
			req := httptest.NewRequest(http.MethodPost, "/v8/management/credentials/subscription-fallback/"+path, strings.NewReader(body))
			req.RemoteAddr = "127.0.0.1:1234"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer synthetic-management-password")
			res := httptest.NewRecorder()
			s.engine.ServeHTTP(res, req)
			if res.Code != want {
				t.Fatalf("%s legacy=%v got=%d want=%d", path, legacy, res.Code, want)
			}
			if !legacy {
				var response map[string]any
				if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response["auth_index"] != selected.EnsureIndex() || response["provider"] != "codex" {
					t.Fatal("selector response contract mismatch")
				}
				if _, exists := response["config_index"]; exists {
					t.Fatal("legacy selector exposed")
				}
			}
			if strings.Contains(res.Body.String(), "synthetic-route-key") || strings.Contains(res.Body.String(), "synthetic-route-oauth") {
				t.Fatal("route exposed secret")
			}
		}
	}
}
