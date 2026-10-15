package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	bolt "go.etcd.io/bbolt"
)

const ConversationIDHeader = "X-CLIProxy-Conversation-ID"
const strictConversationMetadataKey = "strict-conversation-id"

var conversationBucket = []byte("conversation-bindings-v1")

type conversationCallerKey struct{}
type strictPickInProgressKey struct{}

// StrictConversationEnabled reports whether strict routing is active for this manager.
func (m *Manager) StrictConversationEnabled() bool {
	return m != nil && m.runtimeConfig.Load().(*internalconfig.Config).Routing.StrictConversationAffinity
}

func (m *Manager) strictEnabled(ctx context.Context) bool {
	if ctx != nil && ctx.Value(strictPickInProgressKey{}) != nil {
		return false
	}
	return m.runtimeConfig.Load().(*internalconfig.Config).Routing.StrictConversationAffinity
}

func strictPickContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, strictPickInProgressKey{}, true)
}

// WithConversationCaller supplies a trusted caller identity when no authenticated API key is present.
func WithConversationCaller(ctx context.Context, caller string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, conversationCallerKey{}, caller)
}

// InitializeConversationBindings explicitly creates a new, otherwise empty binding database.
// It refuses to replace any existing file, including an invalid database.
func InitializeConversationBindings(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("conversation bindings path must be absolute")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("initialize conversation bindings: %w", err)
	}
	if err = f.Close(); err != nil {
		return err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 30 * time.Second})
	if err != nil {
		return fmt.Errorf("initialize conversation bindings: %w", err)
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error { _, errCreate := tx.CreateBucket(conversationBucket); return errCreate })
}

func strictConversationError(code string, status int) error {
	return &Error{Code: code, Message: code, HTTPStatus: status}
}

// strictConversationID accepts only explicit identities observed on stock clients.
// Request IDs, prompt hashes and connection IDs are not conversation identities.
func strictConversationID(opts cliproxyexecutor.Options) (string, error) {
	if id, ok := opts.Metadata[strictConversationMetadataKey].(string); ok {
		return validateStrictIdentity([]string{id}, "")
	}
	if values, present := strictHeaderValues(opts.Headers, ConversationIDHeader); present {
		return validateStrictIdentity(values, "")
	}
	var identity string
	for _, source := range []struct {
		prefix string
		names  []string
	}{
		{"hermes:", []string{"X-Hermes-Session-ID"}},
		{"claude:", []string{"X-Claude-Code-Session-Id"}},
		{"codex:", []string{"Session-Id", "Session_id"}},
	} {
		var sourceID string
		for _, name := range source.names {
			values, present := strictHeaderValues(opts.Headers, name)
			if !present {
				continue
			}
			id, err := validateStrictIdentity(values, source.prefix)
			if err != nil {
				return "", err
			}
			if sourceID != "" && sourceID != id {
				return "", strictConversationError("conversation_id_conflict", http.StatusBadRequest)
			}
			sourceID = id
		}
		if sourceID != "" {
			if identity != "" {
				return "", strictConversationError("conversation_id_conflict", http.StatusBadRequest)
			}
			identity = sourceID
		}
	}
	if identity == "" {
		return "", strictConversationError("conversation_id_required", http.StatusBadRequest)
	}
	return identity, nil
}

func strictHeaderValues(headers http.Header, name string) ([]string, bool) {
	var values []string
	present := false
	for key, entries := range headers {
		if strings.EqualFold(key, name) {
			present = true
			values = append(values, entries...)
		}
	}
	return values, present
}

func validateStrictIdentity(values []string, prefix string) (string, error) {
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" || len(prefix)+len(values[0]) > 256 || strings.ContainsAny(values[0], ",\r\n\x00") || values[0] != strings.TrimSpace(values[0]) {
		return "", strictConversationError("conversation_id_required", http.StatusBadRequest)
	}
	return prefix + values[0], nil
}

func strictCaller(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if caller, ok := ctx.Value(conversationCallerKey{}).(string); ok && strings.TrimSpace(caller) != "" {
		return caller
	}
	return coreusage.APIKeyFromContext(ctx)
}

// prepareStrictConversation removes the private identity before it reaches an upstream executor.
func (m *Manager) prepareStrictConversation(ctx context.Context, opts cliproxyexecutor.Options) (cliproxyexecutor.Options, error) {
	cfg := m.runtimeConfig.Load().(*internalconfig.Config)
	if !cfg.Routing.StrictConversationAffinity {
		return opts, nil
	}
	if m.HomeEnabled() || m.hasPluginScheduler() {
		return opts, strictConversationError("strict_routing_unsupported", http.StatusServiceUnavailable)
	}
	id, err := strictConversationID(opts)
	if err != nil {
		return opts, err
	}
	opts.EnsureMetadata()
	opts.Metadata[strictConversationMetadataKey] = id
	opts.Headers = opts.Headers.Clone()
	for key := range opts.Headers {
		if strings.EqualFold(key, ConversationIDHeader) || strings.EqualFold(key, "X-Hermes-Session-ID") {
			delete(opts.Headers, key)
		}
	}
	return opts, nil
}

func (m *Manager) strictPick(ctx context.Context, opts cliproxyexecutor.Options, pick func(cliproxyexecutor.Options) (*Auth, ProviderExecutor, string, error)) (*Auth, ProviderExecutor, string, error) {
	cfg := m.runtimeConfig.Load().(*internalconfig.Config)
	if !cfg.Routing.StrictConversationAffinity {
		return pick(opts)
	}
	if m.HomeEnabled() || m.hasPluginScheduler() {
		return nil, nil, "", strictConversationError("strict_routing_unsupported", http.StatusServiceUnavailable)
	}
	id, err := strictConversationID(opts)
	if err != nil {
		return nil, nil, "", err
	}
	caller := strictCaller(ctx)
	if caller == "" {
		return nil, nil, "", strictConversationError("conversation_caller_required", http.StatusUnauthorized)
	}
	path := cfg.Routing.ConversationBindingsFile
	if !filepath.IsAbs(path) {
		return nil, nil, "", strictConversationError("conversation_storage_unavailable", http.StatusServiceUnavailable)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, nil, "", strictConversationError("conversation_storage_unavailable", http.StatusServiceUnavailable)
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 30 * time.Second})
	if err != nil {
		return nil, nil, "", strictConversationError("conversation_storage_unavailable", http.StatusServiceUnavailable)
	}
	defer db.Close()
	digest := sha256.Sum256([]byte(caller + "\x00" + id))
	var auth *Auth
	var executor ProviderExecutor
	var provider string
	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(conversationBucket)
		if bucket == nil {
			return strictConversationError("conversation_storage_unavailable", http.StatusServiceUnavailable)
		}
		if bound := bucket.Get(digest[:]); bound != nil {
			opts.EnsureMetadata()
			opts.Metadata = maps.Clone(opts.Metadata)
			opts.Metadata[cliproxyexecutor.PinnedAuthMetadataKey] = string(bound)
		}
		var errPick error
		auth, executor, provider, errPick = pick(opts)
		if errPick != nil {
			return errPick
		}
		if auth == nil || auth.ID == "" {
			return strictConversationError("auth_not_found", http.StatusServiceUnavailable)
		}
		if bound := bucket.Get(digest[:]); bound != nil {
			if string(bound) != auth.ID {
				return strictConversationError("conversation_account_unavailable", http.StatusServiceUnavailable)
			}
			return nil
		}
		return bucket.Put(digest[:], []byte(auth.ID))
	})
	if err != nil {
		return nil, nil, "", err
	}
	return auth, executor, provider, nil
}
