// Package cachestats aggregates prompt-cache usage per credential and model so the
// management API can report cache hit rates. Records arrive through the usage
// plugin pipeline; counts are kept in hourly buckets and optionally persisted.
package cachestats

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// Retention is how long hourly buckets are kept.
const Retention = 8 * 24 * time.Hour

const fileVersion = 1

func init() {
	coreusage.RegisterNamedPlugin("cache-stats", defaultStore)
}

// Counts holds token and request totals for one aggregation key.
type Counts struct {
	Requests         int64 `json:"requests"`
	Failed           int64 `json:"failed"`
	InputTokens      int64 `json:"input_tokens"`
	CacheReadTokens  int64 `json:"cache_read_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
}

func (c *Counts) add(o Counts) {
	c.Requests += o.Requests
	c.Failed += o.Failed
	c.InputTokens += o.InputTokens
	c.CacheReadTokens += o.CacheReadTokens
	c.CacheWriteTokens += o.CacheWriteTokens
}

type bucketKey struct {
	Hour      int64  `json:"hour"`
	AuthID    string `json:"auth_id"`
	AuthIndex string `json:"auth_index,omitempty"`
	Provider  string `json:"provider,omitempty"`
	Model     string `json:"model"`
}

type persistedBucket struct {
	bucketKey
	Counts
}

type persistedFile struct {
	Version int               `json:"version"`
	Buckets []persistedBucket `json:"buckets"`
}

// Store aggregates usage records. The zero value is not usable; use NewStore.
type Store struct {
	mu      sync.Mutex
	buckets map[bucketKey]*Counts
	path    string
	dirty   bool
	now     func() time.Time

	flushOnce sync.Once
}

// NewStore returns an empty in-memory store.
func NewStore() *Store {
	return &Store{buckets: make(map[bucketKey]*Counts), now: time.Now}
}

var defaultStore = NewStore()

// Default returns the process-wide store fed by the usage pipeline.
func Default() *Store { return defaultStore }

// Configure sets the persistence file for the default store, loads existing
// buckets and starts periodic flushing. An empty path keeps stats in memory only.
func Configure(path string) {
	if err := defaultStore.SetPath(path); err != nil {
		log.WithError(err).Warn("cache stats: persistence disabled")
		return
	}
	defaultStore.startFlusher(time.Minute)
}

// SetPath loads buckets from path and remembers it for later saves.
func (s *Store) SetPath(path string) error {
	path = strings.TrimSpace(path)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.path = path
	if path == "" {
		return nil
	}
	if !filepath.IsAbs(path) {
		s.path = ""
		return errors.New("cache-stats-file must be an absolute path")
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var file persistedFile
	if err = json.Unmarshal(data, &file); err != nil {
		return err
	}
	if file.Version != fileVersion {
		return nil
	}
	for _, b := range file.Buckets {
		counts := b.Counts
		if existing, ok := s.buckets[b.bucketKey]; ok {
			existing.add(counts)
			continue
		}
		s.buckets[b.bucketKey] = &counts
	}
	s.pruneLocked()
	return nil
}

func (s *Store) startFlusher(interval time.Duration) {
	s.flushOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				if err := s.Flush(); err != nil {
					log.WithError(err).Warn("cache stats: flush failed")
				}
			}
		}()
	})
}

// Flush writes buckets to the configured file when they changed since the last write.
func (s *Store) Flush() error {
	s.mu.Lock()
	if s.path == "" || !s.dirty {
		s.mu.Unlock()
		return nil
	}
	s.pruneLocked()
	file := persistedFile{Version: fileVersion, Buckets: make([]persistedBucket, 0, len(s.buckets))}
	for key, counts := range s.buckets {
		file.Buckets = append(file.Buckets, persistedBucket{bucketKey: key, Counts: *counts})
	}
	path := s.path
	s.dirty = false
	s.mu.Unlock()

	data, err := json.Marshal(file)
	if err == nil {
		err = writeFileAtomic(path, data)
	}
	if err != nil {
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()
	}
	return err
}

func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if errClose := tmp.Close(); err == nil {
		err = errClose
	}
	if err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// HandleUsage implements coreusage.Plugin.
func (s *Store) HandleUsage(_ context.Context, record coreusage.Record) {
	s.Add(record)
}

// Add folds one usage record into its hourly bucket.
func (s *Store) Add(record coreusage.Record) {
	authID := strings.TrimSpace(record.AuthID)
	if authID == "" {
		return
	}
	at := record.RequestedAt
	if at.IsZero() {
		at = s.now()
	}
	model := strings.TrimSpace(record.Model)
	if model == "" {
		model = "unknown"
	}
	key := bucketKey{
		Hour:      at.UTC().Truncate(time.Hour).Unix(),
		AuthID:    authID,
		AuthIndex: strings.TrimSpace(record.AuthIndex),
		Provider:  strings.TrimSpace(record.Provider),
		Model:     model,
	}
	delta := countsFromDetail(record.Detail)
	delta.Requests = 1
	if record.Failed {
		delta.Failed = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if counts, ok := s.buckets[key]; ok {
		counts.add(delta)
	} else {
		s.buckets[key] = &delta
	}
	s.dirty = true
}

// countsFromDetail prefers the canonical breakdown, whose input buckets never overlap.
// Without it, InputTokens is treated as inclusive of cached tokens (Responses semantics).
func countsFromDetail(d coreusage.Detail) Counts {
	if b := d.TokenBreakdown; b.Valid() {
		return Counts{
			InputTokens:      b.Input.TotalTokens,
			CacheReadTokens:  b.Input.CacheReadTokens,
			CacheWriteTokens: b.Input.CacheWriteTokens,
		}
	}
	cacheRead := d.CacheReadTokens
	if d.CachedTokens > cacheRead {
		cacheRead = d.CachedTokens
	}
	input := d.InputTokens
	if cacheRead > input {
		input = cacheRead
	}
	return Counts{InputTokens: input, CacheReadTokens: cacheRead, CacheWriteTokens: d.CacheCreationTokens}
}

func (s *Store) pruneLocked() {
	cutoff := s.now().UTC().Add(-Retention).Truncate(time.Hour).Unix()
	for key := range s.buckets {
		if key.Hour < cutoff {
			delete(s.buckets, key)
			s.dirty = true
		}
	}
}

// Summary is one row of a snapshot.
type Summary struct {
	Counts
	// HitRate is cache_read_tokens / input_tokens, nil when there was no input.
	HitRate *float64 `json:"hit_rate"`
}

func summarize(c Counts) Summary {
	out := Summary{Counts: c}
	if c.InputTokens > 0 {
		rate := float64(c.CacheReadTokens) / float64(c.InputTokens)
		out.HitRate = &rate
	}
	return out
}

// CredentialSummary aggregates one credential over the window.
type CredentialSummary struct {
	AuthID    string `json:"auth_id"`
	AuthIndex string `json:"auth_index,omitempty"`
	Provider  string `json:"provider,omitempty"`
	Summary
	Models []ModelSummary `json:"models"`
}

// ModelSummary aggregates one model.
type ModelSummary struct {
	Model string `json:"model"`
	Summary
}

// Snapshot is the management API payload.
type Snapshot struct {
	WindowSeconds int64               `json:"window_seconds"`
	Since         time.Time           `json:"since"`
	GeneratedAt   time.Time           `json:"generated_at"`
	Total         Summary             `json:"total"`
	Credentials   []CredentialSummary `json:"credentials"`
	Models        []ModelSummary      `json:"models"`
}

// Snapshot sums buckets whose hour overlaps the trailing window.
func (s *Store) Snapshot(window time.Duration) Snapshot {
	now := s.now().UTC()
	since := now.Add(-window)
	firstHour := since.Truncate(time.Hour).Unix()

	type credAgg struct {
		index, provider string
		total           Counts
		models          map[string]*Counts
	}
	var total Counts
	creds := map[string]*credAgg{}
	models := map[string]*Counts{}

	s.mu.Lock()
	for key, counts := range s.buckets {
		if key.Hour < firstHour {
			continue
		}
		total.add(*counts)
		agg := creds[key.AuthID]
		if agg == nil {
			agg = &credAgg{models: map[string]*Counts{}}
			creds[key.AuthID] = agg
		}
		if key.AuthIndex != "" {
			agg.index = key.AuthIndex
		}
		if key.Provider != "" {
			agg.provider = key.Provider
		}
		agg.total.add(*counts)
		addTo(agg.models, key.Model, *counts)
		addTo(models, key.Model, *counts)
	}
	s.mu.Unlock()

	out := Snapshot{
		WindowSeconds: int64(window / time.Second),
		Since:         since,
		GeneratedAt:   now,
		Total:         summarize(total),
		Credentials:   make([]CredentialSummary, 0, len(creds)),
		Models:        modelSummaries(models),
	}
	for id, agg := range creds {
		out.Credentials = append(out.Credentials, CredentialSummary{
			AuthID: id, AuthIndex: agg.index, Provider: agg.provider,
			Summary: summarize(agg.total), Models: modelSummaries(agg.models),
		})
	}
	sort.Slice(out.Credentials, func(i, j int) bool { return out.Credentials[i].AuthID < out.Credentials[j].AuthID })
	return out
}

func addTo(m map[string]*Counts, key string, c Counts) {
	if existing, ok := m[key]; ok {
		existing.add(c)
		return
	}
	copied := c
	m[key] = &copied
}

func modelSummaries(m map[string]*Counts) []ModelSummary {
	out := make([]ModelSummary, 0, len(m))
	for model, counts := range m {
		out = append(out, ModelSummary{Model: model, Summary: summarize(*counts)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].InputTokens != out[j].InputTokens {
			return out[i].InputTokens > out[j].InputTokens
		}
		return out[i].Model < out[j].Model
	})
	return out
}
