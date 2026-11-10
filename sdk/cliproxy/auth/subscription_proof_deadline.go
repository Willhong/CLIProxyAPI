package auth

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// subscriptionSpendingDeadline must not read scheduler deadlines: those contain
// jitter, cooldown floors and observations from earlier responses.
func subscriptionSpendingDeadline(provider string, result Result, headers http.Header, now time.Time) time.Time {
	switch canonicalSchedulingProvider(provider) {
	case "claude":
		var latest time.Time
		supplied := false
		for _, window := range []string{"5h", "7d"} {
			status := headers.Values("Anthropic-Ratelimit-Unified-" + window + "-Status")
			if len(status) != 1 || strings.TrimSpace(status[0]) != "rejected" {
				continue
			}
			values := headers.Values("Anthropic-Ratelimit-Unified-" + window + "-Reset")
			if len(values) == 0 {
				continue
			}
			supplied = true
			if len(values) != 1 {
				continue
			}
			deadline := subscriptionRawReset(values[0])
			if deadline.After(now) && deadline.After(latest) {
				latest = deadline
			}
		}
		// Retry-After is raw response evidence; Result.RetryAfter may already have
		// Claude's random 1..30-second scheduling fuzz added by the executor.
		if values := headers.Values("Retry-After"); len(values) > 0 {
			supplied = true
			if len(values) == 1 {
				deadline := subscriptionRawRetryAfter(values[0], now)
				if deadline.After(now) && deadline.After(latest) {
					latest = deadline
				}
			}
		}
		if supplied {
			return latest
		}
	case "codex":
		var body struct {
			Error map[string]json.RawMessage `json:"error"`
		}
		if json.Unmarshal([]byte(result.Error.Message), &body) != nil {
			return time.Time{}
		}
		// Absolute reset is authoritative when supplied. An expired/malformed reset
		// cannot be replaced by a longer scheduler hint or relative reset.
		if raw, ok := body.Error["resets_at"]; ok {
			seconds, valid := subscriptionJSONSeconds(raw)
			if !valid {
				return time.Time{}
			}
			return time.Unix(seconds, 0)
		}
		if raw, ok := body.Error["resets_in_seconds"]; ok {
			seconds, valid := subscriptionJSONSeconds(raw)
			if !valid {
				return time.Time{}
			}
			return subscriptionSecondsAfter(seconds, now)
		}
		// Codex's duration hint has no Claude scheduling fuzz. Preserve existing
		// explicit exhaustion behavior when the response omits reset fields.
		if result.RetryAfter != nil && *result.RetryAfter > 0 {
			return now.Add(*result.RetryAfter)
		}
	}
	// Explicit exhaustion without any raw deadline remains useful only for the
	// immediate retry. Bound that evidence by the initial quota backoff, never by
	// growing scheduler backoff, cooldown floors, or executor fuzz.
	if result.RetryAfter != nil && *result.RetryAfter <= 0 {
		return time.Time{}
	}
	return now.Add(quotaBackoffBase)
}

func subscriptionJSONSeconds(raw json.RawMessage) (int64, bool) {
	seconds, err := strconv.ParseInt(string(raw), 10, 64)
	return seconds, err == nil && seconds > 0
}

func subscriptionSecondsAfter(seconds int64, now time.Time) time.Time {
	if seconds <= 0 || seconds > int64((1<<63-1)/time.Second) {
		return time.Time{}
	}
	return now.Add(time.Duration(seconds) * time.Second)
}

func subscriptionRawReset(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if seconds <= 0 {
			return time.Time{}
		}
		return time.Unix(seconds, 0)
	}
	deadline, _ := time.Parse(time.RFC3339Nano, raw)
	return deadline
}

func subscriptionRawRetryAfter(raw string, now time.Time) time.Time {
	raw = strings.TrimSpace(raw)
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return subscriptionSecondsAfter(seconds, now)
	}
	deadline, _ := http.ParseTime(raw)
	return deadline
}
