package management

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/cachestats"
)

// GetCacheStats reports prompt-cache hit rates per credential and model over a trailing window.
// Accepts ?window=7d or a Go duration such as 1h or 24h, up to the retention period; default 24h.
func (h *Handler) GetCacheStats(c *gin.Context) {
	window, ok := parseCacheStatsWindow(c.Query("window"))
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "window must be a positive duration of at most 8 days, e.g. 1h, 24h or 7d"})
		return
	}
	c.JSON(http.StatusOK, cachestats.Default().Snapshot(window))
}

func parseCacheStatsWindow(raw string) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 24 * time.Hour, true
	}
	var window time.Duration
	if days, found := strings.CutSuffix(raw, "d"); found {
		n, err := strconv.Atoi(days)
		if err != nil {
			return 0, false
		}
		window = time.Duration(n) * 24 * time.Hour
	} else {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return 0, false
		}
		window = parsed
	}
	return window, window > 0 && window <= cachestats.Retention
}
