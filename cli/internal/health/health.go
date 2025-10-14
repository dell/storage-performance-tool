package health

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

const runPath = "/run"

// joinURL safely concatenates a base URL and a path.
func joinURL(base, path string) string {
	switch {
	case base == "":
		return path
	case strings.HasSuffix(base, "/") && strings.HasPrefix(path, "/"):
		return base + path[1:]
	case strings.HasSuffix(base, "/") || strings.HasPrefix(path, "/"):
		return base + path
	default:
		return base + "/" + path
	}
}

// ProbeRun performs a HEAD request to the /run endpoint and returns the status code.
// Expected healthy statuses are 200 (active) or 204 (idle).
func ProbeRun(ctx context.Context, baseURL string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, joinURL(baseURL, runPath), nil)
	if err != nil {
		return 0, fmt.Errorf("creating HEAD /run: %w", err)
	}
	// Use a short-lived client; callers control timeout via ctx.
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("HEAD /run failed: %w", err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// WaitForRunReady polls HEAD /run until it returns 200 or 204, or ctx is done.
func WaitForRunReady(ctx context.Context, baseURL string, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = constants.APIPollingTimeout
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// Immediate attempt before waiting
	if code, err := ProbeRun(ctx, baseURL); err == nil && (code == http.StatusOK || code == http.StatusNoContent) {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for /run readiness: %w", ctx.Err())
		case <-ticker.C:
			code, err := ProbeRun(ctx, baseURL)
			if err == nil && (code == http.StatusOK || code == http.StatusNoContent) {
				return nil
			}
		}
	}
}

// ProbeMetrics fetches /metrics/json (optionally with verbose=1) and returns the body as a string.
func ProbeMetrics(ctx context.Context, baseURL string, verbose bool) (string, error) {
	path := constants.SptMetricsEndpoint
	if verbose {
		path = path + "?verbose=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, path), nil)
	if err != nil {
		return "", fmt.Errorf("creating GET %s: %w", path, err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s failed: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s returned status: %d", path, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading %s response: %w", path, err)
	}
	return string(b), nil
}
