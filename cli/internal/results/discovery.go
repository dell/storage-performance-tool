package results

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// DiscoverStepIDs fetches /metrics/json and returns unique step IDs found.
// This endpoint returns node-local (non-distributed) metrics contexts, which
// may NOT include the correct step IDs in multi-host distributed runs.
// For distributed runs, prefer DiscoverFleetStepIDs.
func DiscoverStepIDs(baseURL string) ([]string, error) {
	return discoverStepIDsFromPath(baseURL, "/metrics/json")
}

// DiscoverFleetStepIDs fetches /metrics/fleet/json and returns unique step IDs
// from the fleet/cluster aggregated metrics. In distributed (multi-host) mode,
// this endpoint exposes DistributedMetricsContext entries that contain the
// engine's actual runtime step IDs — which may differ from the CLI-generated
// expected step IDs due to timestamp reassignment.
//
// Returns (nil, nil) if the fleet endpoint is unavailable (404) so callers can
// fall back gracefully.
func DiscoverFleetStepIDs(baseURL string) ([]string, error) {
	return discoverStepIDsFromPath(baseURL, "/metrics/fleet/json")
}

// discoverStepIDsFromPath is the shared implementation for both node and fleet
// step-ID discovery endpoints.
func discoverStepIDsFromPath(baseURL, metricsPath string) ([]string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(strings.TrimSuffix(baseURL, "/") + metricsPath)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", metricsPath, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		// Fleet endpoint may not be available on all engine versions.
		return nil, nil
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s status: %d", metricsPath, resp.StatusCode)
	}
	var steps []struct {
		StepID string `json:"step_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		return nil, fmt.Errorf("decode %s: %w", metricsPath, err)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(steps))
	for _, s := range steps {
		if s.StepID == "" || seen[s.StepID] {
			continue
		}
		seen[s.StepID] = true
		out = append(out, s.StepID)
	}
	sort.Strings(out)
	return out, nil
}
