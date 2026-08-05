package results

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// DiscoverStepIDs fetches /metrics/json and returns unique step IDs found.
// This endpoint returns node-local (non-distributed) metrics contexts, which
// may NOT include the correct step IDs in multi-host distributed runs.
// For distributed runs, prefer DiscoverFleetStepIDs.
func DiscoverStepIDs(baseURL string) ([]string, error) {
	return DiscoverStepIDsContext(context.Background(), baseURL)
}

// DiscoverStepIDsContext is DiscoverStepIDs with caller cancellation and deadlines.
func DiscoverStepIDsContext(ctx context.Context, baseURL string) ([]string, error) {
	return DiscoverStepIDsForRunContext(ctx, baseURL, 0)
}

// DiscoverStepIDsForRunContext returns node step IDs belonging to expectedRunID.
// A zero expectedRunID preserves the compatibility behavior of returning all runs.
func DiscoverStepIDsForRunContext(ctx context.Context, baseURL string, expectedRunID int64) ([]string, error) {
	return discoverStepIDsFromPath(ctx, baseURL, "/metrics/json", expectedRunID)
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
	return DiscoverFleetStepIDsContext(context.Background(), baseURL)
}

// DiscoverFleetStepIDsContext is DiscoverFleetStepIDs with caller cancellation and deadlines.
func DiscoverFleetStepIDsContext(ctx context.Context, baseURL string) ([]string, error) {
	return DiscoverFleetStepIDsForRunContext(ctx, baseURL, 0)
}

// DiscoverFleetStepIDsForRunContext returns fleet step IDs belonging to expectedRunID.
func DiscoverFleetStepIDsForRunContext(ctx context.Context, baseURL string, expectedRunID int64) ([]string, error) {
	return discoverStepIDsFromPath(ctx, baseURL, "/metrics/fleet/json", expectedRunID)
}

// discoverStepIDsFromPath is the shared implementation for both node and fleet
// step-ID discovery endpoints.
func discoverStepIDsFromPath(ctx context.Context, baseURL, metricsPath string, expectedRunID int64) ([]string, error) {
	client := &http.Client{Timeout: constants.ResultsDiscoveryHTTPTimeout}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, strings.TrimSuffix(baseURL, "/")+metricsPath, nil)
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", metricsPath, err)
	}
	resp, err := client.Do(req)
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
		StepID string          `json:"step_id"`
		RunID  json.RawMessage `json:"run_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		return nil, fmt.Errorf("decode %s: %w", metricsPath, err)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(steps))
	for _, s := range steps {
		if expectedRunID > 0 {
			runID, parseErr := parseMetricsRunID(s.RunID)
			if parseErr != nil || runID != expectedRunID {
				continue
			}
		}
		if s.StepID == "" || seen[s.StepID] {
			continue
		}
		seen[s.StepID] = true
		out = append(out, s.StepID)
	}
	sort.Strings(out)
	return out, nil
}

func parseMetricsRunID(raw json.RawMessage) (int64, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return 0, nil
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err == nil {
		return number.Int64()
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return 0, fmt.Errorf("decode run_id: %w", err)
	}
	return strconv.ParseInt(strings.TrimSpace(text), 10, 64)
}
