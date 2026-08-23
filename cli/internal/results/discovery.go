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
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
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

type terminalDeleteMetricsStep struct {
	MetricsSchema        int                    `json:"metrics_schema"`
	Scope                string                 `json:"scope"`
	Role                 string                 `json:"role"`
	ClusterID            string                 `json:"cluster_id"`
	RunID                json.RawMessage        `json:"run_id"`
	StepID               string                 `json:"step_id"`
	OpType               string                 `json:"op_type"`
	Terminal             bool                   `json:"terminal"`
	Partial              bool                   `json:"partial"`
	NodesCount           int                    `json:"nodes_count"`
	ContributorsPresent  []string               `json:"contributors_present"`
	DeleteDetailExpected bool                   `json:"delete_detail_expected"`
	Delete               *deletemetrics.Metrics `json:"delete"`
}

// CaptureTerminalDeleteMetricsForRunContext captures the additive terminal v4 model used by
// stored summaries. expectedContributorIDs must contain the exact engine contributor identities
// ("local" plus every remote node address). Fleet metrics are authoritative when that endpoint
// exists; node metrics are only a single-node compatibility fallback when fleet metrics are
// unavailable.
func CaptureTerminalDeleteMetricsForRunContext(
	ctx context.Context,
	baseURL string,
	expectedRunID int64,
	expectedStepIDs []string,
	expectedContributorIDs []string,
	allowNodeFallback bool,
) (map[string]*deletemetrics.Metrics, error) {
	if expectedRunID <= 0 {
		return nil, fmt.Errorf("capture terminal DELETE metrics: expected run identity is unavailable")
	}
	expectedSteps, err := exactStepSet(expectedStepIDs)
	if err != nil {
		return nil, fmt.Errorf("capture terminal DELETE metrics: %w", err)
	}
	expectedContributors, err := exactContributorSet(expectedContributorIDs)
	if err != nil {
		return nil, fmt.Errorf("capture terminal DELETE metrics: %w", err)
	}
	expectedClusterID := constants.RunClusterID(expectedRunID)
	fleet, available, err := captureTerminalDeleteMetricsFromPath(
		ctx, baseURL, constants.SptFleetMetricsEndpoint, "fleet", expectedRunID, expectedClusterID,
		expectedSteps, expectedContributors)
	if err != nil {
		return nil, err
	}
	if available {
		return fleet, nil
	}
	if !allowNodeFallback {
		return nil, fmt.Errorf("capture terminal DELETE metrics: authoritative fleet endpoint is unavailable")
	}
	if len(expectedContributors) != 1 {
		return nil, fmt.Errorf("capture terminal DELETE metrics: node fallback requires exactly one expected contributor")
	}
	if _, local := expectedContributors["local"]; !local {
		return nil, fmt.Errorf("capture terminal DELETE metrics: node fallback requires the local contributor identity")
	}
	node, available, err := captureTerminalDeleteMetricsFromPath(
		ctx, baseURL, constants.SptMetricsEndpoint, "node", expectedRunID, expectedClusterID,
		expectedSteps, expectedContributors)
	if err != nil {
		return nil, err
	}
	if !available {
		return nil, fmt.Errorf("capture terminal DELETE metrics: node endpoint is unavailable")
	}
	return node, nil
}

func exactContributorSet(contributorIDs []string) (map[string]struct{}, error) {
	expected := make(map[string]struct{}, len(contributorIDs))
	for _, contributorID := range contributorIDs {
		contributorID = strings.TrimSpace(contributorID)
		if contributorID == "" {
			return nil, fmt.Errorf("expected DELETE contributor identity is empty")
		}
		if _, duplicate := expected[contributorID]; duplicate {
			return nil, fmt.Errorf("expected DELETE contributor identity %q is duplicated", contributorID)
		}
		expected[contributorID] = struct{}{}
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("expected DELETE contributor identity is unavailable")
	}
	return expected, nil
}

func exactStepSet(stepIDs []string) (map[string]struct{}, error) {
	expected := make(map[string]struct{}, len(stepIDs))
	for _, stepID := range stepIDs {
		stepID = strings.TrimSpace(stepID)
		if stepID == "" {
			return nil, fmt.Errorf("expected DELETE step identity is empty")
		}
		if _, duplicate := expected[stepID]; duplicate {
			return nil, fmt.Errorf("expected DELETE step identity %q is duplicated", stepID)
		}
		expected[stepID] = struct{}{}
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("expected DELETE step identity is unavailable")
	}
	return expected, nil
}

func captureTerminalDeleteMetricsFromPath(
	ctx context.Context,
	baseURL string,
	metricsPath string,
	wantScope string,
	expectedRunID int64,
	expectedClusterID string,
	expectedSteps map[string]struct{},
	expectedContributors map[string]struct{},
) (map[string]*deletemetrics.Metrics, bool, error) {
	client := &http.Client{Timeout: constants.ResultsDiscoveryHTTPTimeout}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, strings.TrimSuffix(baseURL, "/")+metricsPath, nil)
	if err != nil {
		return nil, false, fmt.Errorf("create %s request: %w", metricsPath, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("fetch %s: %w", metricsPath, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, true, fmt.Errorf("%s status: %d", metricsPath, resp.StatusCode)
	}
	var steps []terminalDeleteMetricsStep
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		return nil, true, fmt.Errorf("decode %s: %w", metricsPath, err)
	}
	metrics := make(map[string]*deletemetrics.Metrics)
	seen := make(map[string]struct{}, len(expectedSteps))
	for i := range steps {
		step := &steps[i]
		runID, parseErr := parseMetricsRunID(step.RunID)
		_, expectedStep := expectedSteps[strings.TrimSpace(step.StepID)]
		if parseErr != nil {
			if expectedStep && strings.EqualFold(step.OpType, "DELETE") {
				return nil, true, fmt.Errorf("%s expected DELETE step %q has malformed run identity", metricsPath, step.StepID)
			}
			continue
		}
		if runID != expectedRunID {
			continue
		}
		if !strings.EqualFold(step.OpType, "DELETE") {
			continue
		}
		if !expectedStep {
			return nil, true, fmt.Errorf("%s returned unexpected current-run DELETE step %q", metricsPath, step.StepID)
		}
		if _, duplicate := seen[step.StepID]; duplicate {
			return nil, true, fmt.Errorf("%s returned duplicate DELETE step %q", metricsPath, step.StepID)
		}
		seen[step.StepID] = struct{}{}
		if step.MetricsSchema < deletemetrics.SchemaVersion ||
			!strings.EqualFold(step.Scope, wantScope) || !validMetricsRole(wantScope, step.Role) ||
			step.ClusterID != expectedClusterID || !step.Terminal || step.Partial ||
			!step.DeleteDetailExpected {
			return nil, true, fmt.Errorf("%s returned incomplete authoritative DELETE step %q", metricsPath, step.StepID)
		}
		if strings.EqualFold(wantScope, "fleet") &&
			!completeContributorEvidence(step.NodesCount, step.ContributorsPresent, expectedContributors) {
			return nil, true, fmt.Errorf("%s returned incomplete contributor evidence for DELETE step %q", metricsPath, step.StepID)
		}
		if validateErr := deletemetrics.ValidateTerminal(step.Delete); validateErr != nil {
			return nil, true, fmt.Errorf("%s DELETE step %q: %w", metricsPath, step.StepID, validateErr)
		}
		metrics[step.StepID] = step.Delete
	}
	for stepID := range expectedSteps {
		if _, ok := metrics[stepID]; !ok {
			return nil, true, fmt.Errorf("%s is missing authoritative terminal DELETE step %q", metricsPath, stepID)
		}
	}
	return metrics, true, nil
}

func completeContributorEvidence(
	nodeCount int,
	contributors []string,
	expected map[string]struct{},
) bool {
	if nodeCount <= 0 || nodeCount != len(expected) || len(contributors) != nodeCount {
		return false
	}
	seen := make(map[string]struct{}, len(contributors))
	for _, contributor := range contributors {
		contributor = strings.TrimSpace(contributor)
		if contributor == "" {
			return false
		}
		if _, duplicate := seen[contributor]; duplicate {
			return false
		}
		if _, wanted := expected[contributor]; !wanted {
			return false
		}
		seen[contributor] = struct{}{}
	}
	return len(seen) == len(expected)
}

func validMetricsRole(scope, role string) bool {
	if strings.EqualFold(scope, "fleet") {
		return strings.EqualFold(role, "aggregate")
	}
	return strings.EqualFold(role, "entry") || strings.EqualFold(role, "worker")
}
