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
	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
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
	// Fleet endpoint may not be available on all engine versions.
	steps, available, err := fetchMetricsRows[stepIdentityRow](ctx, baseURL, metricsPath)
	if err != nil || !available {
		return nil, err
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

type stepIdentityRow struct {
	StepID string          `json:"step_id"`
	RunID  json.RawMessage `json:"run_id"`
}

// fetchMetricsRows decodes the rows of one metrics endpoint. available is false when the
// endpoint does not exist.
func fetchMetricsRows[R any](ctx context.Context, baseURL, metricsPath string) (rows []R, available bool, err error) {
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
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, true, fmt.Errorf("decode %s: %w", metricsPath, err)
	}
	return rows, true, nil
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

// terminalMetricsRow holds the identity and completeness fields every terminal capture checks.
type terminalMetricsRow struct {
	Scope               string          `json:"scope"`
	Role                string          `json:"role"`
	ClusterID           string          `json:"cluster_id"`
	RunID               json.RawMessage `json:"run_id"`
	StepID              string          `json:"step_id"`
	OpType              string          `json:"op_type"`
	Terminal            bool            `json:"terminal"`
	Partial             bool            `json:"partial"`
	NodesCount          int             `json:"nodes_count"`
	ContributorsPresent []string        `json:"contributors_present"`
}

type terminalDeleteMetricsStep struct {
	terminalMetricsRow
	MetricsSchema        int                    `json:"metrics_schema"`
	DeleteDetailExpected bool                   `json:"delete_detail_expected"`
	Delete               *deletemetrics.Metrics `json:"delete"`
}

type terminalDriverStatsStep struct {
	terminalMetricsRow
	DriverStats json.RawMessage `json:"driver_stats"`
}

const (
	// captureKindDelete and captureKindDriverStats name the captured value in errors.
	captureKindDelete      = "DELETE"
	captureKindDriverStats = "driver statistics"
	metricsOpTypeDelete    = "DELETE"
	metricsScopeFleet      = "fleet"
	metricsScopeNode       = "node"
)

// terminalCaptureTarget identifies the run, steps and contributors a terminal capture accepts
// rows for. kind names the captured value in errors.
type terminalCaptureTarget struct {
	kind         string
	runID        int64
	clusterID    string
	steps        map[string]struct{}
	contributors map[string]struct{}
}

func newTerminalCaptureTarget(
	kind string,
	expectedRunID int64,
	expectedStepIDs []string,
	expectedContributorIDs []string,
) (*terminalCaptureTarget, error) {
	if expectedRunID <= 0 {
		return nil, fmt.Errorf("expected run identity is unavailable")
	}
	steps, err := exactStepSet(kind, expectedStepIDs)
	if err != nil {
		return nil, err
	}
	contributors, err := exactContributorSet(kind, expectedContributorIDs)
	if err != nil {
		return nil, err
	}
	return &terminalCaptureTarget{
		kind:         kind,
		runID:        expectedRunID,
		clusterID:    constants.RunClusterID(expectedRunID),
		steps:        steps,
		contributors: contributors,
	}, nil
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
	const capture = "capture terminal DELETE metrics"
	target, err := newTerminalCaptureTarget(
		captureKindDelete, expectedRunID, expectedStepIDs, expectedContributorIDs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", capture, err)
	}
	return captureFromAuthoritativeEndpoint(
		capture, target, allowNodeFallback,
		func(metricsPath, wantScope string) (map[string]*deletemetrics.Metrics, bool, error) {
			return captureTerminalDeleteMetricsFromPath(ctx, baseURL, metricsPath, wantScope, target)
		})
}

// CaptureTerminalDriverStatsForRunContext captures the terminal driver statistics of every
// expected step that reports them, under the endpoint, identity and completeness rules of
// CaptureTerminalDeleteMetricsForRunContext. A step whose driver reports nothing is left out, so
// an engine without driver statistics yields an empty result. Statistics that fail a check are
// an error and nothing is returned: partial statistics are never stored.
func CaptureTerminalDriverStatsForRunContext(
	ctx context.Context,
	baseURL string,
	expectedRunID int64,
	expectedStepIDs []string,
	expectedContributorIDs []string,
	allowNodeFallback bool,
) (map[string]*driverstats.Stats, error) {
	const capture = "capture terminal driver statistics"
	target, err := newTerminalCaptureTarget(
		captureKindDriverStats, expectedRunID, expectedStepIDs, expectedContributorIDs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", capture, err)
	}
	return captureFromAuthoritativeEndpoint(
		capture, target, allowNodeFallback,
		func(metricsPath, wantScope string) (map[string]*driverstats.Stats, bool, error) {
			return captureTerminalDriverStatsFromPath(ctx, baseURL, metricsPath, wantScope, target)
		})
}

// captureFromAuthoritativeEndpoint applies the endpoint rule terminal captures share: fleet
// metrics are authoritative when that endpoint exists, and node metrics are only a single-node
// compatibility fallback when it does not. fromPath reports whether its endpoint exists.
func captureFromAuthoritativeEndpoint[V any](
	capture string,
	target *terminalCaptureTarget,
	allowNodeFallback bool,
	fromPath func(metricsPath, wantScope string) (V, bool, error),
) (V, error) {
	var none V
	fleet, available, err := fromPath(constants.SptFleetMetricsEndpoint, metricsScopeFleet)
	if err != nil {
		return none, err
	}
	if available {
		return fleet, nil
	}
	if !allowNodeFallback {
		return none, fmt.Errorf("%s: authoritative fleet endpoint is unavailable", capture)
	}
	if len(target.contributors) != 1 {
		return none, fmt.Errorf("%s: node fallback requires exactly one expected contributor", capture)
	}
	if _, local := target.contributors[constants.MetricsLocalContributorID]; !local {
		return none, fmt.Errorf("%s: node fallback requires the local contributor identity", capture)
	}
	node, available, err := fromPath(constants.SptMetricsEndpoint, metricsScopeNode)
	if err != nil {
		return none, err
	}
	if !available {
		return none, fmt.Errorf("%s: node endpoint is unavailable", capture)
	}
	return node, nil
}

func exactContributorSet(kind string, contributorIDs []string) (map[string]struct{}, error) {
	expected := make(map[string]struct{}, len(contributorIDs))
	for _, contributorID := range contributorIDs {
		contributorID = strings.TrimSpace(contributorID)
		if contributorID == "" {
			return nil, fmt.Errorf("expected %s contributor identity is empty", kind)
		}
		if _, duplicate := expected[contributorID]; duplicate {
			return nil, fmt.Errorf("expected %s contributor identity %q is duplicated", kind, contributorID)
		}
		expected[contributorID] = struct{}{}
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("expected %s contributor identity is unavailable", kind)
	}
	return expected, nil
}

func exactStepSet(kind string, stepIDs []string) (map[string]struct{}, error) {
	expected := make(map[string]struct{}, len(stepIDs))
	for _, stepID := range stepIDs {
		stepID = strings.TrimSpace(stepID)
		if stepID == "" {
			return nil, fmt.Errorf("expected %s step identity is empty", kind)
		}
		if _, duplicate := expected[stepID]; duplicate {
			return nil, fmt.Errorf("expected %s step identity %q is duplicated", kind, stepID)
		}
		expected[stepID] = struct{}{}
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("expected %s step identity is unavailable", kind)
	}
	return expected, nil
}

// terminalRowChecker applies the checks every terminal capture shares to the rows of one
// endpoint, and remembers the steps it admitted so a repeated step is refused.
type terminalRowChecker struct {
	target      *terminalCaptureTarget
	metricsPath string
	wantScope   string
	seen        map[string]struct{}
}

func (t *terminalCaptureTarget) checker(metricsPath, wantScope string) *terminalRowChecker {
	return &terminalRowChecker{
		target:      t,
		metricsPath: metricsPath,
		wantScope:   wantScope,
		seen:        make(map[string]struct{}, len(t.steps)),
	}
}

// admit reports whether row belongs to the expected run and carries the captured value.
// Rows of other runs, and rows without the value, are skipped. A current-run row with the value
// is an error when its step is unexpected or repeated; so is an expected step's row with the
// value when its run identity cannot be read.
func (c *terminalRowChecker) admit(row *terminalMetricsRow, carriesValue bool) (bool, error) {
	runID, parseErr := parseMetricsRunID(row.RunID)
	_, expectedStep := c.target.steps[strings.TrimSpace(row.StepID)]
	if parseErr != nil {
		if expectedStep && carriesValue {
			return false, fmt.Errorf(
				"%s expected %s step %q has malformed run identity", c.metricsPath, c.target.kind, row.StepID)
		}
		return false, nil
	}
	if runID != c.target.runID || !carriesValue {
		return false, nil
	}
	if !expectedStep {
		return false, fmt.Errorf(
			"%s returned unexpected current-run %s step %q", c.metricsPath, c.target.kind, row.StepID)
	}
	if _, duplicate := c.seen[row.StepID]; duplicate {
		return false, fmt.Errorf("%s returned duplicate %s step %q", c.metricsPath, c.target.kind, row.StepID)
	}
	c.seen[row.StepID] = struct{}{}
	return true, nil
}

// authoritative reports whether an admitted row is a complete terminal row of the wanted scope
// and role for the run's cluster.
func (c *terminalRowChecker) authoritative(row *terminalMetricsRow) bool {
	return strings.EqualFold(row.Scope, c.wantScope) && validMetricsRole(c.wantScope, row.Role) &&
		row.ClusterID == c.target.clusterID && row.Terminal && !row.Partial
}

// contributorsComplete reports whether a fleet row names exactly the expected contributors.
// A node row carries no contributor evidence to check.
func (c *terminalRowChecker) contributorsComplete(row *terminalMetricsRow) bool {
	return !c.fleet() ||
		completeContributorEvidence(row.NodesCount, row.ContributorsPresent, c.target.contributors)
}

func (c *terminalRowChecker) fleet() bool {
	return strings.EqualFold(c.wantScope, metricsScopeFleet)
}

func captureTerminalDeleteMetricsFromPath(
	ctx context.Context,
	baseURL string,
	metricsPath string,
	wantScope string,
	target *terminalCaptureTarget,
) (map[string]*deletemetrics.Metrics, bool, error) {
	steps, available, err := fetchMetricsRows[terminalDeleteMetricsStep](ctx, baseURL, metricsPath)
	if err != nil || !available {
		return nil, available, err
	}
	checker := target.checker(metricsPath, wantScope)
	metrics := make(map[string]*deletemetrics.Metrics)
	for i := range steps {
		step := &steps[i]
		admitted, admitErr := checker.admit(
			&step.terminalMetricsRow, strings.EqualFold(step.OpType, metricsOpTypeDelete))
		if admitErr != nil {
			return nil, true, admitErr
		}
		if !admitted {
			continue
		}
		if step.MetricsSchema < deletemetrics.SchemaVersion ||
			!checker.authoritative(&step.terminalMetricsRow) || !step.DeleteDetailExpected {
			return nil, true, fmt.Errorf("%s returned incomplete authoritative DELETE step %q", metricsPath, step.StepID)
		}
		if !checker.contributorsComplete(&step.terminalMetricsRow) {
			return nil, true, fmt.Errorf("%s returned incomplete contributor evidence for DELETE step %q", metricsPath, step.StepID)
		}
		if validateErr := deletemetrics.ValidateTerminal(step.Delete); validateErr != nil {
			return nil, true, fmt.Errorf("%s DELETE step %q: %w", metricsPath, step.StepID, validateErr)
		}
		metrics[step.StepID] = step.Delete
	}
	for stepID := range target.steps {
		if _, ok := metrics[stepID]; !ok {
			return nil, true, fmt.Errorf("%s is missing authoritative terminal DELETE step %q", metricsPath, stepID)
		}
	}
	return metrics, true, nil
}

// captureTerminalDriverStatsFromPath differs from the DELETE capture in one rule: a step
// without statistics is not missing. Drivers that report nothing, engines that predate the
// field and the other operation rows of a mixed step all publish rows without driver_stats.
func captureTerminalDriverStatsFromPath(
	ctx context.Context,
	baseURL string,
	metricsPath string,
	wantScope string,
	target *terminalCaptureTarget,
) (map[string]*driverstats.Stats, bool, error) {
	steps, available, err := fetchMetricsRows[terminalDriverStatsStep](ctx, baseURL, metricsPath)
	if err != nil || !available {
		return nil, available, err
	}
	checker := target.checker(metricsPath, wantScope)
	fleetContributors := 0
	if checker.fleet() {
		fleetContributors = len(target.contributors)
	}
	captured := make(map[string]*driverstats.Stats)
	for i := range steps {
		step := &steps[i]
		admitted, admitErr := checker.admit(&step.terminalMetricsRow, hasJSONValue(step.DriverStats))
		if admitErr != nil {
			return nil, true, admitErr
		}
		if !admitted {
			continue
		}
		if !checker.authoritative(&step.terminalMetricsRow) {
			return nil, true, fmt.Errorf(
				"%s returned incomplete authoritative driver statistics step %q", metricsPath, step.StepID)
		}
		if !checker.contributorsComplete(&step.terminalMetricsRow) {
			return nil, true, fmt.Errorf(
				"%s returned incomplete contributor evidence for driver statistics step %q", metricsPath, step.StepID)
		}
		stats := &driverstats.Stats{}
		if decodeErr := json.Unmarshal(step.DriverStats, stats); decodeErr != nil {
			return nil, true, fmt.Errorf("%s driver statistics step %q: %w", metricsPath, step.StepID, decodeErr)
		}
		if validateErr := driverstats.ValidateTerminal(stats, fleetContributors); validateErr != nil {
			return nil, true, fmt.Errorf("%s driver statistics step %q: %w", metricsPath, step.StepID, validateErr)
		}
		captured[step.StepID] = stats
	}
	return captured, true, nil
}

// hasJSONValue reports whether an optional row field is present and not null.
func hasJSONValue(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
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
	if strings.EqualFold(scope, metricsScopeFleet) {
		return strings.EqualFold(role, "aggregate")
	}
	return strings.EqualFold(role, "entry") || strings.EqualFold(role, "worker")
}
