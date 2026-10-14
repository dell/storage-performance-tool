package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

type driverStatsCaptureCall struct {
	baseURL           string
	runID             int64
	stepIDs           []string
	contributorIDs    []string
	allowNodeFallback bool
}

// stubDriverStatsCapture replaces the capture for one test and records its calls.
func stubDriverStatsCapture(
	t *testing.T, captured map[string]*driverstats.Stats, captureErr error,
) *[]driverStatsCaptureCall {
	t.Helper()
	original := captureDriverStatsFunc
	t.Cleanup(func() { captureDriverStatsFunc = original })
	calls := &[]driverStatsCaptureCall{}
	captureDriverStatsFunc = func(
		_ context.Context, baseURL string, runID int64, stepIDs, contributorIDs []string,
		allowNodeFallback bool,
	) (map[string]*driverstats.Stats, error) {
		*calls = append(*calls, driverStatsCaptureCall{
			baseURL: baseURL, runID: runID,
			stepIDs:           append([]string(nil), stepIDs...),
			contributorIDs:    append([]string(nil), contributorIDs...),
			allowNodeFallback: allowNodeFallback,
		})
		return captured, captureErr
	}
	return calls
}

type driverStatsObservationResult struct {
	observation *results.DriverStatsObservation
	err         error
}

// stubDriverStatsObservations replaces the observation for one test and records its calls.
// Each call returns the next scripted result; calls after the last one observe nothing.
func stubDriverStatsObservations(
	t *testing.T, script ...driverStatsObservationResult,
) *[]driverStatsCaptureCall {
	t.Helper()
	original := observeDriverStatsFunc
	t.Cleanup(func() { observeDriverStatsFunc = original })
	calls := &[]driverStatsCaptureCall{}
	observeDriverStatsFunc = func(
		_ context.Context, baseURL string, runID int64, stepIDs, contributorIDs []string,
		allowNodeFallback bool,
	) (*results.DriverStatsObservation, error) {
		next := len(*calls)
		*calls = append(*calls, driverStatsCaptureCall{
			baseURL: baseURL, runID: runID,
			stepIDs:           append([]string(nil), stepIDs...),
			contributorIDs:    append([]string(nil), contributorIDs...),
			allowNodeFallback: allowNodeFallback,
		})
		if next >= len(script) {
			return nil, nil
		}
		return script[next].observation, script[next].err
	}
	return calls
}

func TestCaptureStoredDriverStatsRunsForEveryWorkload(t *testing.T) {
	steps := []string{"mt-001-20260823.100000.001-seed", "mt-002-20260823.100000.001-work"}
	for _, workloadType := range []string{
		WorkloadTypeWrite, WorkloadTypeRead, WorkloadTypeMixed, WorkloadTypeDelete, "replay",
	} {
		t.Run(workloadType, func(t *testing.T) {
			stored := map[string]*driverstats.Stats{
				steps[1]: {Driver: "s3-rdma", Counters: map[string]int64{"rdma_transferred": 5}, Reporting: 1},
			}
			calls := stubDriverStatsCapture(t, stored, nil)
			metadata := &runMetadata{
				WorkloadType: workloadType,
				Hosts:        []runHostMetadata{{Host: "local", IsLocal: true}},
			}

			captureStoredDriverStats(context.Background(), "http://engine", 77, steps, metadata, nil)

			want := []driverStatsCaptureCall{{
				baseURL: "http://engine", runID: 77, stepIDs: steps,
				contributorIDs: []string{constants.MetricsLocalContributorID}, allowNodeFallback: true,
			}}
			if !reflect.DeepEqual(*calls, want) {
				t.Fatalf("capture calls = %+v, want %+v", *calls, want)
			}
			if !reflect.DeepEqual(metadata.DriverStats, stored) || metadata.DriverStatsError != "" {
				t.Fatalf("stored statistics = %+v error %q", metadata.DriverStats, metadata.DriverStatsError)
			}
		})
	}
}

func TestCaptureStoredDriverStatsBindsFleetContributorsOfMultiHostRun(t *testing.T) {
	calls := stubDriverStatsCapture(t, nil, nil)
	metadata := &runMetadata{
		WorkloadType: WorkloadTypeRead,
		Hosts:        []runHostMetadata{{Host: "worker-a"}, {Host: "worker-b"}},
		metricContributors: func() ([]string, error) {
			return []string{"local", "192.0.2.25:1099"}, nil
		},
	}

	captureStoredDriverStats(context.Background(), "http://engine", 77, []string{"mt-001-read"}, metadata, nil)

	if len(*calls) != 1 || (*calls)[0].allowNodeFallback ||
		!reflect.DeepEqual((*calls)[0].contributorIDs, []string{"local", "192.0.2.25:1099"}) {
		t.Fatalf("capture calls = %+v, want the exact fleet contributors without node fallback", *calls)
	}
	if metadata.DriverStats != nil || metadata.DriverStatsError != "" {
		t.Fatalf("a run without statistics stored %+v error %q", metadata.DriverStats, metadata.DriverStatsError)
	}
}

func TestCaptureStoredDriverStatsStoresNothingWhenRefused(t *testing.T) {
	refusal := errors.New("/metrics/fleet/json driver statistics step \"mt-001-read\": statistics are partial")
	t.Run("capture refused", func(t *testing.T) {
		stubDriverStatsCapture(t, map[string]*driverstats.Stats{"mt-001-read": {Driver: "s3-rdma"}}, refusal)
		metadata := &runMetadata{
			Hosts:       []runHostMetadata{{Host: "local", IsLocal: true}},
			DriverStats: map[string]*driverstats.Stats{"mt-001-read": {Driver: "stale"}},
		}

		captureStoredDriverStats(context.Background(), "http://engine", 77, []string{"mt-001-read"}, metadata, nil)

		if metadata.DriverStats != nil || metadata.DriverStatsError != refusal.Error() {
			t.Fatalf("refused capture stored %+v error %q", metadata.DriverStats, metadata.DriverStatsError)
		}
	})

	t.Run("contributors unknown", func(t *testing.T) {
		calls := stubDriverStatsCapture(t, map[string]*driverstats.Stats{"mt-001-read": {Driver: "s3-rdma"}}, nil)
		metadata := &runMetadata{Hosts: []runHostMetadata{{Host: "worker-a"}, {Host: "worker-b"}}}

		captureStoredDriverStats(context.Background(), "http://engine", 77, []string{"mt-001-read"}, metadata, nil)

		if len(*calls) != 0 {
			t.Fatalf("capture ran without contributor identities: %+v", *calls)
		}
		if metadata.DriverStats != nil || !strings.Contains(metadata.DriverStatsError, "contributor identity") {
			t.Fatalf("stored %+v error %q", metadata.DriverStats, metadata.DriverStatsError)
		}
	})

	t.Run("no metadata", func(_ *testing.T) {
		captureStoredDriverStats(context.Background(), "http://engine", 77, []string{"mt-001-read"}, nil, nil)
	})
}

func TestCaptureStoredDriverStatsRecordsThatASelectedS3RDMADriverReportedNone(t *testing.T) {
	reported := map[string]*driverstats.Stats{
		"mt-001-read": {Driver: "s3-rdma", Counters: map[string]int64{"rdma_transferred": 5}, Reporting: 1},
	}
	tests := map[string]struct {
		s3Driver  string
		captured  map[string]*driverstats.Stats
		wantError string
	}{
		"S3-RDMA selected, nothing reported":      {s3Driver: scenario.S3DriverRdma, wantError: driverStatsNotReported},
		"selection in another letter case":        {s3Driver: " RDMA ", wantError: driverStatsNotReported},
		"S3-RDMA selected, statistics reported":   {s3Driver: scenario.S3DriverRdma, captured: reported},
		"another driver, nothing reported":        {s3Driver: "aws"},
		"custom scenario without a selection":     {},
		"another driver, statistics of its own":   {s3Driver: "aws", captured: reported},
		"empty capture is the same as no capture": {s3Driver: scenario.S3DriverRdma, captured: map[string]*driverstats.Stats{}, wantError: driverStatsNotReported},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stubDriverStatsCapture(t, tc.captured, nil)
			metadata := &runMetadata{
				WorkloadType:   WorkloadTypeRead,
				Hosts:          []runHostMetadata{{Host: "local", IsLocal: true}},
				ScenarioParams: scenario.Params{S3Driver: tc.s3Driver},
			}

			captureStoredDriverStats(context.Background(), "http://engine", 77, []string{"mt-001-read"}, metadata, nil)

			if metadata.DriverStatsError != tc.wantError {
				t.Fatalf("stored reason = %q, want %q", metadata.DriverStatsError, tc.wantError)
			}
			if len(tc.captured) == 0 && metadata.DriverStats != nil {
				t.Fatalf("stored statistics = %+v, want none", metadata.DriverStats)
			}
			if len(tc.captured) > 0 && !reflect.DeepEqual(metadata.DriverStats, tc.captured) {
				t.Fatalf("stored statistics = %+v, want %+v", metadata.DriverStats, tc.captured)
			}
		})
	}
}

func TestCaptureStoredDriverStatsAddsTheStepsThatEndedDuringTheRun(t *testing.T) {
	const seedStep, readStep, cleanupStep = "mt-001-seed", "mt-002-read", "mt-003-delete"
	stats := func(transferred int64) *driverstats.Stats {
		return &driverstats.Stats{
			Driver: "s3-rdma", Counters: map[string]int64{"rdma_transferred": transferred}, Reporting: 1,
		}
	}
	ended := func(steps map[string]*driverstats.Stats) driverStatsObservationResult {
		return driverStatsObservationResult{observation: &results.DriverStatsObservation{Steps: steps}}
	}
	refused := func(steps map[string]error) driverStatsObservationResult {
		return driverStatsObservationResult{observation: &results.DriverStatsObservation{Refused: steps}}
	}
	// working is a response in which the rows of the given steps, running or ended, showed their
	// driver at work; ended lists those of them whose terminal statistics passed every check.
	working := func(ended map[string]*driverstats.Stats, stepIDs ...string) driverStatsObservationResult {
		active := make(map[string]struct{}, len(stepIDs))
		for _, stepID := range stepIDs {
			active[stepID] = struct{}{}
		}
		return driverStatsObservationResult{observation: &results.DriverStatsObservation{Steps: ended, Active: active}}
	}
	seedMissed := errors.New(`no terminal driver statistics were read for step "mt-001-seed", whose driver did work`)
	seedRefusal := errors.New(
		`/metrics/fleet/json returned incomplete contributor evidence for driver statistics step "mt-001-seed"`)
	readRefusal := errors.New(`/metrics/fleet/json driver statistics step "mt-002-read": statistics are partial`)
	captureFailure := errors.New("GET /metrics/fleet/json: connection refused")
	type steps = map[string]*driverstats.Stats

	tests := map[string]struct {
		observations []driverStatsObservationResult
		final        steps
		finalErr     error
		want         steps
		wantErr      error
	}{
		"a step that ended early is stored after the engine dropped its row": {
			observations: []driverStatsObservationResult{
				ended(steps{}), ended(steps{seedStep: stats(8)}), ended(steps{seedStep: stats(8)}), ended(steps{}),
			},
			final: steps{readStep: stats(64), cleanupStep: stats(0)},
			want:  steps{seedStep: stats(8), readStep: stats(64), cleanupStep: stats(0)},
		},
		"nothing ended before the capture at the end": {
			final: steps{readStep: stats(64)},
			want:  steps{readStep: stats(64)},
		},
		"the capture at the end decides for a step it still holds": {
			observations: []driverStatsObservationResult{ended(steps{readStep: stats(60)})},
			final:        steps{readStep: stats(64)},
			want:         steps{readStep: stats(64)},
		},
		"the capture at the end clears the refusal of a step it holds": {
			observations: []driverStatsObservationResult{refused(map[string]error{readStep: readRefusal})},
			final:        steps{readStep: stats(64)},
			want:         steps{readStep: stats(64)},
		},
		"a later observation of a step clears its refusal": {
			observations: []driverStatsObservationResult{
				refused(map[string]error{seedStep: seedRefusal}), ended(steps{seedStep: stats(8)}),
			},
			final: steps{readStep: stats(64)},
			want:  steps{seedStep: stats(8), readStep: stats(64)},
		},
		"a later refusal of a step drops its statistics": {
			observations: []driverStatsObservationResult{
				ended(steps{seedStep: stats(8)}), refused(map[string]error{seedStep: seedRefusal}),
			},
			final:   steps{readStep: stats(64)},
			wantErr: seedRefusal,
		},
		"one refused step refuses every step": {
			observations: []driverStatsObservationResult{
				ended(steps{seedStep: stats(8)}), refused(map[string]error{readStep: readRefusal}),
			},
			final:   steps{cleanupStep: stats(0)},
			wantErr: readRefusal,
		},
		"the first of the refused steps gives the reason": {
			observations: []driverStatsObservationResult{
				refused(map[string]error{readStep: readRefusal, seedStep: seedRefusal}),
			},
			wantErr: seedRefusal,
		},
		"an observation that could not be read changes nothing": {
			observations: []driverStatsObservationResult{
				ended(steps{seedStep: stats(8)}), {err: captureFailure}, {},
			},
			final: steps{readStep: stats(64)},
			want:  steps{seedStep: stats(8), readStep: stats(64)},
		},
		"a step seen at work whose terminal statistics were never read refuses every step": {
			// Its row was dropped before an observation read it as ended.
			observations: []driverStatsObservationResult{working(nil, seedStep), working(nil, readStep)},
			final:        steps{readStep: stats(64)},
			wantErr:      seedMissed,
		},
		"a step seen at work is stored once its terminal statistics are read": {
			observations: []driverStatsObservationResult{
				working(nil, seedStep), working(steps{seedStep: stats(8)}, seedStep, readStep),
			},
			final: steps{readStep: stats(64)},
			want:  steps{seedStep: stats(8), readStep: stats(64)},
		},
		"the capture at the end covers a step seen at work": {
			observations: []driverStatsObservationResult{working(nil, readStep)},
			final:        steps{readStep: stats(64)},
			want:         steps{readStep: stats(64)},
		},
		"a step whose driver did no work is not missing": {
			observations: []driverStatsObservationResult{ended(steps{})},
			final:        steps{readStep: stats(64)},
			want:         steps{readStep: stats(64)},
		},
		"a refusal is reported before a missing step": {
			observations: []driverStatsObservationResult{
				working(nil, seedStep), refused(map[string]error{readStep: readRefusal}),
			},
			wantErr: readRefusal,
		},
		"a capture at the end that fails stores nothing": {
			observations: []driverStatsObservationResult{ended(steps{seedStep: stats(8)})},
			finalErr:     captureFailure,
			wantErr:      captureFailure,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stubDriverStatsObservations(t, tc.observations...)
			stubDriverStatsCapture(t, tc.final, tc.finalErr)
			metadata := &runMetadata{
				WorkloadType: WorkloadTypeRead,
				Hosts:        []runHostMetadata{{Host: "local", IsLocal: true}},
			}
			planned := []string{seedStep, readStep, cleanupStep}
			collector := &driverStatsCollector{}
			for range tc.observations {
				collector.observe(context.Background(), "http://engine", 77, planned, metadata)
			}

			captureStoredDriverStats(context.Background(), "http://engine", 77, planned, metadata, collector)

			wantReason := ""
			if tc.wantErr != nil {
				wantReason = tc.wantErr.Error()
			}
			if !reflect.DeepEqual(metadata.DriverStats, tc.want) || metadata.DriverStatsError != wantReason {
				got, _ := json.Marshal(metadata.DriverStats)
				want, _ := json.Marshal(tc.want)
				t.Fatalf("stored statistics = %s reason %q, want %s reason %q",
					got, metadata.DriverStatsError, want, wantReason)
			}
		})
	}
}

func TestCaptureStoredDriverStatsRefusesARunWhoseMetricsWentUnreadForTheRowRetention(t *testing.T) {
	const readStep = "mt-002-read"
	retention := constants.EngineTerminalRowRetention
	type read struct {
		after       time.Duration // since the end of the previous read
		takes       time.Duration
		failed      bool
		beforeSteps bool // a discovery that found no step of the run
	}
	every := func(n int, r read) []read { return slices.Repeat([]read{r}, n) }
	observed := read{after: 2 * time.Second, takes: 100 * time.Millisecond}
	failed := read{after: 2 * time.Second, takes: 100 * time.Millisecond, failed: true}
	discovered := read{after: 500 * time.Millisecond, takes: 10 * time.Millisecond, beforeSteps: true}
	quick := read{after: time.Second}

	tests := map[string]struct {
		reads   []read
		final   read
		wantGap bool
	}{
		"reads well within the retention": {
			reads: every(30, observed),
			final: quick,
		},
		"an engine start read by discovery before any step has a row": {
			reads: append(every(80, discovered), every(3, observed)...),
			final: quick,
		},
		"an engine start without any read": {
			reads:   []read{{after: retention + time.Second}},
			final:   quick,
			wantGap: true,
		},
		"observations that failed for as long as the retention": {
			reads:   append(append(every(2, observed), every(10, failed)...), every(2, observed)...),
			final:   quick,
			wantGap: true,
		},
		"a few failed observations": {
			reads: append(append(every(2, observed), every(3, failed)...), every(2, observed)...),
			final: quick,
		},
		"a capture at the end that comes as late as the retention": {
			reads:   every(2, observed),
			final:   read{after: retention},
			wantGap: true,
		},
		"a capture at the end that comes just in time": {
			reads: every(2, observed),
			final: read{after: retention - time.Second},
		},
		"reads whose duration closes the gap": {
			// From the start of one read to the end of the next: what they saw can lie that far apart.
			reads:   []read{{takes: 6 * time.Second}, {after: retention - 10*time.Second, takes: 6 * time.Second}},
			final:   quick,
			wantGap: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
			var current read
			original := observeDriverStatsFunc
			t.Cleanup(func() { observeDriverStatsFunc = original })
			observeDriverStatsFunc = func(
				context.Context, string, int64, []string, []string, bool,
			) (*results.DriverStatsObservation, error) {
				at = at.Add(current.takes)
				if current.failed {
					return nil, errors.New("GET /metrics/json: connection refused")
				}
				return &results.DriverStatsObservation{}, nil
			}
			stats := &driverstats.Stats{Driver: "s3-rdma", Counters: map[string]int64{"rdma_transferred": 64}}
			originalCapture := captureDriverStatsFunc
			t.Cleanup(func() { captureDriverStatsFunc = originalCapture })
			captureDriverStatsFunc = func(
				context.Context, string, int64, []string, []string, bool,
			) (map[string]*driverstats.Stats, error) {
				at = at.Add(tc.final.takes)
				return map[string]*driverstats.Stats{readStep: stats}, nil
			}
			metadata := &runMetadata{
				WorkloadType: WorkloadTypeRead,
				Hosts:        []runHostMetadata{{Host: "local", IsLocal: true}},
			}
			collector := newDriverStatsCollector(func() time.Time { return at })
			for _, r := range tc.reads {
				at = at.Add(r.after)
				current = r
				if r.beforeSteps {
					start := at
					at = at.Add(r.takes)
					collector.readBeforeSteps(start)
					continue
				}
				collector.observe(context.Background(), "http://engine", 77, []string{readStep}, metadata)
			}
			at = at.Add(tc.final.after)

			captureStoredDriverStats(context.Background(), "http://engine", 77, []string{readStep}, metadata, collector)

			if tc.wantGap {
				if metadata.DriverStats != nil || !strings.Contains(metadata.DriverStatsError, "went unread") {
					t.Fatalf("stored %+v reason %q, want a refusal for the unread time", metadata.DriverStats, metadata.DriverStatsError)
				}
				return
			}
			if metadata.DriverStatsError != "" || metadata.DriverStats[readStep] != stats {
				t.Fatalf("stored %+v reason %q, want the statistics of the step", metadata.DriverStats, metadata.DriverStatsError)
			}
		})
	}
}

func TestDriverStatsCollectorObservesWithTheContributorsOfTheRun(t *testing.T) {
	planned := []string{"mt-001-seed", "mt-002-read"}
	t.Run("single local engine", func(t *testing.T) {
		calls := stubDriverStatsObservations(t)
		metadata := &runMetadata{Hosts: []runHostMetadata{{Host: "local", IsLocal: true}}}

		(&driverStatsCollector{}).observe(context.Background(), "http://engine", 77, planned, metadata)

		want := []driverStatsCaptureCall{{
			baseURL: "http://engine", runID: 77, stepIDs: planned,
			contributorIDs: []string{constants.MetricsLocalContributorID}, allowNodeFallback: true,
		}}
		if !reflect.DeepEqual(*calls, want) {
			t.Fatalf("observation calls = %+v, want %+v", *calls, want)
		}
	})

	t.Run("fleet", func(t *testing.T) {
		calls := stubDriverStatsObservations(t)
		metadata := &runMetadata{
			Hosts: []runHostMetadata{{Host: "worker-a"}, {Host: "worker-b"}},
			metricContributors: func() ([]string, error) {
				return []string{"local", "192.0.2.25:1099"}, nil
			},
		}

		(&driverStatsCollector{}).observe(context.Background(), "http://engine", 77, planned, metadata)

		if len(*calls) != 1 || (*calls)[0].allowNodeFallback ||
			!reflect.DeepEqual((*calls)[0].contributorIDs, []string{"local", "192.0.2.25:1099"}) {
			t.Fatalf("observation calls = %+v, want the exact fleet contributors without node fallback", *calls)
		}
	})

	t.Run("contributors unknown", func(t *testing.T) {
		calls := stubDriverStatsObservations(t)
		metadata := &runMetadata{Hosts: []runHostMetadata{{Host: "worker-a"}, {Host: "worker-b"}}}

		(&driverStatsCollector{}).observe(context.Background(), "http://engine", 77, planned, metadata)

		if len(*calls) != 0 {
			t.Fatalf("observation ran without contributor identities: %+v", *calls)
		}
	})
}

func TestRunMetadataStoresDriverStatsOnlyWhenPresent(t *testing.T) {
	without, err := json.Marshal(&runMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(without), "driverStats") {
		t.Fatalf("metadata without statistics carries the keys: %s", without)
	}

	with, err := json.Marshal(&runMetadata{
		DriverStats: map[string]*driverstats.Stats{"mt-001-step": {
			Driver:   "test-driver",
			Counters: map[string]int64{"widgets_made": 7, "zeta": 0},
			Gauges:   map[string]int64{"queue_depth": -2},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var stored struct {
		DriverStats map[string]json.RawMessage `json:"driverStats"`
	}
	if err = json.Unmarshal(with, &stored); err != nil {
		t.Fatal(err)
	}
	const want = `{"driver":"test-driver","counters":{"widgets_made":7,"zeta":0},"gauges":{"queue_depth":-2}}`
	if got := string(stored.DriverStats["mt-001-step"]); got != want {
		t.Fatalf("stored statistics = %s, want %s", got, want)
	}
}

// installAutoResultsFakes replaces everything a finalizing run needs from an engine except the
// two terminal captures.
func installAutoResultsFakes(t *testing.T, runtimeStepIDs []string) {
	t.Helper()
	originalTracker := newRunTrackerFunc
	originalDiscover := discoverStepIDsFunc
	originalFleetDiscover := discoverFleetStepIDsFunc
	originalFetcher := newResultsFetcherFunc
	originalSummary := generateRunSummaryFunc
	t.Cleanup(func() {
		newRunTrackerFunc = originalTracker
		discoverStepIDsFunc = originalDiscover
		discoverFleetStepIDsFunc = originalFleetDiscover
		newResultsFetcherFunc = originalFetcher
		generateRunSummaryFunc = originalSummary
	})
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &fakeRunTracker{result: &portcheck.RunResult{FinalState: constants.StateCompleted}}
	}
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return append([]string(nil), runtimeStepIDs...), nil
	}
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output, manifest: &results.Manifest{}}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }
}

type storedDriverStats struct {
	DriverStats      map[string]*driverstats.Stats     `json:"driverStats"`
	DriverStatsError string                            `json:"driverStatsError"`
	DeleteMetrics    map[string]*deletemetrics.Metrics `json:"deleteMetrics"`
}

func readStoredDriverStats(t *testing.T, resultsRoot string) (storedDriverStats, string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(resultsRoot, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatalf("read stored run metadata: %v", err)
	}
	var stored storedDriverStats
	if err = json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("decode stored run metadata: %v", err)
	}
	return stored, string(data)
}

func TestAutoResultsStoresDriverStatsReportedByTheEngine(t *testing.T) {
	const runID = int64(42)
	steps := []string{"mt-001-20260823.100000.001-seed", "mt-002-20260823.100000.001-read"}
	fleetRow := func(stepID, opType string, transferred int64) map[string]any {
		return map[string]any{
			"scope": "fleet", "role": "aggregate", "run_id": "42", "cluster_id": constants.RunClusterID(runID),
			"step_id": stepID, "op_type": opType, "terminal": true, "partial": false,
			"nodes_count": 1, "contributors_present": []string{constants.MetricsLocalContributorID},
			"driver_stats": map[string]any{
				"driver":    "s3-rdma",
				"counters":  map[string]int64{"rdma_transferred": transferred, "a_counter_this_cli_does_not_know": 3},
				"gauges":    map[string]int64{"transport_available": 1},
				"reporting": 1, "partial": false,
			},
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc(constants.SptFleetMetricsEndpoint, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{
			fleetRow(steps[0], "CREATE", 8),
			fleetRow(steps[1], "READ", 64),
			// A retained row of an earlier run is never stored.
			map[string]any{
				"scope": "fleet", "role": "aggregate", "run_id": "41", "cluster_id": constants.RunClusterID(41),
				"step_id": "mt-002-20260822.090000.000-read", "op_type": "READ", "terminal": true,
				"driver_stats": map[string]any{"driver": "s3-rdma", "counters": map[string]int64{"rdma_transferred": 999}},
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	installAutoResultsFakes(t, steps)
	stub := captureDriverStatsFunc
	t.Cleanup(func() { captureDriverStatsFunc = stub })
	captureDriverStatsFunc = engineCaptureDriverStats

	resultsRoot := t.TempDir()
	metadata := &runMetadata{
		ResultsRoot:  resultsRoot,
		WorkloadType: WorkloadTypeRead,
		Hosts:        []runHostMetadata{{Host: "local", IsLocal: true}},
	}
	monitor := startAutoResultsMonitor(
		context.Background(), server.URL, "mt", resultsRoot,
		[]string{"mt-001-20260823.100000.000-seed", "mt-002-20260823.100000.000-read"}, runID,
		false, nil, "9999", false, 0, "", metadata, io.Discard, io.Discard, "", nil)
	monitor.Arm()
	outcome := <-monitor.done

	if outcome.ArtifactErr != nil {
		t.Fatalf("ArtifactErr = %v", outcome.ArtifactErr)
	}
	stored, raw := readStoredDriverStats(t, resultsRoot)
	want := map[string]*driverstats.Stats{
		steps[0]: {
			Driver:    "s3-rdma",
			Counters:  map[string]int64{"rdma_transferred": 8, "a_counter_this_cli_does_not_know": 3},
			Gauges:    map[string]int64{"transport_available": 1},
			Reporting: 1,
		},
		steps[1]: {
			Driver:    "s3-rdma",
			Counters:  map[string]int64{"rdma_transferred": 64, "a_counter_this_cli_does_not_know": 3},
			Gauges:    map[string]int64{"transport_available": 1},
			Reporting: 1,
		},
	}
	if !reflect.DeepEqual(stored.DriverStats, want) || stored.DriverStatsError != "" {
		t.Fatalf("stored statistics = %+v error %q in %s", stored.DriverStats, stored.DriverStatsError, raw)
	}
}

func TestAutoResultsDriverStatsFailureDoesNotFailTheRunOrTheDeleteCapture(t *testing.T) {
	const deleteStep = "mt-001-20260823.100000.001-delete"
	installAutoResultsFakes(t, []string{deleteStep})
	originalDelete := captureDeleteMetricsFunc
	t.Cleanup(func() { captureDeleteMetricsFunc = originalDelete })
	deleteModel := &deletemetrics.Metrics{Objects: deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 2}}
	captureDeleteMetricsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (map[string]*deletemetrics.Metrics, error) {
		return map[string]*deletemetrics.Metrics{deleteStep: deleteModel}, nil
	}
	refusal := errors.New("/metrics/fleet/json returned incomplete contributor evidence for driver statistics step")
	calls := stubDriverStatsCapture(t, nil, refusal)

	resultsRoot := t.TempDir()
	metadata := &runMetadata{
		ResultsRoot:     resultsRoot,
		WorkloadType:    WorkloadTypeDelete,
		ExpectedStepIDs: []string{"mt-001-20260823.100000.000-delete"},
		Hosts:           []runHostMetadata{{Host: "local", IsLocal: true}},
	}
	monitor := startAutoResultsMonitor(
		context.Background(), "http://example", "mt", resultsRoot,
		[]string{"mt-001-20260823.100000.000-delete"}, 42,
		false, nil, "9999", false, 0, "", metadata, io.Discard, io.Discard, "", nil)
	monitor.Arm()
	outcome := <-monitor.done

	if len(*calls) != 1 || !reflect.DeepEqual((*calls)[0].stepIDs, []string{deleteStep}) {
		t.Fatalf("driver statistics capture calls = %+v, want one for the runtime step", *calls)
	}
	if outcome.ArtifactErr != nil || outcome.DeleteMetricsErr != nil || outcome.DeleteTerminalErr != nil ||
		outcome.SummaryErr != nil {
		t.Fatalf("driver statistics failure reached the run outcome: %+v", outcome)
	}
	if runErr := resolveRunCompletionError(
		nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeDelete}); runErr != nil {
		t.Fatalf("driver statistics failure failed the run: %v", runErr)
	}
	stored, raw := readStoredDriverStats(t, resultsRoot)
	if stored.DriverStats != nil || stored.DriverStatsError != refusal.Error() {
		t.Fatalf("stored statistics = %+v error %q in %s", stored.DriverStats, stored.DriverStatsError, raw)
	}
	if !reflect.DeepEqual(stored.DeleteMetrics, map[string]*deletemetrics.Metrics{deleteStep: deleteModel}) {
		t.Fatalf("DELETE capture changed: %s", raw)
	}
}

func TestAutoResultsDeleteCaptureFailureDoesNotDropDriverStats(t *testing.T) {
	const deleteStep = "mt-001-20260823.100000.001-delete"
	installAutoResultsFakes(t, []string{deleteStep})
	originalDelete := captureDeleteMetricsFunc
	t.Cleanup(func() { captureDeleteMetricsFunc = originalDelete })
	deleteFailure := errors.New("authoritative DELETE detail is missing")
	captureDeleteMetricsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (map[string]*deletemetrics.Metrics, error) {
		return nil, deleteFailure
	}
	statistics := map[string]*driverstats.Stats{
		deleteStep: {Driver: "s3-rdma", Counters: map[string]int64{"http_ineligible": 2}, Reporting: 1},
	}
	stubDriverStatsCapture(t, statistics, nil)

	resultsRoot := t.TempDir()
	metadata := &runMetadata{
		ResultsRoot:     resultsRoot,
		WorkloadType:    WorkloadTypeDelete,
		ExpectedStepIDs: []string{"mt-001-20260823.100000.000-delete"},
		Hosts:           []runHostMetadata{{Host: "local", IsLocal: true}},
	}
	monitor := startAutoResultsMonitor(
		context.Background(), "http://example", "mt", resultsRoot,
		[]string{"mt-001-20260823.100000.000-delete"}, 42,
		false, nil, "9999", false, 0, "", metadata, io.Discard, io.Discard, "", nil)
	monitor.Arm()
	outcome := <-monitor.done

	if !errors.Is(outcome.DeleteMetricsErr, deleteFailure) {
		t.Fatalf("DeleteMetricsErr = %v, want %v", outcome.DeleteMetricsErr, deleteFailure)
	}
	stored, raw := readStoredDriverStats(t, resultsRoot)
	if !reflect.DeepEqual(stored.DriverStats, statistics) || stored.DriverStatsError != "" {
		t.Fatalf("stored statistics = %+v error %q in %s", stored.DriverStats, stored.DriverStatsError, raw)
	}
}

// stagedRunTracker reports the run as completed when wait returns.
type stagedRunTracker struct{ wait func() }

func (s *stagedRunTracker) WaitForCompletion(context.Context, []string) (*portcheck.RunResult, error) {
	s.wait()
	return &portcheck.RunResult{FinalState: constants.StateCompleted}, nil
}

func (*stagedRunTracker) SetDebug(bool) {}

func (*stagedRunTracker) SetExpectedRunID(int64) {}

func (*stagedRunTracker) SetRequireTerminalState(bool) {}

// The engine keeps the metrics row of a finished step only for a retention period. In a run of
// several steps the row of an early step is gone when the run ends, and its statistics are
// stored all the same.
func TestAutoResultsStoresDriverStatsOfAStepWhoseRowTheEngineDroppedBeforeTheRunEnded(t *testing.T) {
	steps := []string{"mt-001-20260823.100000.001-seed", "mt-002-20260823.100000.001-read"}
	stored, raw := monitorStagedDriverStats(t, steps, [][]any{
		// The seed step is running.
		{stagedFleetRow(steps[0], "CREATE", 300, false)},
		// The seed step has ended and its row is retained; the read step is running.
		{stagedFleetRow(steps[0], "CREATE", 561, true), stagedFleetRow(steps[1], "READ", 40, false)},
		// The retention period of the seed row is over; the read step has ended.
		{stagedFleetRow(steps[1], "READ", 900, true)},
	})

	want := map[string]*driverstats.Stats{steps[0]: stagedStatistics(561), steps[1]: stagedStatistics(900)}
	if !reflect.DeepEqual(stored.DriverStats, want) || stored.DriverStatsError != "" {
		t.Fatalf("stored statistics = %+v error %q in %s", stored.DriverStats, stored.DriverStatsError, raw)
	}
}

func TestAutoResultsRefusesDriverStatsWhenAStepSeenAtWorkEndedUnread(t *testing.T) {
	steps := []string{"mt-001-20260823.100000.001-seed", "mt-002-20260823.100000.001-read"}
	stored, raw := monitorStagedDriverStats(t, steps, [][]any{
		// The seed step is running and its driver is at work.
		{stagedFleetRow(steps[0], "CREATE", 300, false)},
		// Its row is gone before the monitor read it as ended; the read step is running.
		{stagedFleetRow(steps[1], "READ", 40, false)},
		// The read step has ended.
		{stagedFleetRow(steps[1], "READ", 900, true)},
	})

	if stored.DriverStats != nil || !strings.Contains(stored.DriverStatsError, steps[0]) {
		t.Fatalf("stored statistics = %+v error %q in %s, want a refusal naming the seed step",
			stored.DriverStats, stored.DriverStatsError, raw)
	}
}

// stagedFleetRow is the fleet row of a step of run 42 whose S3-RDMA driver counted unavailable
// RDMA transports.
func stagedFleetRow(stepID, opType string, unavailable int64, terminal bool) map[string]any {
	return map[string]any{
		"scope": "fleet", "role": "aggregate", "run_id": "42", "cluster_id": constants.RunClusterID(42),
		"step_id": stepID, "op_type": opType, "terminal": terminal, "partial": false,
		"nodes_count": 1, "contributors_present": []string{constants.MetricsLocalContributorID},
		"driver_stats": map[string]any{
			"driver":    "s3-rdma",
			"counters":  map[string]int64{"http_rdma_unavailable": unavailable},
			"gauges":    map[string]int64{"transport_available": 0},
			"reporting": 1, "partial": false,
		},
	}
}

// stagedStatistics is what the CLI stores from a terminal stagedFleetRow.
func stagedStatistics(unavailable int64) *driverstats.Stats {
	return &driverstats.Stats{
		Driver:    "s3-rdma",
		Counters:  map[string]int64{"http_rdma_unavailable": unavailable},
		Gauges:    map[string]int64{"transport_available": 0},
		Reporting: 1,
	}
}

// monitorStagedDriverStats monitors a single-host S3-RDMA run 42 of the given steps whose engine
// reports the given fleet rows, stage by stage, and returns the driver statistics it stored. The run
// lasts until the monitor has read the engine's metrics in every stage.
func monitorStagedDriverStats(t *testing.T, steps []string, stages [][]any) (storedDriverStats, string) {
	t.Helper()
	const runID = int64(42)
	var stage atomic.Int32
	served := make([]chan struct{}, len(stages))
	servedOnce := make([]sync.Once, len(stages))
	for i := range served {
		served[i] = make(chan struct{})
	}
	mux := http.NewServeMux()
	mux.HandleFunc(constants.SptFleetMetricsEndpoint, func(w http.ResponseWriter, _ *http.Request) {
		at := stage.Load()
		_ = json.NewEncoder(w).Encode(stages[at])
		servedOnce[at].Do(func() { close(served[at]) })
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	installAutoResultsFakes(t, steps)
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &stagedRunTracker{wait: func() {
			for at := range stages {
				stage.Store(int32(at))
				select {
				case <-served[at]:
				case <-time.After(10 * time.Second):
					t.Errorf("the monitor did not read the engine's metrics in stage %d of the run", at)
					return
				}
			}
		}}
	}
	stubCapture, stubObserve, interval := captureDriverStatsFunc, observeDriverStatsFunc, driverStatsCollectInterval
	t.Cleanup(func() {
		captureDriverStatsFunc, observeDriverStatsFunc, driverStatsCollectInterval = stubCapture, stubObserve, interval
	})
	captureDriverStatsFunc = engineCaptureDriverStats
	observeDriverStatsFunc = engineObserveDriverStats
	driverStatsCollectInterval = 5 * time.Millisecond

	resultsRoot := t.TempDir()
	metadata := &runMetadata{
		ResultsRoot:    resultsRoot,
		WorkloadType:   WorkloadTypeRead,
		Hosts:          []runHostMetadata{{Host: "local", IsLocal: true}},
		ScenarioParams: scenario.Params{S3Driver: scenario.S3DriverRdma},
	}
	monitor := startAutoResultsMonitor(
		context.Background(), server.URL, "mt", resultsRoot,
		[]string{"mt-001-20260823.100000.000-seed", "mt-002-20260823.100000.000-read"}, runID,
		false, nil, "9999", false, 0, "", metadata, io.Discard, io.Discard, "", nil)
	monitor.Arm()
	outcome := <-monitor.done

	if outcome.ArtifactErr != nil {
		t.Fatalf("ArtifactErr = %v", outcome.ArtifactErr)
	}
	return readStoredDriverStats(t, resultsRoot)
}
