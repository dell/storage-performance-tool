package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestWriteVerifyZeroSuccessfulCreateStopsReadAndExitsOne(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	t.Cleanup(func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	})

	const runID = int64(701)
	const timestamp = "20260801.120000.000"
	createStep := "mt-001-" + timestamp + "-create"
	readStep := "mt-002-" + timestamp + "-verify"
	params := scenario.Params{
		WorkloadType:  scenario.WorkloadTypeWriteVerify,
		RunID:         runID,
		BaseTimestamp: timestamp,
		Bucket:        "qualification",
		Threads:       1,
		ObjectSize:    "1KiB",
		ObjectCount:   1,
	}
	scenarioContent, err := scenario.GenerateWriteVerifyScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	createAt := strings.Index(scenarioContent, "CreateLoad.config")
	readAt := strings.Index(scenarioContent, "ReadLoad.config")
	if createAt < 0 || readAt <= createAt ||
		!strings.Contains(scenarioContent[createAt:readAt], ").run();") {
		t.Fatalf("generated scenario does not execute CREATE before READ:\n%s", scenarioContent)
	}
	scenarioPath := filepath.Join(t.TempDir(), "write-verify.js")
	if err = os.WriteFile(scenarioPath, []byte(scenarioContent), 0o600); err != nil {
		t.Fatal(err)
	}

	manifestData := []byte("bucket,key,size,version_id\n")
	digest := sha256.Sum256(manifestData)
	completion := integrity.Completion{
		Version: 1, Status: "complete", RunID: runID,
		ProducerKind: constants.IntegrityProvenanceEngineStep,
		ProducerID:   createStep, Artifact: integrity.WrittenName,
		SourceRecordCount: 0, UniqueRecordCount: 0, SelectedRecordCount: 0,
		ManifestBytes: int64(len(manifestData)), ManifestSHA256: hex.EncodeToString(digest[:]),
	}
	completionData, err := json.Marshal(completion)
	if err != nil {
		t.Fatal(err)
	}
	producerRoot := t.TempDir()
	producerManifest := filepath.Join(producerRoot, integrity.WrittenName)
	producerCompletion := filepath.Join(producerRoot, integrity.WrittenCompletionName)
	if err = os.WriteFile(producerManifest, manifestData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(producerCompletion, append(completionData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = integrity.ValidateCompletion(
		producerManifest, producerCompletion, runID,
		constants.IntegrityProvenanceEngineStep, createStep, integrity.WrittenName,
	); err != nil {
		t.Fatalf("zero-row CREATE evidence fixture is invalid: %v", err)
	}

	trackerResult := &portcheck.RunResult{
		FinalState:      constants.StateFailed,
		RunID:           runID,
		FailureStepID:   createStep,
		FailureCategory: "execution",
		FailureMessage:  "write verification produced zero successful objects",
		Steps: map[string]portcheck.StepCompletion{
			createStep: {
				StepID: createStep, Lifecycle: portcheck.StepLifecycleFailed,
				Planned: true, Started: true, Failed: true,
			},
			readStep: {
				StepID: readStep, Lifecycle: portcheck.StepLifecycleNotStarted,
				Planned: true,
			},
		},
	}
	tracker := &fakeRunTracker{result: trackerResult}
	newRunTrackerFunc = func(string) autoResultsRunTracker { return tracker }
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return []string{createStep, readStep}, nil
	}
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return nil, nil
	}

	metricsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics/fleet/json" && r.URL.Path != "/metrics/json" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"run_id": runID, "step_id": readStep, "op_type": "READ",
			"operations": map[string]any{"corrupt_count": 0},
		}})
	}))
	defer metricsServer.Close()

	resultsRoot := t.TempDir()
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output, onFetch: func(root string, stepIDs []string) error {
			if len(stepIDs) != 2 || stepIDs[0] != createStep || stepIDs[1] != readStep {
				return errors.New("unexpected zero-write step set")
			}
			if err := os.MkdirAll(root, 0o750); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(root, createStep+"."+integrity.WrittenName), manifestData, 0o600); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(root, createStep+"."+integrity.WrittenCompletionName), append(completionData, '\n'), 0o600); err != nil {
				return err
			}
			createMetrics := "OpType,CountSucc,CountFail,CountCorrupt\nCREATE,0,1,0\n"
			if err := os.WriteFile(filepath.Join(root, createStep+".metrics.total.csv"), []byte(createMetrics), 0o600); err != nil {
				return err
			}
			index := results.Manifest{Steps: []results.StepManifest{
				{StepID: createStep}, {StepID: readStep},
			}}
			data, err := json.Marshal(index)
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(root, constants.ResultsManifestFileName), data, 0o600)
		}}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }

	monitor := startAutoResultsMonitor(
		context.Background(), metricsServer.URL, "write-verify", t.TempDir(),
		[]string{createStep, readStep}, runID, false, nil, "", false, 0,
		scenarioPath, &runMetadata{ResultsRoot: resultsRoot}, io.Discard, io.Discard, "", nil,
		&integrity.FinalizeOptions{Workload: scenario.WorkloadTypeWriteVerify, RunID: runID},
	)
	monitor.Arm()
	var outcome autoResultsOutcome
	select {
	case outcome = <-monitor.done:
	case <-time.After(3 * time.Second):
		t.Fatal("zero-write auto-results processing did not complete")
	}

	if outcome.TrackerErr != nil || outcome.ArtifactErr != nil || outcome.FinalizationErr != nil {
		t.Fatalf("zero-write outcome lost primary producer cause: %+v", outcome)
	}
	if outcome.Tracker.Steps[readStep].Lifecycle != portcheck.StepLifecycleNotStarted {
		t.Fatalf("READ lifecycle = %q, want not_started", outcome.Tracker.Steps[readStep].Lifecycle)
	}
	if outcome.Finalization == nil || outcome.Finalization.Complete {
		t.Fatalf("zero-write finalization = %+v, want incomplete", outcome.Finalization)
	}
	if !outcome.Finalization.EmptySelection || outcome.Finalization.EmptyAllowed ||
		outcome.Finalization.SelectionCount != 0 || outcome.Finalization.VerificationAttemptedCount != 0 {
		t.Fatalf("zero-write machine outcome = %+v", outcome.Finalization)
	}

	runErr := resolveVerificationRunError(nil, outcome, true, params)
	var exitErr *ExitCodeError
	if !errors.As(runErr, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
		t.Fatalf("command error = %#v, want exit %d", runErr, constants.ExitCodeWorkloadFailure)
	}
	if !strings.Contains(runErr.Error(), trackerResult.FailureMessage) ||
		strings.Contains(runErr.Error(), "missing verified") {
		t.Fatalf("command did not preserve primary CREATE cause: %v", runErr)
	}

	for _, path := range []string{
		filepath.Join(resultsRoot, readStep+"."+integrity.VerifiedName),
		filepath.Join(resultsRoot, integrity.VerifiedName),
		filepath.Join(resultsRoot, integrity.VerifiedCompletionName),
	} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("READ or dependent canonical evidence was published at %s: %v", path, statErr)
		}
	}
	if _, statErr := os.Stat(filepath.Join(resultsRoot, createStep+"."+integrity.WrittenName)); statErr != nil {
		t.Fatalf("valid header-only CREATE evidence was not preserved: %v", statErr)
	}
	for _, name := range []string{integrity.WrittenName, integrity.WrittenCompletionName} {
		if _, statErr := os.Stat(filepath.Join(resultsRoot, name)); statErr != nil {
			t.Fatalf("canonical zero-write producer evidence %s was not promoted: %v", name, statErr)
		}
	}
}
