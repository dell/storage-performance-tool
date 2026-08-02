package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
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
		return []string{createStep}, nil
	}
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return nil, nil
	}

	var metricsRequests atomic.Int32
	metricsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metricsRequests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metricsServer.Close()

	resultsRoot := t.TempDir()
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output, onFetch: func(root string, stepIDs []string) error {
			if len(stepIDs) != 1 || stepIDs[0] != createStep {
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
			index := results.Manifest{Steps: []results.StepManifest{{StepID: createStep}}}
			data, err := json.Marshal(index)
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(root, constants.ResultsManifestFileName), data, 0o600)
		}}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }

	metadata := &runMetadata{ResultsRoot: resultsRoot}
	monitor := startAutoResultsMonitor(
		context.Background(), metricsServer.URL, "write-verify", t.TempDir(),
		[]string{createStep, readStep}, runID, false, nil, "", false, 0,
		scenarioPath, metadata, io.Discard, io.Discard, "", nil,
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
	if !tracker.requireTerminalCalled || !tracker.requireTerminalValue {
		t.Fatalf("verification terminal policy = called %t value %t, want mandatory true",
			tracker.requireTerminalCalled, tracker.requireTerminalValue)
	}
	if metricsRequests.Load() != 0 {
		t.Fatalf("not-started READ issued %d live metrics request(s), want zero", metricsRequests.Load())
	}
	if strings.Join(outcome.StepIDs, ",") != createStep || strings.Join(metadata.ActualStepIDs, ",") != createStep {
		t.Fatalf("runtime execution IDs = outcome %v metadata %v, want producer only %s",
			outcome.StepIDs, metadata.ActualStepIDs, createStep)
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

func TestReadVerifyFailedListStopsReadAndRunEExitsOne(t *testing.T) {
	t.Chdir(t.TempDir())
	previousGOOS := integrityRuntimeGOOS
	integrityRuntimeGOOS = integritySupportedGOOS
	t.Cleanup(func() { integrityRuntimeGOOS = previousGOOS })

	var metricsRequests atomic.Int32
	metricsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metricsRequests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metricsServer.Close()
	serverURL, err := url.Parse(metricsServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, apiPort, err := net.SplitHostPort(serverURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	for flag, value := range map[string]string{
		"test-hosts": "127.0.0.1", "min-hosts": "1",
		"endpoints": "http://s3.example", "access-key": "access", "secret-key": "secret",
		"bucket": "qualification", "object-count": "1", "threads": "1", "api-port": apiPort,
		"headless": "true", "auto-results": "true", "shutdown-on-complete": "false",
		"generate-only": "false", "results-dir": t.TempDir(),
	} {
		setGlobalRunFlagForTest(t, flag, value)
	}

	originalPort := resolvePortConflictFunc
	originalLocal := startLocalHeadlessRunFunc
	originalAutoResults := startAutoResultsFunc
	originalTracker := newRunTrackerFunc
	originalDiscover := discoverStepIDsFunc
	originalFleetDiscover := discoverFleetStepIDsFunc
	originalFetcher := newResultsFetcherFunc
	originalSummary := generateRunSummaryFunc
	t.Cleanup(func() {
		resolvePortConflictFunc = originalPort
		startLocalHeadlessRunFunc = originalLocal
		startAutoResultsFunc = originalAutoResults
		newRunTrackerFunc = originalTracker
		discoverStepIDsFunc = originalDiscover
		discoverFleetStepIDsFunc = originalFleetDiscover
		newResultsFetcherFunc = originalFetcher
		generateRunSummaryFunc = originalSummary
	})
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
		options.LaunchHooks.NotifySubmitted()
		return nil
	}

	const runtimeList = "mt-001-20260802.190000.000-list"
	const producerCause = "LIST failed before selecting verification objects"
	var tracker *fakeRunTracker
	var capturedMetadata *runMetadata
	var fetchedStepIDs []string
	startAutoResultsFunc = func(
		parentCtx context.Context, baseURL, label, resultsDir string, plannedStepIDs []string,
		runID int64, debug bool, hosts []*hostparse.HostInfo, port string, shutdownOn bool,
		lingerSec int, scenarioPath string, metadata *runMetadata, progressOut, summaryOut io.Writer,
		traceFile string, preSummaryHook func(context.Context), options ...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		if len(plannedStepIDs) != 2 {
			t.Fatalf("planned read-verify IDs = %v, want LIST and READ", plannedStepIDs)
		}
		plannedList, plannedRead := plannedStepIDs[0], plannedStepIDs[1]
		tracker = &fakeRunTracker{result: &portcheck.RunResult{
			FinalState: constants.StateFailed, RunID: runID,
			FailureStepID: runtimeList, FailureCategory: "execution", FailureMessage: producerCause,
			Steps: map[string]portcheck.StepCompletion{
				runtimeList: {StepID: runtimeList, Lifecycle: portcheck.StepLifecycleFailed, Started: true, Failed: true},
				plannedList: {StepID: plannedList, Lifecycle: portcheck.StepLifecycleFailed, Planned: true, Started: true, Failed: true},
				plannedRead: {StepID: plannedRead, Lifecycle: portcheck.StepLifecycleNotStarted, Planned: true},
			},
		}}
		newRunTrackerFunc = func(string) autoResultsRunTracker { return tracker }
		discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
			return []string{runtimeList}, nil
		}
		discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }

		manifestData := []byte("bucket,key,size,version_id\n")
		digest := sha256.Sum256(manifestData)
		completionData, marshalErr := json.Marshal(integrity.Completion{
			Version: 1, Status: "complete", RunID: runID,
			ProducerKind: constants.IntegrityProvenanceEngineStep,
			ProducerID:   runtimeList, Artifact: integrity.VerifyInputName,
			SourceRecordCount: 0, UniqueRecordCount: 0, SelectedRecordCount: 0,
			ManifestBytes: int64(len(manifestData)), ManifestSHA256: hex.EncodeToString(digest[:]),
		})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
			return &fakeFetcher{output: output, onFetch: func(root string, stepIDs []string) error {
				fetchedStepIDs = append([]string(nil), stepIDs...)
				if len(stepIDs) != 1 || stepIDs[0] != runtimeList {
					return fmt.Errorf("unexpected failed-LIST fetch set: %v", stepIDs)
				}
				if mkdirErr := os.MkdirAll(root, 0o750); mkdirErr != nil {
					return mkdirErr
				}
				files := map[string][]byte{
					runtimeList + "." + integrity.VerifyInputName:           manifestData,
					runtimeList + "." + integrity.VerifyInputCompletionName: append(completionData, '\n'),
					runtimeList + ".metrics.total.csv":                      []byte("OpType,CountSucc,CountFail,CountCorrupt\nLIST,0,1,0\n"),
				}
				for name, data := range files {
					if writeErr := os.WriteFile(filepath.Join(root, name), data, 0o600); writeErr != nil {
						return writeErr
					}
				}
				indexData, marshalErr := json.Marshal(results.Manifest{Steps: []results.StepManifest{{StepID: runtimeList}}})
				if marshalErr != nil {
					return marshalErr
				}
				return os.WriteFile(filepath.Join(root, constants.ResultsManifestFileName), indexData, 0o600)
			}}
		}
		generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }
		capturedMetadata = metadata
		return startAutoResultsMonitor(
			parentCtx, baseURL, label, resultsDir, plannedStepIDs, runID, debug, hosts, port,
			shutdownOn, lingerSec, scenarioPath, metadata, progressOut, summaryOut, traceFile,
			preSummaryHook, options...,
		)
	}

	err = runCmd.RunE(runCmd, []string{WorkloadTypeReadVerify})
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure ||
		!strings.Contains(err.Error(), producerCause) {
		t.Fatalf("RunE() error = %#v, want exit %d preserving LIST cause", err, constants.ExitCodeWorkloadFailure)
	}
	if tracker == nil || !tracker.requireTerminalCalled || !tracker.requireTerminalValue {
		t.Fatalf("read-verify terminal policy tracker = %+v", tracker)
	}
	if metricsRequests.Load() != 0 {
		t.Fatalf("not-started READ issued %d live metrics request(s), want zero", metricsRequests.Load())
	}
	if strings.Join(fetchedStepIDs, ",") != runtimeList || capturedMetadata == nil ||
		strings.Join(capturedMetadata.ActualStepIDs, ",") != runtimeList {
		t.Fatalf("runtime/fetch IDs = fetched %v metadata %+v, want LIST only", fetchedStepIDs, capturedMetadata)
	}
	for _, name := range []string{integrity.VerifyInputName, integrity.VerifyInputCompletionName} {
		if _, statErr := os.Stat(filepath.Join(capturedMetadata.ResultsRoot, name)); statErr != nil {
			t.Fatalf("canonical failed-LIST producer evidence %s missing: %v", name, statErr)
		}
	}
	for _, name := range []string{integrity.VerifiedName, integrity.VerifiedCompletionName, integrity.VerifyRemainingName} {
		if _, statErr := os.Stat(filepath.Join(capturedMetadata.ResultsRoot, name)); !os.IsNotExist(statErr) {
			t.Fatalf("not-started READ artifact %s unexpectedly exists: %v", name, statErr)
		}
	}
}
