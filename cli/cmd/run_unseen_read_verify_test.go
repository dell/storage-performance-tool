package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const (
	unseenReadRunID = int64(4242)
	unseenListStep  = "mt-001-20261114.100000.000-list"
	unseenReadStep  = "mt-002-20261114.100000.000-verify"
)

// unseenReadRun describes a read-verify run whose READ step the live step list never showed.
type unseenReadRun struct {
	allowEmpty    bool
	readLifecycle portcheck.StepLifecycle
	selected      int // objects LIST selected
	corrupt       int // corrupt READs recorded in the READ step's files
}

type unseenReadResult struct {
	outcome autoResultsOutcome
	fetched []string
	root    string
	err     error
}

// runUnseenRead drives the real auto-results monitor and integrity finalization. The live step list
// and the final discovery show only the LIST step, the engine's live metrics hold no READ row, and
// the fetcher writes the step files a real engine leaves for the steps it is asked for.
func runUnseenRead(t *testing.T, run unseenReadRun) unseenReadResult {
	t.Helper()
	installAutoResultsFakes(t, []string{unseenListStep})
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &fakeRunTracker{result: &portcheck.RunResult{
			FinalState: constants.StateCompleted,
			RunID:      unseenReadRunID,
			Steps: map[string]portcheck.StepCompletion{
				unseenListStep: {
					StepID: unseenListStep, Lifecycle: portcheck.StepLifecycleCompleted, Planned: true, Started: true,
				},
				unseenReadStep: {StepID: unseenReadStep, Lifecycle: run.readLifecycle, Planned: true},
			},
		}}
	}
	metrics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			_, _ = io.WriteString(w, "[]")
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(metrics.Close)

	fetcher := &fakeFetcher{onFetch: func(root string, stepIDs []string) error {
		return writeUnseenReadStepFiles(root, stepIDs, run)
	}}
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		fetcher.output = output
		return fetcher
	}
	plan := integrityplan.Plan{
		RunID: unseenReadRunID, Workload: scenario.WorkloadTypeReadVerify, Kind: integrityplan.PlanKindReadDiscovered,
		Producer:   &integrityplan.PlannedStep{ID: unseenListStep, Number: 1, Role: integrityplan.StepRoleList},
		Verifier:   integrityplan.PlannedStep{ID: unseenReadStep, Number: 2, Role: integrityplan.StepRoleVerify},
		Input:      integrityplan.InputDiscovered,
		AllowEmpty: run.allowEmpty,
	}
	if !plan.Valid() {
		t.Fatal("invalid plan")
	}
	root := t.TempDir()
	monitor := startAutoResultsMonitor(
		context.Background(), metrics.URL, "mt", root, []string{unseenListStep, unseenReadStep}, unseenReadRunID,
		false, nil, "", false, 0, "", &runMetadata{ResultsRoot: root}, io.Discard, io.Discard, "", nil,
		&integrity.FinalizeOptions{
			Workload: scenario.WorkloadTypeReadVerify, RunID: unseenReadRunID,
			AllowEmptySelection: run.allowEmpty, Plan: plan.Clone(),
		})
	monitor.Arm()
	var outcome autoResultsOutcome
	select {
	case outcome = <-monitor.done:
	case <-time.After(10 * time.Second):
		t.Fatal("auto-results processing did not complete")
	}
	fetcher.mu.Lock()
	fetched := slices.Clone(fetcher.stepIDs)
	fetcher.mu.Unlock()
	err := resolveVerificationRunError(nil, outcome, true, scenario.Params{
		WorkloadType: scenario.WorkloadTypeReadVerify, AllowEmptySelection: run.allowEmpty,
	})
	return unseenReadResult{outcome: outcome, fetched: fetched, root: root, err: err}
}

func writeUnseenReadStepFiles(root string, stepIDs []string, run unseenReadRun) error {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return err
	}
	index := results.Manifest{}
	for _, step := range stepIDs {
		index.Steps = append(index.Steps, results.StepManifest{StepID: step})
		switch step {
		case unseenListStep:
			rows := [][]string{{"bucket", "key", "size", "version_id"}}
			for i := range run.selected {
				rows = append(rows, []string{"b", fmt.Sprintf("k%03d", i), "1", ""})
			}
			if err := writeUnseenCommittedPair(root, step, integrity.VerifyInputName, rows); err != nil {
				return err
			}
			if err := writeUnseenCSV(filepath.Join(root, step+".metrics.total.csv"), [][]string{
				{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"LIST", "1", "0", "0"},
			}); err != nil {
				return err
			}
		case unseenReadStep:
			verified := run.selected - run.corrupt
			rows := [][]string{{"bucket", "key", "size", "version_id"}}
			for i := range verified {
				rows = append(rows, []string{"b", fmt.Sprintf("k%03d", i), "1", ""})
			}
			if err := writeUnseenCommittedPair(root, step, integrity.VerifiedName, rows); err != nil {
				return err
			}
			if err := writeUnseenCSV(filepath.Join(root, step+".metrics.total.csv"), [][]string{
				{"OpType", "CountSucc", "CountFail", "CountCorrupt"},
				{"READ", fmt.Sprint(verified), fmt.Sprint(run.corrupt), fmt.Sprint(run.corrupt)},
			}); err != nil {
				return err
			}
			if err := writeUnseenCSV(filepath.Join(root, step+"."+integrity.IntegrityFailuresName), [][]string{{
				"timestamp", "node", "step", "driver", "key", "requested_version_id", "returned_version_id",
				"request_id", "reason", "algorithm", "expected_digest", "actual_digest", "expected_size",
				"actual_size", "first_mismatch_offset", "attempt",
			}}); err != nil {
				return err
			}
			if err := writeUnseenCSV(filepath.Join(root, step+"."+integrity.IntegrityPerformanceName), [][]string{{
				"node", "step", "driver", "phase", "algorithm", "objects", "bytes", "hash_worker_seconds",
				"mean_worker_hash_mib_per_second", "time_to_first_request_seconds", "additional_payload_passes",
			}}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected step %q", step)
		}
	}
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, constants.ResultsManifestFileName), data, 0o600)
}

func writeUnseenCommittedPair(root, step, artifact string, rows [][]string) error {
	manifestPath := filepath.Join(root, step+"."+artifact)
	if err := writeUnseenCSV(manifestPath, rows); err != nil {
		return err
	}
	data, err := os.ReadFile(manifestPath) // #nosec G304 -- private test directory
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	completion, err := json.Marshal(integrity.Completion{
		Version: 1, Status: "complete", RunID: unseenReadRunID,
		ProducerKind: constants.IntegrityProvenanceEngineStep, ProducerID: step, Artifact: artifact,
		SourceRecordCount: len(rows) - 1, UniqueRecordCount: len(rows) - 1, SelectedRecordCount: len(rows) - 1,
		ManifestBytes: int64(len(data)), ManifestSHA256: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return err
	}
	completionName := strings.TrimSuffix(artifact, ".csv") + ".complete.json"
	return os.WriteFile(filepath.Join(root, step+"."+completionName), completion, 0o600)
}

func writeUnseenCSV(path string, rows [][]string) error {
	file, err := os.Create(path) // #nosec G304 -- private test directory
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	if err = writer.WriteAll(rows); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func indexedSteps(t *testing.T, root string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, constants.ResultsManifestFileName)) // #nosec G304 -- test directory
	if err != nil {
		t.Fatal(err)
	}
	var manifest results.Manifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	var steps []string
	for _, step := range manifest.Steps {
		steps = append(steps, step.StepID)
	}
	return steps
}

func requireExitCode(t *testing.T, err error, code int) {
	t.Helper()
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != code {
		t.Fatalf("run error = %#v, want exit %d", err, code)
	}
}

// Without --allow-empty-selection, a READ of an empty selection that ended between two polls must still
// give the real reason, not a missing-evidence error.
func TestReadVerifyUnseenCompletedReadOfAnEmptySelectionReportsZeroObjects(t *testing.T) {
	got := runUnseenRead(t, unseenReadRun{readLifecycle: portcheck.StepLifecycleCompleted})

	if want := []string{unseenListStep, unseenReadStep}; !slices.Equal(got.fetched, want) {
		t.Fatalf("fetched %v, want %v", got.fetched, want)
	}
	requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
	message := got.err.Error()
	if !strings.Contains(message, "verification selected zero objects") {
		t.Fatalf("reason does not name the empty selection: %s", message)
	}
	for _, wrong := range []string{
		"missing the required verification step", "could not be identified", "live corruption",
		"did not produce an outcome", "artifact retrieval",
	} {
		if strings.Contains(message, wrong) {
			t.Fatalf("reason holds %q: %s", wrong, message)
		}
	}
	final := got.outcome.Finalization
	if final == nil || !final.Complete || !final.EmptySelection || final.EmptyAllowed ||
		final.VerificationAttemptedCount != 0 || got.outcome.LiveCorruptionUnobserved {
		t.Fatalf("finalization = %+v, unobserved = %t", final, got.outcome.LiveCorruptionUnobserved)
	}
	if steps := indexedSteps(t, got.root); !slices.Contains(steps, unseenReadStep) {
		t.Fatalf("index.json steps = %v, want the completed READ", steps)
	}
}

// A non-empty selection whose READ went unseen keeps failing safe: exit 1, no corruption count from
// its files, and a reason that says the live count was not observed.
func TestReadVerifyUnseenCompletedReadOfANonEmptySelectionExitsOne(t *testing.T) {
	for _, corrupt := range []int{0, 1} {
		t.Run(fmt.Sprintf("corrupt=%d", corrupt), func(t *testing.T) {
			got := runUnseenRead(t, unseenReadRun{
				readLifecycle: portcheck.StepLifecycleCompleted, selected: 3, corrupt: corrupt,
			})

			requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
			message := got.err.Error()
			want := "live corruption metrics: verification READ step " + unseenReadStep +
				" completed, but its live corruption count was not observed"
			if !strings.Contains(message, want) {
				t.Fatalf("reason = %s, want containing %q", message, want)
			}
			for _, wrong := range []string{"corrupt object", "missing the required verification step", "zero objects"} {
				if strings.Contains(message, wrong) {
					t.Fatalf("reason holds %q: %s", wrong, message)
				}
			}
			if got.outcome.ObservedCorruptCount != nil || !got.outcome.LiveCorruptionUnobserved {
				t.Fatalf("observed count = %v, unobserved = %t",
					got.outcome.ObservedCorruptCount, got.outcome.LiveCorruptionUnobserved)
			}
		})
	}
}

// A READ the tracker does not say completed is neither fetched nor indexed, and keeps today's outcome.
func TestReadVerifyUnseenReadNotKnownToHaveCompletedIsNotReported(t *testing.T) {
	for _, lifecycle := range []portcheck.StepLifecycle{
		portcheck.StepLifecycleStarted, portcheck.StepLifecyclePlanned, portcheck.StepLifecycleFailed, "",
	} {
		t.Run(string(lifecycle), func(t *testing.T) {
			got := runUnseenRead(t, unseenReadRun{readLifecycle: lifecycle})

			if len(got.fetched) != 0 {
				t.Fatalf("fetched %v, want nothing", got.fetched)
			}
			requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
			message := got.err.Error()
			for _, want := range []string{
				"runtime evidence is missing the required verification step",
				"integrity result finalization did not produce an outcome",
			} {
				if !strings.Contains(message, want) {
					t.Fatalf("reason = %s, want containing %q", message, want)
				}
			}
			if got.outcome.LiveCorruptionUnobserved {
				t.Fatal("a READ not known to have completed was treated as unseen")
			}
			if _, err := os.Stat(filepath.Join(got.root, constants.ResultsManifestFileName)); !os.IsNotExist(err) {
				t.Fatalf("index.json was written without a fetch: %v", err)
			}
		})
	}
	t.Run(string(portcheck.StepLifecycleNotStarted), func(t *testing.T) {
		got := runUnseenRead(t, unseenReadRun{readLifecycle: portcheck.StepLifecycleNotStarted})
		if want := []string{unseenListStep}; !slices.Equal(got.fetched, want) {
			t.Fatalf("fetched %v, want %v", got.fetched, want)
		}
		if steps := indexedSteps(t, got.root); slices.Contains(steps, unseenReadStep) {
			t.Fatalf("index.json steps = %v, want no READ that never started", steps)
		}
		requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
		if got.outcome.LiveCorruptionUnobserved {
			t.Fatal("a READ that never started was treated as unseen")
		}
	})
}

// --allow-empty-selection keeps its handling of an unseen READ.
func TestReadVerifyUnseenCompletedReadWithAllowEmptySelectionIsUnchanged(t *testing.T) {
	got := runUnseenRead(t, unseenReadRun{allowEmpty: true, readLifecycle: portcheck.StepLifecycleCompleted})
	if want := []string{unseenListStep, unseenReadStep}; !slices.Equal(got.fetched, want) {
		t.Fatalf("fetched %v, want %v", got.fetched, want)
	}
	if got.err != nil {
		t.Fatalf("allowed clean empty selection failed: %v", got.err)
	}

	got = runUnseenRead(t, unseenReadRun{
		allowEmpty: true, readLifecycle: portcheck.StepLifecycleCompleted, selected: 2,
	})
	requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
	if message := got.err.Error(); !strings.Contains(message, "verification READ step could not be identified") ||
		strings.Contains(message, "was not observed") || got.outcome.LiveCorruptionUnobserved {
		t.Fatalf("allowed non-empty unseen READ reason changed: %s", message)
	}
}
