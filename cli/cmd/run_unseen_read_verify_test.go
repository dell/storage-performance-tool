package cmd

import (
	"bytes"
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
	"sync"
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
	unseenReadRunID  = int64(4242)
	unseenListStep   = "mt-001-20261114.100000.000-list"
	unseenCreateStep = "mt-001-20261114.100000.000-create"
	unseenReadStep   = "mt-002-20261114.100000.000-verify"
	// unseenSoleReadStep is the READ of a read-verify over an --items-file, its only step.
	unseenSoleReadStep = "mt-001-20261114.100000.000-verify"
)

// unseenPlanKind names the verification route a test run takes.
type unseenPlanKind int

const (
	unseenReadDiscovered unseenPlanKind = iota // read-verify of a LIST selection
	unseenReadExternal                         // read-verify of an --items-file
	unseenWriteRead                            // write-verify
)

// unseenReadRun describes a verification run whose steps the live step list may not have shown.
type unseenReadRun struct {
	kind              unseenPlanKind
	allowEmpty        bool
	observed          []string // runtime step IDs from discovery; nil leaves only the planned IDs
	readLifecycle     portcheck.StepLifecycle
	producerLifecycle portcheck.StepLifecycle // the tracker's LIST or CREATE lifecycle; empty means completed
	selected          int                     // objects the producer selected
	corrupt           int                     // corrupt READs recorded in the READ step's files
	readFailures      int                     // READs that failed with an ordinary error, not corruption
	liveReadRow       bool                    // live metrics still hold the READ row, with corrupt as its count
}

type unseenReadResult struct {
	outcome  autoResultsOutcome
	fetched  []string
	root     string
	progress string
	err      error
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (run unseenReadRun) steps() (producer, read string) {
	switch run.kind {
	case unseenReadExternal:
		return "", unseenSoleReadStep
	case unseenWriteRead:
		return unseenCreateStep, unseenReadStep
	default:
		return unseenListStep, unseenReadStep
	}
}

func (run unseenReadRun) workload() string {
	if run.kind == unseenWriteRead {
		return scenario.WorkloadTypeWriteVerify
	}
	return scenario.WorkloadTypeReadVerify
}

func (run unseenReadRun) plan() integrityplan.Plan {
	producer, read := run.steps()
	plan := integrityplan.Plan{RunID: unseenReadRunID, Workload: run.workload(), AllowEmpty: run.allowEmpty}
	switch run.kind {
	case unseenReadExternal:
		plan.Kind, plan.Input = integrityplan.PlanKindReadExternal, integrityplan.InputExternal
		plan.Verifier = integrityplan.PlannedStep{ID: read, Number: 1, Role: integrityplan.StepRoleVerify}
	case unseenWriteRead:
		plan.Kind, plan.Input = integrityplan.PlanKindWriteRead, integrityplan.InputWritten
		plan.Producer = &integrityplan.PlannedStep{ID: producer, Number: 1, Role: integrityplan.StepRoleCreate}
		plan.Verifier = integrityplan.PlannedStep{ID: read, Number: 2, Role: integrityplan.StepRoleVerify}
	default:
		plan.Kind, plan.Input = integrityplan.PlanKindReadDiscovered, integrityplan.InputDiscovered
		plan.Producer = &integrityplan.PlannedStep{ID: producer, Number: 1, Role: integrityplan.StepRoleList}
		plan.Verifier = integrityplan.PlannedStep{ID: read, Number: 2, Role: integrityplan.StepRoleVerify}
	}
	return plan
}

// runUnseenRead drives the real auto-results monitor, integrity finalization and exit resolution.
// Discovery returns run.observed, the engine's live metrics hold a READ row only when run.liveReadRow
// is set, and the fetcher writes the step files a real engine leaves for the steps it is asked for.
func runUnseenRead(t *testing.T, run unseenReadRun) unseenReadResult {
	t.Helper()
	producer, read := run.steps()
	installAutoResultsFakes(t, run.observed)
	producerLifecycle := run.producerLifecycle
	if producerLifecycle == "" {
		producerLifecycle = portcheck.StepLifecycleCompleted
	}
	steps := map[string]portcheck.StepCompletion{
		read: {StepID: read, Lifecycle: run.readLifecycle, Planned: true},
	}
	if producer != "" {
		steps[producer] = portcheck.StepCompletion{
			StepID: producer, Lifecycle: producerLifecycle, Planned: true, Started: true,
		}
	}
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &fakeRunTracker{result: &portcheck.RunResult{
			FinalState: constants.StateCompleted, RunID: unseenReadRunID, Steps: steps,
		}}
	}
	liveRows := "[]"
	if run.liveReadRow {
		liveRows = fmt.Sprintf(`[{"step_id":%q,"op_type":"READ","operations":{"corrupt_count":%d}}]`, read, run.corrupt)
	}
	metrics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			_, _ = io.WriteString(w, liveRows)
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
	plan := run.plan()
	if !plan.Valid() {
		t.Fatalf("invalid plan %+v", plan)
	}
	options := &integrity.FinalizeOptions{
		Workload: run.workload(), RunID: unseenReadRunID, AllowEmptySelection: run.allowEmpty, Plan: plan.Clone(),
	}
	if run.kind == unseenReadExternal {
		source := filepath.Join(t.TempDir(), "items.csv")
		if err := writeUnseenCSV(source, unseenManifestRows(run.selected)); err != nil {
			t.Fatal(err)
		}
		staging, manifest, completion, err := integrity.StageInputManifest(source, unseenReadRunID)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(staging) })
		options.StagedManifest, options.StagedCompletion = manifest, completion
	}
	root := t.TempDir()
	progress := &lockedBuffer{}
	monitor := startAutoResultsMonitor(
		context.Background(), metrics.URL, "mt", root, plan.StepIDs(), unseenReadRunID,
		false, nil, "", false, 0, "", &runMetadata{ResultsRoot: root}, progress, io.Discard, "", nil, options)
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
		WorkloadType: run.workload(), AllowEmptySelection: run.allowEmpty,
	})
	return unseenReadResult{outcome: outcome, fetched: fetched, root: root, progress: progress.String(), err: err}
}

func unseenManifestRows(count int) [][]string {
	rows := [][]string{{"bucket", "key", "size", "version_id"}}
	for i := range count {
		rows = append(rows, []string{"b", fmt.Sprintf("k%03d", i), "1", ""})
	}
	return rows
}

func writeUnseenReadStepFiles(root string, stepIDs []string, run unseenReadRun) error {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return err
	}
	performanceHeader := []string{
		"node", "step", "driver", "phase", "algorithm", "objects", "bytes", "hash_worker_seconds",
		"mean_worker_hash_mib_per_second", "time_to_first_request_seconds", "additional_payload_passes",
	}
	index := results.Manifest{}
	for _, step := range stepIDs {
		index.Steps = append(index.Steps, results.StepManifest{StepID: step})
		var err error
		switch step {
		case unseenListStep:
			err = errors.Join(
				writeUnseenCommittedPair(root, step, integrity.VerifyInputName, unseenManifestRows(run.selected)),
				writeUnseenCSV(filepath.Join(root, step+".metrics.total.csv"), [][]string{
					{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"LIST", "1", "0", "0"},
				}))
		case unseenCreateStep:
			err = errors.Join(
				writeUnseenCommittedPair(root, step, integrity.WrittenName, unseenManifestRows(run.selected)),
				writeUnseenCSV(filepath.Join(root, step+".metrics.total.csv"), [][]string{
					{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"CREATE", fmt.Sprint(run.selected), "0", "0"},
				}),
				writeUnseenCSV(filepath.Join(root, step+"."+integrity.IntegrityPerformanceName), [][]string{performanceHeader}))
		case unseenReadStep, unseenSoleReadStep:
			verified := run.selected - run.corrupt - run.readFailures
			failures := [][]string{{
				"timestamp", "node", "step", "driver", "key", "requested_version_id", "returned_version_id",
				"request_id", "reason", "algorithm", "expected_digest", "actual_digest", "expected_size",
				"actual_size", "first_mismatch_offset", "attempt",
			}}
			for i := verified + run.readFailures; i < run.selected; i++ {
				failures = append(failures, []string{
					"2026-11-14T10:00:00Z", "n0", step, "s3", fmt.Sprintf("k%03d", i), "", "", "",
					"digest_mismatch", "sha256", "aa", "bb", "1", "1", "0", "1",
				})
			}
			err = errors.Join(
				writeUnseenCommittedPair(root, step, integrity.VerifiedName, unseenManifestRows(verified)),
				writeUnseenCSV(filepath.Join(root, step+".metrics.total.csv"), [][]string{
					{"OpType", "CountSucc", "CountFail", "CountCorrupt"},
					{"READ", fmt.Sprint(verified), fmt.Sprint(run.corrupt + run.readFailures), fmt.Sprint(run.corrupt)},
				}),
				writeUnseenCSV(filepath.Join(root, step+"."+integrity.IntegrityFailuresName), failures),
				writeUnseenCSV(filepath.Join(root, step+"."+integrity.IntegrityPerformanceName), [][]string{performanceHeader}))
		default:
			err = fmt.Errorf("unexpected step %q", step)
		}
		if err != nil {
			return err
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

func requireReasons(t *testing.T, err error, want, wrong []string) {
	t.Helper()
	message := err.Error()
	for _, text := range want {
		if !strings.Contains(message, text) {
			t.Fatalf("reason = %s, want containing %q", message, text)
		}
	}
	for _, text := range wrong {
		if strings.Contains(message, text) {
			t.Fatalf("reason holds %q: %s", text, message)
		}
	}
}

func requireEmptySelectionReason(t *testing.T, got unseenReadResult) {
	t.Helper()
	requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
	requireReasons(t, got.err, []string{"verification selected zero objects"}, []string{
		"missing the required verification step", "could not be identified", "live corruption",
		"did not produce an outcome", "artifact retrieval", "finalization",
	})
	final := got.outcome.Finalization
	if final == nil || !final.Complete || !final.EmptySelection || final.EmptyAllowed ||
		final.VerificationAttemptedCount != 0 || got.outcome.LiveCorruptionUnobserved {
		t.Fatalf("finalization = %+v, unobserved = %t", final, got.outcome.LiveCorruptionUnobserved)
	}
}

func unobservedReason(read string) string {
	return "live corruption metrics: verification READ step " + read +
		" completed, but its live corruption count was not observed"
}

// Without --allow-empty-selection, a READ of an empty selection that ended between two polls must still
// give the real reason, not a missing-evidence error.
func TestReadVerifyUnseenCompletedReadOfAnEmptySelectionReportsZeroObjects(t *testing.T) {
	got := runUnseenRead(t, unseenReadRun{
		observed: []string{unseenListStep}, readLifecycle: portcheck.StepLifecycleCompleted,
	})

	if want := []string{unseenListStep, unseenReadStep}; !slices.Equal(got.fetched, want) {
		t.Fatalf("fetched %v, want %v", got.fetched, want)
	}
	requireEmptySelectionReason(t, got)
	if steps := indexedSteps(t, got.root); !slices.Contains(steps, unseenReadStep) {
		t.Fatalf("index.json steps = %v, want the completed READ", steps)
	}
	want := "Auto-results: fetching 2 step(s): " + unseenListStep + ", " + unseenReadStep + "\n"
	if !strings.Contains(got.progress, want) {
		t.Fatalf("progress = %q, want the line %q", got.progress, want)
	}
}

// A LIST with nothing to list can also end between two polls while the READ is seen. The local-mode
// check hit this on an empty bucket: only the READ was fetched, and finalization had no LIST files.
func TestReadVerifyUnseenCompletedListOfAnEmptySelectionReportsZeroObjects(t *testing.T) {
	for _, allowEmpty := range []bool{false, true} {
		t.Run(fmt.Sprintf("allowEmpty=%t", allowEmpty), func(t *testing.T) {
			got := runUnseenRead(t, unseenReadRun{
				allowEmpty: allowEmpty, observed: []string{unseenReadStep},
				readLifecycle: portcheck.StepLifecycleCompleted,
			})

			if want := []string{unseenListStep, unseenReadStep}; !slices.Equal(got.fetched, want) {
				t.Fatalf("fetched %v, want %v", got.fetched, want)
			}
			if allowEmpty {
				if got.err != nil {
					t.Fatalf("allowed clean empty selection failed: %v", got.err)
				}
				return
			}
			requireEmptySelectionReason(t, got)
		})
	}
}

// A non-empty selection whose READ went unseen keeps failing safe: exit 1, no corruption count from
// its files, and a reason that says the live count was not observed.
func TestReadVerifyUnseenCompletedReadOfANonEmptySelectionExitsOne(t *testing.T) {
	for _, kind := range []unseenPlanKind{unseenReadDiscovered, unseenWriteRead} {
		for _, corrupt := range []int{0, 1} {
			t.Run(fmt.Sprintf("kind=%d/corrupt=%d", kind, corrupt), func(t *testing.T) {
				run := unseenReadRun{
					kind: kind, readLifecycle: portcheck.StepLifecycleCompleted, selected: 3, corrupt: corrupt,
				}
				producer, read := run.steps()
				run.observed = []string{producer}
				got := runUnseenRead(t, run)

				if want := []string{producer, read}; !slices.Equal(got.fetched, want) {
					t.Fatalf("fetched %v, want %v", got.fetched, want)
				}
				requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
				requireReasons(t, got.err, []string{unobservedReason(read)},
					[]string{"corrupt object", "missing the required verification step", "zero objects"})
				if got.outcome.ObservedCorruptCount != nil || !got.outcome.LiveCorruptionUnobserved {
					t.Fatalf("observed count = %v, unobserved = %t",
						got.outcome.ObservedCorruptCount, got.outcome.LiveCorruptionUnobserved)
				}
			})
		}
	}
}

// When discovery shows no step at all, the run falls back to the planned IDs and already fetched and
// finalized them before this change. A corrupt READ there keeps exiting 20.
func TestVerificationWithNoDiscoveredStepKeepsTheCorruptionExit(t *testing.T) {
	for _, kind := range []unseenPlanKind{unseenReadDiscovered, unseenReadExternal, unseenWriteRead} {
		for _, live := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind=%d/liveRow=%t", kind, live), func(t *testing.T) {
				run := unseenReadRun{
					kind: kind, readLifecycle: portcheck.StepLifecycleCompleted, selected: 3, corrupt: 1,
					liveReadRow: live,
				}
				got := runUnseenRead(t, run)

				if want := run.plan().StepIDs(); !slices.Equal(got.fetched, want) {
					t.Fatalf("fetched %v, want the planned %v", got.fetched, want)
				}
				requireExitCode(t, got.err, constants.ExitCodeIntegrityCorruption)
				requireReasons(t, got.err, []string{"detected 1 corrupt object(s)"}, []string{"was not observed"})
				if got.outcome.LiveCorruptionUnobserved {
					t.Fatal("the planned-ID fallback was treated as an unseen READ")
				}
			})
		}
	}
}

// A binding that fails for another reason, such as conflicting runtime IDs, keeps today's reason.
func TestReadVerifyBindingFailureKeepsItsReason(t *testing.T) {
	got := runUnseenRead(t, unseenReadRun{
		observed:      []string{unseenListStep, "mt-001-20261114.110000.000-list"},
		readLifecycle: portcheck.StepLifecycleCompleted,
	})

	if len(got.fetched) != 0 {
		t.Fatalf("fetched %v, want nothing", got.fetched)
	}
	requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
	requireReasons(t, got.err,
		[]string{"verification READ step could not be identified", "conflicting runtime step identities"},
		[]string{"was not observed"})
	if got.outcome.LiveCorruptionUnobserved {
		t.Fatal("a failed binding was treated as an unseen READ")
	}
}

// A READ the tracker does not say completed is neither fetched nor indexed, and keeps today's outcome.
func TestReadVerifyUnseenReadNotKnownToHaveCompletedIsNotReported(t *testing.T) {
	for _, lifecycle := range []portcheck.StepLifecycle{
		portcheck.StepLifecycleStarted, portcheck.StepLifecyclePlanned, portcheck.StepLifecycleFailed, "",
	} {
		t.Run(string(lifecycle), func(t *testing.T) {
			got := runUnseenRead(t, unseenReadRun{observed: []string{unseenListStep}, readLifecycle: lifecycle})

			if len(got.fetched) != 0 {
				t.Fatalf("fetched %v, want nothing", got.fetched)
			}
			requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
			requireReasons(t, got.err, []string{
				"runtime evidence is missing the required verification step",
				"integrity result finalization did not produce an outcome",
			}, nil)
			if got.outcome.LiveCorruptionUnobserved {
				t.Fatal("a READ not known to have completed was treated as unseen")
			}
			if _, err := os.Stat(filepath.Join(got.root, constants.ResultsManifestFileName)); !os.IsNotExist(err) {
				t.Fatalf("index.json was written without a fetch: %v", err)
			}
		})
	}
	t.Run(string(portcheck.StepLifecycleNotStarted), func(t *testing.T) {
		got := runUnseenRead(t, unseenReadRun{
			observed: []string{unseenListStep}, readLifecycle: portcheck.StepLifecycleNotStarted,
		})
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

// An unseen producer the tracker does not say completed is not fetched either.
func TestReadVerifyUnseenListNotKnownToHaveCompletedIsNotFetched(t *testing.T) {
	for _, lifecycle := range []portcheck.StepLifecycle{
		portcheck.StepLifecycleStarted, portcheck.StepLifecycleFailed, portcheck.StepLifecyclePlanned,
	} {
		t.Run(string(lifecycle), func(t *testing.T) {
			got := runUnseenRead(t, unseenReadRun{
				observed: []string{unseenReadStep}, readLifecycle: portcheck.StepLifecycleCompleted,
				producerLifecycle: lifecycle,
			})
			if want := []string{unseenReadStep}; !slices.Equal(got.fetched, want) {
				t.Fatalf("fetched %v, want %v", got.fetched, want)
			}
			requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
			if got.outcome.ProducerRecovered {
				t.Fatal("a producer not known to have completed was recovered")
			}
		})
	}
}

// --allow-empty-selection keeps its handling of an unseen READ.
func TestReadVerifyUnseenCompletedReadWithAllowEmptySelectionIsUnchanged(t *testing.T) {
	got := runUnseenRead(t, unseenReadRun{
		allowEmpty: true, observed: []string{unseenListStep}, readLifecycle: portcheck.StepLifecycleCompleted,
	})
	if want := []string{unseenListStep, unseenReadStep}; !slices.Equal(got.fetched, want) {
		t.Fatalf("fetched %v, want %v", got.fetched, want)
	}
	if got.err != nil {
		t.Fatalf("allowed clean empty selection failed: %v", got.err)
	}

	got = runUnseenRead(t, unseenReadRun{
		allowEmpty: true, observed: []string{unseenListStep}, readLifecycle: portcheck.StepLifecycleCompleted,
		selected: 2,
	})
	requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
	requireReasons(t, got.err, []string{"verification READ step could not be identified"}, []string{"was not observed"})
	if got.outcome.LiveCorruptionUnobserved {
		t.Fatal("an allowed run was marked unobserved")
	}
}

// A missed producer of a non-empty selection whose READ was seen, with its live count, now finalizes
// from the producer's files. A clean, fully verified selection passes, where the missing producer
// files used to fail it with exit 1. A corrupt one exits 20, as it already did; only the secondary
// finalization error about the producer's files goes away.
func TestVerificationUnseenCompletedProducerOfANonEmptySelectionFinalizes(t *testing.T) {
	for _, kind := range []unseenPlanKind{unseenReadDiscovered, unseenWriteRead} {
		for _, corrupt := range []int{0, 1} {
			t.Run(fmt.Sprintf("kind=%d/corrupt=%d", kind, corrupt), func(t *testing.T) {
				run := unseenReadRun{
					kind: kind, readLifecycle: portcheck.StepLifecycleCompleted, selected: 3, corrupt: corrupt,
					liveReadRow: true,
				}
				producer, read := run.steps()
				run.observed = []string{read}
				got := runUnseenRead(t, run)

				if want := []string{producer, read}; !slices.Equal(got.fetched, want) {
					t.Fatalf("fetched %v, want %v", got.fetched, want)
				}
				if corrupt == 0 {
					if got.err != nil {
						t.Fatalf("clean selection failed: %v", got.err)
					}
					return
				}
				requireExitCode(t, got.err, constants.ExitCodeIntegrityCorruption)
			})
		}
	}
}

// unverifiedReason is the exit-1 reason for a complete selection that left objects unverified.
func unverifiedReason(verified, selected int) string {
	verb := "remain"
	if selected-verified == 1 {
		verb = "remains"
	}
	return fmt.Sprintf("verification verified %d of %d selected objects; %d %s unverified",
		verified, selected, selected-verified, verb)
}

// A recovered producer must not let a selection pass when ordinary READ failures, which are not
// corruption, left some selected objects unverified.
func TestVerificationRecoveredProducerRequiresEveryObjectVerified(t *testing.T) {
	for _, kind := range []unseenPlanKind{unseenReadDiscovered, unseenWriteRead} {
		for _, verified := range []int{0, 2} {
			t.Run(fmt.Sprintf("kind=%d/verified=%d", kind, verified), func(t *testing.T) {
				run := unseenReadRun{
					kind: kind, readLifecycle: portcheck.StepLifecycleCompleted, selected: 3,
					readFailures: 3 - verified, liveReadRow: true,
				}
				producer, read := run.steps()
				run.observed = []string{read}
				got := runUnseenRead(t, run)

				requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
				requireReasons(t, got.err, []string{unverifiedReason(verified, 3)}, []string{"corrupt object"})
				if want := []string{producer, read}; !slices.Equal(got.fetched, want) {
					t.Fatalf("fetched %v, want %v", got.fetched, want)
				}
				if !got.outcome.ProducerRecovered {
					t.Fatal("the missed producer was not marked recovered")
				}
			})
		}
	}
}

// Every route, not only a recovered producer, exits 1 when ordinary READ failures, which are not
// corruption, left selected objects unverified: S3_INTEGRITY.md says a verify run exits 1 when its
// selection was not fully verified. Every step is seen live with a live corruption count of 0. A
// fully verified selection still passes.
func TestVerificationEveryStepSeenRequiresEveryObjectVerified(t *testing.T) {
	for _, kind := range []unseenPlanKind{unseenReadDiscovered, unseenReadExternal, unseenWriteRead} {
		for _, verified := range []int{0, 2, 3} {
			t.Run(fmt.Sprintf("kind=%d/verified=%d", kind, verified), func(t *testing.T) {
				run := unseenReadRun{
					kind: kind, readLifecycle: portcheck.StepLifecycleCompleted,
					producerLifecycle: portcheck.StepLifecycleCompleted, selected: 3,
					readFailures: 3 - verified, liveReadRow: true,
				}
				producer, read := run.steps()
				run.observed = []string{read}
				if producer != "" {
					run.observed = []string{producer, read}
				}
				got := runUnseenRead(t, run)

				if verified == 3 {
					if got.err != nil {
						t.Fatalf("fully verified selection failed: %v", got.err)
					}
				} else {
					requireExitCode(t, got.err, constants.ExitCodeWorkloadFailure)
					requireReasons(t, got.err, []string{unverifiedReason(verified, 3)}, []string{"corrupt object"})
				}
				final := got.outcome.Finalization
				if final == nil || !final.Complete || final.SelectionCount != 3 ||
					final.VerifiedCount != int64(verified) || final.CorruptCount != 0 {
					t.Fatalf("finalization = %+v", final)
				}
				if got.outcome.ProducerRecovered || got.outcome.LiveCorruptionUnobserved {
					t.Fatalf("recovered = %t, unobserved = %t",
						got.outcome.ProducerRecovered, got.outcome.LiveCorruptionUnobserved)
				}
			})
		}
	}
}
