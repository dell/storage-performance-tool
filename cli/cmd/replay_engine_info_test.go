package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

func TestReplayRoutesInstallCurrentParticipantIdentityGate(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()

	tests := []struct {
		name     string
		hosts    string
		headless bool
		wantPath string
		wantRole engineinfo.ParticipantRole
	}{
		{name: "local headless", hosts: "127.0.0.1", headless: true, wantPath: "local-headless", wantRole: engineinfo.RoleStandalone},
		{name: "local tui", hosts: "127.0.0.1", wantPath: "local-tui", wantRole: engineinfo.RoleStandalone},
		{name: "single remote headless", hosts: "entry.example", headless: true, wantPath: "remote-headless", wantRole: engineinfo.RoleEntry},
		{name: "single remote tui", hosts: "entry.example", wantPath: "remote-tui", wantRole: engineinfo.RoleEntry},
		{name: "multi worker headless", hosts: "entry.example,worker.example", headless: true, wantPath: "remote-headless", wantRole: engineinfo.RoleEntry},
		{name: "multi worker tui", hosts: "entry.example,worker.example", wantPath: "remote-tui", wantRole: engineinfo.RoleEntry},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := installReplayEngineInfoSeams(t)
			defer restore()

			port := "9999"
			standalone := replayDescriptor(t, "127.0.0.1", port, engineinfo.RoleStandalone)
			entry := replayDescriptor(t, "entry.example", port, engineinfo.RoleEntry)
			worker := replayDescriptor(t, "worker.example", port, engineinfo.RoleWorker)
			wantDescriptors := []engineinfo.ParticipantDescriptor{standalone}
			if test.wantRole == engineinfo.RoleEntry {
				wantDescriptors = []engineinfo.ParticipantDescriptor{entry}
				if strings.Contains(test.hosts, ",") {
					wantDescriptors = append(wantDescriptors, worker)
				}
			}
			replayLocalEnginePlan = func(string) ([]engineinfo.ParticipantDescriptor, error) {
				return []engineinfo.ParticipantDescriptor{standalone}, nil
			}
			replayDistributedEnginePlan = func(*tui.MultiHostOrchestrator, string, bool) ([]engineinfo.ParticipantDescriptor, error) {
				return append([]engineinfo.ParticipantDescriptor(nil), wantDescriptors...), nil
			}

			var collectorCalls atomic.Int32
			newReplayEngineIdentityCollector = func() engineinfo.FleetCollector {
				return &replayGateCollector{
					t: t, wantDescriptors: wantDescriptors, calls: &collectorCalls,
					fleet: replayFleetForDescriptors(wantDescriptors, engineinfo.ConsistencyConsistent, engineinfo.StatusCollected),
				}
			}

			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			confirmReplayLaunchCommand = func(io.Writer) error { return nil }
			shouldReplayRunHeadless = func(*cobra.Command) bool { return test.headless }

			var submissions atomic.Int32
			var out bytes.Buffer
			runHook := func(ctx context.Context, hooks tui.LaunchHooks) error {
				return executeReplayHookForTest(ctx, hooks, &out, &submissions)
			}
			startReplayLocalHeadless = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte) error {
				if test.wantPath != "local-headless" {
					t.Fatalf("unexpected local headless route for %s", test.wantPath)
				}
				return runHook(options.Context, options.LaunchHooks)
			}
			startReplayLocalTUI = func(_ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				if test.wantPath != "local-tui" {
					t.Fatalf("unexpected local TUI route for %s", test.wantPath)
				}
				return runHook(options.Context, options.LaunchHooks)
			}
			startReplayRemoteHeadless = func(_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte) error {
				if test.wantPath != "remote-headless" {
					t.Fatalf("unexpected remote headless route for %s", test.wantPath)
				}
				return runHook(options.Context, options.LaunchHooks)
			}
			startReplayRemoteTUI = func(_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				if test.wantPath != "remote-tui" {
					t.Fatalf("unexpected remote TUI route for %s", test.wantPath)
				}
				return runHook(options.Context, options.LaunchHooks)
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(replayIdentityArgs(server.URL, test.hosts, t.TempDir(), false, test.headless, false))
			if err := cmd.Execute(); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if collectorCalls.Load() != 1 || submissions.Load() != 1 {
				t.Fatalf("collector/submission calls = %d/%d, want 1/1", collectorCalls.Load(), submissions.Load())
			}
			if !strings.Contains(out.String(), "Engine identity: consistent") {
				t.Fatalf("output missing fleet result:\n%s", out.String())
			}
		})
	}
}

func TestReplayIdentityGatePolicyAndPartialEvidence(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()

	tests := []struct {
		name             string
		fleet            engineinfo.FleetResult
		collectErr       error
		force            bool
		autoResults      bool
		wantErr          bool
		wantSubmit       int32
		wantOutput       string
		wantArtifact     bool
		wantForced       bool
		wantArtifactKind engineinfo.GateDecision
	}{
		{name: "consistent", fleet: runGateFleet(engineinfo.ConsistencyConsistent, engineinfo.StatusCollected), wantSubmit: 1, wantOutput: "consistent"},
		{name: "mismatch rejected", fleet: runGateFleet(engineinfo.ConsistencyMismatch, engineinfo.StatusCollected), autoResults: true, wantErr: true, wantOutput: "mismatch rejected", wantArtifact: true, wantArtifactKind: engineinfo.GateRejectedMismatch},
		{name: "mismatch forced", fleet: runGateFleet(engineinfo.ConsistencyMismatch, engineinfo.StatusCollected), force: true, autoResults: true, wantSubmit: 1, wantOutput: "MISMATCH FORCED", wantArtifact: true, wantForced: true, wantArtifactKind: engineinfo.GateProceed},
		{name: "legacy continues", fleet: runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusLegacyEndpointUnavailable), wantSubmit: 1, wantOutput: "legacy_endpoint_unavailable"},
		{name: "future schema continues", fleet: replayFutureSchemaFleet(), wantSubmit: 1, wantOutput: "unsupported_schema"},
		{name: "incomplete continues", fleet: runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusIncompleteBuildInfo), wantSubmit: 1, wantOutput: "incomplete_build_info"},
		{name: "malformed supported response is not forceable", fleet: runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusCollectionFailed), collectErr: errors.New("malformed schema 1"), force: true, autoResults: true, wantErr: true, wantOutput: "collection failed", wantArtifact: true, wantArtifactKind: engineinfo.GateCollectionFailure},
		{name: "transport exhaustion is not forceable", fleet: runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusCollectionFailed), collectErr: errors.New("transport exhausted"), force: true, wantErr: true, wantOutput: "collection failed"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := installReplayEngineInfoSeams(t)
			defer restore()
			entry := replayDescriptor(t, "entry.example", "9999", engineinfo.RoleEntry)
			worker := replayDescriptor(t, "worker.example", "9999", engineinfo.RoleWorker)
			wantDescriptors := []engineinfo.ParticipantDescriptor{entry, worker}
			replayDistributedEnginePlan = func(*tui.MultiHostOrchestrator, string, bool) ([]engineinfo.ParticipantDescriptor, error) {
				return wantDescriptors, nil
			}
			newReplayEngineIdentityCollector = func() engineinfo.FleetCollector {
				return &replayGateCollector{t: t, wantDescriptors: wantDescriptors, fleet: test.fleet, err: test.collectErr}
			}
			connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			shouldReplayRunHeadless = func(*cobra.Command) bool { return true }

			var submissions atomic.Int32
			var out bytes.Buffer
			startReplayRemoteHeadless = func(_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte) error {
				return executeReplayHookForTest(options.Context, options.LaunchHooks, &out, &submissions)
			}
			var monitoredMetadata *runMetadata
			if test.autoResults {
				if test.wantForced {
					startReplayAutoResultsMonitor = func(
						_ context.Context, _, _, _ string, _ []string, _ int64, _ bool, _ []*hostparse.HostInfo,
						_ string, _ bool, _ int, _ string, metadata *runMetadata, _, _ io.Writer, _ string,
						_ func(context.Context), _ ...*integrity.FinalizeOptions,
					) *autoResultsMonitor {
						monitoredMetadata = metadata
						monitor := &autoResultsMonitor{done: make(chan autoResultsOutcome, 1), armed: make(chan struct{})}
						go func() {
							<-monitor.armed
							monitor.done <- autoResultsOutcome{ArtifactErr: writeRunMetadata(metadata, metadata.ResultsRoot)}
						}()
						return monitor
					}
				} else {
					startReplayAutoResultsMonitor = immediateCancelableReplayMonitor
				}
			}

			resultsDir := filepath.Join(t.TempDir(), "results")
			const replayRunID = int64(606060)
			newReplayRunID = func() int64 { return replayRunID }
			cmd := newReplayCommandForTest(t)
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(replayIdentityArgs(server.URL, "entry.example,worker.example", resultsDir, test.autoResults, true, test.force))
			err := cmd.Execute()
			if (err != nil) != test.wantErr {
				t.Fatalf("Execute() error = %v, wantErr=%t", err, test.wantErr)
			}
			if submissions.Load() != test.wantSubmit {
				t.Fatalf("submission calls = %d, want %d", submissions.Load(), test.wantSubmit)
			}
			if !strings.Contains(out.String(), test.wantOutput) {
				t.Fatalf("output missing %q:\n%s", test.wantOutput, out.String())
			}

			manifestPaths, globErr := filepath.Glob(filepath.Join(resultsDir, "replay-*", "engine.info.json"))
			if globErr != nil {
				t.Fatalf("glob manifest: %v", globErr)
			}
			if len(manifestPaths) != boolInt(test.wantArtifact) {
				t.Fatalf("manifest paths = %v, want artifact=%t", manifestPaths, test.wantArtifact)
			}
			if test.wantArtifact {
				manifest := readReplayAbortManifest(t, manifestPaths[0])
				if manifest.RunID != replayRunID || len(manifest.Participants) != len(test.fleet.Participants) {
					t.Fatalf("manifest run/participants = %d/%d, want %d/%d", manifest.RunID, len(manifest.Participants), replayRunID, len(test.fleet.Participants))
				}
				wantReason := "engine identity collection failed before scenario submission"
				if test.wantArtifactKind == engineinfo.GateRejectedMismatch {
					wantReason = "engine build identity mismatch rejected before scenario submission"
				} else if test.wantArtifactKind == engineinfo.GateProceed {
					wantReason = "engine build identity mismatch overridden by --force"
				}
				if manifest.Consistency.Reason != wantReason {
					t.Fatalf("manifest reason = %q, want %q", manifest.Consistency.Reason, wantReason)
				}
			}
			if test.wantForced {
				if monitoredMetadata == nil || monitoredMetadata.engineIdentity == nil {
					t.Fatal("forced current-fleet assessment was not retained in replay metadata")
				}
				consistency := monitoredMetadata.engineIdentity.Fleet.Consistency
				if consistency.Status != engineinfo.ConsistencyMismatch || !consistency.Forced ||
					consistency.Reason != "engine build identity mismatch overridden by --force" {
					t.Fatalf("forced replay consistency = %+v", consistency)
				}
			}
		})
	}
}

func TestReplayIdentityGateCancellationStopsBeforeSubmission(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	restore := installReplayEngineInfoSeams(t)
	defer restore()

	standalone := replayDescriptor(t, "127.0.0.1", "9999", engineinfo.RoleStandalone)
	replayLocalEnginePlan = func(string) ([]engineinfo.ParticipantDescriptor, error) {
		return []engineinfo.ParticipantDescriptor{standalone}, nil
	}
	newReplayEngineIdentityCollector = func() engineinfo.FleetCollector {
		return replayCancelCollector{}
	}
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
	var submissions atomic.Int32
	startReplayLocalHeadless = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte) error {
		ctx, cancel := context.WithCancel(options.Context)
		cancel()
		return executeReplayHookForTest(ctx, options.LaunchHooks, io.Discard, &submissions)
	}

	cmd := newReplayCommandForTest(t)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(replayIdentityArgs(server.URL, "127.0.0.1", t.TempDir(), false, true, true))
	err := cmd.Execute()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context canceled", err)
	}
	if submissions.Load() != 0 {
		t.Fatalf("submission calls = %d, want 0", submissions.Load())
	}
}

func TestReplaySourceEngineIdentityIsProvenanceOnly(t *testing.T) {
	var sourceIdentityFetches atomic.Int32
	server := newReplayArchiveServerWithSourceIdentity(t, &sourceIdentityFetches)
	defer server.Close()
	restore := installReplayEngineInfoSeams(t)
	defer restore()

	standalone := replayDescriptor(t, "127.0.0.1", "9999", engineinfo.RoleStandalone)
	replayLocalEnginePlan = func(string) ([]engineinfo.ParticipantDescriptor, error) {
		return []engineinfo.ParticipantDescriptor{standalone}, nil
	}
	currentFleet := replayFleetForDescriptors([]engineinfo.ParticipantDescriptor{standalone}, engineinfo.ConsistencyConsistent, engineinfo.StatusCollected)
	newReplayEngineIdentityCollector = func() engineinfo.FleetCollector {
		return &replayGateCollector{t: t, wantDescriptors: []engineinfo.ParticipantDescriptor{standalone}, fleet: currentFleet}
	}
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
	var submissions atomic.Int32
	startReplayLocalHeadless = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte) error {
		return executeReplayHookForTest(options.Context, options.LaunchHooks, io.Discard, &submissions)
	}
	const replayRunID = int64(606061)
	newReplayRunID = func() int64 { return replayRunID }

	outputDir := filepath.Join(t.TempDir(), "generated")
	resultsDir := filepath.Join(t.TempDir(), "identity-only-results")
	cmd := newReplayCommandForTest(t)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--from", server.URL, "--endpoints", "http://s3.example", "--test-hosts", "127.0.0.1",
		"--headless", "--auto-results=false", "--output-dir", outputDir, "--results-dir", resultsDir,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if submissions.Load() != 1 {
		t.Fatalf("submission calls = %d, want 1", submissions.Load())
	}
	if sourceIdentityFetches.Load() != 0 {
		t.Fatalf("source engine.info.json fetched %d time(s); source identity must not enter the current fleet gate", sourceIdentityFetches.Load())
	}
	metadataData, err := os.ReadFile(filepath.Join(outputDir, "replay-metadata.json"))
	if err != nil {
		t.Fatalf("read replay metadata: %v", err)
	}
	var metadata struct {
		SourceURL string `json:"sourceUrl"`
		RunID     int64  `json:"runId"`
	}
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		t.Fatalf("parse replay metadata: %v", err)
	}
	if metadata.SourceURL != server.URL+"/" {
		t.Fatalf("source provenance URL = %q, want %q", metadata.SourceURL, server.URL+"/")
	}
	if metadata.RunID != replayRunID {
		t.Fatalf("replay metadata run ID = %d, want new replay run ID %d", metadata.RunID, replayRunID)
	}
	if _, err := os.Stat(resultsDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("auto-results disabled created identity-only results directory: %v", err)
	}
}

type replayGateCollector struct {
	t               *testing.T
	wantDescriptors []engineinfo.ParticipantDescriptor
	fleet           engineinfo.FleetResult
	err             error
	calls           *atomic.Int32
}

func (c *replayGateCollector) Collect(ctx context.Context, descriptors []engineinfo.ParticipantDescriptor) (engineinfo.FleetResult, error) {
	c.t.Helper()
	if c.calls != nil {
		c.calls.Add(1)
	}
	if !reflect.DeepEqual(descriptors, c.wantDescriptors) {
		c.t.Fatalf("current replay descriptors = %#v, want %#v", descriptors, c.wantDescriptors)
	}
	if err := ctx.Err(); err != nil {
		return c.fleet, err
	}
	return c.fleet, c.err
}

type replayCancelCollector struct{}

func (replayCancelCollector) Collect(ctx context.Context, _ []engineinfo.ParticipantDescriptor) (engineinfo.FleetResult, error) {
	return engineinfo.FleetResult{}, ctx.Err()
}

func replayDescriptor(t *testing.T, host, port string, role engineinfo.ParticipantRole) engineinfo.ParticipantDescriptor {
	t.Helper()
	descriptor, err := engineinfo.NewParticipantDescriptor(&hostparse.HostInfo{Host: host, Original: host}, port, role)
	if err != nil {
		t.Fatalf("NewParticipantDescriptor(%s): %v", host, err)
	}
	return descriptor
}

func replayFleetForDescriptors(descriptors []engineinfo.ParticipantDescriptor, status engineinfo.ConsistencyStatus, collectionStatus engineinfo.CollectionStatus) engineinfo.FleetResult {
	participants := make([]engineinfo.ParticipantResult, len(descriptors))
	for i := range descriptors {
		role := engineinfo.RoleWorker
		if len(descriptors) == 1 {
			role = engineinfo.RoleStandalone
		} else if i == 0 {
			role = engineinfo.RoleEntry
		}
		participants[i] = engineinfo.ParticipantResult{
			NodeID: fmt.Sprintf("participant-%d:9999", i+1), Role: role,
			CollectionStatus: collectionStatus, ReportedSchemaVersion: 1, BuildID: "build-1",
		}
	}
	dirty := false
	return engineinfo.FleetResult{
		Consistency:  engineinfo.ConsistencyAssessment{Status: status, Reason: "current replay fleet assessment"},
		Participants: participants,
		Builds: []engineinfo.GroupedBuild{{BuildID: "build-1", Information: engineinfo.BuildInformation{
			SchemaVersion: 1, Product: "spt-engine", Version: "dev-current", Revision: strings.Repeat("c", 40),
			BuildTime: "2026-08-26T00:00:00Z", Development: true, SourceDirty: &dirty,
		}}},
	}
}

func replayFutureSchemaFleet() engineinfo.FleetResult {
	fleet := runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusUnsupportedSchema)
	fleet.Participants[1].ReportedSchemaVersion = 2
	fleet.Participants[1].Reason = "unsupported engine build information schema 2"
	return fleet
}

func executeReplayHookForTest(ctx context.Context, hooks tui.LaunchHooks, out io.Writer, submissions *atomic.Int32) error {
	lines, err := hooks.RunPreSubmissionCheck(ctx)
	for _, line := range lines {
		_, _ = fmt.Fprintln(out, line)
	}
	if err != nil {
		return err
	}
	submissions.Add(1)
	hooks.NotifySubmitted()
	return nil
}

func replayIdentityArgs(sourceURL, hosts, resultsDir string, autoResults, headless, force bool) []string {
	args := []string{
		"--from", sourceURL, "--endpoints", "http://s3.example", "--test-hosts", hosts,
		"--results-dir", resultsDir, fmt.Sprintf("--auto-results=%t", autoResults),
	}
	if !autoResults {
		args = append(args, "--output-dir", filepath.Join(resultsDir, "generated"))
	}
	if headless {
		args = append(args, "--headless")
	}
	if force {
		args = append(args, "--force")
	}
	return args
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func readReplayAbortManifest(t *testing.T, path string) engineinfo.Manifest {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read abort manifest: %v", err)
	}
	var manifest engineinfo.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("parse abort manifest: %v", err)
	}
	return manifest
}

func immediateCancelableReplayMonitor(
	context.Context, string, string, string, []string, int64, bool, []*hostparse.HostInfo,
	string, bool, int, string, *runMetadata, io.Writer, io.Writer, string,
	func(context.Context), ...*integrity.FinalizeOptions,
) *autoResultsMonitor {
	done := make(chan autoResultsOutcome, 1)
	var once sync.Once
	return &autoResultsMonitor{
		done: done,
		cancel: func() {
			once.Do(func() { done <- autoResultsOutcome{} })
		},
		armed: make(chan struct{}),
	}
}

func newReplayArchiveServerWithSourceIdentity(t *testing.T, identityFetches *atomic.Int32) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="scenario.json">scenario</a><a href="engine.info.json">source identity</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/scenario.json`)
	})
	mux.HandleFunc("/scenario.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"type":"sequential","config":{"storage":{"driver":{"type":"s3"}}},"steps":[{"type":"load","config":{"item":{"data":{"size":"1KB"}},"test":{"step":{"id":"W","limit":{"count":1}}},"load":{"limit":{"concurrency":1}}}}]}`)
	})
	mux.HandleFunc("/engine.info.json", func(w http.ResponseWriter, _ *http.Request) {
		identityFetches.Add(1)
		_, _ = fmt.Fprint(w, `{"schema_version":1,"run_id":1,"consistency":{"status":"mismatch"}}`)
	})
	return httptest.NewServer(mux)
}

func installReplayEngineInfoSeams(t *testing.T) func() {
	t.Helper()
	origPort := resolvePortConflictFunc
	origConnect := connectReplayOrchestrator
	origConfirm := confirmReplayLaunchCommand
	origShouldHeadless := shouldReplayRunHeadless
	origLocalHeadless := startReplayLocalHeadless
	origLocalTUI := startReplayLocalTUI
	origRemoteHeadless := startReplayRemoteHeadless
	origRemoteTUI := startReplayRemoteTUI
	origAutoResults := startReplayAutoResultsMonitor
	origNewRunID := newReplayRunID
	origCollector := newReplayEngineIdentityCollector
	origLocalPlan := replayLocalEnginePlan
	origDistributedPlan := replayDistributedEnginePlan
	return func() {
		resolvePortConflictFunc = origPort
		connectReplayOrchestrator = origConnect
		confirmReplayLaunchCommand = origConfirm
		shouldReplayRunHeadless = origShouldHeadless
		startReplayLocalHeadless = origLocalHeadless
		startReplayLocalTUI = origLocalTUI
		startReplayRemoteHeadless = origRemoteHeadless
		startReplayRemoteTUI = origRemoteTUI
		startReplayAutoResultsMonitor = origAutoResults
		newReplayRunID = origNewRunID
		newReplayEngineIdentityCollector = origCollector
		replayLocalEnginePlan = origLocalPlan
		replayDistributedEnginePlan = origDistributedPlan
	}
}
