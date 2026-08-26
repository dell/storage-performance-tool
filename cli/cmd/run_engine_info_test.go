package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

func TestDistributedRunEngineParticipantsFreezesExecutionSetBeforeCollection(t *testing.T) {
	// The first two hosts are the post-readiness execution plan. The third
	// configured host never joined and must not become an engine participant.
	ready := []*tui.HostConnection{
		{Info: &hostparse.HostInfo{Host: "entry.example", Original: "admin@entry.example"}},
		{Info: &hostparse.HostInfo{Host: "worker.example", Original: "admin@worker.example"}},
	}
	descriptors, err := engineParticipantsFromReadyHosts(ready, "9999")
	if err != nil {
		t.Fatal(err)
	}
	collector := engineinfo.NewCollector(mappedRunVersionClient{results: map[string]engineinfo.CollectionResult{
		"http://entry.example:9999": collectedRunBuild(),
		"http://worker.example:9999": {
			Status: engineinfo.StatusCollectionFailed,
			Reason: "engine version request failed",
		},
	}})
	result, err := collector.Collect(context.Background(), descriptors)
	if err == nil {
		t.Fatal("Collect() error = nil, want worker collection failure")
	}
	if len(result.Participants) != 2 {
		t.Fatalf("participants = %+v, want frozen entry and worker", result.Participants)
	}
	if result.Participants[0].NodeID != "entry.example:9999" ||
		result.Participants[1].NodeID != "worker.example:9999" ||
		result.Participants[1].CollectionStatus != engineinfo.StatusCollectionFailed {
		t.Fatalf("participants = %+v, successful responders redefined execution set", result.Participants)
	}
}

func TestEngineIdentityGateLinesMakeForcedAndRejectedMismatchDiagnostic(t *testing.T) {
	mismatch := runGateFleet(engineinfo.ConsistencyMismatch, engineinfo.StatusCollected)
	for _, test := range []struct {
		name      string
		force     bool
		decision  engineinfo.GateDecision
		wantAlert string
	}{
		{name: "forced", force: true, decision: engineinfo.GateProceed, wantAlert: "WARNING: ENGINE BUILD MISMATCH FORCED"},
		{name: "rejected", decision: engineinfo.GateRejectedMismatch, wantAlert: "ERROR: Engine build mismatch rejected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := engineinfo.GateOutcome{Fleet: mismatch, Decision: test.decision, Proceed: test.force}
			outcome.Fleet.Consistency.Forced = test.force
			lines := engineIdentityGateLines(outcome, false, errors.New("gate rejected"))
			joined := strings.Join(lines, "\n")
			alertAt := strings.Index(joined, test.wantAlert)
			firstGroupAt := strings.Index(joined, "Engine build group: 5.14.2 (aaaaaaaaaaaa), 1 node")
			secondGroupAt := strings.Index(joined, "Engine build group: 5.14.2 (bbbbbbbbbbbb), 1 node")
			if alertAt < 0 || firstGroupAt <= alertAt || secondGroupAt <= firstGroupAt {
				t.Fatalf("mismatch diagnostic order = %q", lines)
			}
			if strings.Contains(joined, "entry.example:9999") || strings.Contains(joined, "attempts=") {
				t.Fatalf("normal mismatch output exposed verbose node detail: %q", lines)
			}
		})
	}
}

func TestEngineExecutionHostsExcludesIdleListWorkers(t *testing.T) {
	entry := &tui.HostConnection{Info: &hostparse.HostInfo{Host: "entry.example"}}
	idleWorker := &tui.HostConnection{Info: &hostparse.HostInfo{Host: "idle.example"}}
	ready := []*tui.HostConnection{entry, idleWorker}

	listHosts := engineExecutionHosts(append([]*tui.HostConnection(nil), ready...), true)
	if len(listHosts) != 1 || listHosts[0] != entry {
		t.Fatalf("LIST execution hosts = %+v, want entry only", listHosts)
	}
	distributedHosts := engineExecutionHosts(append([]*tui.HostConnection(nil), ready...), false)
	if len(distributedHosts) != 2 || distributedHosts[0] != entry || distributedHosts[1] != idleWorker {
		t.Fatalf("distributed execution hosts = %+v, want entry and worker", distributedHosts)
	}
}

func TestWriteEngineIdentityAbortManifestIsAtomicCompleteAndDeterministic(t *testing.T) {
	root := filepath.Join(t.TempDir(), "abort-results")
	dirty := false
	outcome := engineinfo.GateOutcome{
		Decision: engineinfo.GateCollectionFailure,
		Fleet: engineinfo.FleetResult{
			Consistency: engineinfo.ConsistencyAssessment{Status: engineinfo.ConsistencyIndeterminate},
			Builds: []engineinfo.GroupedBuild{{BuildID: "build-1", Information: engineinfo.BuildInformation{
				SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
				Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:34:56Z",
				SourceDirty: &dirty,
			}}},
			Participants: []engineinfo.ParticipantResult{
				{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollectionFailed, Reason: "engine version request failed"},
				{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1"},
			},
		},
	}
	previousNow := runEngineIdentityNow
	runEngineIdentityNow = func() time.Time {
		return time.Date(2026, 8, 26, 13, 21, 42, 0, time.FixedZone("offset", -4*60*60))
	}
	t.Cleanup(func() { runEngineIdentityNow = previousNow })

	if _, err := writeEngineIdentityManifest(root, 1787750472685, outcome, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, constants.EngineInfoManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest engineinfo.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode manifest: %v\n%s", err, data)
	}
	if manifest.RunID != 1787750472685 || manifest.GeneratedAt != "2026-08-26T17:21:42Z" ||
		manifest.Consistency.Status != engineinfo.ConsistencyIndeterminate || manifest.Consistency.Forced ||
		!strings.Contains(manifest.Consistency.Reason, "collection failed") {
		t.Fatalf("manifest identity/decision = %+v", manifest)
	}
	if len(manifest.Participants) != 2 || manifest.Participants[0].Role != engineinfo.RoleEntry ||
		manifest.Participants[1].CollectionStatus != engineinfo.StatusCollectionFailed {
		t.Fatalf("manifest participants = %+v", manifest.Participants)
	}
	temps, err := filepath.Glob(filepath.Join(root, ".summary-*.tmp"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("temporary manifest files = %v, error = %v", temps, err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o750 {
		t.Fatalf("results root mode = %04o, want 0750", got)
	}
}

func TestRunCommandInvokesEngineIdentityGateAcrossTopologyPresentationAndLimitModes(t *testing.T) {
	tests := []struct {
		name      string
		workload  string
		hosts     string
		minHosts  string
		headless  bool
		duration  string
		wantLocal bool
		entryOnly bool
	}{
		{name: "local headless timed", hosts: "127.0.0.1", minHosts: "1", headless: true, duration: "1s", wantLocal: true},
		{name: "local tui untimed", hosts: "127.0.0.1", minHosts: "1", headless: false, wantLocal: true},
		{name: "single remote headless untimed", hosts: "entry.example", minHosts: "1", headless: true},
		{name: "single remote tui timed", hosts: "entry.example", minHosts: "1", headless: false, duration: "1s"},
		{name: "multi worker headless timed", hosts: "entry.example,worker.example", minHosts: "2", headless: true, duration: "1s"},
		{name: "multi worker tui untimed", hosts: "entry.example,worker.example", minHosts: "2", headless: false},
		{name: "LIST excludes idle workers", workload: scenario.WorkloadTypeList, hosts: "entry.example,worker.example", minHosts: "2", headless: true, entryOnly: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setRunEngineInfoFlags(t, test.hosts, test.minHosts, test.duration, false, false)
			restore := installRunEngineInfoCommandSeams(t)
			defer restore()
			shouldRunHeadlessForRun = func(*cobra.Command) bool { return test.headless }
			var gotEntryOnly atomic.Bool
			distributedRunEnginePlan = func(_ *tui.MultiHostOrchestrator, _ string, boolValue bool) ([]engineinfo.ParticipantDescriptor, error) {
				gotEntryOnly.Store(boolValue)
				return nil, nil
			}

			var gateCalls, localCalls, remoteCalls atomic.Int64
			evaluateRunEngineIdentityGate = func(
				context.Context, engineinfo.FleetCollector, []engineinfo.ParticipantDescriptor, bool,
			) (engineinfo.GateOutcome, error) {
				gateCalls.Add(1)
				return consistentRunGateOutcome(), nil
			}
			launch := func(hooks tui.LaunchHooks) error {
				lines, err := hooks.RunPreSubmissionCheck(context.Background())
				if err != nil {
					return err
				}
				if len(lines) != 1 || !strings.Contains(lines[0], "consistent") {
					t.Fatalf("gate output = %v", lines)
				}
				hooks.NotifySubmitted()
				return nil
			}
			startLocalTUIRunFunc = func(_ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				localCalls.Add(1)
				return launch(options.LaunchHooks)
			}
			startMultiHostHeadlessRunFunc = func(_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
				remoteCalls.Add(1)
				return launch(options.LaunchHooks)
			}
			startMultiHostTUIRunFunc = func(_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				remoteCalls.Add(1)
				return launch(options.LaunchHooks)
			}
			// Headless option capture is assigned separately because named parameters
			// are unavailable in the compact function literal above.
			startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
				localCalls.Add(1)
				return launch(options.LaunchHooks)
			}

			workload := test.workload
			if workload == "" {
				workload = WorkloadTypeMock
			}
			if err := runCmd.RunE(runCmd, []string{workload}); err != nil {
				t.Fatalf("RunE() error = %v", err)
			}
			if gateCalls.Load() != 1 {
				t.Fatalf("gate calls = %d, want one", gateCalls.Load())
			}
			if test.wantLocal && (localCalls.Load() != 1 || remoteCalls.Load() != 0) {
				t.Fatalf("local/remote calls = %d/%d", localCalls.Load(), remoteCalls.Load())
			}
			if !test.wantLocal && (remoteCalls.Load() != 1 || localCalls.Load() != 0) {
				t.Fatalf("local/remote calls = %d/%d", localCalls.Load(), remoteCalls.Load())
			}
			if !test.wantLocal && gotEntryOnly.Load() != test.entryOnly {
				t.Fatalf("entry-only participant plan = %t, want %t", gotEntryOnly.Load(), test.entryOnly)
			}
		})
	}
}

func TestRunCommandLockedParticipantVersionFailureBlocksSubmission(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Host, "localhost:") {
			http.Error(w, "worker unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"schema_version":1,
			"product":"spt-engine",
			"version":"5.14.2",
			"revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"build_time":"2026-08-26T12:34:56Z",
			"development":false,
			"source_dirty":false
		}`)
	}))
	defer server.Close()
	_, apiPort, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	t.Chdir(t.TempDir())
	setRunEngineInfoFlags(t, "entry.example,worker.example", "2", "", false, false)
	setGlobalRunFlagForTest(t, "api-port", apiPort)
	restore := installRunEngineInfoCommandSeams(t)
	defer restore()
	shouldRunHeadlessForRun = func(*cobra.Command) bool { return true }
	evaluateRunEngineIdentityGate = engineinfo.EvaluateGate

	entry, err := engineinfo.NewParticipantDescriptor(
		&hostparse.HostInfo{Host: "127.0.0.1"}, apiPort, engineinfo.RoleEntry)
	if err != nil {
		t.Fatal(err)
	}
	failedWorker, err := engineinfo.NewParticipantDescriptor(
		&hostparse.HostInfo{Host: "localhost"}, apiPort, engineinfo.RoleWorker)
	if err != nil {
		t.Fatal(err)
	}
	distributedRunEnginePlan = func(*tui.MultiHostOrchestrator, string, bool) ([]engineinfo.ParticipantDescriptor, error) {
		// This is the post-start locked RMI topology. The worker remains planned
		// even though its HTTP path cannot complete identity collection.
		return []engineinfo.ParticipantDescriptor{entry, failedWorker}, nil
	}

	var submitted atomic.Int64
	var output []string
	startMultiHostHeadlessRunFunc = func(_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
		lines, gateErr := options.LaunchHooks.RunPreSubmissionCheck(options.Context)
		output = append(output, lines...)
		if gateErr != nil {
			return gateErr
		}
		submitted.Add(1)
		options.LaunchHooks.NotifySubmitted()
		return nil
	}

	err = runCmd.RunE(runCmd, []string{WorkloadTypeMock})
	if err == nil || !strings.Contains(err.Error(), "collection gate failed") {
		t.Fatalf("RunE() error = %v, want locked worker collection failure", err)
	}
	if submitted.Load() != 0 {
		t.Fatalf("scenario submissions = %d, want zero", submitted.Load())
	}
	if !strings.Contains(strings.Join(output, "\n"), "collection failed") {
		t.Fatalf("gate output = %v, want collection failure", output)
	}
}

func TestRunCommandEngineIdentityGatePolicyAndAbortArtifacts(t *testing.T) {
	mismatchFleet := runGateFleet(engineinfo.ConsistencyMismatch, engineinfo.StatusCollected)
	compatibilityFleet := func(status engineinfo.CollectionStatus) engineinfo.FleetResult {
		fleet := runGateFleet(engineinfo.ConsistencyIndeterminate, status)
		fleet.Participants[0].Reason = "compatible identity evidence is unavailable"
		if status == engineinfo.StatusUnsupportedSchema {
			fleet.Participants[0].ReportedSchemaVersion = 2
		}
		return fleet
	}
	tests := []struct {
		name           string
		fleet          engineinfo.FleetResult
		collectErr     error
		force          bool
		autoResults    bool
		cancelAtGate   bool
		wantErr        bool
		wantSubmit     int64
		wantArtifact   bool
		wantOutputText string
	}{
		{name: "default mismatch rejects", fleet: mismatchFleet, autoResults: true, wantErr: true, wantArtifact: true, wantOutputText: "mismatch rejected"},
		{name: "force permits only mismatch", fleet: mismatchFleet, force: true, autoResults: true, wantSubmit: 1, wantArtifact: true, wantOutputText: "MISMATCH FORCED"},
		{name: "legacy absence continues", fleet: compatibilityFleet(engineinfo.StatusLegacyEndpointUnavailable), wantSubmit: 1, wantOutputText: "indeterminate"},
		{name: "future schema continues", fleet: compatibilityFleet(engineinfo.StatusUnsupportedSchema), wantSubmit: 1, wantOutputText: "indeterminate"},
		{name: "incomplete identity continues", fleet: compatibilityFleet(engineinfo.StatusIncompleteBuildInfo), wantSubmit: 1, wantOutputText: "indeterminate"},
		{name: "malformed supported response is not forceable", fleet: runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusCollectionFailed), collectErr: errors.New("malformed schema 1"), force: true, autoResults: true, wantErr: true, wantArtifact: true, wantOutputText: "collection failed"},
		{name: "transport exhaustion is not forceable", fleet: runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusCollectionFailed), collectErr: errors.New("transport exhausted"), force: true, wantErr: true, wantOutputText: "collection failed"},
		{name: "cancellation prevents submission", fleet: runGateFleet(engineinfo.ConsistencyIndeterminate, engineinfo.StatusCollectionFailed), cancelAtGate: true, wantErr: true, wantOutputText: "collection failed"},
		{name: "auto results off creates no identity directory", fleet: mismatchFleet, wantErr: true, wantOutputText: "mismatch rejected"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workDir := t.TempDir()
			t.Chdir(workDir)
			resultsDir := filepath.Join(workDir, "identity-results")
			setRunEngineInfoFlags(t, "127.0.0.1", "1", "", test.force, test.autoResults)
			setGlobalRunFlagForTest(t, "results-dir", resultsDir)
			setGlobalRunFlagForTest(t, "label", "gate")
			restore := installRunEngineInfoCommandSeams(t)
			defer restore()
			shouldRunHeadlessForRun = func(*cobra.Command) bool { return true }
			previousAuto := startAutoResultsFunc
			defer func() { startAutoResultsFunc = previousAuto }()
			if test.autoResults {
				startAutoResultsFunc = immediateCleanupOnlyMonitor
			}

			evaluateRunEngineIdentityGate = func(
				ctx context.Context,
				_ engineinfo.FleetCollector,
				descriptors []engineinfo.ParticipantDescriptor,
				force bool,
			) (engineinfo.GateOutcome, error) {
				if test.cancelAtGate {
					<-ctx.Done()
					return engineinfo.EvaluateGate(ctx, fixedFleetCollector{
						fleet: test.fleet, err: ctx.Err(),
					}, descriptors, force)
				}
				return engineinfo.EvaluateGate(ctx, fixedFleetCollector{
					fleet: test.fleet, err: test.collectErr,
				}, descriptors, force)
			}

			var submitted atomic.Int64
			var output []string
			startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
				ctx := options.Context
				if test.cancelAtGate {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				lines, err := options.LaunchHooks.RunPreSubmissionCheck(ctx)
				output = append(output, lines...)
				if err != nil {
					return err
				}
				options.LaunchHooks.NotifySubmitted()
				submitted.Add(1)
				return nil
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeMock})
			if (err != nil) != test.wantErr {
				t.Fatalf("RunE() error = %v, wantErr %t", err, test.wantErr)
			}
			if submitted.Load() != test.wantSubmit {
				t.Fatalf("submission count = %d, want %d", submitted.Load(), test.wantSubmit)
			}
			if !strings.Contains(strings.Join(output, "\n"), test.wantOutputText) {
				t.Fatalf("gate output = %v, want %q", output, test.wantOutputText)
			}
			artifacts, globErr := filepath.Glob(filepath.Join(resultsDir, "gate-*", constants.EngineInfoManifestName))
			if globErr != nil {
				t.Fatal(globErr)
			}
			if test.wantArtifact {
				if len(artifacts) != 1 {
					t.Fatalf("abort manifests = %v, want one", artifacts)
				}
				data, readErr := os.ReadFile(artifacts[0])
				if readErr != nil {
					t.Fatal(readErr)
				}
				var manifest engineinfo.Manifest
				if unmarshalErr := json.Unmarshal(data, &manifest); unmarshalErr != nil {
					t.Fatal(unmarshalErr)
				}
				if len(manifest.Participants) != len(test.fleet.Participants) {
					t.Fatalf("partial participants = %d, want %d", len(manifest.Participants), len(test.fleet.Participants))
				}
			} else if len(artifacts) != 0 {
				t.Fatalf("unexpected identity artifacts = %v", artifacts)
			}
			if !test.autoResults {
				if _, statErr := os.Stat(resultsDir); !os.IsNotExist(statErr) {
					t.Fatalf("auto-results off results directory error = %v, want not exist", statErr)
				}
			}
		})
	}
}

type mappedRunVersionClient struct {
	results map[string]engineinfo.CollectionResult
}

func (c mappedRunVersionClient) Fetch(_ context.Context, baseURL string) (engineinfo.CollectionResult, error) {
	result := c.results[baseURL]
	if result.Status == engineinfo.StatusCollectionFailed {
		return result, errors.New(result.Reason)
	}
	return result, nil
}

func collectedRunBuild() engineinfo.CollectionResult {
	dirty := false
	return engineinfo.CollectionResult{
		Status: engineinfo.StatusCollected, Complete: true, ReportedSchemaVersion: 1,
		Build: &engineinfo.BuildInformation{
			SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
			Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:34:56Z",
			SourceDirty: &dirty,
		},
	}
}

func consistentRunGateOutcome() engineinfo.GateOutcome {
	return engineinfo.GateOutcome{
		Decision: engineinfo.GateProceed,
		Proceed:  true,
		Fleet: engineinfo.FleetResult{
			Consistency: engineinfo.ConsistencyAssessment{Status: engineinfo.ConsistencyConsistent, Reason: "all participants match"},
			Participants: []engineinfo.ParticipantResult{{
				NodeID: "127.0.0.1:9999", Role: engineinfo.RoleStandalone,
				CollectionStatus: engineinfo.StatusCollected,
			}},
		},
	}
}

func runGateFleet(
	status engineinfo.ConsistencyStatus,
	collectionStatus engineinfo.CollectionStatus,
) engineinfo.FleetResult {
	dirty := false
	baseBuild := engineinfo.BuildInformation{
		SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
		Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:34:56Z",
		SourceDirty: &dirty,
	}
	fleet := engineinfo.FleetResult{
		Consistency: engineinfo.ConsistencyAssessment{Status: status, Reason: "test fleet assessment"},
		Participants: []engineinfo.ParticipantResult{
			{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1"},
			{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: collectionStatus},
		},
		Builds: []engineinfo.GroupedBuild{{BuildID: "build-1", Information: baseBuild}},
	}
	switch collectionStatus {
	case engineinfo.StatusCollected:
		fleet.Participants[1].ReportedSchemaVersion = 1
		fleet.Participants[1].BuildID = "build-1"
		if status == engineinfo.ConsistencyMismatch {
			mismatchedBuild := baseBuild
			mismatchedBuild.Revision = strings.Repeat("b", 40)
			fleet.Builds = append(fleet.Builds, engineinfo.GroupedBuild{BuildID: "build-2", Information: mismatchedBuild})
			fleet.Participants[1].BuildID = "build-2"
		}
	case engineinfo.StatusIncompleteBuildInfo:
		incompleteBuild := baseBuild
		incompleteBuild.Revision = constants.EngineBuildInfoUnknown
		fleet.Builds = append(fleet.Builds, engineinfo.GroupedBuild{BuildID: "build-2", Information: incompleteBuild})
		fleet.Participants[1].ReportedSchemaVersion = 1
		fleet.Participants[1].BuildID = "build-2"
		fleet.Participants[1].Reason = "engine build comparison fields are incomplete"
	case engineinfo.StatusLegacyEndpointUnavailable:
		fleet.Participants[1].Reason = "engine version endpoint is unavailable"
	case engineinfo.StatusUnsupportedSchema:
		fleet.Participants[1].ReportedSchemaVersion = 2
		fleet.Participants[1].Reason = "engine version schema is newer than this CLI supports"
	case engineinfo.StatusCollectionFailed:
		fleet.Participants[1].Reason = "engine version collection failed"
	}
	return fleet
}

type fixedFleetCollector struct {
	fleet engineinfo.FleetResult
	err   error
}

func (c fixedFleetCollector) Collect(
	context.Context,
	[]engineinfo.ParticipantDescriptor,
) (engineinfo.FleetResult, error) {
	return c.fleet, c.err
}

func immediateCleanupOnlyMonitor(
	_ context.Context,
	_ string,
	_ string,
	_ string,
	_ []string,
	_ int64,
	_ bool,
	_ []*hostparse.HostInfo,
	_ string,
	_ bool,
	_ int,
	_ string,
	metadata *runMetadata,
	_ io.Writer,
	_ io.Writer,
	_ string,
	_ func(context.Context),
	_ ...*integrity.FinalizeOptions,
) *autoResultsMonitor {
	monitor := &autoResultsMonitor{done: make(chan autoResultsOutcome, 1), armed: make(chan struct{})}
	go func() {
		<-monitor.armed
		outcome := autoResultsOutcome{}
		if metadata != nil {
			outcome.ArtifactErr = writeRunMetadata(metadata, metadata.ResultsRoot)
		}
		outcome.Lifecycle.PreparedInputs = cleanupPreparedForMonitorTest(metadata)
		monitor.done <- outcome
	}()
	return monitor
}

func setRunEngineInfoFlags(t *testing.T, hosts, minHosts, duration string, force, autoResults bool) {
	t.Helper()
	for name, value := range map[string]string{
		"test-hosts": hosts, "min-hosts": minHosts, "duration": duration,
		"endpoints":   "http://s3.example",
		"object-size": "1KiB", "object-count": "1", "threads": "1",
		"headless": "false", "auto-results": boolString(autoResults),
		"shutdown-on-complete": "false", "generate-only": "false",
		"force": boolString(force), "verbose": "false",
	} {
		setGlobalRunFlagForTest(t, name, value)
	}
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func installRunEngineInfoCommandSeams(t *testing.T) func() {
	t.Helper()
	previousHeadless := shouldRunHeadlessForRun
	previousPort := resolvePortConflictFunc
	previousConnect := connectMultiHostOrchestratorFunc
	previousLocalHeadless := startLocalHeadlessRunFunc
	previousLocalTUI := startLocalTUIRunFunc
	previousRemoteHeadless := startMultiHostHeadlessRunFunc
	previousRemoteTUI := startMultiHostTUIRunFunc
	previousEvaluate := evaluateRunEngineIdentityGate
	previousLocalPlan := localRunEnginePlan
	previousDistributedPlan := distributedRunEnginePlan
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
	localRunEnginePlan = func(string) ([]engineinfo.ParticipantDescriptor, error) { return nil, nil }
	distributedRunEnginePlan = func(*tui.MultiHostOrchestrator, string, bool) ([]engineinfo.ParticipantDescriptor, error) {
		return nil, nil
	}
	return func() {
		shouldRunHeadlessForRun = previousHeadless
		resolvePortConflictFunc = previousPort
		connectMultiHostOrchestratorFunc = previousConnect
		startLocalHeadlessRunFunc = previousLocalHeadless
		startLocalTUIRunFunc = previousLocalTUI
		startMultiHostHeadlessRunFunc = previousRemoteHeadless
		startMultiHostTUIRunFunc = previousRemoteTUI
		evaluateRunEngineIdentityGate = previousEvaluate
		localRunEnginePlan = previousLocalPlan
		distributedRunEnginePlan = previousDistributedPlan
	}
}
