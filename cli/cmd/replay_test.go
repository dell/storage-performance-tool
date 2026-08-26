package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func TestReplayCommandWiresMultiHostOrchestratorFlags(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"item": {"data": {"size": "10KB"}}, "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	connectErr := errors.New("stop after replay orchestrator wiring")
	var capturedHosts []*hostparse.HostInfo
	var capturedMinHosts int
	var capturedRMIConfig tui.RMIConfig
	var capturedAttachExisting bool
	var factoryCalled bool
	var connectCalled bool
	origMultiHostFactory := newReplayMultiHostOrchestrator
	origConnect := connectReplayOrchestrator
	t.Cleanup(func() {
		newReplayMultiHostOrchestrator = origMultiHostFactory
		connectReplayOrchestrator = origConnect
	})
	newReplayMultiHostOrchestrator = func(hostInfos []*hostparse.HostInfo, minHosts int, rmiConfig tui.RMIConfig) *tui.MultiHostOrchestrator {
		factoryCalled = true
		capturedHosts = append([]*hostparse.HostInfo(nil), hostInfos...)
		capturedMinHosts = minHosts
		capturedRMIConfig = rmiConfig
		return origMultiHostFactory(hostInfos, minHosts, rmiConfig)
	}
	connectReplayOrchestrator = func(_ context.Context, orchestrator *tui.MultiHostOrchestrator) error {
		connectCalled = true
		capturedAttachExisting = orchestrator.AttachExistingWorkersEnabled()
		return connectErr
	}

	var out bytes.Buffer
	cmd := newReplayCommandForTest(t)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://10.0.0.1:9020",
		"--test-hosts", "qa-entry-01,qa-worker-01",
		"--min-hosts", "1",
		"--attach-existing",
		"--network-mode", "bridge",
		"--rmi-port-start", "40123",
		"--rmi-port-count", "7",
		"--results-dir", t.TempDir(),
	})
	err := cmd.Execute()
	if !errors.Is(err, connectErr) {
		t.Fatalf("Execute() error = %v, want wrapped connect sentinel", err)
	}
	if strings.Contains(err.Error(), "multi-host replay execution is not implemented yet") {
		t.Fatalf("multi-host replay should be accepted before launch, got: %v", err)
	}
	if strings.Contains(err.Error(), "RDMA replay launch is not implemented yet") {
		t.Fatalf("test should not depend on RDMA replay guard, got: %v", err)
	}
	if !factoryCalled || !connectCalled {
		t.Fatalf("factoryCalled=%t connectCalled=%t, want both true", factoryCalled, connectCalled)
	}
	if len(capturedHosts) != 2 || capturedHosts[0].Original != "qa-entry-01" || capturedHosts[1].Original != "qa-worker-01" {
		t.Fatalf("capturedHosts = %+v", capturedHosts)
	}
	if capturedMinHosts != 1 {
		t.Fatalf("minHosts = %d, want 1", capturedMinHosts)
	}
	if capturedRMIConfig.NetworkMode != "bridge" || capturedRMIConfig.PortStart != 40123 || capturedRMIConfig.PortCount != 7 {
		t.Fatalf("RMI config = %+v, want bridge/40123/7", capturedRMIConfig)
	}
	if !capturedAttachExisting {
		t.Fatal("attach-existing was not applied to replay orchestrator")
	}
	if !strings.Contains(out.String(), "Multi-host replay: 2 hosts, minimum required: 1") {
		t.Fatalf("output missing multi-host summary:\n%s", out.String())
	}
}

func TestReplayCommandAcceptsSingleRemoteHostBeforeLaunch(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"item": {"data": {"size": "10KB"}}, "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	connectErr := errors.New("stop after single replay orchestrator wiring")
	var capturedHosts []*hostparse.HostInfo
	var capturedMinHosts int
	var capturedRMIConfig tui.RMIConfig
	var factoryCalled bool
	var connectCalled bool
	origSingleHostFactory := newReplaySingleHostOrchestrator
	origConnect := connectReplayOrchestrator
	t.Cleanup(func() {
		newReplaySingleHostOrchestrator = origSingleHostFactory
		connectReplayOrchestrator = origConnect
	})
	newReplaySingleHostOrchestrator = func(hostInfos []*hostparse.HostInfo, minHosts int, rmiConfig tui.RMIConfig) *tui.MultiHostOrchestrator {
		factoryCalled = true
		capturedHosts = append([]*hostparse.HostInfo(nil), hostInfos...)
		capturedMinHosts = minHosts
		capturedRMIConfig = rmiConfig
		return origSingleHostFactory(hostInfos, minHosts, rmiConfig)
	}
	connectReplayOrchestrator = func(_ context.Context, _ *tui.MultiHostOrchestrator) error {
		connectCalled = true
		return connectErr
	}

	var out bytes.Buffer
	cmd := newReplayCommandForTest(t)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://10.0.0.1:9020",
		"--test-hosts", "qa-client-01",
		"--network-mode", "bridge",
		"--rmi-port-start", "40123",
		"--rmi-port-count", "7",
		"--results-dir", t.TempDir(),
	})
	err := cmd.Execute()
	if !errors.Is(err, connectErr) {
		t.Fatalf("Execute() error = %v, want wrapped connect sentinel", err)
	}
	if strings.Contains(err.Error(), "single remote-host replay execution is not implemented yet") {
		t.Fatalf("single remote host should be accepted before launch, got: %v", err)
	}
	if strings.Contains(err.Error(), "RDMA replay launch is not implemented yet") {
		t.Fatalf("test should not depend on RDMA replay guard, got: %v", err)
	}
	if !factoryCalled || !connectCalled {
		t.Fatalf("factoryCalled=%t connectCalled=%t, want both true", factoryCalled, connectCalled)
	}
	if len(capturedHosts) != 1 || capturedHosts[0].Original != "qa-client-01" {
		t.Fatalf("capturedHosts = %+v", capturedHosts)
	}
	if capturedMinHosts != 1 {
		t.Fatalf("minHosts = %d, want 1", capturedMinHosts)
	}
	if capturedRMIConfig.NetworkMode != "bridge" || capturedRMIConfig.PortStart != 40123 || capturedRMIConfig.PortCount != 7 {
		t.Fatalf("RMI config = %+v, want bridge/40123/7", capturedRMIConfig)
	}
	if !strings.Contains(out.String(), "Replay host: qa-client-01") {
		t.Fatalf("output missing single-host summary:\n%s", out.String())
	}
}

func TestReplayCommandSharesPositiveRunIdentityWithLaunchAndCompletionMonitor(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()

	origPort := resolvePortConflictFunc
	origStartLocalHeadless := startReplayLocalHeadless
	origStartAutoResults := startReplayAutoResultsMonitor
	origShouldHeadless := shouldReplayRunHeadless
	origNewRunID := newReplayRunID
	t.Cleanup(func() {
		resolvePortConflictFunc = origPort
		startReplayLocalHeadless = origStartLocalHeadless
		startReplayAutoResultsMonitor = origStartAutoResults
		shouldReplayRunHeadless = origShouldHeadless
		newReplayRunID = origNewRunID
	})
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
	const replayRunID = int64(1717)
	newReplayRunID = func() int64 { return replayRunID }

	var launchedParams scenario.Params
	var launchedDefaults []byte
	startReplayLocalHeadless = func(
		_ string, _ string, params scenario.Params, options headless.HeadlessOptions,
		_ []byte, defaults []byte,
	) error {
		launchedParams = params
		launchedDefaults = append([]byte(nil), defaults...)
		options.LaunchHooks.NotifySubmitted()
		return nil
	}

	var monitoredRunID int64
	var monitoredStepIDs []string
	var monitoredMetadata *runMetadata
	startReplayAutoResultsMonitor = func(
		_ context.Context, _, _, _ string, expectedStepIDs []string, expectedRunID int64,
		_ bool, _ []*hostparse.HostInfo, _ string, _ bool, _ int, _ string,
		metadata *runMetadata, _, _ io.Writer, _ string, _ func(context.Context),
		_ ...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		monitoredRunID = expectedRunID
		monitoredStepIDs = append([]string(nil), expectedStepIDs...)
		monitoredMetadata = metadata
		done := make(chan autoResultsOutcome, 1)
		done <- autoResultsOutcome{}
		return &autoResultsMonitor{done: done, armed: make(chan struct{})}
	}

	cmd := newReplayCommandForTest(t)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://s3.example",
		"--headless",
		"--results-dir", t.TempDir(),
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if launchedParams.RunID != replayRunID {
		t.Fatalf("launched replay run ID = %d, want generated %d", launchedParams.RunID, replayRunID)
	}
	if monitoredRunID != launchedParams.RunID {
		t.Fatalf("completion monitor run ID = %d, want launched %d", monitoredRunID, launchedParams.RunID)
	}
	if monitoredMetadata == nil || monitoredMetadata.ScenarioParams.RunID != launchedParams.RunID {
		t.Fatalf("run metadata identity = %+v, want %d", monitoredMetadata, launchedParams.RunID)
	}
	if len(monitoredStepIDs) != 1 || !strings.HasPrefix(monitoredStepIDs[0], "replay-001-") ||
		len(monitoredMetadata.ExpectedStepIDs) != 1 || monitoredStepIDs[0] != monitoredMetadata.ExpectedStepIDs[0] {
		t.Fatalf("completion step identities = %v metadata=%v", monitoredStepIDs, monitoredMetadata.ExpectedStepIDs)
	}
	var defaults struct {
		Run struct {
			ID      int64 `yaml:"id"`
			Cluster struct {
				ID string `yaml:"id"`
			} `yaml:"cluster"`
		} `yaml:"run"`
	}
	if err := yaml.Unmarshal(launchedDefaults, &defaults); err != nil {
		t.Fatalf("parse launched defaults: %v", err)
	}
	wantClusterID := fmt.Sprintf("spt-run-%d", launchedParams.RunID)
	if defaults.Run.ID != launchedParams.RunID || defaults.Run.Cluster.ID != wantClusterID {
		t.Fatalf("launched defaults identity = run %d cluster %q, want run %d cluster %q",
			defaults.Run.ID, defaults.Run.Cluster.ID, launchedParams.RunID, wantClusterID)
	}
}

func TestReplayBaseURL(t *testing.T) {
	tests := []struct {
		name  string
		port  string
		hosts []*hostparse.HostInfo
		want  string
	}{
		{
			name: "remote single host",
			port: "10080",
			hosts: []*hostparse.HostInfo{
				{Host: "qa-client-01", IsLocal: false, Original: "qa-client-01"},
			},
			want: "http://qa-client-01:10080",
		},
		{
			name: "local single host",
			port: "10080",
			hosts: []*hostparse.HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
			},
			want: "http://localhost:10080",
		},
		{
			name: "multi host uses entry host",
			port: "10080",
			hosts: []*hostparse.HostInfo{
				{Host: "qa-entry-01", IsLocal: false, Original: "qa-entry-01"},
				{Host: "qa-worker-01", IsLocal: false, Original: "qa-worker-01"},
			},
			want: "http://qa-entry-01:10080",
		},
		{
			name:  "no hosts",
			port:  "10080",
			hosts: nil,
			want:  "http://localhost:10080",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replayBaseURL(tt.port, tt.hosts); got != tt.want {
				t.Fatalf("replayBaseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCleanupReplayContainers(t *testing.T) {
	wantErr := errors.New("cleanup failed")
	cleaner := &fakeReplayContainerCleaner{err: wantErr}

	err := cleanupReplayContainers(cleaner, time.Second)
	if !errors.Is(err, wantErr) {
		t.Fatalf("cleanupReplayContainers() error = %v, want %v", err, wantErr)
	}
	if cleaner.calls != 1 {
		t.Fatalf("StopAllContainers calls = %d, want 1", cleaner.calls)
	}
	if cleaner.deadline.IsZero() {
		t.Fatal("expected cleanup context to have a deadline")
	}

	if err := cleanupReplayContainers(nil, time.Second); err != nil {
		t.Fatalf("cleanupReplayContainers(nil) error = %v", err)
	}
}

func TestNormalizeHeadlessAutoTerminate(t *testing.T) {
	firstStopErr := errors.New("first cleanup failed")
	secondStopErr := errors.New("second cleanup failed")

	tests := []struct {
		name         string
		inputErr     error
		cleanerErr   error
		wantAuto     bool
		wantErr      error
		wantCleanup  int
		wantCleanup2 error
	}{
		{
			name:     "nil error",
			wantAuto: false,
		},
		{
			name:     "clean auto terminate",
			inputErr: &headless.AutoTerminateError{CleanupComplete: true},
			wantAuto: true,
		},
		{
			name:        "auto terminate with first cleanup failure retries",
			inputErr:    errors.Join(&headless.AutoTerminateError{CleanupComplete: false}, firstStopErr),
			wantAuto:    true,
			wantCleanup: 1,
		},
		{
			name:         "auto terminate retry failure is returned",
			inputErr:     errors.Join(&headless.AutoTerminateError{CleanupComplete: false}, firstStopErr),
			cleanerErr:   secondStopErr,
			wantAuto:     true,
			wantErr:      secondStopErr,
			wantCleanup:  1,
			wantCleanup2: firstStopErr,
		},
		{
			name:     "ordinary error passes through",
			inputErr: firstStopErr,
			wantErr:  firstStopErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleaner := &fakeReplayContainerCleaner{err: tt.cleanerErr}
			gotAuto, gotErr := normalizeHeadlessAutoTerminate(tt.inputErr, cleaner, time.Second)
			if gotAuto != tt.wantAuto {
				t.Fatalf("autoTerminated = %t, want %t", gotAuto, tt.wantAuto)
			}
			if cleaner.calls != tt.wantCleanup {
				t.Fatalf("cleanup calls = %d, want %d", cleaner.calls, tt.wantCleanup)
			}
			if tt.wantErr == nil {
				if gotErr != nil {
					t.Fatalf("error = %v, want nil", gotErr)
				}
				return
			}
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", gotErr, tt.wantErr)
			}
			if tt.wantCleanup2 != nil && !errors.Is(gotErr, tt.wantCleanup2) {
				t.Fatalf("error = %v, want joined first cleanup error %v", gotErr, tt.wantCleanup2)
			}
		})
	}
}

type fakeReplayContainerCleaner struct {
	calls    int
	deadline time.Time
	err      error
}

func (f *fakeReplayContainerCleaner) StopAllContainers(ctx context.Context) error {
	f.calls++
	if deadline, ok := ctx.Deadline(); ok {
		f.deadline = deadline
	}
	return f.err
}

func TestReplayCommandGenerateOnlyWritesArtifacts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export RUN_TIME=900
export RUN_TIME_FOR_SMALL_OBJ=1800
export WAIT_TIME=60
export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}}, "test": {"step": {"id": "MAX-W10KB", "limit": {"time": "${RUN_TIME_FOR_SMALL_OBJ}"}}}, "load": {"limit": {"concurrency": 2}}}},
    {"type": "command", "value": "sleep ${WAIT_TIME}", "blocking": true},
    {"type": "load", "config": {"item": {"data": {"size": "10KB"}, "input": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}}, "test": {"step": {"id": "MAX-R10KB", "limit": {"time": "${RUN_TIME}"}}}, "load": {"type": "read", "limit": {"concurrency": 2}}}}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	outDir := filepath.Join(t.TempDir(), "replay-out")
	var out bytes.Buffer
	cmd := newReplayCommandForTest(t)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--generate-only",
		"--endpoints", "http://10.0.0.1:9020",
		"--bucket", "local-bucket",
		"--output-dir", outDir,
		"--label", "replay",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(out.String(), "Replay preflight") {
		t.Fatalf("output missing preflight:\n%s", out.String())
	}
	scenarioPath := filepath.Join(outDir, "replay-scenario.js")
	scenarioBody, err := os.ReadFile(scenarioPath)
	if err != nil {
		t.Fatalf("read generated scenario: %v", err)
	}
	if strings.Contains(string(scenarioBody), "/log/MAX-W10KB") {
		t.Fatalf("generated scenario retained legacy item path:\n%s", string(scenarioBody))
	}
	if !strings.Contains(string(scenarioBody), "replay-001-") {
		t.Fatalf("generated scenario missing canonical step id:\n%s", string(scenarioBody))
	}
	if _, err := os.Stat(filepath.Join(outDir, "defaults.yaml")); err != nil {
		t.Fatalf("defaults.yaml not written: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "replay-metadata.json")); err != nil {
		t.Fatalf("replay-metadata.json not written: %v", err)
	}
}

func TestReplayCommandPrintsPreflightForRejectedCommand(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}},
    {"type": "command", "value": "rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB", "blocking": true}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var out bytes.Buffer
	cmd := newReplayCommandForTest(t)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--generate-only",
		"--endpoints", "http://10.0.0.1:9020",
		"--bucket", "local-bucket",
	})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil, want rejected command error")
	}
	if !strings.Contains(err.Error(), "unsupported command step") {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{
		"Replay preflight",
		"\nCommand operations\n",
		"  - rejected: rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB (not recognized by replay command allowlist)\n",
		"\nErrors\n  - unsupported command step: rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB\n",
		"Replay failure class: unsupported_command_step\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q\n%s", want, out.String())
		}
	}
}

func TestReplayCommandLaunchStagesArtifactsInResultsDirWhenAutoResultsDisabled(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"item": {"data": {"size": "10KB"}}, "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	resultsDir := filepath.Join(t.TempDir(), "results")
	var out bytes.Buffer
	cmd := newReplayCommandForTest(t)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://10.0.0.1:9020",
		"--auto-results=false",
		"--results-dir", resultsDir,
		"--label", "replaycase",
		"--s3-driver", "rdma",
	})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "RDMA replay launch is not implemented yet") {
		t.Fatalf("Execute() error = %v, want RDMA launch not implemented", err)
	}
	if !strings.Contains(out.String(), "  Directory: "+filepath.Join(resultsDir, "replaycase-")) {
		t.Fatalf("generated directory should be staged under results dir, output:\n%s", out.String())
	}
	matches, globErr := filepath.Glob(filepath.Join(resultsDir, "replaycase-*"))
	if globErr != nil {
		t.Fatalf("glob generated replay directory: %v", globErr)
	}
	if len(matches) != 1 {
		t.Fatalf("generated replay dirs = %v, want one", matches)
	}
	if _, statErr := os.Stat(filepath.Join(matches[0], "defaults.yaml")); !os.IsNotExist(statErr) {
		t.Fatalf("defaults.yaml stat error = %v, want not exist for default launch staging", statErr)
	}
	for _, name := range []string{"replay-scenario.js", "replay-metadata.json"} {
		if _, statErr := os.Stat(filepath.Join(matches[0], name)); statErr != nil {
			t.Fatalf("%s not staged: %v", name, statErr)
		}
	}
	if !strings.Contains(out.String(), "  Defaults: in memory only") {
		t.Fatalf("output should explain defaults were not persisted:\n%s", out.String())
	}
	if strings.Contains(out.String(), "/tmp/spt-replay-") {
		t.Fatalf("launch path should not use transient replay temp dir when results-dir is available:\n%s", out.String())
	}
}

func TestConfirmReplayLaunch(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "enter starts", input: "\n"},
		{name: "q aborts", input: "q\n", wantErr: "replay aborted by user"},
		{name: "esc aborts", input: "\x1b\n", wantErr: "replay aborted by user"},
		{name: "other aborts", input: "x\n", wantErr: "press Enter to start"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := confirmReplayLaunch(&out, func(_ string, _ int, _ os.FileMode) (*os.File, error) {
				f, createErr := os.CreateTemp(t.TempDir(), "tty-*")
				if createErr != nil {
					return nil, createErr
				}
				if _, writeErr := f.WriteString(tt.input); writeErr != nil {
					_ = f.Close()
					return nil, writeErr
				}
				if _, seekErr := f.Seek(0, 0); seekErr != nil {
					_ = f.Close()
					return nil, seekErr
				}
				return f, nil
			})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("confirmReplayLaunch() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("confirmReplayLaunch() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestConfirmReplayLaunchNoTTY(t *testing.T) {
	var out bytes.Buffer
	err := confirmReplayLaunch(&out, func(_ string, _ int, _ os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("no tty")
	})
	if err == nil || !strings.Contains(err.Error(), "requires an interactive terminal") {
		t.Fatalf("confirmReplayLaunch() error = %v", err)
	}
}

func replayTestLaunchHooks(state tui.SubmissionState) tui.LaunchHooks {
	hooks := tui.NewLaunchHooks(nil)
	switch state {
	case tui.SubmissionSubmitted:
		hooks.NotifySubmitted()
	case tui.SubmissionUnknown:
		hooks.NotifySubmissionUnknown()
	}
	return hooks
}

func TestWaitForReplayAutoResultsCancelsFailedLaunchBeforeWaiting(t *testing.T) {
	for _, launchErr := range []error{
		errors.New("pre-submission launch failure"),
		context.Canceled,
		context.DeadlineExceeded,
	} {
		t.Run(launchErr.Error(), func(t *testing.T) {
			done := make(chan autoResultsOutcome, 1)
			var cancelCalls atomic.Int32
			var once sync.Once
			monitor := &autoResultsMonitor{
				done: done,
				cancel: func() {
					cancelCalls.Add(1)
					once.Do(func() { done <- autoResultsOutcome{} })
				},
			}
			started := time.Now()
			waitForReplayAutoResults(monitor, launchErr, replayTestLaunchHooks(tui.SubmissionNotSubmitted))
			if cancelCalls.Load() != 1 || time.Since(started) > 100*time.Millisecond {
				t.Fatalf("failed launch teardown calls=%d elapsed=%s",
					cancelCalls.Load(), time.Since(started))
			}
		})
	}
}

func TestWaitForReplayAutoResultsDoesNotCancelSuccessfulLaunch(t *testing.T) {
	done := make(chan autoResultsOutcome, 1)
	done <- autoResultsOutcome{}
	var cancelCalls atomic.Int32
	monitor := &autoResultsMonitor{done: done, cancel: func() { cancelCalls.Add(1) }}
	waitForReplayAutoResults(monitor, nil, replayTestLaunchHooks(tui.SubmissionSubmitted))
	if cancelCalls.Load() != 0 {
		t.Fatalf("successful launch canceled monitor %d time(s)", cancelCalls.Load())
	}
}

func TestWaitForReplayAutoResultsPreservesArmedMonitorAfterLaunchError(t *testing.T) {
	done := make(chan autoResultsOutcome, 1)
	done <- autoResultsOutcome{}
	var cancelCalls atomic.Int32
	monitor := &autoResultsMonitor{done: done, cancel: func() { cancelCalls.Add(1) }}
	waitForReplayAutoResults(monitor, errors.New("post-submission UI failure"), replayTestLaunchHooks(tui.SubmissionSubmitted))
	if cancelCalls.Load() != 0 {
		t.Fatalf("post-submission launch error canceled monitor %d time(s)", cancelCalls.Load())
	}
}

func TestWaitForReplayAutoResultsCancelsAmbiguousSubmissionExactlyOnce(t *testing.T) {
	done := make(chan autoResultsOutcome, 1)
	var cancelCalls atomic.Int32
	var once sync.Once
	monitor := &autoResultsMonitor{
		done: done,
		cancel: func() {
			cancelCalls.Add(1)
			once.Do(func() { done <- autoResultsOutcome{} })
		},
	}
	waitForReplayAutoResults(
		monitor, errors.New("ambiguous submission"), replayTestLaunchHooks(tui.SubmissionUnknown))
	if cancelCalls.Load() != 1 {
		t.Fatalf("ambiguous submission cleanup calls = %d, want 1", cancelCalls.Load())
	}
}

func TestWaitForReplayAutoResultsCancelsAcceptedForCleanup(t *testing.T) {
	done := make(chan autoResultsOutcome, 1)
	var cancelCalls atomic.Int32
	monitor := &autoResultsMonitor{
		done: done,
		cancel: func() {
			cancelCalls.Add(1)
			done <- autoResultsOutcome{}
		},
	}
	hooks := tui.NewLaunchHooks(nil)
	hooks.NotifyAcceptedForCleanup()
	waitForReplayAutoResults(monitor, errors.New("invalid response identity"), hooks)
	if cancelCalls.Load() != 1 {
		t.Fatalf("accepted-for-cleanup cancellation calls = %d, want 1", cancelCalls.Load())
	}
}

func TestWaitForReplayAutoResultsJoinsCoordinator(t *testing.T) {
	done := make(chan autoResultsOutcome)
	returned := make(chan struct{})
	go func() {
		waitForReplayAutoResults(&autoResultsMonitor{done: done}, nil, replayTestLaunchHooks(tui.SubmissionSubmitted))
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("replay returned before coordinator completion")
	case <-time.After(20 * time.Millisecond):
	}
	done <- autoResultsOutcome{}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("replay did not return after coordinator completion")
	}
}

func TestReplayCommandFailedLaunchJoinsBoundedAutoResults(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	tests := []struct {
		name     string
		hosts    string
		headless bool
		wantPath string
		wantErr  error
	}{
		{name: "local headless", hosts: "127.0.0.1", headless: true, wantPath: "local-headless", wantErr: errors.New("local headless failed")},
		{name: "local tui", hosts: "127.0.0.1", wantPath: "local-tui", wantErr: errors.New("local tui failed")},
		{name: "remote headless cancellation", hosts: "qa-entry.example", headless: true, wantPath: "remote-headless", wantErr: context.Canceled},
		{name: "remote tui", hosts: "qa-entry.example", wantPath: "remote-tui", wantErr: errors.New("remote tui failed")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			origPort := resolvePortConflictFunc
			origConnect := connectReplayOrchestrator
			origConfirm := confirmReplayLaunchCommand
			origRemoteHeadless := startReplayRemoteHeadless
			origRemoteTUI := startReplayRemoteTUI
			origLocalHeadless := startReplayLocalHeadless
			origLocalTUI := startReplayLocalTUI
			origShouldHeadless := shouldReplayRunHeadless
			t.Cleanup(func() {
				resolvePortConflictFunc = origPort
				connectReplayOrchestrator = origConnect
				confirmReplayLaunchCommand = origConfirm
				startReplayRemoteHeadless = origRemoteHeadless
				startReplayRemoteTUI = origRemoteTUI
				startReplayLocalHeadless = origLocalHeadless
				startReplayLocalTUI = origLocalTUI
				shouldReplayRunHeadless = origShouldHeadless
			})
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			confirmReplayLaunchCommand = func(io.Writer) error { return nil }
			shouldReplayRunHeadless = func(*cobra.Command) bool { return test.headless }
			var pathCalls atomic.Int32
			var sessionManagedCalls atomic.Int32
			startReplayRemoteHeadless = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
			) error {
				if test.wantPath == "remote-headless" {
					pathCalls.Add(1)
					if !options.Verbose {
						t.Fatal("--verbose did not reach remote headless options")
					}
					if options.LaunchHooks.SessionManaged() {
						sessionManagedCalls.Add(1)
					}
				}
				return test.wantErr
			}
			startReplayRemoteTUI = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options tui.RunOptions,
			) error {
				if test.wantPath == "remote-tui" {
					pathCalls.Add(1)
					if !options.Verbose {
						t.Fatal("--verbose did not reach remote TUI options")
					}
					if options.LaunchHooks.SessionManaged() {
						sessionManagedCalls.Add(1)
					}
				}
				return test.wantErr
			}
			startReplayLocalHeadless = func(
				_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
			) error {
				if test.wantPath == "local-headless" {
					pathCalls.Add(1)
					if !options.Verbose {
						t.Fatal("--verbose did not reach local headless options")
					}
					if options.LaunchHooks.SessionManaged() {
						sessionManagedCalls.Add(1)
					}
				}
				return test.wantErr
			}
			startReplayLocalTUI = func(_ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				if test.wantPath == "local-tui" {
					pathCalls.Add(1)
					if !options.Verbose {
						t.Fatal("--verbose did not reach local TUI options")
					}
					if options.LaunchHooks.SessionManaged() {
						sessionManagedCalls.Add(1)
					}
				}
				return test.wantErr
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			args := []string{
				"--from", server.URL, "--endpoints", "http://s3.example",
				"--test-hosts", test.hosts, "--results-dir", t.TempDir(), "--verbose",
			}
			if test.headless {
				args = append(args, "--headless")
			}
			cmd.SetArgs(args)
			started := time.Now()
			err := cmd.Execute()
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Execute() error = %v, want %v", err, test.wantErr)
			}
			if pathCalls.Load() != 1 || time.Since(started) > 2*time.Second {
				t.Fatalf("replay failure path calls=%d elapsed=%s", pathCalls.Load(), time.Since(started))
			}
			if sessionManagedCalls.Load() != 1 {
				t.Fatalf("session-managed replay route calls=%d, want 1", sessionManagedCalls.Load())
			}
		})
	}
}

func newReplayArchiveServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="scenario.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/scenario.json`)
	})
	mux.HandleFunc("/scenario.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type":"sequential",
  "config":{"storage":{"driver":{"type":"s3"}}},
  "steps":[{"type":"load","config":{"item":{"data":{"size":"1KB"}},"test":{"step":{"id":"W","limit":{"count":1}}},"load":{"limit":{"concurrency":1}}}}]
}`)
	})
	return httptest.NewServer(mux)
}

func newReplayCommandForTest(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{
		Use:          "replay",
		SilenceUsage: true,
		RunE:         runReplay,
	}
	cmd.Flags().String("from", "", "")
	cmd.Flags().Bool("generate-only", false, "")
	cmd.Flags().StringP("output-dir", "O", "", "")
	cmd.Flags().StringSliceP("endpoints", "e", []string{}, "")
	cmd.Flags().String("endpoint", "", "")
	cmd.Flags().StringP("access-key", "a", "", "")
	cmd.Flags().StringP("secret-key", "s", "", "")
	cmd.Flags().StringP("bucket", "b", "", "")
	cmd.Flags().Int("auth-version", 4, "")
	cmd.Flags().String("test-hosts", "127.0.0.1", "")
	cmd.Flags().String("label", "replay", "")
	cmd.Flags().String("s3-driver", "default", "")
	cmd.Flags().Bool("headless", false, "")
	cmd.Flags().Bool("minimal", false, "")
	cmd.Flags().Int("auto-terminate-seconds", 0, "")
	cmd.Flags().Bool("force", false, "")
	cmd.Flags().String("api-port", "", "")
	cmd.Flags().Int("min-hosts", 0, "")
	cmd.Flags().Bool(flagAttachExistingWorkers, false, "")
	cmd.Flags().String("network-mode", "host", "")
	cmd.Flags().Int("rmi-port-start", 40000, "")
	cmd.Flags().Int("rmi-port-count", 10, "")
	cmd.Flags().Bool(flagSkipImagePull, false, "")
	cmd.Flags().String(flagSptImage, "", "")
	cmd.Flags().String("trace-file", "", "")
	cmd.Flags().Bool("trace-append", false, "")
	cmd.Flags().Bool("verbose", false, "")
	cmd.Flags().Bool("auto-results", true, "")
	cmd.Flags().String("results-dir", "./results", "")
	cmd.Flags().Bool("auto-results-debug", false, "")
	cmd.Flags().Bool("shutdown-on-complete", true, "")
	cmd.Flags().Int("shutdown-linger", 5, "")
	return cmd
}
