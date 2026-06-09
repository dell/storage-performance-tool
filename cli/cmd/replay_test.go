package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/spf13/cobra"
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
		"  - rejected: rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB (not recognized by replay command whitelist)\n",
		"\nErrors\n  - unsupported command step: rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB\n",
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
