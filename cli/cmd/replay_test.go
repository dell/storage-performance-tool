package cmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestReplayCommandRejectsMultiHostLaunchForNow(t *testing.T) {
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

	cmd := newReplayCommandForTest(t)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://10.0.0.1:9020",
		"--test-hosts", "127.0.0.1,127.0.0.2",
	})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil, want multi-host not implemented")
	}
	if !strings.Contains(err.Error(), "multi-host replay execution is not implemented yet") {
		t.Fatalf("error = %v", err)
	}
}

func TestReplayCommandRejectsSingleRemoteHostLaunchForNow(t *testing.T) {
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

	cmd := newReplayCommandForTest(t)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://10.0.0.1:9020",
		"--test-hosts", "qa-client-01",
	})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil, want single remote-host not implemented")
	}
	if !strings.Contains(err.Error(), "single remote-host replay execution is not implemented yet") {
		t.Fatalf("error = %v", err)
	}
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
