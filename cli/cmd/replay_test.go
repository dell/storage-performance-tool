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

func TestReplayCommandRequiresGenerateOnlyForNow(t *testing.T) {
	cmd := newReplayCommandForTest(t)
	cmd.SetArgs([]string{"--from", "http://example.invalid/archive/"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil, want not implemented")
	}
	if !strings.Contains(err.Error(), "replay execution is not implemented yet") {
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
	return cmd
}
