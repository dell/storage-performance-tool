package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func TestMaskScenarioParams(t *testing.T) {
	orig := scenario.Params{
		AccessKey: "ABCDEF",
		SecretKey: "SECRET",
	}
	masked := maskScenarioParams(orig)
	if masked.AccessKey != "ABC***" {
		t.Fatalf("AccessKey mask = %q, want %q", masked.AccessKey, "ABC***")
	}
	if masked.SecretKey != "SEC***" {
		t.Fatalf("SecretKey mask = %q, want %q", masked.SecretKey, "SEC***")
	}
	if orig.AccessKey != "ABCDEF" || orig.SecretKey != "SECRET" {
		t.Fatalf("original params were modified: %+v", orig)
	}
}

func TestSanitizeCommandArgsMasksSensitiveFlags(t *testing.T) {
	args := []string{
		"spt", "run", "write",
		"--secret-key=verysecret",
		"--access-key", "OPENKEY",
		"-s", "short",
		"-a=value",
	}
	got := sanitizeCommandArgs(args)
	want := []string{
		"spt", "run", "write",
		"--secret-key=***",
		"--access-key", "***",
		"-s", "***",
		"-a=***",
	}
	if len(got) != len(want) {
		t.Fatalf("sanitized args length %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sanitized args[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCaptureChangedFlagsMasksSensitiveValues(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("secret-key", "", "")
	cmd.Flags().String("access-key", "", "")
	cmd.Flags().Int("threads", 1, "")

	_ = cmd.Flags().Set("secret-key", "supersecret")
	_ = cmd.Flags().Set("threads", "16")

	flags := captureChangedFlags(cmd)
	if flags["secret-key"] != "sup***" {
		t.Fatalf("secret-key stored as %q, want sup***", flags["secret-key"])
	}
	if flags["threads"] != "16" {
		t.Fatalf("threads stored as %q, want 16", flags["threads"])
	}
	if _, ok := flags["access-key"]; ok {
		t.Fatalf("access-key should not be present when unchanged")
	}
}

func TestCopyScenarioForResults(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "scenario.js")
	if err := os.WriteFile(src, []byte("content"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	destDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatalf("mkdir out: %v", err)
	}

	rel, err := copyScenarioForResults(src, destDir)
	if err != nil {
		t.Fatalf("copyScenarioForResults error: %v", err)
	}
	if rel != "scenario.js" {
		t.Fatalf("expected relative name scenario.js, got %q", rel)
	}
	destPath := filepath.Join(destDir, rel)
	data, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read copied file: %v", err)
	}
	if string(data) != "content" {
		t.Fatalf("copied content = %q, want %q", string(data), "content")
	}
}

func TestWriteRunMetadata(t *testing.T) {
	dir := t.TempDir()
	meta := &runMetadata{
		WorkloadType: "write",
		SptImage:     "repo/spt:latest",
		APIPort:      "9999",
		ResultsDir:   "./results",
	}
	if err := writeRunMetadata(meta, dir); err != nil {
		t.Fatalf("writeRunMetadata error: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(content, &decoded); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if decoded["workloadType"] != "write" {
		t.Fatalf("workloadType = %v, want write", decoded["workloadType"])
	}
	if decoded["resultsDir"] != "./results" {
		t.Fatalf("resultsDir = %v, want ./results", decoded["resultsDir"])
	}
}

func TestBuildRunMetadataPopulatesCoreFields(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}}
	cmd := &cobra.Command{Use: "spt"}
	cmd.Flags().Int("threads", 1, "")
	_ = cmd.Flags().Set("threads", "4")

	meta := buildRunMetadata(runMetadataInput{
		WorkloadType:    "write",
		Params:          scenario.Params{AccessKey: "ACCESS", SecretKey: "SECRET", Threads: 4},
		ScenarioPath:    "./tmp/scenario.js",
		ResultsOptions:  ResultsOptions{AutoResults: true, ResultsDir: "./results", Label: "mt", Debug: false},
		HostInfos:       hostInfos,
		APIPort:         "9999",
		BaseURL:         "http://localhost:9999",
		ExpectedStepIDs: []string{"step-001"},
		SptImage:        "repo/spt:latest",
		Command:         cmd,
	})

	if meta.ScenarioFile != "scenario.js" {
		t.Fatalf("ScenarioFile = %q, want scenario.js", meta.ScenarioFile)
	}
	if meta.ScenarioParams.AccessKey != "ACC***" {
		t.Fatalf("ScenarioParams.AccessKey = %q, want ACC***", meta.ScenarioParams.AccessKey)
	}
	if len(meta.Hosts) != 1 || meta.Hosts[0].Host != "127.0.0.1" {
		t.Fatalf("Hosts = %+v, want single localhost", meta.Hosts)
	}
	if meta.CLI.Command == nil || len(meta.CLI.Command) == 0 {
		t.Fatalf("CLI command should not be empty")
	}
	if len(meta.ExpectedStepIDs) != 1 || meta.ExpectedStepIDs[0] != "step-001" {
		t.Fatalf("ExpectedStepIDs = %+v, want [step-001]", meta.ExpectedStepIDs)
	}
}
