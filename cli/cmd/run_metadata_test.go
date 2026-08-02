package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

func TestBuildRunMetadata_CommandIsSanitized(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"spt", "run", "write", "--secret-key", "verysecret", "--access-key=OPENKEY"}
	defer func() { os.Args = oldArgs }()

	meta := buildRunMetadata(runMetadataInput{
		WorkloadType: "write",
		Params:       scenario.Params{},
		ScenarioPath: "./tmp/scenario.js",
		ResultsOptions: ResultsOptions{
			AutoResults: false,
			ResultsDir:  "./results",
			Label:       "mt",
		},
		SptImage: "repo/spt:latest",
	})

	if len(meta.CLI.Command) == 0 {
		t.Fatalf("CLI command should not be empty")
	}
	joined := strings.Join(meta.CLI.Command, " ")
	if strings.Contains(joined, "verysecret") || strings.Contains(joined, "OPENKEY") {
		t.Fatalf("metadata command leaked credentials: %q", joined)
	}
	if !strings.Contains(joined, "--secret-key ***") || !strings.Contains(joined, "--access-key=***") {
		t.Fatalf("metadata command missing masked values: %q", joined)
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

// TestCaptureChangedFlagsExcludesEnvApplied guards spt_run_params.json's
// core purpose here: pflag's FlagSet.Set() marks a flag Changed=true
// regardless of who called it, so without markEnvApplied/this exclusion,
// an env-injected default would be indistinguishable from something the
// user actually typed on the command line.
func TestCaptureChangedFlagsExcludesEnvApplied(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Int("threads", 1, "")
	cmd.Flags().String("bucket", "", "")

	_ = cmd.Flags().Set("threads", "16") // explicit CLI flag
	markEnvApplied(cmd, "bucket", "env-bucket")
	_ = cmd.Flags().Set("bucket", "env-bucket") // env default, via setFromEnv in practice

	changed := captureChangedFlags(cmd)
	if changed["threads"] != "16" {
		t.Fatalf("threads stored as %q, want 16", changed["threads"])
	}
	if _, ok := changed["bucket"]; ok {
		t.Fatalf("env-applied bucket should not appear in changedFlags: %+v", changed)
	}

	applied := captureEnvAppliedFlags(cmd)
	if applied["bucket"] != "env-bucket" {
		t.Fatalf("envAppliedFlags[bucket] = %q, want env-bucket", applied["bucket"])
	}
	if _, ok := applied["threads"]; ok {
		t.Fatalf("explicit threads flag should not appear in envAppliedFlags: %+v", applied)
	}
}

func TestCaptureEnvAppliedFlagsMasksSensitiveValues(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("secret-key", "", "")
	markEnvApplied(cmd, "secret-key", "supersecret")

	applied := captureEnvAppliedFlags(cmd)
	if applied["secret-key"] != "sup***" {
		t.Fatalf("secret-key stored as %q, want sup***", applied["secret-key"])
	}
}

// TestBuildRunMetadata_DistinguishesExplicitFromEnvAppliedFlags is an
// end-to-end check through the real applyEnvDefaultsToRunFlags +
// buildRunMetadata path, not just the two capture helpers in isolation.
func TestBuildRunMetadata_DistinguishesExplicitFromEnvAppliedFlags(t *testing.T) {
	cmd := newRunLikeCmd()
	clearEnvDefaultsTestEnv(t)
	_ = cmd.Flags().Set("threads", "8") // explicit CLI flag

	t.Setenv("S3_BUCKET", "env-bucket")
	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatalf("applyEnvDefaultsToRunFlags error: %v", err)
	}

	meta := buildRunMetadata(runMetadataInput{
		WorkloadType: "write",
		Params:       scenario.Params{},
		ScenarioPath: "./tmp/scenario.js",
		ResultsOptions: ResultsOptions{
			ResultsDir: "./results",
			Label:      "mt",
		},
		Command: cmd,
	})

	if meta.CLI.ChangedFlags["threads"] != "8" {
		t.Fatalf("ChangedFlags[threads] = %q, want 8", meta.CLI.ChangedFlags["threads"])
	}
	if _, ok := meta.CLI.ChangedFlags["bucket"]; ok {
		t.Fatalf("env-applied bucket leaked into ChangedFlags: %+v", meta.CLI.ChangedFlags)
	}
	if meta.CLI.EnvAppliedFlags["bucket"] != "env-bucket" {
		t.Fatalf("EnvAppliedFlags[bucket] = %q, want env-bucket", meta.CLI.EnvAppliedFlags["bucket"])
	}
	if _, ok := meta.CLI.EnvAppliedFlags["threads"]; ok {
		t.Fatalf("explicit threads flag leaked into EnvAppliedFlags: %+v", meta.CLI.EnvAppliedFlags)
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

func TestArchivePreparedRunInputsUsesExactSubmittedBytes(t *testing.T) {
	dir := t.TempDir()
	scenarioPath := filepath.Join(dir, "prepared.js")
	if err := os.WriteFile(scenarioPath, []byte("later file content"), 0o600); err != nil {
		t.Fatal(err)
	}
	resultsDir := filepath.Join(dir, "results")
	if err := os.MkdirAll(resultsDir, 0o750); err != nil {
		t.Fatal(err)
	}
	meta := &runMetadata{
		preparedInputs:       true,
		preparedScenarioJS:   []byte("exact submitted scenario"),
		preparedDefaultsYAML: []byte("exact submitted defaults"),
	}
	if err := archivePreparedRunInputs(meta, scenarioPath, resultsDir); err != nil {
		t.Fatalf("archivePreparedRunInputs() error = %v", err)
	}
	archivedScenario, err := os.ReadFile(filepath.Join(resultsDir, "prepared.js"))
	if err != nil {
		t.Fatal(err)
	}
	archivedDefaults, err := os.ReadFile(
		filepath.Join(resultsDir, constants.ResultsPreparedDefaultsFileName))
	if err != nil {
		t.Fatal(err)
	}
	if string(archivedScenario) != "exact submitted scenario" ||
		string(archivedDefaults) != "exact submitted defaults" {
		t.Fatalf("archived scenario/defaults = %q/%q", archivedScenario, archivedDefaults)
	}
	if meta.ScenarioStoredPath != "prepared.js" ||
		meta.DefaultsStoredPath != constants.ResultsPreparedDefaultsFileName {
		t.Fatalf("stored paths = %q/%q", meta.ScenarioStoredPath, meta.DefaultsStoredPath)
	}
}

func TestCopyScenarioForResultsSkipsSelfCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "scenario.js")
	if err := os.WriteFile(src, []byte("content"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	rel, err := copyScenarioForResults(src, dir)
	if err != nil {
		t.Fatalf("copyScenarioForResults error = %v", err)
	}
	if rel != "scenario.js" {
		t.Fatalf("relative name = %q, want scenario.js", rel)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read scenario: %v", err)
	}
	if string(data) != "content" {
		t.Fatalf("scenario content = %q, want content", string(data))
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
	meta.TraceFile = "results/mt-20260506.204108.064/spt-20260506.204108.064.trace.log"
	meta.TraceAuto = true

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
	if meta.TraceFile == "" {
		t.Fatalf("TraceFile should be populated")
	}
	if !meta.TraceAuto {
		t.Fatalf("TraceAuto should be true")
	}
}
