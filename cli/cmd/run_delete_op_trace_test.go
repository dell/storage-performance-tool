package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const opTraceRefusal = "operation tracing (output.metrics.trace.persist=true) is not supported for standalone DELETE"

// deleteManifest writes an --items-file manifest for the bucket, outside the working directory.
func deleteManifest(t *testing.T, bucket string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "delete.csv")
	if err := os.WriteFile(path, []byte("bucket,key,size,version_id\n"+bucket+",key,1,\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// standaloneDeleteInputs are the ways spt run delete selects what it deletes; each runs one
// standalone DELETE step.
func standaloneDeleteInputs(t *testing.T) map[string][]string {
	t.Helper()
	return map[string][]string{
		"seeded":     {"--bucket", "owned"},
		"items file": {"--bucket", "owned", "--items-file", deleteManifest(t, "owned")},
	}
}

func overrideArgs(overrides ...string) []string {
	args := make([]string, 0, 2*len(overrides))
	for _, override := range overrides {
		args = append(args, "--"+flagEngineOverride, override)
	}
	return args
}

// generatedOpTracePersist returns the output.metrics.trace.persist value in the defaults the run
// sends to the engine, or nil when they don't set it.
func generatedOpTracePersist(t *testing.T, params scenario.Params) any {
	t.Helper()
	data, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Output struct {
			Metrics struct {
				Trace map[string]any `yaml:"trace"`
			} `yaml:"metrics"`
		} `yaml:"output"`
	}
	if err := yaml.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	return root.Output.Metrics.Trace["persist"]
}

func TestStandaloneDeleteRefusesOperationTracingFromEveryOverrideRoute(t *testing.T) {
	for _, test := range []struct {
		name      string
		overrides []string
		env       string
	}{
		{name: "dot path", overrides: []string{"output.metrics.trace.persist=true"}},
		{name: "dash path", overrides: []string{"output-metrics-trace-persist=true"}},
		{name: "map value of an ancestor", overrides: []string{"output.metrics.trace={persist: true}"}},
		{name: "string the engine reads as true", overrides: []string{`output.metrics.trace.persist="TRUE"`}},
		{name: "a later override turns it on", overrides: []string{
			"output.metrics.trace.persist=false", "output.metrics.trace.persist=true",
		}},
		{name: "SPT_ENGINE_OVERRIDES", env: "storage.driver.threads=6; output.metrics.trace.persist=true"},
	} {
		for input, args := range standaloneDeleteInputs(t) {
			t.Run(test.name+"/"+input, func(t *testing.T) {
				clearEnvDefaultsTestEnv(t)
				if test.env != "" {
					t.Setenv(constants.EnvEngineOverrides, test.env)
				}
				run := prepareBucketRun(t, WorkloadTypeDelete, append(args, overrideArgs(test.overrides...)...)...)
				if run.stage != "validate" || run.err == nil || !strings.Contains(run.err.Error(), opTraceRefusal) {
					t.Fatalf("stage %q error = %v, want validation to refuse %q", run.stage, run.err, opTraceRefusal)
				}
				if !strings.Contains(run.err.Error(), "--engine-override or SPT_ENGINE_OVERRIDES") {
					t.Fatalf("refusal %q does not say where the setting came from", run.err)
				}
			})
		}
	}
}

func TestStandaloneDeleteKeepsOverridesThatLeaveOperationTracingOff(t *testing.T) {
	for _, test := range []struct {
		name      string
		overrides []string
	}{
		{name: "no override"},
		{name: "tracing off", overrides: []string{"output.metrics.trace.persist=false"}},
		{name: "a later override turns it off", overrides: []string{
			"output.metrics.trace.persist=true", "output.metrics.trace.persist=false",
		}},
		{name: "a later map value replaces it", overrides: []string{
			"output.metrics.trace.persist=true", "output.metrics.trace={}",
		}},
		{name: "unrelated override", overrides: []string{"storage.driver.threads=6"}},
	} {
		for input, args := range standaloneDeleteInputs(t) {
			t.Run(test.name+"/"+input, func(t *testing.T) {
				clearEnvDefaultsTestEnv(t)
				run := prepareBucketRun(t, WorkloadTypeDelete, append(args, overrideArgs(test.overrides...)...)...).
					mustSucceed(t)
				if persist := generatedOpTracePersist(t, run.params); persist == true {
					t.Fatalf("accepted DELETE sends output.metrics.trace.persist=%v to the engine", persist)
				}
			})
		}
	}
}

// Op tracing stays available to every workload that runs no standalone DELETE step, including
// one whose cleanup deletes what it wrote.
func TestOperationTracingStaysAvailableOutsideStandaloneDelete(t *testing.T) {
	for _, test := range []struct {
		workload string
		args     []string
	}{
		{workload: WorkloadTypeWrite, args: []string{"--bucket", "ps"}},
		{workload: WorkloadTypeWriteVerify, args: []string{"--bucket", "ps", "--cleanup"}},
	} {
		t.Run(test.workload, func(t *testing.T) {
			clearEnvDefaultsTestEnv(t)
			run := prepareBucketRun(t, test.workload,
				append(test.args, overrideArgs("output.metrics.trace.persist=true")...)...).mustSucceed(t)
			if persist := generatedOpTracePersist(t, run.params); persist != true {
				t.Fatalf("traced %s sends output.metrics.trace.persist=%v, want true", test.workload, persist)
			}
		})
	}
}

// A refused DELETE stops in spt run's PreRunE, before RunE prepares, writes or launches anything,
// so a seeded DELETE never writes its seed objects. The hooks are spt run's own, except that
// PersistentPreRunE leaves out starting the logger, which writes spt.log.
func TestTracedStandaloneDeleteStopsBeforeTheRunStarts(t *testing.T) {
	errRunStarted := errors.New("RunE started")
	inputs := standaloneDeleteInputs(t)
	for _, test := range []struct {
		name    string
		input   string
		args    []string
		dotEnv  string
		refused bool
	}{
		{name: "seeded, from --engine-override", input: "seeded",
			args: overrideArgs("output.metrics.trace.persist=true"), refused: true},
		{name: "items file, from --engine-override", input: "items file",
			args: overrideArgs("output.metrics.trace.persist=true"), refused: true},
		{name: "seeded, from SPT_ENGINE_OVERRIDES in .env", input: "seeded",
			dotEnv: constants.EnvEngineOverrides + "=output.metrics.trace.persist=true\n", refused: true},
		{name: "seeded, untraced", input: "seeded"},
		{name: "items file, untraced", input: "items file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			workdir := t.TempDir()
			t.Chdir(workdir)
			clearEnvDefaultsTestEnv(t)
			if test.dotEnv != "" {
				if err := os.WriteFile(".env", []byte(test.dotEnv), 0o600); err != nil {
					t.Fatal(err)
				}
				// .env never replaces a variable that is set, even to empty; t.Setenv restores it.
				if err := os.Unsetenv(constants.EnvEngineOverrides); err != nil {
					t.Fatal(err)
				}
			}

			cmd := freshCommandFrom(t, runCmd)
			cmd.Args = runCmd.Args
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			cmd.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
				_ = config.LoadDotEnv()
				return applyEnvDefaultsToRunFlags(cmd)
			}
			cmd.PreRunE = runCmd.PreRunE
			var runs int
			cmd.RunE = func(*cobra.Command, []string) error {
				runs++
				return errRunStarted
			}
			args := append([]string{WorkloadTypeDelete}, bucketListConnection...)
			args = append(append(args, inputs[test.input]...), test.args...)
			cmd.SetArgs(args)

			err := cmd.Execute()
			if !test.refused {
				if !errors.Is(err, errRunStarted) || runs != 1 {
					t.Fatalf("untraced DELETE error = %v after %d RunE calls, want it to start", err, runs)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), opTraceRefusal) || runs != 0 {
				t.Fatalf("traced DELETE error = %v after %d RunE calls, want %q before RunE", err, runs, opTraceRefusal)
			}
			entries, readErr := os.ReadDir(workdir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			for _, entry := range entries {
				if entry.Name() != ".env" {
					t.Fatalf("refused DELETE wrote %s", entry.Name())
				}
			}
		})
	}
}
