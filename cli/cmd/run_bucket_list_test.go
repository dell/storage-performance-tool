package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

// freshCommandFrom returns a command with a fresh copy of every flag of source at its default, so
// a test drives the real validation and parameter path without sharing the command's state.
func freshCommandFrom(t *testing.T, source *cobra.Command) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: source.Use}
	source.Flags().VisitAll(func(f *pflag.Flag) {
		scratch := pflag.NewFlagSet("scratch", pflag.ContinueOnError)
		switch f.Value.Type() {
		case "bool":
			value, _ := strconv.ParseBool(f.DefValue)
			scratch.Bool(f.Name, value, f.Usage)
		case "int":
			value, _ := strconv.Atoi(f.DefValue)
			scratch.Int(f.Name, value, f.Usage)
		case "int64":
			value, _ := strconv.ParseInt(f.DefValue, 10, 64)
			scratch.Int64(f.Name, value, f.Usage)
		case "float64":
			value, _ := strconv.ParseFloat(f.DefValue, 64)
			scratch.Float64(f.Name, value, f.Usage)
		case "duration":
			value, _ := time.ParseDuration(f.DefValue)
			scratch.Duration(f.Name, value, f.Usage)
		case "string":
			scratch.String(f.Name, f.DefValue, f.Usage)
		case "stringSlice":
			scratch.StringSlice(f.Name, defaultList(f.DefValue), f.Usage)
		case "stringArray":
			scratch.StringArray(f.Name, defaultList(f.DefValue), f.Usage)
		default:
			t.Fatalf("flag --%s has type %s, which the copy does not handle", f.Name, f.Value.Type())
		}
		clone := scratch.Lookup(f.Name)
		clone.Shorthand = f.Shorthand
		clone.NoOptDefVal = f.NoOptDefVal
		clone.Hidden = f.Hidden
		clone.Deprecated = f.Deprecated
		clone.Annotations = f.Annotations
		cmd.Flags().AddFlag(clone)
	})
	return cmd
}

func defaultList(value string) []string {
	value = strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

var bucketListConnection = []string{"--endpoints", "http://127.0.0.1:9000", "--access-key", "a", "--secret-key", "s"}

type preparedBucketRun struct {
	cmd    *cobra.Command
	params scenario.Params
	stage  string // where it stopped: "flags", "env", "validate", "params" or "" when it succeeded
	err    error
	stderr string
}

// prepareBucketRun takes spt run's flags through the steps the command takes before it runs:
// environment defaults (PersistentPreRunE), ValidateRunCommand (PreRunE), then buildScenarioParams.
func prepareBucketRun(t *testing.T, workload string, args ...string) preparedBucketRun {
	t.Helper()
	cmd := freshCommandFrom(t, runCmd)
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	run := preparedBucketRun{cmd: cmd}
	fail := func(stage string, err error) preparedBucketRun {
		run.stage, run.err, run.stderr = stage, err, stderr.String()
		return run
	}
	if err := cmd.ParseFlags(append(append([]string{}, bucketListConnection...), args...)); err != nil {
		return fail("flags", err)
	}
	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		return fail("env", err)
	}
	if err := ValidateRunCommand(cmd, []string{workload}); err != nil {
		return fail("validate", err)
	}
	params, err := buildScenarioParams(workload, cmd)
	if err != nil {
		return fail("params", err)
	}
	run.params, run.stderr = params, stderr.String()
	return run
}

func (r preparedBucketRun) mustSucceed(t *testing.T) preparedBucketRun {
	t.Helper()
	if r.err != nil {
		t.Fatalf("%s: %v", r.stage, r.err)
	}
	return r
}

func writeBucketsFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "buckets.txt")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func generatedTargetBucketSetting(t *testing.T, params scenario.Params) map[string]any {
	t.Helper()
	data, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Storage struct {
			TargetBucket map[string]any `yaml:"targetBucket"`
		} `yaml:"storage"`
	}
	if err := yaml.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	return root.Storage.TargetBucket
}

func TestRunRegistersTheBucketListFlags(t *testing.T) {
	buckets := runCmd.Flags().Lookup(flagBuckets)
	file := runCmd.Flags().Lookup(flagBucketsFile)
	if buckets == nil || buckets.Value.Type() != "stringSlice" || file == nil || file.Value.Type() != "string" {
		t.Fatal("spt run must register --buckets (comma-separated or repeated) and --buckets-file")
	}
	for _, want := range []string{"used as written and in this order", "--provision-target-bucket=false",
		"never lists, reads or deletes an object it did not write", "except those an items file", "one credential set",
		constants.EnvS3Buckets} {
		if !strings.Contains(buckets.Usage, want) {
			t.Fatalf("--buckets help lacks %q: %s", want, buckets.Usage)
		}
	}
	if !strings.Contains(file.Usage, "one per line") || !strings.Contains(file.Usage, "starting with # are ignored") {
		t.Fatalf("--buckets-file help: %s", file.Usage)
	}
}

func TestBucketListFlagExclusionsNameBothFlags(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	file := writeBucketsFile(t, "ps-eu\nPS_US\n")
	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{"--buckets", "ps-eu,PS_US", "--buckets-file", file}, []string{"--buckets", "--buckets-file"}},
		{[]string{"--buckets", "ps-eu,PS_US", "--bucket-count", "4"}, []string{"--buckets", "--bucket-count"}},
		{[]string{"--buckets", "ps-eu,PS_US", "--bucket-count", "1"}, []string{"--buckets", "--bucket-count"}},
		{[]string{"--buckets", "ps-eu,PS_US", "--bucket", "ps"}, []string{"--buckets and --bucket "}},
		{[]string{"--buckets", "ps-eu", "--bucket", "ps-eu"}, []string{"--buckets and --bucket "}},
		{[]string{"--buckets-file", file, "--bucket-count", "4"}, []string{"--buckets-file", "--bucket-count"}},
		{[]string{"--buckets-file", file, "--bucket-count", "1"}, []string{"--buckets-file", "--bucket-count"}},
		{[]string{"--buckets-file", file, "--bucket", "ps"}, []string{"--buckets-file and --bucket "}},
	} {
		run := prepareBucketRun(t, WorkloadTypeWrite, c.args...)
		if run.stage != "validate" {
			t.Fatalf("%q: stage %q err %v, want a validation error", c.args, run.stage, run.err)
		}
		for _, want := range c.want {
			if !strings.Contains(run.err.Error(), want) {
				t.Fatalf("%q: err = %v, want %q", c.args, run.err, want)
			}
		}
	}
}

// --bucket NAME --bucket-count N stays the way to choose a generated set and its base.
func TestShippedBucketCountSyntaxIsUnchanged(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	set := prepareBucketRun(t, WorkloadTypeWrite, "--bucket", "ps", "--bucket-count", "4").mustSucceed(t).params
	if set.Bucket != "ps" || set.BucketCount != 4 || set.BucketNames != nil {
		t.Fatalf("--bucket ps --bucket-count 4: %+v", set)
	}
	if node := generatedTargetBucketSetting(t, set); !reflect.DeepEqual(node, map[string]any{"count": 4}) {
		t.Fatalf("--bucket ps --bucket-count 4: storage.targetBucket = %v, want only count 4", node)
	}
	one := prepareBucketRun(t, WorkloadTypeWrite, "--bucket", "ps", "--bucket-count", "1").mustSucceed(t).params
	plain := prepareBucketRun(t, WorkloadTypeWrite, "--bucket", "ps").mustSucceed(t).params
	if !reflect.DeepEqual(one, plain) || generatedTargetBucketSetting(t, one) != nil {
		t.Fatalf("--bucket-count 1 must equal --bucket alone: %+v vs %+v", one, plain)
	}
}

func TestBucketEnvironmentPrecedence(t *testing.T) {
	file := writeBucketsFile(t, "file-a\nfile-b\n")
	for _, c := range []struct {
		name        string
		env         map[string]string
		args        []string
		wantBucket  string
		wantNames   []string
		wantCount   int
		wantNotice  bool
		wantFromEnv []string // flags the environment supplied
	}{
		{"S3_BUCKETS alone", map[string]string{constants.EnvS3Buckets: "a,b"}, nil, "a", []string{"a", "b"}, 0, false, []string{flagBuckets}},
		{"S3_BUCKETS wins over S3_BUCKET", map[string]string{constants.EnvS3Buckets: "a,b", constants.EnvS3Bucket: "ps"}, nil, "a", []string{"a", "b"}, 0, true, []string{flagBuckets}},
		{"S3_BUCKETS with one name", map[string]string{constants.EnvS3Buckets: "solo", constants.EnvS3Bucket: "ps"}, nil, "solo", nil, 0, true, []string{flagBuckets}},
		{"S3_BUCKET alone", map[string]string{constants.EnvS3Bucket: "ps"}, nil, "ps", nil, 0, false, []string{"bucket"}},
		{"explicit bucket beats S3_BUCKETS", map[string]string{constants.EnvS3Buckets: "a,b", constants.EnvS3Bucket: "ps"}, []string{"--bucket", "x"}, "x", nil, 0, false, nil},
		{"explicit count keeps S3_BUCKET as base", map[string]string{constants.EnvS3Buckets: "a,b", constants.EnvS3Bucket: "ps"}, []string{"--bucket-count", "4"}, "ps", nil, 4, false, []string{"bucket"}},
		{"explicit count of 1 keeps S3_BUCKET", map[string]string{constants.EnvS3Buckets: "a,b", constants.EnvS3Bucket: "ps"}, []string{"--bucket-count", "1"}, "ps", nil, 0, false, []string{"bucket"}},
		{"explicit bucket and count", map[string]string{constants.EnvS3Buckets: "a,b", constants.EnvS3Bucket: "ps"}, []string{"--bucket", "x", "--bucket-count", "4"}, "x", nil, 4, false, nil},
		{"explicit list beats both", map[string]string{constants.EnvS3Buckets: "a,b", constants.EnvS3Bucket: "ps"}, []string{"--buckets", "c,d"}, "c", []string{"c", "d"}, 0, false, nil},
		{"explicit file beats both", map[string]string{constants.EnvS3Buckets: "a,b", constants.EnvS3Bucket: "ps"}, []string{"--buckets-file", file}, "file-a", []string{"file-a", "file-b"}, 0, false, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnvDefaultsTestEnv(t)
			for key, value := range c.env {
				t.Setenv(key, value)
			}
			run := prepareBucketRun(t, WorkloadTypeWrite, c.args...).mustSucceed(t)
			if run.params.Bucket != c.wantBucket || !reflect.DeepEqual(run.params.BucketNames, c.wantNames) || run.params.BucketCount != c.wantCount {
				t.Fatalf("Bucket %q, BucketNames %q, BucketCount %d; want %q, %q, %d",
					run.params.Bucket, run.params.BucketNames, run.params.BucketCount, c.wantBucket, c.wantNames, c.wantCount)
			}
			notices := strings.Count(run.stderr, "Using S3_BUCKETS for the target buckets; S3_BUCKET is ignored")
			if (c.wantNotice && notices != 1) || (!c.wantNotice && notices != 0) {
				t.Fatalf("notice printed %d times, want it: %v\n%s", notices, c.wantNotice, run.stderr)
			}
			applied := captureEnvAppliedFlags(run.cmd)
			for _, flag := range []string{"bucket", flagBuckets} {
				_, fromEnv := applied[flag]
				want := false
				for _, f := range c.wantFromEnv {
					want = want || f == flag
				}
				if fromEnv != want {
					t.Fatalf("--%s from the environment = %v, want %v (%v)", flag, fromEnv, want, applied)
				}
			}
			// Nothing writes a listed name into --bucket.
			if c.wantNames != nil || c.env[constants.EnvS3Buckets] == "solo" {
				if bucket, _ := run.cmd.Flags().GetString("bucket"); bucket != "" {
					t.Fatalf("--bucket = %q, want it left empty", bucket)
				}
			}
		})
	}
}

func TestBucketListFromDotEnv(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	for _, key := range []string{constants.EnvS3Buckets, constants.EnvS3Bucket} {
		previous, set := os.LookupEnv(key)
		_ = os.Unsetenv(key)
		t.Cleanup(func() {
			if set {
				_ = os.Setenv(key, previous)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	t.Setenv("HOME", t.TempDir()) // keep the user's own ~/.env out of the test
	withTempDir(t, func(dir string) {
		if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("S3_BUCKETS=ps-eu,PS_US\nS3_BUCKET=ps\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		config.ResetDotEnvLoadedForTests()
		if loaded := config.LoadDotEnv(); len(loaded.Errors) > 0 {
			t.Fatalf("load .env: %v", loaded.Errors)
		}
		run := prepareBucketRun(t, WorkloadTypeWrite).mustSucceed(t)
		if !reflect.DeepEqual(run.params.BucketNames, []string{"ps-eu", "PS_US"}) || run.params.Bucket != "ps-eu" {
			t.Fatalf(".env list: %+v", run.params)
		}
		if !strings.Contains(run.stderr, "Using S3_BUCKETS") {
			t.Fatalf(".env list: no notice:\n%s", run.stderr)
		}
	})
}

// A valid list must never fail with "--bucket flag is required" when S3_BUCKET is unset.
func TestBucketListWiringWithoutS3Bucket(t *testing.T) {
	one := writeBucketsFile(t, "solo\n")
	two := writeBucketsFile(t, "# admin buckets\nps-eu\n\nPS_US\n")
	missing := filepath.Join(t.TempDir(), "absent.txt")
	for _, c := range []struct {
		name      string
		env       string
		args      []string
		wantNames []string
		wantErr   string
	}{
		{"--buckets one name", "", []string{"--buckets", "solo"}, nil, ""},
		{"--buckets two names", "", []string{"--buckets", "ps-eu,PS_US"}, []string{"ps-eu", "PS_US"}, ""},
		{"--buckets repeated", "", []string{"--buckets", "ps-eu", "--buckets", "PS_US"}, []string{"ps-eu", "PS_US"}, ""},
		{"--buckets-file one name", "", []string{"--buckets-file", one}, nil, ""},
		{"--buckets-file two names", "", []string{"--buckets-file", two}, []string{"ps-eu", "PS_US"}, ""},
		{"--buckets-file unreadable", "", []string{"--buckets-file", missing}, nil, "--buckets-file " + missing + " cannot be read"},
		{"S3_BUCKETS one name", "solo", nil, nil, ""},
		{"S3_BUCKETS two names", "ps-eu,PS_US", nil, []string{"ps-eu", "PS_US"}, ""},
		{"S3_BUCKETS bad name", "ps-eu,ps-eu", nil, nil, "S3_BUCKETS: bucket name 2 \"ps-eu\" repeats bucket name 1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnvDefaultsTestEnv(t)
			t.Setenv(constants.EnvS3Buckets, c.env)
			for _, workload := range []string{WorkloadTypeWrite, WorkloadTypeRead, WorkloadTypeWriteVerify, WorkloadTypeMixed} {
				args := c.args
				if workload == WorkloadTypeMixed {
					args = append(append([]string{}, args...), "--duration", "10s")
				}
				run := prepareBucketRun(t, workload, args...)
				if c.wantErr != "" {
					if run.stage != "validate" || !strings.Contains(run.err.Error(), c.wantErr) {
						t.Fatalf("%s: stage %q err %v, want a validation error %q", workload, run.stage, run.err, c.wantErr)
					}
					continue
				}
				run.mustSucceed(t)
				wantBucket := "solo"
				if c.wantNames != nil {
					wantBucket = c.wantNames[0]
				}
				if run.params.Bucket != wantBucket || !reflect.DeepEqual(run.params.BucketNames, c.wantNames) {
					t.Fatalf("%s: Bucket %q BucketNames %q", workload, run.params.Bucket, run.params.BucketNames)
				}
			}
		})
	}
}

func TestBucketsFileParsing(t *testing.T) {
	var thousand, tooMany strings.Builder
	for i := 0; i <= scenario.MaxBucketCount; i++ {
		if i < scenario.MaxBucketCount {
			fmt.Fprintf(&thousand, "b-%d\n", i)
		}
		fmt.Fprintf(&tooMany, "b-%d\n", i)
	}
	for _, c := range []struct {
		name    string
		content string
		want    []string
		wantErr string
	}{
		{"comments and blank lines", "# PowerStore buckets\n\nps-eu\n   # indented comment\nPS_US\n\n", []string{"ps-eu", "PS_US"}, ""},
		{"CRLF and surrounding spaces", "  ps-eu  \r\nPS_US\r\n", []string{"ps-eu", "PS_US"}, ""},
		{"no trailing newline", "ps-eu\nPS_US", []string{"ps-eu", "PS_US"}, ""},
		{"a thousand names", thousand.String(), nil, ""},
		{"more than a thousand", tooMany.String(), nil, "lists more than 1000 bucket names"},
		{"empty", "", nil, "lists no bucket names"},
		{"only comments", "# none yet\n\n", nil, "lists no bucket names"},
		{"duplicate", "ps-eu\nPS_US\nps-eu\n", nil, `bucket name 3 "ps-eu" repeats bucket name 1`},
		{"case differs", "ps\nPS\n", []string{"ps", "PS"}, ""},
		{"whitespace inside", "ps eu\n", nil, "must not contain whitespace"},
		{"expression", "ps-${x}\n", nil, "expression marker ${"},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnvDefaultsTestEnv(t)
			path := writeBucketsFile(t, c.content)
			run := prepareBucketRun(t, WorkloadTypeWrite, "--buckets-file", path)
			if c.wantErr != "" {
				if run.err == nil || !strings.Contains(run.err.Error(), c.wantErr) || !strings.Contains(run.err.Error(), "--buckets-file") {
					t.Fatalf("err = %v, want %q naming --buckets-file", run.err, c.wantErr)
				}
				return
			}
			run.mustSucceed(t)
			if c.want != nil && !reflect.DeepEqual(run.params.BucketNames, c.want) {
				t.Fatalf("BucketNames = %q, want %q", run.params.BucketNames, c.want)
			}
			if c.want == nil && len(run.params.BucketNames) != scenario.MaxBucketCount {
				t.Fatalf("BucketNames has %d names, want %d", len(run.params.BucketNames), scenario.MaxBucketCount)
			}
		})
	}
	clearEnvDefaultsTestEnv(t)
	for _, path := range []string{t.TempDir(), filepath.Join(t.TempDir(), "absent.txt"), " "} {
		run := prepareBucketRun(t, WorkloadTypeWrite, "--buckets-file", path)
		if run.err == nil || !strings.Contains(run.err.Error(), "--buckets-file") {
			t.Fatalf("%q: err = %v, want an error naming --buckets-file", path, run.err)
		}
	}
}

func TestBucketListFlagValues(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	run := prepareBucketRun(t, WorkloadTypeWrite, "--buckets", " ps-eu , PS_US ").mustSucceed(t)
	if !reflect.DeepEqual(run.params.BucketNames, []string{"ps-eu", "PS_US"}) {
		t.Fatalf("surrounding spaces: %q", run.params.BucketNames)
	}
	for _, c := range []struct{ value, want string }{
		{"ps-eu,,PS_US", `bucket name 2 "" must not be empty`},
		{"ps-eu,", `bucket name 2 "" must not be empty`},
		{"a/b,c", "must not contain '/'"},
		{"ps,PS,ps", `bucket name 3 "ps" repeats bucket name 1`},
	} {
		run := prepareBucketRun(t, WorkloadTypeWrite, "--buckets", c.value)
		if run.stage != "validate" || !strings.Contains(run.err.Error(), c.want) || !strings.HasPrefix(run.err.Error(), "--buckets:") {
			t.Fatalf("%q: stage %q err %v, want %q", c.value, run.stage, run.err, c.want)
		}
	}
}

// A one-name list is --bucket NAME with every one of its rules, down to the generated files.
func TestOneNameListIsTheBucketItself(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	itemsFile := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(itemsFile, []byte("/ps/key-1,0,1024,0/0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	file := writeBucketsFile(t, "# one bucket\nps\n")
	for _, c := range []struct {
		workload string
		args     []string
	}{
		{WorkloadTypeWrite, []string{"--cleanup"}},
		{WorkloadTypeRead, nil},
		{WorkloadTypeList, nil},
		{WorkloadTypeRead, []string{"--items-file", itemsFile}},
		{WorkloadTypeDelete, []string{"--items-file", itemsFile}},
	} {
		name := c.workload + " " + strings.Join(c.args, " ")
		plain := prepareBucketRun(t, c.workload, append([]string{"--bucket", "ps"}, c.args...)...).mustSucceed(t).params
		for _, listArgs := range [][]string{{"--buckets", "ps"}, {"--buckets-file", file}} {
			listed := prepareBucketRun(t, c.workload, append(listArgs, c.args...)...).mustSucceed(t).params
			if !reflect.DeepEqual(listed, plain) {
				t.Fatalf("%s with %q: params differ from --bucket ps\n%+v\n%+v", name, listArgs, listed, plain)
			}
			stamped := plain
			listed.BaseTimestamp, stamped.BaseTimestamp = "20261031.120000.000", "20261031.120000.000"
			listed.RunID, stamped.RunID = 77, 77
			listedScenario, err := scenario.GenerateScenario(listed)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			plainScenario, err := scenario.GenerateScenario(stamped)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			listedDefaults, err := scenario.GenerateDefaults(listed)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			plainDefaults, err := scenario.GenerateDefaults(stamped)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if listedScenario != plainScenario || !bytes.Equal(listedDefaults, plainDefaults) {
				t.Fatalf("%s with %q: generated files differ from --bucket ps", name, listArgs)
			}
		}
	}
	for _, c := range []struct {
		workload string
		args     []string
		want     string
	}{
		{WorkloadTypeList, nil, "list, which lists a single bucket"},
		{WorkloadTypeRead, []string{"--items-file", itemsFile}, "read --items-file"},
		{WorkloadTypeDelete, []string{"--items-file", itemsFile}, "delete --items-file"},
	} {
		run := prepareBucketRun(t, c.workload, append([]string{"--buckets", "ps,ps-eu"}, c.args...)...)
		if run.err == nil || !strings.Contains(run.err.Error(), c.want) || !strings.Contains(run.err.Error(), "--buckets with 2 buckets") {
			t.Fatalf("%s %q with two names: err = %v, want %q", c.workload, c.args, run.err, c.want)
		}
	}
}

func TestBucketListWorkloads(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	itemsFile := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(itemsFile, []byte("/ps/key-1,0,1024,0/0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	list := []string{"--buckets", "ps-eu,PS_US,ab"}
	for _, c := range []struct {
		workload string
		args     []string
		want     string // "" when supported
	}{
		{WorkloadTypeWrite, nil, ""},
		{WorkloadTypeWrite, []string{"--cleanup"}, ""},
		{WorkloadTypeWriteVerify, nil, ""},
		{WorkloadTypeRead, []string{"--cleanup"}, ""},
		{WorkloadTypeDelete, []string{"--delete-batch-size", "1"}, ""},
		{WorkloadTypeMixed, []string{"--duration", "10s"}, ""},
		{WorkloadTypeMixed, []string{"--duration", "10s", "--read-items-file", itemsFile}, ""},
		{WorkloadTypeDelete, nil, "seeded delete requires --delete-batch-size=1"},
		{WorkloadTypeRead, []string{"--items-file", itemsFile}, "read --items-file"},
		{WorkloadTypeDelete, []string{"--items-file", itemsFile}, "delete --items-file"},
		{WorkloadTypeDelete, []string{"--delete-existing", "--prefix", "owned/"}, "delete --delete-existing"},
		{WorkloadTypeReadVerify, nil, "read-verify"},
		{WorkloadTypeList, nil, "list"},
	} {
		name := c.workload + " " + strings.Join(c.args, " ")
		run := prepareBucketRun(t, c.workload, append(append([]string{}, list...), c.args...)...)
		if c.want == "" {
			run.mustSucceed(t)
			if !reflect.DeepEqual(run.params.BucketNames, []string{"ps-eu", "PS_US", "ab"}) || run.params.Bucket != "ps-eu" {
				t.Fatalf("%s: %+v", name, run.params)
			}
			if node := generatedTargetBucketSetting(t, run.params); !reflect.DeepEqual(node["names"], []any{"ps-eu", "PS_US", "ab"}) {
				t.Fatalf("%s: storage.targetBucket = %v", name, node)
			}
			continue
		}
		if run.err == nil || !strings.Contains(run.err.Error(), c.want) || !strings.Contains(run.err.Error(), "--buckets with 3 buckets") {
			t.Fatalf("%s: stage %q err = %v, want %q", name, run.stage, run.err, c.want)
		}
	}
}

// Workloads without an S3 target bucket reject an explicit list and ignore one from a profile.
func TestBucketListForWorkloadsWithoutTargetBuckets(t *testing.T) {
	for _, workload := range []string{WorkloadTypeMock, WorkloadTypeTables} {
		explicit := scenario.Params{WorkloadType: workload}
		err := applyBucketList(freshCommandFrom(t, runCmd), &explicit, bucketSet{Bucket: "a", Names: []string{"a", "b"}, Source: "--" + flagBuckets})
		if err == nil || !strings.Contains(err.Error(), workload+" workload") {
			t.Fatalf("%s: explicit list err = %v", workload, err)
		}
		fromEnv := scenario.Params{WorkloadType: workload}
		if err := applyBucketList(freshCommandFrom(t, runCmd), &fromEnv, bucketSet{Bucket: "a", Names: []string{"a", "b"}, Source: constants.EnvS3Buckets}); err != nil || fromEnv.BucketNames != nil {
			t.Fatalf("%s: S3_BUCKETS must be ignored, err = %v, names = %q", workload, err, fromEnv.BucketNames)
		}
	}
	clearEnvDefaultsTestEnv(t)
	t.Setenv(constants.EnvS3Buckets, "a,b")
	run := prepareBucketRun(t, WorkloadTypeMock).mustSucceed(t)
	if run.params.BucketNames != nil {
		t.Fatalf("mock with S3_BUCKETS: %+v", run.params)
	}
}

func TestBucketListEngineOverrideIsRejected(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	for _, override := range []string{
		"storage.targetBucket.names=[a, b]",
		"storage-targetBucket-names=a,b",
		" storage.targetBucket.names = [] ",
		"storage.targetBucket={names: [a, b]}",
		"storage-targetBucket={names: [a], provision: false}",
		"storage={targetBucket: {names: [a]}}",
	} {
		for _, args := range [][]string{{"--bucket", "ps"}, {"--buckets", "ps-eu,PS_US"}, {"--buckets", "ps-eu"}} {
			run := prepareBucketRun(t, WorkloadTypeWrite, append(args, "--engine-override", override)...)
			if run.err == nil || !strings.Contains(run.err.Error(), "use --buckets or --buckets-file") {
				t.Fatalf("override %q with %q: err = %v", override, args, run.err)
			}
		}
	}
	run := prepareBucketRun(t, WorkloadTypeWrite, "--buckets", "ps-eu,PS_US", "--engine-override", "storage.targetBucket.provision=false").mustSucceed(t)
	if len(run.params.BucketNames) != 2 {
		t.Fatalf("an override that leaves the names alone: %+v", run.params)
	}
}

// Ticket 04's review R1 for the list: an override that replaces storage.targetBucket keeps the
// validated names, through CLI validation and defaults generation together.
func TestListedNamesSurviveAnOverrideThatReplacesTheirMap(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	for _, c := range []struct {
		override  string
		provision any
	}{
		{"storage.targetBucket={provision: false}", false},
		{"storage-targetBucket={}", nil},
		{"storage.targetBucket=null", nil},
	} {
		run := prepareBucketRun(t, WorkloadTypeWrite, "--buckets", "ps-eu,PS_US", "--engine-override", c.override).mustSucceed(t)
		node := generatedTargetBucketSetting(t, run.params)
		if !reflect.DeepEqual(node["names"], []any{"ps-eu", "PS_US"}) || node["provision"] != c.provision {
			t.Fatalf("override %q: storage.targetBucket = %v", c.override, node)
		}
	}
}

func TestScenarioSummaryNamesTheFirstAndLastListedBucket(t *testing.T) {
	summary := formatScenarioParams(scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "ps-eu", BucketNames: []string{"ps-eu", "PS_US", "ab"}})
	if !strings.Contains(summary, "Buckets: 3 (ps-eu ... ab)") || strings.Contains(summary, "Bucket: ps-eu") || strings.Contains(summary, "Bucket Count") {
		t.Fatalf("summary:\n%s", summary)
	}
	if summary := formatScenarioParams(scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "ps"}); !strings.Contains(summary, "Bucket: ps") || strings.Contains(summary, "Buckets:") {
		t.Fatalf("one bucket summary:\n%s", summary)
	}
}

func TestRunMetadataKeepsTheFullList(t *testing.T) {
	listed, err := json.Marshal(maskScenarioParams(scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "ps-eu", BucketNames: []string{"ps-eu", "PS_US", "ab"}}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(listed), `"BucketNames":["ps-eu","PS_US","ab"]`) {
		t.Fatalf("scenarioParams: %s", listed)
	}
	single, err := json.Marshal(maskScenarioParams(scenario.Params{WorkloadType: WorkloadTypeWrite, Bucket: "ps"}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(single), "BucketNames") {
		t.Fatalf("a run without a list adds nothing: %s", single)
	}
}

// Replay reads S3_BUCKET, never S3_BUCKETS, and tells an environment bucket from --bucket.
func TestReplayBucketEnvironmentDefaults(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	t.Setenv(constants.EnvS3Buckets, "a,b")
	t.Setenv(constants.EnvS3Bucket, "ps")
	fromEnv := freshCommandFrom(t, replayCmd)
	if err := applyEnvDefaultsToRunFlags(fromEnv); err != nil {
		t.Fatal(err)
	}
	if bucket, _ := fromEnv.Flags().GetString("bucket"); bucket != "ps" || !flagFromEnv(fromEnv, "bucket") {
		t.Fatalf("replay --bucket = %q, from env %v", bucket, flagFromEnv(fromEnv, "bucket"))
	}
	explicit := freshCommandFrom(t, replayCmd)
	if err := explicit.ParseFlags([]string{"--bucket", "local"}); err != nil {
		t.Fatal(err)
	}
	if err := applyEnvDefaultsToRunFlags(explicit); err != nil {
		t.Fatal(err)
	}
	if bucket, _ := explicit.Flags().GetString("bucket"); bucket != "local" || flagFromEnv(explicit, "bucket") {
		t.Fatalf("replay explicit --bucket = %q, from env %v", bucket, flagFromEnv(explicit, "bucket"))
	}
}
