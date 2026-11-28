package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/verification"
)

// ignoredFlagRun takes spt run's flags through environment defaults, validation, the scenario
// parameters and the item-file preparation, and returns the prepared parameters and the warnings
// the run prints at start.
func ignoredFlagRun(t *testing.T, partSizeEnv, workload string, args ...string) (scenario.Params, string, error) {
	t.Helper()
	t.Setenv(constants.EnvPartSize, partSizeEnv)
	run := prepareBucketRun(t, workload, append([]string{"--bucket", "bench", "--threads", "2"}, args...)...)
	if run.err != nil {
		return scenario.Params{}, "", run.err
	}
	params, err := prepareExternalItemFilesForRun(run.params)
	if err != nil {
		return params, "", err
	}
	var warnings bytes.Buffer
	writeIgnoredFlagWarnings(&warnings, run.cmd, workload)
	return params, warnings.String(), nil
}

func writeIgnoredFlagItems(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(path, []byte("/bench/k1,0,4194304\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

var ignoredFlagMixed = []string{"--duration", "10s", "--seed-objects", "10"}

// A workload that never reads --items-file warns once, and neither mounts nor records the file.
func TestRunWarnsOfAnIgnoredItemsFile(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	for _, test := range []struct {
		workload string
		args     []string
		want     string
	}{
		{WorkloadTypeMixed, ignoredFlagMixed, "Warning: mixed ignores --items-file and does not read the file; use --read-items-file for the READ pool or --delete-items-file for the DELETE queue\n"},
		{WorkloadTypeWrite, []string{"--object-count", "5"}, "Warning: write ignores --items-file and does not read the file\n"},
		{WorkloadTypeMock, []string{"--object-count", "5"}, "Warning: mock ignores --items-file and does not read the file\n"},
		{WorkloadTypeTables, []string{"--test-vector", "catalog"}, "Warning: tables ignores --items-file and does not read the file\n"},
	} {
		t.Run(test.workload, func(t *testing.T) {
			params, warnings, err := ignoredFlagRun(t, "", test.workload, append(test.args, "--items-file", items)...)
			if err != nil {
				t.Fatal(err)
			}
			if warnings != test.want {
				t.Fatalf("warnings = %q, want %q", warnings, test.want)
			}
			if params.ItemsFile != "" || len(params.ItemFileMounts) != 0 {
				t.Fatalf("ItemsFile = %q, ItemFileMounts = %+v, want neither", params.ItemsFile, params.ItemFileMounts)
			}
		})
	}
}

// The ignored file is still checked: a missing path or a directory fails the run as before.
func TestRunStillChecksAnIgnoredItemsFile(t *testing.T) {
	for name, path := range map[string]string{
		"missing":   filepath.Join(t.TempDir(), "missing.csv"),
		"directory": t.TempDir(),
	} {
		t.Run(name, func(t *testing.T) {
			_, warnings, err := ignoredFlagRun(t, "", WorkloadTypeMixed, append(ignoredFlagMixed, "--items-file", path)...)
			if err == nil || !strings.HasPrefix(err.Error(), "--items-file: ") {
				t.Fatalf("error = %v, want the --items-file check", err)
			}
			if warnings != "" {
				t.Fatalf("warnings = %q before a refusal", warnings)
			}
		})
	}
}

// read, delete and mixed's own item files keep their mounts and print no warning.
func TestRunKeepsUsedItemFiles(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	for name, test := range map[string]struct {
		workload string
		args     []string
	}{
		"read":                  {WorkloadTypeRead, []string{"--duration", "10s", "--items-file", items}},
		"mixed read-items-file": {WorkloadTypeMixed, []string{"--duration", "10s", "--read-items-file", items}},
	} {
		t.Run(name, func(t *testing.T) {
			params, warnings, err := ignoredFlagRun(t, "", test.workload, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			if warnings != "" || len(params.ItemFileMounts) != 1 {
				t.Fatalf("warnings = %q, mounts = %+v, want none and one mount", warnings, params.ItemFileMounts)
			}
		})
	}
}

// --save-items warns and is not recorded on every workload but write, which keeps it.
func TestRunWarnsOfAnIgnoredSaveItems(t *testing.T) {
	for _, test := range []struct {
		workload string
		args     []string
	}{
		{WorkloadTypeMixed, ignoredFlagMixed},
		{WorkloadTypeRead, []string{"--duration", "10s"}},
		{WorkloadTypeDelete, nil},
		{WorkloadTypeMock, []string{"--object-count", "5"}},
		{WorkloadTypeWriteVerify, []string{"--object-count", "5"}},
	} {
		t.Run(test.workload, func(t *testing.T) {
			params, warnings, err := ignoredFlagRun(t, "", test.workload, append(test.args, "--save-items")...)
			if err != nil {
				t.Fatal(err)
			}
			want := "Warning: " + test.workload + " ignores --save-items and saves no items list; only write saves one\n"
			if warnings != want || params.SaveItems {
				t.Fatalf("warnings = %q, SaveItems = %t, want %q and false", warnings, params.SaveItems, want)
			}
		})
	}
	params, warnings, err := ignoredFlagRun(t, "", WorkloadTypeWrite, "--object-count", "5", "--save-items")
	if err != nil || warnings != "" || !params.SaveItems {
		t.Fatalf("write: err = %v, warnings = %q, SaveItems = %t, want it kept", err, warnings, params.SaveItems)
	}
	if _, warnings, err := ignoredFlagRun(t, "", WorkloadTypeMixed, append(ignoredFlagMixed, "--save-items=false")...); err != nil || warnings != "" {
		t.Fatalf("--save-items=false: err = %v, warnings = %q", err, warnings)
	}
}

// mixed and a read of an items file warn of a part size from either source, name the multipart
// limits they drop with it, and no longer record or print it.
func TestRunWarnsOfAnIgnoredPartSize(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	mixed := append([]string{"-o", "16MiB"}, ignoredFlagMixed...)
	readFile := []string{"--duration", "10s", "--items-file", items}
	const mixedEffect = ": its seed and CREATE operations send single PUTs and its READs whole GETs\n"
	for name, test := range map[string]struct {
		env, workload string
		args          []string
		want          string
	}{
		"mixed typed": {"", WorkloadTypeMixed, append(mixed, "--part-size", "4MiB"),
			"Warning: mixed ignores --part-size 4MiB" + mixedEffect},
		"mixed env": {"4MiB", WorkloadTypeMixed, mixed,
			"Warning: mixed ignores SPT_PART_SIZE=4MiB" + mixedEffect},
		"mixed mpu": {"", WorkloadTypeMixed, append(mixed, "--part-size", "4MiB", "--mpu-concurrent-objects", "2", "--mpu-concurrent-parts", "3"),
			"Warning: mixed ignores --part-size 4MiB and --mpu-concurrent-objects and --mpu-concurrent-parts" + mixedEffect},
		"read items-file typed": {"", WorkloadTypeRead, append(readFile, "--part-size", "4MiB"),
			"Warning: read --items-file ignores --part-size 4MiB: each object is read whole\n"},
		"read items-file env": {"4MiB", WorkloadTypeRead, readFile,
			"Warning: read --items-file ignores SPT_PART_SIZE=4MiB: each object is read whole\n"},
	} {
		t.Run(name, func(t *testing.T) {
			params, warnings, err := ignoredFlagRun(t, test.env, test.workload, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			if warnings != test.want {
				t.Fatalf("warnings = %q, want %q", warnings, test.want)
			}
			if params.PartSize != "" || params.MpuObjects != 0 || params.MpuParts != 0 {
				t.Fatalf("PartSize = %q, MpuObjects = %d, MpuParts = %d, want none", params.PartSize, params.MpuObjects, params.MpuParts)
			}
			if summary := formatScenarioParams(params); strings.Contains(summary, "Part Size") {
				t.Fatalf("--generate-only options still show a part size:\n%s", summary)
			}
			defaults, err := scenario.GenerateDefaults(params)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(defaults), "multipart") {
				t.Fatalf("defaults.yaml still limits multipart:\n%s", defaults)
			}
		})
	}
}

// An engine override of the ranges threshold makes --mpu-concurrent-* bound the run's split reads,
// so they are kept, as before, and only the part size is dropped.
func TestRunKeepsMultipartLimitsUnderAThresholdOverride(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	params, warnings, err := ignoredFlagRun(t, "", WorkloadTypeRead, "--duration", "10s", "--items-file", items,
		"--part-size", "4MiB", "--mpu-concurrent-parts", "2", "--engine-override", "item.data.ranges.threshold=1MiB")
	if err != nil {
		t.Fatal(err)
	}
	want := "Warning: read --items-file ignores --part-size 4MiB: the engine override of item.data.ranges.threshold applies instead\n"
	if warnings != want || params.PartSize != "" || params.MpuParts != 2 {
		t.Fatalf("warnings = %q, PartSize = %q, MpuParts = %d, want %q, none and 2", warnings, params.PartSize, params.MpuParts, want)
	}
}

// SPT_PART_SIZE, which a read of an items file ignores, no longer refuses -o or --range-size there;
// a typed --part-size keeps those refusals, as does SPT_PART_SIZE=0.
func TestReadItemsFilePartSizeRefusals(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	readFile := []string{"--duration", "10s", "--items-file", items}
	for name, args := range map[string][]string{
		"object size": append(readFile, "-o", "1MiB"),
		"range size":  append(readFile, "--range-size", "64KiB"),
	} {
		t.Run(name+"/env runs", func(t *testing.T) {
			if _, warnings, err := ignoredFlagRun(t, "4MiB", WorkloadTypeRead, args...); err != nil || warnings == "" {
				t.Fatalf("err = %v, warnings = %q, want a run with the warning", err, warnings)
			}
		})
		t.Run(name+"/typed refused", func(t *testing.T) {
			if _, _, err := ignoredFlagRun(t, "", WorkloadTypeRead, append(args, "--part-size", "4MiB")...); err == nil {
				t.Fatal("typed --part-size accepted")
			}
		})
	}
	if _, _, err := ignoredFlagRun(t, "0", WorkloadTypeRead, readFile...); err == nil || !strings.Contains(err.Error(), "must be a positive size") {
		t.Fatalf("SPT_PART_SIZE=0: err = %v, want the positive-size refusal", err)
	}
	if _, _, err := ignoredFlagRun(t, "8MiB", WorkloadTypeMixed, "-o", "1MiB", "--duration", "10s", "--seed-objects", "10"); err == nil {
		t.Fatal("mixed: SPT_PART_SIZE at or above -o accepted")
	}
}

// A seeded read and write keep their part size, with no warning.
func TestRunKeepsAUsedPartSize(t *testing.T) {
	for _, test := range []struct {
		workload string
		args     []string
	}{
		{WorkloadTypeRead, []string{"--duration", "10s"}},
		{WorkloadTypeWrite, []string{"--object-count", "5"}},
	} {
		params, warnings, err := ignoredFlagRun(t, "", test.workload, append(test.args, "-o", "16MiB", "--part-size", "4MiB", "--mpu-concurrent-parts", "2")...)
		if err != nil || warnings != "" || params.PartSize != "4MiB" || params.MpuParts != 2 {
			t.Fatalf("%s: err = %v, warnings = %q, PartSize = %q, MpuParts = %d", test.workload, err, warnings, params.PartSize, params.MpuParts)
		}
	}
}

// spt verify --workload mixed warns of a part size before it checks any host.
func TestVerifyMixedWarnsOfAnIgnoredPartSize(t *testing.T) {
	for workload, want := range map[string]string{
		WorkloadTypeMixed: "Warning: mixed ignores --part-size 4MiB: its seed and CREATE operations send single PUTs and its READs whole GETs\n",
		WorkloadTypeWrite: "",
	} {
		cmd := freshCommandFrom(t, verifyCmd)
		var stderr bytes.Buffer
		cmd.SetErr(&stderr)
		if err := cmd.ParseFlags([]string{"--test-hosts", "127.0.0.1", "--workload", workload, "--part-size", "4MiB"}); err != nil {
			t.Fatal(err)
		}
		called := false
		previous := verifyHostsFunc
		verifyHostsFunc = func(_ []*hostparse.HostInfo, _ verification.Config) *verification.Report {
			called = true
			if got := stderr.String(); got != want {
				t.Errorf("%s: stderr = %q, want %q", workload, got, want)
			}
			return &verification.Report{Results: map[string]*verification.NodeResult{}, Ready: true}
		}
		t.Cleanup(func() { verifyHostsFunc = previous })
		_ = runVerify(cmd, nil)
		if !called {
			t.Fatalf("%s: verify did not reach the host check", workload)
		}
	}
}
