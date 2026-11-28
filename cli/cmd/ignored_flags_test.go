package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/verification"
	"github.com/dell/storage-performance-tool/cli/tui"
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

// mixed's --items-file is dropped with a warning while its --read-items-file keeps its mount.
func TestRunMixedDropsItemsFileAndKeepsReadItemsFile(t *testing.T) {
	ignored, used := writeIgnoredFlagItems(t), writeIgnoredFlagItems(t)
	params, warnings, err := ignoredFlagRun(t, "", WorkloadTypeMixed, "--duration", "10s", "--items-file", ignored, "--read-items-file", used)
	if err != nil {
		t.Fatal(err)
	}
	want := "Warning: mixed ignores --items-file and does not read the file; use --read-items-file for the READ pool or --delete-items-file for the DELETE queue\n"
	if warnings != want {
		t.Fatalf("warnings = %q, want %q", warnings, want)
	}
	if params.ItemsFile != "" || params.ReadItemsFile == "" || len(params.ItemFileMounts) != 1 || params.ItemFileMounts[0].HostPath != used {
		t.Fatalf("ItemsFile = %q, ReadItemsFile = %q, mounts = %+v, want only %s mounted", params.ItemsFile, params.ReadItemsFile, params.ItemFileMounts, used)
	}
}

// read never reads mixed's --read-items-file: it warns, naming --items-file, and the run is the one
// it would be without the flag.
func TestRunWarnsOfAnIgnoredReadItemsFile(t *testing.T) {
	items, ignored := writeIgnoredFlagItems(t), writeIgnoredFlagItems(t)
	const want = "Warning: read ignores --read-items-file and does not read the file; use --items-file for the items to read\n"
	for name, args := range map[string][]string{
		"seeded":     {"--duration", "10s"},
		"items file": {"--duration", "10s", "--items-file", items},
	} {
		t.Run(name, func(t *testing.T) {
			plain, _, err := ignoredFlagRun(t, "", WorkloadTypeRead, args...)
			if err != nil {
				t.Fatal(err)
			}
			params, warnings, err := ignoredFlagRun(t, "", WorkloadTypeRead, append(args, "--read-items-file", ignored)...)
			if err != nil {
				t.Fatal(err)
			}
			if warnings != want {
				t.Fatalf("warnings = %q, want %q", warnings, want)
			}
			if !reflect.DeepEqual(params, plain) {
				t.Fatalf("params with --read-items-file = %+v, want %+v", params, plain)
			}
		})
	}
	if _, warnings, err := ignoredFlagRun(t, "", WorkloadTypeRead, "--duration", "10s", "--read-items-file="); err != nil || warnings != "" {
		t.Fatalf("--read-items-file=\"\": err = %v, warnings = %q", err, warnings)
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
		{WorkloadTypeTables, []string{"--test-vector", "catalog"}},
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
	// The LIST workload is frozen until its refactor sprint, so --save-items is left as it was.
	params, warnings, err = ignoredFlagRun(t, "", WorkloadTypeList, "--save-items")
	if err != nil || warnings != "" || !params.SaveItems {
		t.Fatalf("list: err = %v, warnings = %q, SaveItems = %t, want it left as it was", err, warnings, params.SaveItems)
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
			"Warning: mixed ignores --part-size 4MiB, --mpu-concurrent-objects and --mpu-concurrent-parts" + mixedEffect},
		"mixed env mpu": {"4MiB", WorkloadTypeMixed, append(mixed, "--mpu-concurrent-parts", "3"),
			"Warning: mixed ignores SPT_PART_SIZE=4MiB and --mpu-concurrent-parts" + mixedEffect},
		"read items-file typed": {"", WorkloadTypeRead, append(readFile, "--part-size", "4MiB"),
			"Warning: read --items-file ignores --part-size 4MiB: each object is read whole\n"},
		"read items-file mpu": {"", WorkloadTypeRead, append(readFile, "--part-size", "4MiB", "--mpu-concurrent-objects", "2", "--mpu-concurrent-parts", "3"),
			"Warning: read --items-file ignores --part-size 4MiB, --mpu-concurrent-objects and --mpu-concurrent-parts: each object is read whole\n"},
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
	const override = ": the engine override of item.data.ranges.threshold applies instead\n"
	for _, test := range []struct {
		workload string
		args     []string
		want     string
	}{
		{WorkloadTypeRead, []string{"--duration", "10s", "--items-file", items},
			"Warning: read --items-file ignores --part-size 4MiB" + override},
		{WorkloadTypeMixed, append([]string{"-o", "16MiB"}, ignoredFlagMixed...),
			"Warning: mixed ignores --part-size 4MiB" + override},
	} {
		t.Run(test.workload, func(t *testing.T) {
			params, warnings, err := ignoredFlagRun(t, "", test.workload, append(test.args,
				"--part-size", "4MiB", "--mpu-concurrent-parts", "2", "--engine-override", "item.data.ranges.threshold=1MiB")...)
			if err != nil {
				t.Fatal(err)
			}
			if warnings != test.want || params.PartSize != "" || params.MpuParts != 2 {
				t.Fatalf("warnings = %q, PartSize = %q, MpuParts = %d, want %q, none and 2", warnings, params.PartSize, params.MpuParts, test.want)
			}
			defaults, err := scenario.GenerateDefaults(params)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(defaults), "multipart") {
				t.Fatalf("defaults.yaml lost the multipart limits:\n%s", defaults)
			}
		})
	}
}

// The mixed part-size warning names only the operations the run has: no seed with
// --read-items-file, no CREATE with --put-distrib 0, no READ with --get-distrib 0.
func TestMixedPartSizeWarningNamesTheRunsOperations(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"read items file": {[]string{"--read-items-file", items},
			"its CREATE operations send single PUTs and its READs whole GETs"},
		"no CREATE": {[]string{"--seed-objects", "10", "--get-distrib", "70", "--put-distrib", "0", "--stat-distrib", "30", "--delete-distrib", "0"},
			"its seed sends single PUTs and its READs whole GETs"},
		"no READ": {[]string{"--seed-objects", "10", "--get-distrib", "0", "--put-distrib", "60", "--stat-distrib", "30", "--delete-distrib", "10"},
			"its seed and CREATE operations send single PUTs"},
		"read items file, no CREATE": {[]string{"--read-items-file", items, "--get-distrib", "70", "--put-distrib", "0", "--stat-distrib", "30", "--delete-distrib", "0"},
			"its READs send whole GETs"},
	} {
		t.Run(name, func(t *testing.T) {
			_, warnings, err := ignoredFlagRun(t, "", WorkloadTypeMixed, append([]string{"--duration", "10s", "-o", "16MiB", "--part-size", "4MiB"}, test.args...)...)
			if err != nil {
				t.Fatal(err)
			}
			if want := "Warning: mixed ignores --part-size 4MiB: " + test.want + "\n"; warnings != want {
				t.Fatalf("warnings = %q, want %q", warnings, want)
			}
		})
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
	// --part-size="" clears SPT_PART_SIZE: the run has no part size, so nothing is warned.
	for workload, args := range map[string][]string{
		WorkloadTypeRead:  append(readFile, "-o", "1MiB", "--range-size", "64KiB"),
		WorkloadTypeMixed: append([]string{"-o", "1MiB"}, ignoredFlagMixed...),
	} {
		params, warnings, err := ignoredFlagRun(t, "4MiB", workload, append(args, "--part-size=")...)
		if err != nil || warnings != "" || params.PartSize != "" {
			t.Fatalf("%s --part-size=\"\": err = %v, warnings = %q, PartSize = %q, want a run with no part size", workload, err, warnings, params.PartSize)
		}
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

// spt verify warns of a part size mixed or a read of an items file ignores, and of --read-items-file
// on read, before it checks any host.
func TestVerifyWarnsOfIgnoredFlags(t *testing.T) {
	const mixedEffect = ": its seed and CREATE operations send single PUTs and its READs whole GETs\n"
	const readItems = "Warning: read ignores --read-items-file and does not read the file; use --items-file for the items to read\n"
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"mixed part size": {[]string{"--workload", "mixed", "--part-size", "4MiB"}, "Warning: mixed ignores --part-size 4MiB" + mixedEffect},
		"mixed read items file": {[]string{"--workload", "mixed", "--read-items-file", "items.csv", "--part-size", "4MiB"},
			"Warning: mixed ignores --part-size 4MiB: its CREATE operations send single PUTs and its READs whole GETs\n"},
		"read items file part size": {[]string{"--workload", "read", "--items-file", "items.csv", "--part-size", "4MiB", "--mpu-concurrent-parts", "2"},
			"Warning: read --items-file ignores --part-size 4MiB and --mpu-concurrent-parts: each object is read whole\n"},
		"read read-items-file":  {[]string{"--workload", "read", "--read-items-file", "items.csv"}, readItems},
		"seeded read part size": {[]string{"--workload", "read", "--part-size", "4MiB"}, ""},
		"mixed read-items-file": {[]string{"--workload", "mixed", "--read-items-file", "items.csv"}, ""},
		"write part size":       {[]string{"--workload", "write", "--part-size", "4MiB"}, ""},
		"write read-items-file": {[]string{"--workload", "write", "--read-items-file", "items.csv"}, ""},
		"read both, part size": {[]string{"--workload", "read", "--read-items-file", "a.csv", "--items-file", "b.csv", "--part-size", "4MiB"},
			readItems + "Warning: read --items-file ignores --part-size 4MiB: each object is read whole\n"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := freshCommandFrom(t, verifyCmd)
			var stderr bytes.Buffer
			cmd.SetErr(&stderr)
			if err := cmd.ParseFlags(append([]string{"--test-hosts", "127.0.0.1"}, test.args...)); err != nil {
				t.Fatal(err)
			}
			called := false
			previous := verifyHostsFunc
			verifyHostsFunc = func(_ []*hostparse.HostInfo, _ verification.Config) *verification.Report {
				called = true
				if got := stderr.String(); got != test.want {
					t.Errorf("stderr = %q, want %q", got, test.want)
				}
				return &verification.Report{Results: map[string]*verification.NodeResult{}, Ready: true}
			}
			t.Cleanup(func() { verifyHostsFunc = previous })
			_ = captureStdout(t, func() { _ = runVerify(cmd, nil) })
			if !called {
				t.Fatal("verify did not reach the host check")
			}
		})
	}
}

// ignoredFlagRunE runs spt run's RunE, after the environment defaults and validation, with stdout
// captured, and returns the command's stderr. A stub of the remote host connection ends the run.
func ignoredFlagRunE(t *testing.T, workload string, args ...string) (string, error) {
	t.Helper()
	t.Chdir(t.TempDir())
	t.Setenv(constants.EnvPartSize, "")
	cmd := freshCommandFrom(t, runCmd)
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	base := append(append([]string{}, bucketListConnection...), "--bucket", "bench", "--threads", "2",
		"--headless", "--auto-results=false", "--shutdown-on-complete=false")
	if err := cmd.ParseFlags(append(base, args...)); err != nil {
		t.Fatal(err)
	}
	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRunCommand(cmd, []string{workload}); err != nil {
		t.Fatal(err)
	}
	previous := connectMultiHostOrchestratorFunc
	t.Cleanup(func() { connectMultiHostOrchestratorFunc = previous })
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
		_, _ = fmt.Fprint(&stderr, "<connect>")
		return errIgnoredFlagRunConnect
	}
	var err error
	_ = captureStdout(t, func() { err = runCmd.RunE(cmd, []string{workload}) })
	return stderr.String(), err
}

var errIgnoredFlagRunConnect = errors.New("the run reached the host connection")

// spt run prints the warnings itself: with --generate-only, and before it connects to its hosts.
func TestRunCommandPrintsIgnoredFlagWarnings(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	const mixedWarnings = "Warning: mixed ignores --items-file and does not read the file; use --read-items-file for the READ pool or --delete-items-file for the DELETE queue\n" +
		"Warning: mixed ignores --save-items and saves no items list; only write saves one\n" +
		"Warning: mixed ignores --part-size 4MiB: its seed and CREATE operations send single PUTs and its READs whole GETs\n"
	mixed := []string{"--duration", "10s", "--seed-objects", "10", "-o", "16MiB", "--items-file", items, "--save-items", "--part-size", "4MiB"}
	const readItems = "Warning: read ignores --read-items-file and does not read the file; use --items-file for the items to read\n"
	read := []string{"--duration", "10s", "--read-items-file", items}
	for name, test := range map[string]struct {
		workload string
		args     []string
		want     string
		wantErr  error
	}{
		"mixed generate-only":  {WorkloadTypeMixed, append(mixed, "--generate-only"), mixedWarnings, nil},
		"read generate-only":   {WorkloadTypeRead, append(read, "--generate-only"), readItems, nil},
		"mixed before connect": {WorkloadTypeMixed, append(mixed, "--test-hosts", "entry.example"), mixedWarnings + "<connect>", errIgnoredFlagRunConnect},
		"read before connect":  {WorkloadTypeRead, append(read, "--test-hosts", "entry.example"), readItems + "<connect>", errIgnoredFlagRunConnect},
		"plain mixed":          {WorkloadTypeMixed, []string{"--duration", "10s", "--seed-objects", "10", "--generate-only"}, "", nil},
	} {
		t.Run(name, func(t *testing.T) {
			stderr, err := ignoredFlagRunE(t, test.workload, test.args...)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("RunE() error = %v, want %v", err, test.wantErr)
			}
			if stderr != test.want {
				t.Fatalf("stderr = %q, want %q", stderr, test.want)
			}
		})
	}
}

// A run refused by a check before its launch prints the error alone, with no warning first.
func TestRunCommandRefusalPrintsNoIgnoredFlagWarnings(t *testing.T) {
	items := writeIgnoredFlagItems(t)
	stderr, err := ignoredFlagRunE(t, WorkloadTypeMixed, "--duration", "10s", "--seed-objects", "10", "--items-file", items, "--save-items",
		"--test-hosts", "127.0.0.1", "--min-hosts", "5")
	if err == nil || !strings.Contains(err.Error(), "min-hosts (5) cannot exceed total hosts (1)") {
		t.Fatalf("RunE() error = %v, want the min-hosts refusal", err)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q before a refusal", stderr)
	}
}
