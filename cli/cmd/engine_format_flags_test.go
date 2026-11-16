/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const (
	objectSizeRefusal = `--object-size must be a size such as 4KiB, 1MB or 1KB-1MB, got %q`
	durationRefusal   = `--duration must be a time such as 30s, 5m or 1h, got %q`
)

// --object-size is checked only where the scenario sends it to the engine; --duration everywhere.
func TestValidateEngineFormatFlagsChecksEachValueWhereTheEngineReadsIt(t *testing.T) {
	sizeSent := []scenario.Params{
		{WorkloadType: WorkloadTypeMock},
		{WorkloadType: WorkloadTypeWrite},
		{WorkloadType: WorkloadTypeRead},
		{WorkloadType: WorkloadTypeRead, ItemsFile: "items.csv"},
		{WorkloadType: WorkloadTypeMixed},
		{WorkloadType: WorkloadTypeWriteVerify},
		{WorkloadType: WorkloadTypeDelete},
	}
	sizeIgnored := []scenario.Params{
		{WorkloadType: WorkloadTypeList},
		{WorkloadType: WorkloadTypeTables},
		{WorkloadType: WorkloadTypeReadVerify},
		{WorkloadType: WorkloadTypeDelete, ItemsFile: "items.csv"},
		{WorkloadType: WorkloadTypeDelete, DeleteExisting: true},
	}
	for _, params := range sizeSent {
		params.ObjectSize = "bogus"
		err := validateEngineFormatFlags(params)
		if want := fmt.Sprintf(objectSizeRefusal, "bogus"); err == nil || err.Error() != want {
			t.Errorf("%s items=%q existing=%v: error = %v, want %q",
				params.WorkloadType, params.ItemsFile, params.DeleteExisting, err, want)
		}
	}
	for _, params := range sizeIgnored {
		params.ObjectSize = "bogus"
		if err := validateEngineFormatFlags(params); err != nil {
			t.Errorf("%s items=%q existing=%v: error = %v, want the ignored size to stay unchecked",
				params.WorkloadType, params.ItemsFile, params.DeleteExisting, err)
		}
	}
	for _, params := range append(sizeSent, sizeIgnored...) {
		params.ObjectSize = "1MB"
		params.Duration = "1h30m"
		err := validateEngineFormatFlags(params)
		if want := fmt.Sprintf(durationRefusal, "1h30m"); err == nil || err.Error() != want {
			t.Errorf("%s: error = %v, want %q", params.WorkloadType, err, want)
		}
	}
}

// spt run refuses a size or duration the engine cannot parse before any port check, connection or
// launch, and --generate-only refuses it before it writes a scenario.
func TestRunCmdRefusesASizeOrDurationTheEngineCannotParse(t *testing.T) {
	cases := []struct {
		workload, flag, value, want string
	}{
		{WorkloadTypeWrite, "duration", "bogus", fmt.Sprintf(durationRefusal, "bogus")},
		{WorkloadTypeWrite, "duration", "5mins", fmt.Sprintf(durationRefusal, "5mins")},
		{WorkloadTypeWrite, "duration", "1h30m", fmt.Sprintf(durationRefusal, "1h30m")},
		{WorkloadTypeList, "duration", "bogus", fmt.Sprintf(durationRefusal, "bogus")},
		{WorkloadTypeDelete, "duration", "500ms", fmt.Sprintf(durationRefusal, "500ms")},
		{WorkloadTypeWrite, "object-size", "bogus", fmt.Sprintf(objectSizeRefusal, "bogus")},
		{WorkloadTypeWrite, "object-size", "1Ki", fmt.Sprintf(objectSizeRefusal, "1Ki")},
		{WorkloadTypeRead, "object-size", "1 MB", fmt.Sprintf(objectSizeRefusal, "1 MB")},
		{WorkloadTypeDelete, "object-size", "bogus", fmt.Sprintf(objectSizeRefusal, "bogus")},
	}
	for _, generateOnly := range []bool{false, true} {
		for _, tc := range cases {
			name := tc.workload + "/" + tc.flag + "=" + tc.value
			if generateOnly {
				name += "/generate-only"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				t.Chdir(dir)
				setDeleteRouteFlags(t, "127.0.0.1", "1")
				if tc.flag == "duration" {
					setGlobalRunFlagForTest(t, "object-count", "0")
				}
				if generateOnly {
					setGlobalRunFlagForTest(t, "generate-only", "true")
				}
				setGlobalRunFlagForTest(t, tc.flag, tc.value)
				got := stubAPIPortRunRoutes(t)

				err := runCmd.RunE(runCmd, []string{tc.workload})
				if err == nil || err.Error() != tc.want {
					t.Fatalf("RunE() error = %v, want %q", err, tc.want)
				}
				if got.calls != 0 {
					t.Fatalf("the run checked ports, connected or launched %d time(s) before refusing", got.calls)
				}
				entries, readErr := os.ReadDir(dir)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if len(entries) != 0 {
					t.Fatalf("the run wrote %d file(s) before refusing", len(entries))
				}
			})
		}
	}
}

// Command lines that run today still run: an --object-size the scenario never sends, and values
// the engine accepts.
func TestRunCmdKeepsAcceptingWhatRunsToday(t *testing.T) {
	cases := []struct {
		name, workload string
		flags          map[string]string
	}{
		{"list ignores the size", WorkloadTypeList, map[string]string{"object-size": "bogus"}},
		{"tables ignores the size", WorkloadTypeTables, map[string]string{"object-size": "bogus"}},
		{"existing-prefix delete ignores the size", WorkloadTypeDelete, map[string]string{
			"object-size": "bogus", flagDeleteExisting: "true", "prefix": "p/",
		}},
		{"size range with bias", WorkloadTypeWrite, map[string]string{"object-size": "1KB-1MB, 2"}},
		{"padded size", WorkloadTypeWrite, map[string]string{"object-size": " 1MiB "}},
		{"day duration", WorkloadTypeWrite, map[string]string{"object-count": "0", "duration": "1d"}},
		{"TimeUnit duration", WorkloadTypeList, map[string]string{"object-count": "0", "duration": "5.minutes"}},
		{"padded duration", WorkloadTypeDelete, map[string]string{"object-count": "0", "duration": " 5m"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setDeleteRouteFlags(t, "127.0.0.1", "1")
			for name, value := range tc.flags {
				setGlobalRunFlagForTest(t, name, value)
			}
			got := stubAPIPortRunRoutes(t)

			err := runCmd.RunE(runCmd, []string{tc.workload})
			if !errors.Is(err, got.errReached) {
				t.Fatalf("RunE() error = %v, want the route to be reached", err)
			}
		})
	}
	for _, tc := range cases {
		t.Run(tc.name+"/generate-only", func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			setDeleteRouteFlags(t, "127.0.0.1", "1")
			setGlobalRunFlagForTest(t, "generate-only", "true")
			for name, value := range tc.flags {
				setGlobalRunFlagForTest(t, name, value)
			}
			got := stubAPIPortRunRoutes(t)

			if err := runCmd.RunE(runCmd, []string{tc.workload}); err != nil {
				t.Fatalf("RunE() error = %v, want the scenario to be generated", err)
			}
			if got.calls != 0 {
				t.Fatalf("generate-only checked ports, connected or launched %d time(s)", got.calls)
			}
			entries, readErr := os.ReadDir(dir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) == 0 {
				t.Fatal("generate-only wrote no scenario")
			}
		})
	}
}
