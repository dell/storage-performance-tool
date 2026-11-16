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

// Each value is checked only where the scenario sends it to the engine.
func TestValidateEngineFormatFlagsChecksEachValueWhereTheEngineReadsIt(t *testing.T) {
	type route struct {
		params scenario.Params
		vector string
	}
	sizeSent := []route{
		{scenario.Params{WorkloadType: WorkloadTypeMock}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeWrite}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeRead}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeRead, ItemsFile: "items.csv"}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeMixed}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeWriteVerify}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeDelete}, ""},
	}
	sizeIgnored := []route{
		{scenario.Params{WorkloadType: WorkloadTypeList}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeTables}, "tps"},
		{scenario.Params{WorkloadType: WorkloadTypeTables}, "catalog"},
		{scenario.Params{WorkloadType: WorkloadTypeReadVerify}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeDelete, ItemsFile: "items.csv"}, ""},
		{scenario.Params{WorkloadType: WorkloadTypeDelete, DeleteExisting: true}, ""},
	}
	// The tables compaction vector sends neither value.
	compaction := route{scenario.Params{WorkloadType: WorkloadTypeTables}, "compaction"}
	// read-verify refuses --duration in PreRunE; the helper does not single it out.
	durationSent := append(append([]route{}, sizeSent...), sizeIgnored...)

	for _, r := range sizeSent {
		r.params.ObjectSize = "bogus"
		err := validateEngineFormatFlags(r.params, r.vector)
		if want := fmt.Sprintf(objectSizeRefusal, "bogus"); err == nil || err.Error() != want {
			t.Errorf("%s items=%q existing=%v: error = %v, want %q",
				r.params.WorkloadType, r.params.ItemsFile, r.params.DeleteExisting, err, want)
		}
	}
	for _, r := range append(sizeIgnored, compaction) {
		r.params.ObjectSize = "bogus"
		if err := validateEngineFormatFlags(r.params, r.vector); err != nil {
			t.Errorf("%s vector=%q items=%q existing=%v: error = %v, want the ignored size to stay unchecked",
				r.params.WorkloadType, r.vector, r.params.ItemsFile, r.params.DeleteExisting, err)
		}
	}
	for _, r := range durationSent {
		r.params.ObjectSize = "1MB"
		r.params.Duration = "1h30m"
		err := validateEngineFormatFlags(r.params, r.vector)
		if want := fmt.Sprintf(durationRefusal, "1h30m"); err == nil || err.Error() != want {
			t.Errorf("%s vector=%q: error = %v, want %q", r.params.WorkloadType, r.vector, err, want)
		}
	}
	compaction.params.Duration = "1h30m"
	if err := validateEngineFormatFlags(compaction.params, compaction.vector); err != nil {
		t.Errorf("tables compaction: error = %v, want the ignored duration to stay unchecked", err)
	}
}

// spt run refuses a size or duration the engine cannot parse before any port check, connection or
// launch, and --generate-only refuses it before it writes a scenario.
func TestRunCmdRefusesASizeOrDurationTheEngineCannotParse(t *testing.T) {
	cases := []struct {
		workload, flag, value, want string
		extra                       map[string]string
	}{
		{WorkloadTypeWrite, "duration", "bogus", fmt.Sprintf(durationRefusal, "bogus"), nil},
		{WorkloadTypeWrite, "duration", "5mins", fmt.Sprintf(durationRefusal, "5mins"), nil},
		{WorkloadTypeWrite, "duration", "1h30m", fmt.Sprintf(durationRefusal, "1h30m"), nil},
		{WorkloadTypeList, "duration", "bogus", fmt.Sprintf(durationRefusal, "bogus"), nil},
		{WorkloadTypeDelete, "duration", "500ms", fmt.Sprintf(durationRefusal, "500ms"), nil},
		{WorkloadTypeMock, "duration", "bogus", fmt.Sprintf(durationRefusal, "bogus"), nil},
		{WorkloadTypeMixed, "duration", "5mins", fmt.Sprintf(durationRefusal, "5mins"), nil},
		{WorkloadTypeTables, "duration", "bogus", fmt.Sprintf(durationRefusal, "bogus"), nil},
		{WorkloadTypeTables, "duration", "1h30m", fmt.Sprintf(durationRefusal, "1h30m"),
			map[string]string{"test-vector": "catalog"}},
		{WorkloadTypeWrite, "object-size", "bogus", fmt.Sprintf(objectSizeRefusal, "bogus"), nil},
		{WorkloadTypeWrite, "object-size", "1Ki", fmt.Sprintf(objectSizeRefusal, "1Ki"), nil},
		{WorkloadTypeRead, "object-size", "1 MB", fmt.Sprintf(objectSizeRefusal, "1 MB"), nil},
		{WorkloadTypeDelete, "object-size", "bogus", fmt.Sprintf(objectSizeRefusal, "bogus"), nil},
		{WorkloadTypeMock, "object-size", "bogus", fmt.Sprintf(objectSizeRefusal, "bogus"), nil},
		{WorkloadTypeMixed, "object-size", "1Ki", fmt.Sprintf(objectSizeRefusal, "1Ki"),
			map[string]string{"object-count": "0", "duration": "30s"}},
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
				for name, value := range tc.extra {
					setGlobalRunFlagForTest(t, name, value)
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
		{"tables compaction ignores the duration", WorkloadTypeTables, map[string]string{
			"object-count": "0", "test-vector": "compaction", "duration": "bogus",
		}},
		{"existing-prefix delete ignores the size", WorkloadTypeDelete, map[string]string{
			"object-size": "bogus", flagDeleteExisting: "true", "prefix": "p/",
		}},
		{"size range with bias", WorkloadTypeWrite, map[string]string{"object-size": "1KB-1MB, 2"}},
		{"padded size", WorkloadTypeWrite, map[string]string{"object-size": " 1MiB "}},
		{"day duration", WorkloadTypeWrite, map[string]string{"object-count": "0", "duration": "1d"}},
		{"TimeUnit duration", WorkloadTypeList, map[string]string{"object-count": "0", "duration": "5.minutes"}},
		{"padded duration", WorkloadTypeDelete, map[string]string{"object-count": "0", "duration": " 5m"}},
		{"tables duration", WorkloadTypeTables, map[string]string{"object-count": "0", "duration": "2D"}},
		{"mock duration", WorkloadTypeMock, map[string]string{"object-count": "0", "duration": "5.minutes"}},
		{"mixed size and duration", WorkloadTypeMixed, map[string]string{
			"object-count": "0", "object-size": "1KiB-4MiB", "duration": "30s",
		}},
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
