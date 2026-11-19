package summary

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/results"
)

const (
	testKiB = int64(1024)
	testMiB = 1024 * testKiB
)

func TestReadObjectSizeCellAndBullet(t *testing.T) {
	t.Parallel()

	const discovered = "discovered at runtime"
	for _, test := range []struct {
		name          string
		objectSize    string
		success       int64
		bytes         int64
		rangeEvidence bool
		rangeExpected bool
		wantCell      string
		wantBullet    string
	}{
		{name: "one size", success: 2190, bytes: 2190 * 64 * testKiB, wantCell: "64.00 KiB avg", wantBullet: discovered},
		{
			name: "mixed sizes", success: 1000, bytes: 500*16*testKiB + 500*256*testKiB,
			wantCell: "136.00 KiB avg", wantBullet: discovered,
		},
		{name: "large objects", success: 100, bytes: 100 * 4 * testMiB, wantCell: "4.00 MiB avg", wantBullet: discovered},
		{
			name: "partial read", success: 12525, bytes: 12525 * 4 * testKiB, rangeEvidence: true, rangeExpected: true,
			wantCell: notApplicableCell, wantBullet: discovered,
		},
		{
			name: "partial read without evidence", success: 12525, bytes: 12525 * 4 * testKiB, rangeExpected: true,
			wantCell: notApplicableCell, wantBullet: discovered,
		},
		{
			name: "partial read known only from its evidence", success: 12525, bytes: 12525 * 4 * testKiB,
			rangeEvidence: true, wantCell: notApplicableCell, wantBullet: discovered,
		},
		{
			name: "partial read of a size range", objectSize: "16KiB-256KiB", success: 1, bytes: 3,
			rangeEvidence: true, rangeExpected: true, wantCell: "16KiB-256KiB", wantBullet: "16KiB-256KiB",
		},
		{name: "no successes", wantCell: notApplicableCell, wantBullet: discovered},
		{
			name: "explicit size", objectSize: "64KiB", success: 2190, bytes: 2190 * 64 * testKiB,
			wantCell: "64.00 KiB", wantBullet: "64.00 KiB",
		},
		{
			name: "explicit size unlike the objects", objectSize: "5MiB", success: 2190, bytes: 2190 * 64 * testKiB,
			wantCell: "5.00 MiB", wantBullet: "5.00 MiB",
		},
		{
			name: "seeded default", objectSize: "1MB", success: 100, bytes: 100 * testMiB,
			wantCell: "1.00 MiB", wantBullet: "1.00 MiB",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stepID := "mt-001-20261118.120000.000-read"
			step := &StepData{
				StepID: stepID, Status: StepStatusComplete, RangeReadExpected: test.rangeExpected,
				Metrics: &MetricsTotals{StepID: stepID, Rows: []MetricsTotalsRow{{
					Operation: "READ", SuccessCount: test.success, SizeBytes: test.bytes,
					FailureCount: 5, StepDurationSeconds: 10, NodeCount: 1,
				}}},
			}
			if test.rangeEvidence {
				step.RangeRead = &RangeReadEvidence{Complete: true}
			}
			summary, err := Aggregate(&RunData{
				RunID: "mt-20261118.120000.000", StepOrder: []string{stepID},
				Steps: map[string]*StepData{stepID: step},
				Params: &RunParams{WorkloadType: "read", ScenarioParams: ScenarioParams{
					WorkloadType: "read", Bucket: "bench", Threads: 1, ObjectSize: test.objectSize,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			renderer := NewRenderer(RenderOptions{MaxWidth: 100})
			if got := phaseObjectSizeCell(t, renderer.phaseTable(summary)); got != test.wantCell {
				t.Fatalf("Object Size cell = %q, want %q", got, test.wantCell)
			}
			mustContain(t, renderer.FullReport(summary), "• Object size        "+test.wantBullet+"\n")
		})
	}
}

func phaseObjectSizeCell(t *testing.T, table string) string {
	t.Helper()
	for _, line := range strings.Split(table, "\n") {
		cells := strings.Split(line, "│")
		if len(cells) > 2 && strings.TrimSpace(cells[1]) == "Read" {
			return strings.TrimSpace(cells[2])
		}
	}
	t.Fatalf("no Read row in table:\n%s", table)
	return ""
}

func TestLoaderMarksPartialReadWithoutEvidence(t *testing.T) {
	for _, test := range []struct {
		name, rangeSize string
		want            bool
	}{
		{name: "partial read", rangeSize: "4KiB", want: true},
		{name: "whole-object read"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			step := "step-read"
			configName := step + ".config.yaml"
			config := "output:\n  metrics:\n    summary:\n      persist: false\nload:\n  op:\n    type: read\n"
			if err := os.WriteFile(filepath.Join(dir, configName), []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			writeManifest(t, dir, &results.Manifest{OutputDir: dir, Steps: []results.StepManifest{{
				StepID: step, Files: []results.FileStatus{{Name: configName, Status: "ok"}},
			}}})
			writeParams(t, dir, &RunParams{ScenarioParams: ScenarioParams{WorkloadType: "read", RangeSize: test.rangeSize}})
			data, err := NewLoader().Load(context.Background(), dir)
			if (err != nil) != test.want {
				t.Fatalf("Load error = %v", err)
			}
			if got := data.Steps[step].RangeReadExpected; got != test.want {
				t.Fatalf("RangeReadExpected = %t, want %t", got, test.want)
			}
		})
	}
}
