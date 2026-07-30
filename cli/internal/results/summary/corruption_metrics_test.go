/*
Copyright © 2026 Dell Technologies
*/

package summary

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseMetricsTotalsCorruptCountPresence(t *testing.T) {
	const baseTail = ",1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1\n"
	tests := []struct {
		name   string
		header string
		row    string
		want   int64
		has    bool
	}{
		{
			name:   "new engine",
			header: "OpType,CountSucc,CountFail,CountCorrupt,Size,TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],LatencyAvg[us],Concurrency,ConcurrencyMean,NodeCount,StepDuration[s],DurationSum[s],DurationQ_0.5[us],LatencyQ_0.5[us]\n",
			row:    "READ,2,3,1" + baseTail,
			want:   1,
			has:    true,
		},
		{
			name:   "legacy ordinary engine",
			header: "OpType,CountSucc,CountFail,Size,TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],LatencyAvg[us],Concurrency,ConcurrencyMean,NodeCount,StepDuration[s],DurationSum[s],DurationQ_0.5[us],LatencyQ_0.5[us]\n",
			row:    "READ,2,3" + baseTail,
			want:   0,
			has:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metrics.total.csv")
			if err := os.WriteFile(path, []byte(tt.header+tt.row), 0o600); err != nil {
				t.Fatal(err)
			}
			totals, err := parseMetricsTotals("read", path)
			if err != nil {
				t.Fatal(err)
			}
			got := totals.Rows[0]
			if got.CorruptCount != tt.want || got.HasCorruptCount != tt.has {
				t.Fatalf("corrupt count/presence = %d/%t, want %d/%t", got.CorruptCount, got.HasCorruptCount, tt.want, tt.has)
			}
		})
	}
}
