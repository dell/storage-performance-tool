package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const snapshotRunID = "mt-dashboard-snapshot-storage"

func snapshotSample(index int) map[string]any {
	return map[string]any{keyTimestamp: fmt.Sprintf("2026-10-31T12:%02d:%02dZ", index/60%60, index%60),
		"step_id": "create", "op_type": phaseCreate, keyCumulativeOps: float64(index),
		keyCountFailed: float64(0), keyIntervalIOPS: float64(10), metricBandwidth: float64(2), metricP50Latency: float64(3)}
}

func snapshotMetrics(samples ...map[string]any) map[string]any {
	return map[string]any{keyRunID: snapshotRunID, keySamples: samples}
}

func writeSnapshotBytes(t *testing.T, a *app, data []byte) string {
	t.Helper()
	path := filepath.Join(a.stateDir, snapshotRunID+".samples.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func marshalSnapshot(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertSnapshotIndices(t *testing.T, rows []map[string]any, first, count int) {
	t.Helper()
	if len(rows) != count {
		t.Fatalf("history length = %d; want %d", len(rows), count)
	}
	for i, row := range rows {
		if got, ok := num(row, keyCumulativeOps); !ok || got != float64(first+i) {
			t.Fatalf("history row %d = %#v; want operation index %d", i, row, first+i)
		}
	}
}

func TestHistorySnapshotByteBoundaries(t *testing.T) {
	for _, extra := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			a := &app{stateDir: t.TempDir()}
			body := marshalSnapshot(t, []map[string]any{snapshotSample(7)})
			body = append(body, []byte(strings.Repeat(" ", maxSamplesBytes+extra-len(body)))...)
			path := writeSnapshotBytes(t, a, body)
			got := a.savedLiveSamples(snapshotRunID)
			if extra > 0 {
				if len(got) != 0 {
					t.Fatal("accepted oversized snapshot")
				}
			} else {
				assertSnapshotIndices(t, got, 7, 1)
			}
			after, err := os.ReadFile(path)
			if err != nil || !reflect.DeepEqual(after, body) {
				t.Fatal("snapshot loading changed the source file")
			}
		})
	}
}

func TestHistorySnapshotSampleBoundaries(t *testing.T) {
	for _, count := range []int{maxSamples - 1, maxSamples, maxSamples + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			a := &app{stateDir: t.TempDir()}
			rows := make([]map[string]any, count)
			for i := range rows {
				rows[i] = snapshotSample(i)
			}
			writeSnapshotBytes(t, a, marshalSnapshot(t, rows))
			first := max(0, count-maxSamples)
			assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), first, count-first)
		})
	}
}

func TestHistorySnapshotRecoversUsableRows(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	valid := []map[string]any{snapshotSample(1), snapshotSample(2)}
	// Syntactically valid JSON may contain individually damaged rows. Keep
	// useful measurements while rejecting scalars and invalid timestamps.
	rows := []any{valid[0], nil, false, 4, "row", []any{}, map[string]any{},
		map[string]any{keyTimestamp: nil}, map[string]any{keyTimestamp: 12},
		map[string]any{keyTimestamp: "not-a-date"}, map[string]any{keyTimestamp: "2026-02-30T12:00:00Z"}, valid[1]}
	writeSnapshotBytes(t, a, marshalSnapshot(t, rows))
	assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 2)
	// Invalid trailing rows must not evict the newest usable measurements.
	rows = make([]any, 0, maxSamples+2)
	rows = append(rows, valid[0], valid[1])
	for range maxSamples {
		rows = append(rows, nil)
	}
	writeSnapshotBytes(t, a, marshalSnapshot(t, rows))
	assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 2)
}

func TestHistorySnapshotRejectsDamagedFilesAndIDs(t *testing.T) {
	for _, body := range []string{"", "[", "[{", "null", "{}", "1", `[{"timestamp":"2026-10-31T12:00:00Z"}] extra`} {
		t.Run(body, func(t *testing.T) {
			a := &app{stateDir: t.TempDir()}
			writeSnapshotBytes(t, a, []byte(body))
			if got := a.savedLiveSamples(snapshotRunID); len(got) != 0 {
				t.Fatalf("damaged snapshot returned measurements: %#v", got)
			}
		})
	}
	a := &app{stateDir: t.TempDir()}
	if got := a.savedLiveSamples(snapshotRunID); len(got) != 0 {
		t.Fatal("missing snapshot returned measurements")
	}
	path := filepath.Join(a.stateDir, snapshotRunID+".samples.json")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if got := a.savedLiveSamples(snapshotRunID); len(got) != 0 {
		t.Fatal("directory used as a snapshot")
	}
	for _, id := range []string{"", "../outside", "mt-../outside", `mt-..\outside`, "/mt-outside", "mt-one\x00two"} {
		if got := a.savedLiveSamples(id); len(got) != 0 {
			t.Errorf("unsafe run id %q returned measurements", id)
		}
	}
}
