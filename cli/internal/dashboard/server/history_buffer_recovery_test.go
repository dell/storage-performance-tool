package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHistorySnapshotRestartDoesNotDuplicateLastSample(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	a.rememberLive(snapshotMetrics(snapshotSample(1), snapshotSample(2)))
	restarted := &app{stateDir: a.stateDir}
	got := restarted.rememberLive(snapshotMetrics(snapshotSample(2)))
	assertSnapshotIndices(t, got[keyChartSamples].([]map[string]any), 1, 2)
	next := restarted.rememberLive(snapshotMetrics(snapshotSample(3)))
	assertSnapshotIndices(t, next[keyChartSamples].([]map[string]any), 1, 3)
}

func TestHistorySnapshotRetriesUnchangedSampleAfterSaveFailure(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	a.rememberLive(snapshotMetrics(snapshotSample(1)))
	path := filepath.Join(a.stateDir, snapshotRunID+".samples.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block := filepath.Join(a.stateDir, snapshotRunID+".samples.tmp")
	if err := os.Mkdir(block, 0700); err != nil {
		t.Fatal(err)
	}
	a.rememberLive(snapshotMetrics(snapshotSample(2)))
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(before) {
		t.Fatal("failed save replaced previously persisted history")
	}
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	got := a.rememberLive(snapshotMetrics(snapshotSample(2)))
	assertSnapshotIndices(t, got[keyChartSamples].([]map[string]any), 1, 2)
	assertSnapshotIndices(t, (&app{stateDir: a.stateDir}).savedLiveSamples(snapshotRunID), 1, 2)
}

func TestHistorySnapshotWriterEnforcesByteLimit(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	for i := range 9 {
		sample := snapshotSample(i)
		sample["step_id"] = strings.Repeat("s", 600_000) // Below one live-metrics response's byte limit.
		out := a.rememberLive(snapshotMetrics(sample))
		rows := out[keyChartSamples].([]map[string]any)
		body := marshalSnapshot(t, rows)
		if len(body) > maxSamplesBytes || len(rows) > maxSamples {
			t.Fatalf("history exceeded bounds: bytes=%d samples=%d", len(body), len(rows))
		}
		if numberValue(rows[len(rows)-1], keyCumulativeOps) != float64(i) {
			t.Fatal("byte trimming lost the latest measurement")
		}
	}
	// The writer must never produce a snapshot that the reader refuses.
	loaded := (&app{stateDir: a.stateDir}).savedLiveSamples(snapshotRunID)
	assertSnapshotIndices(t, loaded, 3, 6)
}

func TestHistorySnapshotRejectsUnencodableSampleWithoutLosingHistory(t *testing.T) {
	for _, value := range []any{math.NaN(), math.Inf(1), make(chan int)} {
		a := &app{stateDir: t.TempDir()}
		a.rememberLive(snapshotMetrics(snapshotSample(1)))
		bad := snapshotSample(2)
		bad[metricP50Latency] = value
		out := a.rememberLive(snapshotMetrics(bad))
		if _, err := json.Marshal(out); err != nil {
			t.Errorf("invalid sample made the public history unencodable: %v", err)
		}
		assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 1)
		next := a.rememberLive(snapshotMetrics(snapshotSample(2)))
		assertSnapshotIndices(t, next[keyChartSamples].([]map[string]any), 1, 2)
	}
}

func TestHistorySnapshotWriterByteBoundaries(t *testing.T) {
	for _, extra := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			a := &app{stateDir: t.TempDir()}
			a.rememberLive(snapshotMetrics(snapshotSample(1)))
			sample := snapshotSample(2)
			sample["padding"] = ""
			base := len(marshalSnapshot(t, []map[string]any{sample}))
			sample["padding"] = strings.Repeat("s", maxSamplesBytes+extra-base)
			out := a.rememberLive(snapshotMetrics(sample))
			rows := out[keyChartSamples].([]map[string]any)
			if extra > 0 {
				assertSnapshotIndices(t, rows, 1, 1)
			} else {
				assertSnapshotIndices(t, rows, 2, 1)
				if got := len(marshalSnapshot(t, rows)); got != maxSamplesBytes+extra {
					t.Fatalf("encoded history length = %d; want %d", got, maxSamplesBytes+extra)
				}
			}
			loaded := (&app{stateDir: a.stateDir}).savedLiveSamples(snapshotRunID)
			if len(loaded) != 1 || numberValue(loaded[0], keyCumulativeOps) != numberValue(rows[0], keyCumulativeOps) {
				t.Fatal("reader rejected the writer's boundary snapshot")
			}
		})
	}
}

func TestHistorySnapshotBufferCountBoundaries(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	rows := make([]map[string]any, maxSamples+7)
	for i := range rows {
		rows[i] = snapshotSample(i)
	}
	metrics := snapshotMetrics(rows...)
	out := a.rememberLive(metrics)
	assertSnapshotIndices(t, out[keyChartSamples].([]map[string]any), 7, maxSamples)
	assertSnapshotIndices(t, out[keySamples].([]map[string]any), len(rows)-1, 1)
	assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 7, maxSamples)
	if _, changed := metrics[keyChartSamples]; changed {
		t.Fatal("history retention mutated the input metrics envelope")
	}
}

func TestHistorySnapshotMeasurementIdentity(t *testing.T) {
	for _, field := range []string{"step_id", keyCumulativeOps, keyCountFailed, keyIntervalIOPS, metricBandwidth, metricP50Latency} {
		t.Run(field, func(t *testing.T) {
			a := &app{stateDir: t.TempDir()}
			original := snapshotSample(1)
			a.rememberLive(snapshotMetrics(original))
			// A new polling timestamp without a new measurement is not another
			// sample. Any change to measured values or phase identity is one.
			same := cloneMap(original)
			same[keyTimestamp] = snapshotSample(2)[keyTimestamp]
			if out := a.rememberLive(snapshotMetrics(same)); len(out[keyChartSamples].([]map[string]any)) != 1 {
				t.Fatal("polling timestamp alone duplicated a measurement")
			}
			changed := cloneMap(same)
			if field == "step_id" {
				changed[field] = "verify"
			} else {
				changed[field] = float64(99)
			}
			out := a.rememberLive(snapshotMetrics(changed))
			if len(out[keyChartSamples].([]map[string]any)) != 2 || out[keySamples].([]map[string]any)[0][field] != changed[field] {
				t.Fatalf("change to %s did not produce a new latest measurement", field)
			}
		})
	}
	a := &app{stateDir: t.TempDir()}
	missing := snapshotSample(1)
	missing[metricP50Latency] = nil
	a.rememberLive(snapshotMetrics(missing))
	zero := cloneMap(missing)
	zero[metricP50Latency] = float64(0)
	out := a.rememberLive(snapshotMetrics(zero))
	if len(out[keyChartSamples].([]map[string]any)) != 2 || out[keySamples].([]map[string]any)[0][metricP50Latency] != float64(0) {
		t.Fatal("missing P50 and measured zero were treated as the same observation")
	}
}

func TestHistorySnapshotRecoversAfterCorruptionAndRunSwitch(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	writeSnapshotBytes(t, a, []byte("truncated ["))
	a.rememberLive(snapshotMetrics(snapshotSample(1)))
	assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 1)
	nextID := "mt-dashboard-snapshot-next"
	next := snapshotMetrics(snapshotSample(2))
	next[keyRunID] = nextID
	a.rememberLive(next)
	assertSnapshotIndices(t, a.savedLiveSamples(nextID), 2, 1)
	// Switching back restores the first file, including its deduplication key.
	back := a.rememberLive(snapshotMetrics(snapshotSample(1)))
	assertSnapshotIndices(t, back[keyChartSamples].([]map[string]any), 1, 1)
	assertSnapshotIndices(t, a.savedLiveSamples(nextID), 2, 1)
}

func TestHistorySnapshotCleanPollDoesNotRewriteFile(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	metrics := snapshotMetrics(snapshotSample(1))
	a.rememberLive(metrics)
	path := filepath.Join(a.stateDir, snapshotRunID+".samples.json")
	stamp := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	a.rememberLive(metrics)
	info, err := os.Stat(path)
	if err != nil || !info.ModTime().Equal(stamp) {
		t.Fatal("an unchanged clean poll rewrote the snapshot")
	}
}

func TestHistorySnapshotRetriesAfterDirectoryRecovery(t *testing.T) {
	a := &app{stateDir: filepath.Join(t.TempDir(), "temporarily-missing")}
	a.rememberLive(snapshotMetrics(snapshotSample(1)))
	if err := os.Mkdir(a.stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	// A polling gap supplies no new sample. Dirty memory must still be saved.
	got := a.rememberLive(snapshotMetrics())
	assertSnapshotIndices(t, got[keyChartSamples].([]map[string]any), 1, 1)
	assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 1)
}

func TestHistorySnapshotRetriesAfterRenameFailure(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	a.rememberLive(snapshotMetrics(snapshotSample(1)))
	path := filepath.Join(a.stateDir, snapshotRunID+".samples.json")
	backup := path + ".previous"
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	got := a.rememberLive(snapshotMetrics(snapshotSample(2)))
	assertSnapshotIndices(t, got[keyChartSamples].([]map[string]any), 1, 2)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatal(err)
	}
	a.rememberLive(snapshotMetrics(snapshotSample(2)))
	assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 2)
}

func TestHistorySnapshotReadersObserveCompleteReplacements(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	a.rememberLive(snapshotMetrics(snapshotSample(0)))
	reader := &app{stateDir: a.stateDir}
	ready, stop, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	var once sync.Once
	finish := func() { once.Do(func() { close(stop) }) }
	t.Cleanup(func() {
		finish()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("snapshot reader did not stop during cleanup")
		}
	})
	go func() {
		defer close(finished)
		first := true
		for {
			rows := reader.savedLiveSamples(snapshotRunID)
			if first {
				close(ready)
				first = false
			}
			if len(rows) == 0 || numberValue(rows[len(rows)-1], keyCumulativeOps) != float64(len(rows)-1) {
				result <- errors.New("reader observed a missing or incomplete snapshot during replacement")
				return
			}
			select {
			case <-stop:
				result <- nil
				return
			default:
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot reader did not start")
	}
	for i := 1; i <= 40; i++ {
		a.rememberLive(snapshotMetrics(snapshotSample(i)))
	}
	finish()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot reader did not finish")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	// A platform may refuse rename while a reader has the file open. Once
	// readers finish, a polling gap must flush any still-dirty measurements.
	a.rememberLive(snapshotMetrics())
	assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 0, 41)
}
