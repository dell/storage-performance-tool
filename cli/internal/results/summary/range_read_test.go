package summary

import (
	"bytes"
	"context"
	"encoding/csv"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func rangeFixture(changes ...map[string]string) string {
	header := append(strings.Fields("schema_version engine_run_id step_id worker_id context_index terminal mode size fixed_offset_present fixed_offset alignment overflow"), rangeCountColumns...)
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	_ = w.Write(header)
	for _, change := range changes {
		v := map[string]string{"schema_version": "1", "engine_run_id": "42", "step_id": "step-read", "worker_id": "worker-a", "context_index": "0", "terminal": "true", "mode": "fixed", "size": "3", "fixed_offset_present": "true", "fixed_offset": "0", "alignment": "1", "overflow": "false"}
		for _, k := range rangeCountColumns {
			v[k] = "0"
		}
		for k, val := range map[string]string{"selected": "1", "accepted": "1", "terminal_results": "1", "attempted": "1", "requests_sent": "1", "successful_bytes": "3"} {
			v[k] = val
		}
		for k, val := range change {
			v[k] = val
		}
		row := make([]string, len(header))
		for i, k := range header {
			row[i] = v[k]
		}
		_ = w.Write(row)
	}
	w.Flush()
	return b.String()
}

func TestRangeReadAggregationAndPolicyPresence(t *testing.T) {
	s := rangeFixture(nil, map[string]string{"worker_id": "worker-b", "mode": "random", "fixed_offset_present": "false", "fixed_offset": "", "alignment": "8"})
	e, err := parseRangeRead(strings.NewReader(s), "step-read")
	if err != nil {
		t.Fatal(err)
	}
	if !e.Complete || e.Totals["successful_bytes"] != 6 || e.Totals["accepted"] != 2 || e.Rows[0].FixedOffset == nil || *e.Rows[0].FixedOffset != 0 || e.Rows[1].FixedOffset != nil {
		t.Fatalf("evidence=%+v", e)
	}
	var b strings.Builder
	new(Renderer).renderRangeRead(&b, &RunSummary{Steps: []StepSummary{{StepID: "step-read", RangeRead: e}}})
	for _, want := range []string{"validated successful bytes: 6", "fixed offset 0", "random", "alignment 8"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q: %s", want, b.String())
		}
	}
}

func TestRangeReadRejectsInvalidEvidence(t *testing.T) {
	for _, change := range []map[string]string{
		{"schema_version": "2"}, {"step_id": "other"}, {"worker_id": ""}, {"size": "0"}, {"alignment": "0"},
		{"fixed_offset": "1", "alignment": "2"}, {"fixed_offset": "9223372036854775807"},
		{"fixed_offset_present": "false"}, {"terminal": "yes"}, {"overflow": "true"}, {"accepted": "-1"},
		{"successful_bytes": "8"}, {"selected": "2"}, {"terminal_results": "0"}, {"attempted": "0"},
		{"failed": "1"}, {"in_flight": "1"}, {"requests_sent": "9223372036854775808"},
	} {
		if _, err := parseRangeRead(strings.NewReader(rangeFixture(change)), "step-read"); err == nil {
			t.Fatalf("accepted %v", change)
		}
	}
	for _, s := range []string{rangeFixture(nil, nil), rangeFixture(nil, map[string]string{"worker_id": "worker-b", "engine_run_id": "43"}), rangeFixture()} {
		if _, err := parseRangeRead(strings.NewReader(s), "step-read"); err == nil {
			t.Fatal("accepted duplicate/mixed/empty artifact")
		}
	}
	large := map[string]string{"size": "1", "selected": "9223372036854775807", "accepted": "9223372036854775807", "terminal_results": "9223372036854775807", "attempted": "9223372036854775807", "requests_sent": "9223372036854775807", "successful_bytes": "9223372036854775807"}
	if _, err := parseRangeRead(strings.NewReader(rangeFixture(large, map[string]string{"worker_id": "worker-b"})), "step-read"); err == nil {
		t.Fatal("aggregate overflow accepted")
	}
	e, err := parseRangeRead(strings.NewReader(rangeFixture(map[string]string{"terminal": "false", "selected": "2", "generator_buffered": "1"})), "step-read")
	if err != nil || e.Complete {
		t.Fatalf("incomplete=%+v err=%v", e, err)
	}
}

func TestRangeReadLoadsOnlyCanonicalArtifact(t *testing.T) {
	dir := t.TempDir()
	name := "step-read.range.read.csv"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(rangeFixture(nil)), 0600); err != nil {
		t.Fatal(err)
	}
	sm := &results.StepManifest{StepID: "step-read", Files: []results.FileStatus{{Name: name, Status: "ok"}, {Name: "step-read.range.read.node-000.csv", Status: "ok"}}}
	e, err := loadRangeRead(dir, sm)
	if err != nil || e.Totals["accepted"] != 1 {
		t.Fatalf("e=%+v err=%v", e, err)
	}
	sm.Files = sm.Files[1:]
	e, err = loadRangeRead(dir, sm)
	if err != nil || e != nil {
		t.Fatal("node source counted or absence rejected")
	}
}

func TestRangeReadFlowsThroughLoaderAndSummary(t *testing.T) {
	for _, terminal := range []string{"true", "false"} {
		dir := t.TempDir()
		step := "step-read"
		rangeName := step + ".range.read.csv"
		configName := step + ".config.yaml"
		if err := os.WriteFile(filepath.Join(dir, rangeName), []byte(rangeFixture(map[string]string{"terminal": terminal})), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, configName), []byte("output:\n  metrics:\n    summary:\n      persist: false\n"), 0600); err != nil {
			t.Fatal(err)
		}
		writeManifest(t, dir, &results.Manifest{OutputDir: dir, Steps: []results.StepManifest{{StepID: step, Files: []results.FileStatus{{Name: rangeName, Status: "ok"}, {Name: configName, Status: "ok"}}}}})
		writeParams(t, dir, &RunParams{ScenarioParams: ScenarioParams{WorkloadType: "read", RunID: 42}})
		data, err := NewLoader().Load(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		if data.Steps[step].RangeRead == nil {
			t.Fatal("range evidence lost in loader")
		}
		if terminal == "false" && data.Steps[step].Status != StepStatusPartial {
			t.Fatal("incomplete evidence marked complete")
		}
		summary, err := Aggregate(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(summary.Steps) != 1 || summary.Steps[0].RangeRead.Totals["successful_bytes"] != 3 {
			t.Fatal("range evidence lost in aggregation")
		}
		writeParams(t, dir, &RunParams{ScenarioParams: ScenarioParams{WorkloadType: "read", RunID: 43}})
		if _, err := NewLoader().Load(context.Background(), dir); err == nil {
			t.Fatal("stale range run identity accepted")
		}
	}
}

func TestRangeReadMissingEvidenceIsRequiredOnlyForRangeReadSteps(t *testing.T) {
	for _, tc := range []struct {
		name, op, size, configSize string
		required                   bool
	}{
		{"metadata", "read", "3", "", true},
		{"config", "read", "", "3", true},
		{"ordinary", "read", "", "", false},
		{"seed", "create", "3", "", false},
		{"cleanup", "delete", "3", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			step := "step-read"
			configName := step + ".config.yaml"
			config := "output:\n  metrics:\n    summary:\n      persist: false\nload:\n  op:\n    type: " + tc.op + "\n"
			if tc.configSize != "" {
				config += "    read:\n      range:\n        size: " + tc.configSize + "\n"
			}
			if err := os.WriteFile(filepath.Join(dir, configName), []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			writeManifest(t, dir, &results.Manifest{OutputDir: dir, Steps: []results.StepManifest{{StepID: step, Files: []results.FileStatus{{Name: configName, Status: "ok"}, {Name: step + ".range.read.csv", Status: "missing"}}}}})
			writeParams(t, dir, &RunParams{ScenarioParams: ScenarioParams{WorkloadType: "read", RangeSize: tc.size}})
			data, err := NewLoader().Load(context.Background(), dir)
			if (err != nil) != tc.required {
				t.Fatalf("required=%t err=%v", tc.required, err)
			}
			found := false
			for _, s := range data.Steps[step].MissingRequired {
				if s == "range.read.csv" {
					found = true
				}
			}
			if found != tc.required {
				t.Fatalf("missing required=%v", data.Steps[step].MissingRequired)
			}
		})
	}
}

func TestRangeReadUnavailableLegacyConfigDoesNotIntroduceAnError(t *testing.T) {
	dir := t.TempDir()
	name := "step-read.config.yaml"
	sm := &results.StepManifest{StepID: "step-read", Files: []results.FileStatus{{Name: name, Status: "ok"}}}
	for _, body := range []string{"", "load: [invalid yaml"} {
		if body != "" {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
		expected, err := NewLoader().rangeReadExpected(dir, sm, &RunParams{}, nil)
		if expected || err != nil {
			t.Fatalf("ordinary expected=%t err=%v", expected, err)
		}
		_, err = NewLoader().rangeReadExpected(dir, sm, &RunParams{ScenarioParams: ScenarioParams{RangeSize: "3"}}, nil)
		if err == nil {
			t.Fatal("known range configuration error hidden")
		}
	}
}
