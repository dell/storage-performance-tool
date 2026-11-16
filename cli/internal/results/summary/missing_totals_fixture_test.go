package summary

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	missingTotalsSeedStep   = "spt-001-20261115.120000.000-seed"
	missingTotalsCreateStep = "spt-002-20261115.120000.000-create"
	missingTotalsReadStep   = "spt-003-20261115.120000.000-read"
)

// parsedTotalsLine is what tools match to read the run total from the console summary.
var parsedTotalsLine = regexp.MustCompile(`Totals:\s+duration\s+`)

type missingTotalsStep struct {
	id         string
	op         string
	bytes      int64
	seconds    float64
	failures   int64
	missing    bool
	suppressed bool
	// absent leaves an expected step out of the results index altogether.
	absent bool
}

// missingTotalsRun builds a synthetic run: a suppressed seed step, then the given steps.
func missingTotalsRun(t *testing.T, steps ...missingTotalsStep) *RunSummary {
	t.Helper()
	return missingTotalsRunSteps(t, "", append([]missingTotalsStep{{id: missingTotalsSeedStep, suppressed: true}}, steps...)...)
}

// missingTotalsRunSteps builds a synthetic run of exactly the given steps; an absent step is
// listed only as a missing expected step, as the loader does for one not in the index.
func missingTotalsRunSteps(t *testing.T, workload string, all ...missingTotalsStep) *RunSummary {
	t.Helper()
	runData := &RunData{
		RunID:        "spt-20261115.120000.000",
		Steps:        make(map[string]*StepData),
		Params:       &RunParams{GeneratedAt: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)},
		ManifestPath: "index.json",
		MetadataPath: "params.json",
	}
	runData.Params.ScenarioParams.ObjectSize = "1MiB"
	runData.Params.WorkloadType = workload
	for _, step := range all {
		if step.absent {
			runData.MissingExpectedSteps = append(runData.MissingExpectedSteps, step.id)
			continue
		}
		runData.StepOrder = append(runData.StepOrder, step.id)
		data := &StepData{StepID: step.id, Status: StepStatusComplete, MetricsSuppressed: step.suppressed}
		switch {
		case step.suppressed:
		case step.missing:
			data.Status = StepStatusPartial
			data.MissingRequired = []string{"metrics.total.csv"}
		default:
			data.Metrics = &MetricsTotals{StepID: step.id, Rows: []MetricsTotalsRow{{
				Operation:           step.op,
				SuccessCount:        100,
				FailureCount:        step.failures,
				SizeBytes:           step.bytes,
				StepDurationSeconds: step.seconds,
				ThroughputAvgOps:    10,
				BandwidthAvgMiBps:   100,
				LatencyAvgMicros:    1000,
				LatencyP50Micros:    1000,
				Concurrency:         1,
				NodeCount:           1,
			}}}
		}
		runData.Steps[step.id] = data
	}
	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	return summary
}

func totalsLine(t *testing.T, snippet string) string {
	t.Helper()
	for _, line := range strings.Split(snippet, "\n") {
		if strings.HasPrefix(line, "Totals") {
			return line
		}
	}
	t.Fatalf("no totals line in:\n%s", snippet)
	return ""
}
