package server

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
)

// The compare view reports a threshold check, not statistical significance: one run
// per side, no repetitions, no confidence interval. The defaults are deliberately
// conservative so ordinary run-to-run noise is not called a regression.
const compareRuleLabel = "Threshold check, not statistical significance"

func defaultCompareSettings() compareSettings {
	return compareSettings{ThresholdPct: 10, LatencyFloorUS: 500, MinOps: 10}
}

func (c compareSettings) validate() error {
	if c.ThresholdPct < 1 || c.ThresholdPct > 100 || math.IsNaN(c.ThresholdPct) {
		return errors.New("compare threshold_pct must be between 1 and 100")
	}
	if c.LatencyFloorUS < 0 || c.LatencyFloorUS > 10_000_000 || math.IsNaN(c.LatencyFloorUS) {
		return errors.New("compare latency_floor_us must be between 0 and 10000000")
	}
	if c.MinOps < 1 || c.MinOps > 1e12 || math.IsNaN(c.MinOps) {
		return errors.New("compare min_ops must be between 1 and 1000000000000")
	}
	return nil
}

type compareMetric struct {
	Key          string
	Label        string
	HigherBetter bool
	Latency      bool
	get          func(catalogPhase) *float64
}

var compareMetrics = []compareMetric{
	{Key: metricIOPS, Label: "IOPS", HigherBetter: true, get: func(p catalogPhase) *float64 { return p.IOPS }},
	{Key: metricBandwidth, Label: "Bandwidth (MiB/s)", HigherBetter: true, get: func(p catalogPhase) *float64 { return p.Bandwidth }},
	{Key: metricP50Latency, Label: "P50 latency (µs)", Latency: true, get: func(p catalogPhase) *float64 { return p.P50 }},
	{Key: metricP99Latency, Label: "P99 latency (µs)", Latency: true, get: func(p catalogPhase) *float64 { return p.P99 }},
	{Key: metricFailed, Label: "Failed operations", get: func(p catalogPhase) *float64 { v := p.Failed; return &v }},
	{Key: metricCorrupt, Label: "Corrupt objects", get: func(p catalogPhase) *float64 { return p.Corrupt }},
}

type compareDelta struct {
	RunID    string   `json:"run_id"`
	Phase    string   `json:"phase"`
	Metric   string   `json:"metric"`
	Label    string   `json:"label"`
	Baseline *float64 `json:"baseline"`
	Value    *float64 `json:"value"`
	DeltaPct *float64 `json:"delta_pct"`
	Flag     string   `json:"flag"`
	Reason   string   `json:"reason,omitempty"`
}

// settingsDifferences lists the run settings that make a performance comparison unfair.
func settingsDifferences(base, run catalogRun) []string {
	diffs := []string{}
	add := func(name, a, b string) {
		if a != b {
			diffs = append(diffs, fmt.Sprintf("%s differs (%s vs %s)", name, orUnknown(a), orUnknown(b)))
		}
	}
	add("workload", base.Workload, run.Workload)
	add("object size", base.ObjectSize, run.ObjectSize)
	add("object count", intText(base.ObjectCount), intText(run.ObjectCount))
	add(keyThreads, intText(base.Threads), intText(run.Threads))
	add("host count", intText(base.HostCount), intText(run.HostCount))
	add("S3 driver", base.S3Driver, run.S3Driver)
	return diffs
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func intText(v int64) string {
	if v == 0 {
		return ""
	}
	return strconv.FormatInt(v, 10)
}

// compareRuns compares each run's phases with the baseline phase of the same key.
func compareRuns(base catalogRun, runs []catalogRun, rule compareSettings) []compareDelta {
	basePhases := map[string]catalogPhase{}
	for _, p := range base.Phases {
		basePhases[p.Key] = p
	}
	out := []compareDelta{}
	for _, run := range runs {
		if run.ID == base.ID {
			continue
		}
		diffs := settingsDifferences(base, run)
		for _, p := range run.Phases {
			bp, ok := basePhases[p.Key]
			for _, m := range compareMetrics {
				d := compareDelta{RunID: run.ID, Phase: p.Key, Metric: m.Key, Label: m.Label, Value: m.get(p)}
				if !ok {
					d.Flag, d.Reason = "not_comparable", "Baseline has no "+p.Key+" phase."
					out = append(out, d)
					continue
				}
				d.Baseline = m.get(bp)
				d.Flag, d.Reason = judge(m, bp, p, d.Baseline, d.Value, rule)
				if d.Baseline != nil && d.Value != nil && *d.Baseline != 0 {
					pct := (*d.Value - *d.Baseline) / math.Abs(*d.Baseline) * 100
					d.DeltaPct = &pct
				}
				integrityFailure := m.Key == metricCorrupt && d.Flag == flagRegressed
				if len(diffs) > 0 && !integrityFailure && d.Flag != flagNoData {
					d.Flag, d.Reason = "not_comparable", strings.Join(diffs, "; ")+"."
				}
				out = append(out, d)
			}
		}
	}
	return out
}

func judge(m compareMetric, bp, p catalogPhase, base, value *float64, rule compareSettings) (string, string) {
	if m.Key == metricCorrupt {
		if value != nil && *value > 0 {
			return flagRegressed, "Corrupt objects recorded; this is always flagged."
		}
		if value == nil {
			return flagNoData, "This run did not record corrupt counts."
		}
		return flagWithin, ""
	}
	if base == nil || value == nil {
		return flagNoData, "Metric not recorded in one of the runs."
	}
	if m.Key == metricFailed {
		if *value > *base {
			return flagRegressed, "More failed operations than the baseline."
		}
		if *value < *base {
			return "improved", "Fewer failed operations than the baseline."
		}
		return flagWithin, ""
	}
	if bp.Successful < rule.MinOps || p.Successful < rule.MinOps {
		return "insufficient", fmt.Sprintf("Fewer than %.0f successful operations in one of the runs.", rule.MinOps)
	}
	if *base == 0 {
		return flagNoData, "Baseline value is zero."
	}
	if m.Latency && math.Abs(*value-*base) < rule.LatencyFloorUS {
		return flagWithin, fmt.Sprintf("Change is under the %.0f µs latency floor.", rule.LatencyFloorUS)
	}
	pct := (*value - *base) / math.Abs(*base) * 100
	worse := pct < -rule.ThresholdPct
	better := pct > rule.ThresholdPct
	if !m.HigherBetter {
		worse, better = pct > rule.ThresholdPct, pct < -rule.ThresholdPct
	}
	switch {
	case worse:
		return flagRegressed, fmt.Sprintf("Worse than the baseline by more than %.0f%%.", rule.ThresholdPct)
	case better:
		return "improved", fmt.Sprintf("Better than the baseline by more than %.0f%%.", rule.ThresholdPct)
	}
	return flagWithin, fmt.Sprintf("Within ±%.0f%% of the baseline.", rule.ThresholdPct)
}

func queryFloat(r *http.Request, key string, def float64) (float64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("%s must be a number", key)
	}
	return v, nil
}

func (a *app) handleCatalogCompare(w http.ResponseWriter, r *http.Request) {
	if !a.loopbackHost(r) {
		writeJSON(w, 403, map[string]any{keyError: msgLoopbackOnly})
		return
	}
	ids := r.URL.Query()["id"]
	if len(ids) < 2 || len(ids) > catalogMaxCompare {
		writeJSON(w, 400, map[string]any{keyError: fmt.Sprintf("Choose between 2 and %d runs to compare.", catalogMaxCompare)})
		return
	}
	rule := a.effectiveSettings().Compare
	var err error
	if rule.ThresholdPct, err = queryFloat(r, "threshold_pct", rule.ThresholdPct); err == nil {
		if rule.LatencyFloorUS, err = queryFloat(r, "latency_floor_us", rule.LatencyFloorUS); err == nil {
			rule.MinOps, err = queryFloat(r, "min_ops", rule.MinOps)
		}
	}
	if err == nil {
		err = rule.validate()
	}
	if err != nil {
		writeJSON(w, 400, map[string]any{keyError: err.Error()})
		return
	}
	all, _ := a.scanCatalog()
	byID := map[string]catalogRun{}
	for _, run := range all {
		byID[run.ID] = run
	}
	runs := []catalogRun{}
	seen := map[string]bool{}
	for _, id := range ids {
		run, ok := byID[id]
		if !ok {
			writeJSON(w, 404, map[string]any{keyError: msgCatalogRunNotFound})
			return
		}
		if seen[id] {
			writeJSON(w, 400, map[string]any{keyError: "Each run can be chosen once."})
			return
		}
		seen[id] = true
		runs = append(runs, run)
	}
	baseID := r.URL.Query().Get("baseline")
	if baseID == "" {
		baseID = ids[0]
	}
	if !seen[baseID] {
		writeJSON(w, 400, map[string]any{keyError: "The baseline must be one of the compared runs."})
		return
	}
	base := byID[baseID]
	series := map[string]map[string][]seriesPoint{}
	differences := map[string][]string{}
	for _, run := range runs {
		series[run.ID] = a.runSeries(run)
		differences[run.ID] = settingsDifferences(base, run)
	}
	writeJSON(w, 200, map[string]any{
		"baseline": baseID, keyRuns: runs, "series": series, "deltas": compareRuns(base, runs, rule), "settings_differences": differences,
		"rule": map[string]any{"label": compareRuleLabel, "threshold_pct": rule.ThresholdPct, "latency_floor_us": rule.LatencyFloorUS, "min_ops": rule.MinOps,
			"description": "A metric is flagged when it is worse than the baseline by more than the threshold. Latency changes under the floor and phases with too few operations are not flagged. Any corrupt object is always flagged. Runs with different settings are marked not comparable."},
	})
}
