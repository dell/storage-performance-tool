package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// How old a live sample_ts may be before the dashboard labels it delayed.
	// The live poll is ~500 ms–2 s; 10 s means several missed refreshes.
	sampleStaleAfter = 10 * time.Second
	maxSavedWarnings = 64

	warnFleetPartial  = "fleet_partial"
	warnNodeNoData    = "node_no_data"
	warnSampleDelayed = "sample_delayed"
	warnMetricsGap    = "metrics_gap"
	warnFailedOps     = "failed_operations"
	dataPresent       = "present"
	dataNoData        = "no_data"
	dataDelayed       = "delayed"
	keyWarnings       = "warnings"
	keyDataStatus     = "data_status"
	keyNodesCount     = "nodes_count"
	keyNodesPresent   = "nodes_present"
	keyContributors   = "contributors_present"
	keyObserved       = "observed"
	keyCode           = "code"
	keySeverity       = "severity"
	keyAffectedHost   = "host"
	keyAffectedMetric = "metric"
	keyWarningSource  = "source"
)

// runWarning is one actionable observation about unreliable telemetry.
// Text must state what was observed; it must not invent a cause (SCRUM-21 AC4).
type runWarning struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Observed string `json:"observed"`
	Host     string `json:"host,omitempty"`
	Metric   string `json:"metric,omitempty"`
	Source   string `json:"source"`
}

func stringList(v any) []string {
	switch t := v.(type) {
	case []string:
		return append([]string(nil), t...)
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s := strings.TrimSpace(str(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func sampleAge(ts string, now time.Time) (time.Duration, bool) {
	t, err := parseTimestamp(ts)
	if err != nil {
		return 0, false
	}
	age := now.Sub(t)
	if age < 0 {
		age = 0
	}
	return age, true
}

func annotateNodeDataStatus(row map[string]any, now time.Time) {
	if str(row[keyStatus]) != nodeStatusReporting {
		row[keyDataStatus] = dataNoData
		return
	}
	age, ok := sampleAge(str(row[keySampleTS]), now)
	if !ok {
		row[keyDataStatus] = dataPresent
		return
	}
	if age > sampleStaleAfter {
		row[keyDataStatus] = dataDelayed
		return
	}
	row[keyDataStatus] = dataPresent
}

type warningSink struct {
	out   []runWarning
	index map[string]int
}

// add keeps one slot per code|host|metric so age-changing sample_delayed
// text refreshes in place instead of filling maxSavedWarnings and evicting
// earlier distinct warnings.
func (s *warningSink) add(w runWarning) {
	if w.Severity == "" {
		w.Severity = levelWarn
	}
	key := w.Code + "|" + w.Host + "|" + w.Metric
	if s.index == nil {
		s.index = map[string]int{}
	}
	if i, ok := s.index[key]; ok {
		s.out[i] = w
		return
	}
	s.index[key] = len(s.out)
	s.out = append(s.out, w)
}

func appendFleetPartialWarning(metrics map[string]any, add func(runWarning)) {
	if metrics["fleet_partial"] != true {
		return
	}
	parts := []string{"SPT fleet metrics report partial=true (aggregate incomplete or contributor evidence inconsistent)."}
	if n, ok := num(metrics, keyNodesCount); ok {
		parts = append(parts, "nodes_count="+str(n))
	}
	if present := stringList(metrics[keyNodesPresent]); len(present) > 0 {
		parts = append(parts, "nodes_present=["+strings.Join(present, ", ")+"]")
	}
	if contrib := stringList(metrics[keyContributors]); len(contrib) > 0 {
		parts = append(parts, "contributors_present=["+strings.Join(contrib, ", ")+"]")
	}
	add(runWarning{
		Code:     warnFleetPartial,
		Observed: strings.Join(parts, " "),
		Source:   sourceFleetMetrics,
		Metric:   "fleet_aggregate",
	})
}

func warnMetricsGapIfEmpty(active bool, samples []map[string]any, add func(runWarning)) {
	if !active || len(samples) > 0 {
		return
	}
	add(runWarning{
		Code:     warnMetricsGap,
		Observed: "No live metric sample is available in this poll (empty samples while the run is active). This is a telemetry gap, not a measured value of zero.",
		Source:   sourceLiveMetrics,
		Metric:   "samples",
	})
}

func warnSampleDelay(metrics map[string]any, samples []map[string]any, active bool, now time.Time, add func(runWarning)) {
	updated := str(metrics[keyUpdatedAt])
	if updated == "" && len(samples) > 0 {
		updated = str(samples[0][keyTimestamp])
	}
	if !active || updated == "" {
		return
	}
	age, ok := sampleAge(updated, now)
	if !ok || age <= sampleStaleAfter {
		return
	}
	add(runWarning{
		Code:     warnSampleDelayed,
		Observed: "Latest live metrics timestamp is " + updated + " (" + age.Truncate(time.Second).String() + " ago). Showing delayed data, not a fresh zero.",
		Source:   sourceLiveMetrics,
		Metric:   "updated_at",
	})
	metrics[keyDataStatus] = dataDelayed
}

func setDefaultDataStatus(metrics map[string]any, samples []map[string]any, active bool) {
	if _, ok := metrics[keyDataStatus]; ok {
		return
	}
	if active && len(samples) == 0 {
		metrics[keyDataStatus] = dataNoData
		return
	}
	if len(samples) > 0 {
		metrics[keyDataStatus] = dataPresent
	}
}

func warnFailedOpsInSample(samples []map[string]any, add func(runWarning)) {
	if len(samples) == 0 {
		return
	}
	failed, hasFailed := num(samples[0], keyCountFailed)
	if !hasFailed || failed <= 0 {
		return
	}
	add(runWarning{
		Code:     warnFailedOps,
		Observed: "Live metrics report " + str(failed) + " failed operation(s) in the current sample.",
		Source:   "operations.failed_count",
		Metric:   metricFailed,
	})
}

func warnActiveTelemetry(metrics map[string]any, samples []map[string]any, active bool, now time.Time, add func(runWarning)) {
	warnMetricsGapIfEmpty(active, samples, add)
	warnSampleDelay(metrics, samples, active, now, add)
	setDefaultDataStatus(metrics, samples, active)
	warnFailedOpsInSample(samples, add)
}

func warnNodeRows(nodes []map[string]any, now time.Time, add func(runWarning)) {
	for _, row := range nodes {
		annotateNodeDataStatus(row, now)
		host := str(row[keyHost])
		switch str(row[keyDataStatus]) {
		case dataNoData:
			add(runWarning{
				Code:     warnNodeNoData,
				Observed: "Client machine " + host + ": SPT /metrics/json did not return a usable live sample in this poll. Metric cells show no data (not zero).",
				Host:     host,
				Source:   sourceNodeMetrics,
				Metric:   "node_sample",
			})
		case dataDelayed:
			add(runWarning{
				Code:     warnSampleDelayed,
				Observed: "Client machine " + host + ": latest sample_ts is " + str(row[keySampleTS]) + ". Treating per-node metrics as delayed.",
				Host:     host,
				Source:   sourceNodeMetrics,
				Metric:   "sample_ts",
			})
		}
		if str(row[keyStatus]) != nodeStatusReporting {
			continue
		}
		failed, ok := num(row, metricFailed)
		if !ok || failed <= 0 {
			continue
		}
		add(runWarning{
			Code:     warnFailedOps,
			Observed: "Client machine " + host + ": live metrics report " + str(failed) + " failed operation(s).",
			Host:     host,
			Source:   sourceNodeMetrics,
			Metric:   metricFailed,
		})
	}
}

// buildRunWarnings turns SPT/live fields the dashboard already has into
// observed warnings. It never invents a network or client failure cause.
func buildRunWarnings(metrics map[string]any, now time.Time) []runWarning {
	if metrics == nil {
		return nil
	}
	sink := &warningSink{index: map[string]int{}}
	add := sink.add
	appendFleetPartialWarning(metrics, add)
	active, _ := metrics[keyIsActive].(bool)
	samples, _ := metrics[keySamples].([]map[string]any)
	warnActiveTelemetry(metrics, samples, active, now, add)
	nodes, _ := metrics["nodes"].([]map[string]any)
	warnNodeRows(nodes, now, add)
	sort.SliceStable(sink.out, func(i, j int) bool {
		if sink.out[i].Code != sink.out[j].Code {
			return sink.out[i].Code < sink.out[j].Code
		}
		return sink.out[i].Host < sink.out[j].Host
	})
	return sink.out
}

func warningsAsMaps(list []runWarning) []map[string]any {
	out := make([]map[string]any, 0, len(list))
	for _, w := range list {
		item := map[string]any{
			keyCode: w.Code, keySeverity: w.Severity, keyObserved: w.Observed, keyWarningSource: w.Source,
		}
		if w.Host != "" {
			item[keyAffectedHost] = w.Host
		}
		if w.Metric != "" {
			item[keyAffectedMetric] = w.Metric
		}
		out = append(out, item)
	}
	return out
}

// attachWarnings adds the warnings list to metrics and optionally remembers them for the run.
func (a *app) attachWarnings(metrics map[string]any, now time.Time) map[string]any {
	if metrics == nil {
		return nil
	}
	list := buildRunWarnings(metrics, now)
	metrics[keyWarnings] = warningsAsMaps(list)
	runID := str(metrics[keyRunID])
	if a != nil && runPattern.MatchString(runID) && len(list) > 0 {
		a.saveRunWarnings(runID, list)
	}
	return metrics
}

// mergeRunWarnings concatenates prior and next warnings, keeping one entry per
// code|host|metric (Observed refreshes in place). Caps at maxSavedWarnings (newest tail).
func mergeRunWarnings(prior, next []runWarning) []runWarning {
	sink := &warningSink{index: map[string]int{}}
	for _, w := range prior {
		sink.add(w)
	}
	for _, w := range next {
		sink.add(w)
	}
	out := sink.out
	if len(out) > maxSavedWarnings {
		out = append([]runWarning(nil), out[len(out)-maxSavedWarnings:]...)
	}
	return out
}

func (a *app) loadRunWarningStructs(runID string) []runWarning {
	if a == nil || a.stateDir == "" || !runPattern.MatchString(runID) {
		return nil
	}
	path := filepath.Join(a.stateDir, runID+".warnings.json")
	info, err := os.Stat(path)
	if err != nil || info.Size() > 256<<10 {
		return nil
	}
	b, err := os.ReadFile(path) // #nosec G304 -- runID matches runPattern
	if err != nil {
		return nil
	}
	var list []runWarning
	if json.Unmarshal(b, &list) != nil {
		return nil
	}
	return list
}

func (a *app) saveRunWarnings(runID string, list []runWarning) {
	if a.stateDir == "" || !runPattern.MatchString(runID) {
		return
	}
	merged := mergeRunWarnings(a.loadRunWarningStructs(runID), list)
	if len(merged) == 0 {
		return
	}
	b, err := json.Marshal(merged)
	if err != nil {
		return
	}
	tmp := filepath.Join(a.stateDir, runID+".warnings.tmp")
	path := filepath.Join(a.stateDir, runID+".warnings.json")
	if os.WriteFile(tmp, b, 0600) != nil {
		return
	}
	_ = os.Chmod(tmp, 0600)
	_ = os.Rename(tmp, path)
}

func (a *app) loadRunWarnings(runID string) []map[string]any {
	return warningsAsMaps(a.loadRunWarningStructs(runID))
}
