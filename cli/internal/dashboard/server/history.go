package server

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
)

func (a *app) handleRunHistory(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		writeJSON(w, 404, map[string]any{keyError: msgEndpointNotFound})
		return
	}
	entries, err := os.ReadDir(a.results)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		writeJSON(w, 500, map[string]any{keyError: "Could not read saved SPT runs."})
		return
	}
	latest := a.latestJob()
	items := []map[string]any{}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "mt-dashboard-") || !runPattern.MatchString(entry.Name()) {
			continue
		}
		item, err := a.savedRun(entry.Name(), latest, false)
		if err == nil {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return str(items[i][keyStartedAt]) > str(items[j][keyStartedAt])
	})
	if len(items) > 200 {
		items = items[:200]
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": items})
}

func (a *app) handleSavedRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runID")
	if r.URL.RawQuery != "" || !runPattern.MatchString(runID) || filepath.Base(runID) != runID || !strings.HasPrefix(runID, "mt-dashboard-") {
		writeJSON(w, http.StatusNotFound, map[string]any{keyError: msgSavedRunNotFound})
		return
	}
	latest := a.latestJob()
	item, err := a.savedRun(runID, latest, true)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{keyError: msgSavedRunNotFound})
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (a *app) handleRunCSV(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runID")
	query := r.URL.Query()
	for key := range query {
		if key != "kind" && key != keyPhase && key != "start" && key != "end" {
			writeJSON(w, http.StatusBadRequest, map[string]any{keyError: "Unsupported CSV export options."})
			return
		}
	}
	if !runPattern.MatchString(runID) || filepath.Base(runID) != runID || !strings.HasPrefix(runID, "mt-dashboard-") {
		writeJSON(w, http.StatusNotFound, map[string]any{keyError: msgSavedRunNotFound})
		return
	}
	item, err := a.savedRun(runID, nil, true)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{keyError: msgSavedRunNotFound})
		return
	}
	metrics, _ := item["metrics"].(map[string]any)
	kind := query.Get("kind")
	if kind == "" {
		kind = "summary"
	}
	rows := []map[string]any{}
	phase := strings.ToUpper(query.Get(keyPhase))
	switch kind {
	case "summary":
		rows = metricsSummaries(metrics)
		if len(rows) == 0 && metrics != nil {
			rows, _ = metrics[keyChartSamples].([]map[string]any)
		}
	case keySamples:
		if metrics != nil {
			rows, _ = metrics[keyChartSamples].([]map[string]any)
		}
	case keyPhase, kindWindow:
		if phase != phaseCreate && phase != phaseVerify && phase != "READ" && phase != phaseDelete {
			writeJSON(w, http.StatusBadRequest, map[string]any{keyError: "Choose a supported run phase."})
			return
		}
		if metrics != nil {
			all, _ := metrics[keyChartSamples].([]map[string]any)
			started, _ := time.Parse(time.RFC3339Nano, str(item[keyJob].(map[string]any)[keyStartedAt]))
			var lo, hi float64
			if kind == kindWindow {
				var e1, e2 error
				lo, e1 = strconv.ParseFloat(query.Get("start"), 64)
				hi, e2 = strconv.ParseFloat(query.Get("end"), 64)
				if e1 != nil || e2 != nil || math.IsNaN(lo) || math.IsNaN(hi) || math.IsInf(lo, 0) || math.IsInf(hi, 0) || lo < 0 || hi < lo {
					writeJSON(w, http.StatusBadRequest, map[string]any{keyError: "Choose a valid visible time window."})
					return
				}
			}
			for _, row := range all {
				if phaseCodeForBackend(str(row["op_type"])) != phase {
					continue
				}
				if kind == kindWindow {
					t, e := time.Parse(time.RFC3339Nano, str(row[keyTimestamp]))
					if e != nil || started.IsZero() {
						continue
					}
					elapsed := t.Sub(started).Seconds()
					if elapsed < lo || elapsed > hi {
						continue
					}
				}
				rows = append(rows, row)
			}
		}
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{keyError: "Choose summary, samples, phase, or window CSV."})
		return
	}
	if len(rows) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{keyError: "No measurements are available for this CSV export."})
		return
	}
	filename := "spt-" + runID + "-" + kind + ".csv"
	if phase != "" {
		filename = "spt-" + runID + "-" + strings.ToLower(phase) + "-" + kind + ".csv"
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	writer := csv.NewWriter(w)
	fields := map[string]bool{}
	for _, row := range rows {
		for key := range row {
			if keyPattern.MatchString(key) {
				fields[key] = true
			}
		}
	}
	columns := make([]string, 0, len(fields))
	for key := range fields {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	_ = writer.Write(columns)
	for _, row := range rows {
		record := make([]string, len(columns))
		for i, key := range columns {
			if value := row[key]; value != nil {
				record[i] = fmt.Sprint(value)
			}
		}
		_ = writer.Write(record)
	}
	writer.Flush()
}

func phaseCodeForBackend(value string) string {
	s := strings.ToUpper(value)
	switch {
	case strings.Contains(s, phaseCreate), strings.Contains(s, "WRITE"):
		return phaseCreate
	case strings.Contains(s, phaseVerify), strings.Contains(s, "READ"):
		return phaseVerify
	case strings.Contains(s, phaseDelete), strings.Contains(s, "CLEANUP"):
		return phaseDelete
	default:
		return "SPT"
	}
}

func (a *app) savedRun(runID string, latest map[string]any, includeMetrics bool) (map[string]any, error) {
	dir := filepath.Join(a.results, runID)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() || filepath.Dir(dir) != a.results {
		return nil, os.ErrNotExist
	}
	metrics := a.readRealRun(dir, runID, false)
	var params map[string]any
	paramsDir := dir
	for _, candidate := range []string{filepath.Join(dir, "spt_run_params.json")} {
		if b, e := os.ReadFile(candidate); e == nil { // #nosec G304 -- dir is checked above to be a direct child of the results directory
			if json.Unmarshal(b, &params) == nil {
				paramsDir = filepath.Dir(candidate)
			}
		}
	}
	if params == nil {
		children, _ := os.ReadDir(dir)
		for _, child := range children {
			if !child.IsDir() {
				continue
			}
			childDir := filepath.Join(dir, child.Name())
			if b, e := os.ReadFile(filepath.Join(childDir, "spt_run_params.json")); e == nil && json.Unmarshal(b, &params) == nil { // #nosec G304 -- child folder listed from the checked run directory
				paramsDir = childDir
				break
			}
		}
	}
	if params == nil {
		return nil, os.ErrNotExist
	}
	if metrics != nil {
		if data, loadErr := summary.NewLoader().Load(context.Background(), paramsDir); loadErr == nil {
			if aggregated, aggregateErr := summary.Aggregate(data); aggregateErr == nil {
				metrics["spt_summary_report"] = summary.NewRenderer(summary.RenderOptions{}).FullReport(aggregated)
			}
		}
	}

	startedAt := ""
	if match := runTimePattern.FindStringSubmatch(runID); len(match) == 2 {
		if stamp, e := time.ParseInLocation("20060102T150405", match[1], time.UTC); e == nil {
			startedAt = stamp.Format(time.RFC3339)
		}
	}
	if startedAt == "" {
		startedAt = str(params["generatedAt"])
	}
	scenario, _ := params["scenarioParams"].(map[string]any)
	options := map[string]any{
		"object_count":       scenario["ObjectCount"],
		"object_size":        scenario["ObjectSize"],
		keyThreads:           scenario["Threads"],
		keyMaxRuntimeSeconds: params["autoTerminateSeconds"],
		"driver":             savedRunDriver(params),
	}
	status := savedRunStatus(dir, paramsDir, params, metricsSummaries(metrics))
	job := map[string]any{keyRunID: runID, keyStatus: status, keyStartedAt: startedAt, keyOptions: options}
	if record := a.readRunRecord(runID); record != nil {
		// The dashboard recorded how this run ended; that outranks the file heuristic.
		status = str(record[keyStatus])
		job = record
		job[keyOptions] = options
	}
	if latest != nil && str(latest[keyRunID]) == runID {
		job = publicJob(latest)
		if opts, ok := latest[keyOptions].(map[string]any); ok {
			options = opts
		}
		job[keyOptions] = options
	}
	if metrics != nil {
		metrics[keyRunID] = runID
	}
	item := map[string]any{keyJob: job, "metrics": metrics}
	if includeMetrics {
		item["target"] = a.targetConfiguration()
	} else {
		item = map[string]any{
			keyRunID: runID, keyStartedAt: startedAt, keyStatus: status,
			keyOptions: options, "phase_summaries": metricsSummaries(metrics),
		}
		if latest != nil && str(latest[keyRunID]) == runID {
			item[keyStatus] = str(latest[keyStatus])
		}
	}
	return item, nil
}

// savedRunStatus classifies a saved run from its files when the dashboard holds no
// record of how it ended (runs from older builds). SPT writes
// spt_<label>-<stamp>_results_summary.txt when a run finishes. A write-verify run
// has create, verify and delete phases; a mock run has only the create phase, so
// it is complete once that phase stored every object.
func savedRunStatus(dir, paramsDir string, params map[string]any, phases []map[string]any) string {
	for _, d := range []string{dir, paramsDir} {
		if regularFile(filepath.Join(d, "results_summary.txt")) {
			return jobCompleted
		}
		if matches, _ := filepath.Glob(filepath.Join(d, constants.ResultsSummaryFilePrefix+"*"+constants.ResultsSummaryFileSuffix)); len(matches) > 0 {
			return jobCompleted
		}
	}
	if ended := traceLogOutcome(paramsDir); ended != "" {
		return ended
	}
	if len(phases) >= 3 {
		return jobCompleted
	}
	scenario, _ := params["scenarioParams"].(map[string]any)
	if str(params["workloadType"]) == driverMock && len(phases) >= 1 {
		want := numberValue(scenario, "ObjectCount")
		for _, row := range phases {
			if phaseCodeForBackend(str(row[keyPhase])) == phaseCreate && want > 0 &&
				numberValue(row, keySuccessfulOps) >= want && numberValue(row, "failed_ops") == 0 {
				return jobCompleted
			}
		}
	}
	return jobPartial
}

// traceLogOutcome reads how SPT's own trace log says a run ended, for runs the
// dashboard holds no record of. SPT's headless runner (cli/tui/headless/runner.go)
// logs "[SIGNAL] Received signal" when it is stopped and "[ERROR] Failed to start
// test" when the start fails. A stop outranks the start failure it causes. The
// files are only read.
func traceLogOutcome(dir string) string {
	logs, _ := filepath.Glob(filepath.Join(dir, "spt-*.trace.log"))
	outcome := ""
	for _, p := range logs {
		f, err := os.Open(p) // #nosec G304 -- dir is the checked run directory or a child listed from it
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "[SIGNAL] Received signal") {
				_ = f.Close()
				return jobInterrupted
			}
			if strings.Contains(line, "[ERROR] Failed to start test") {
				outcome = jobFailed
			}
		}
		_ = f.Close()
	}
	return outcome
}

// savedRunDriver is the storage driver a saved run used, from its run parameters.
func savedRunDriver(params map[string]any) string {
	if str(params["workloadType"]) == driverMock {
		return driverMock
	}
	return driverS3
}

func metricsSummaries(metrics map[string]any) []map[string]any {
	if metrics == nil {
		return []map[string]any{}
	}
	if rows, ok := metrics["phase_summaries"].([]map[string]any); ok {
		return rows
	}
	return []map[string]any{}
}
