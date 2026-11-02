package server

import (
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func (a *app) readRealRun(runDir, runID string, active bool) map[string]any {
	abs, _ := filepath.Abs(runDir)
	root, _ := filepath.Abs(a.results)
	parent := filepath.Dir(abs)
	if parent != root {
		return nil
	}
	info, e := os.Stat(abs)
	if e != nil || !info.IsDir() {
		return nil
	}
	folders := []string{}
	if regularFile(filepath.Join(abs, "spt_run_params.json")) {
		folders = append(folders, abs)
	} else {
		entries, _ := os.ReadDir(abs)
		for _, item := range entries {
			if !item.IsDir() {
				continue
			}
			child := filepath.Join(abs, item.Name())
			if regularFile(filepath.Join(child, "spt_run_params.json")) {
				folders = append(folders, child)
			}
		}
	}
	sort.Slice(folders, func(i, j int) bool {
		a, _ := os.Stat(folders[i])
		b, _ := os.Stat(folders[j])
		return a != nil && b != nil && a.ModTime().After(b.ModTime())
	})
	for _, folder := range folders {
		b, err := os.ReadFile(filepath.Join(folder, "spt_run_params.json")) // #nosec G304 -- folder is the checked run directory or a child listed from it
		if err != nil {
			continue
		}
		var params map[string]any
		if json.Unmarshal(b, &params) != nil {
			continue
		}
		workload := strings.ToLower(strings.TrimSpace(str(params["workloadType"])))
		if workload == "" {
			workload = strings.ToLower(strings.TrimSpace(str(params["workload"])))
		}
		if workload == "" {
			continue
		}
		path, aggregate := a.metricsFiles(folder, active)
		if path == "" {
			continue
		}
		result, err := a.parseMetricsFile(path, filepath.Base(folder), workload, active, aggregate)
		if err != nil {
			continue
		}
		result[keyRunID] = runID
		history := map[string][]map[string]any{}
		files, _ := filepath.Glob(filepath.Join(folder, "*.metrics.csv"))
		sort.Strings(files)
		for _, p := range files {
			part, err := a.parseMetricsFile(p, filepath.Base(folder), workload, active, false)
			if err != nil {
				continue
			}
			for _, sample := range part[keySamples].([]map[string]any) {
				phase := strings.ToUpper(str(sample[keyOpType]))
				if validPhase(phase) {
					history[phase] = append(history[phase], sample)
				}
			}
		}
		captured := map[string][]map[string]any{}
		for _, sample := range a.savedLiveSamples(runID) {
			phase := strings.ToUpper(str(sample[keyOpType]))
			if validPhase(phase) {
				captured[phase] = append(captured[phase], sample)
			}
		}
		for phase, samples := range captured {
			if len(samples) > len(history[phase]) {
				history[phase] = samples
			}
		}
		chart := []map[string]any{}
		for _, samples := range history {
			chart = append(chart, samples...)
		}
		sort.Slice(chart, func(i, j int) bool { return str(chart[i][keyTimestamp]) < str(chart[j][keyTimestamp]) })
		if len(chart) > maxSamples {
			chart = chart[len(chart)-maxSamples:]
		}
		result[keyChartSamples] = chart
		if !active {
			totals, _ := filepath.Glob(filepath.Join(folder, "*.metrics.total.csv"))
			sort.Strings(totals)
			summaries := []map[string]any{}
			for _, p := range totals {
				low := strings.ToLower(filepath.Base(p))
				phase := "SPT"
				for _, name := range [][2]string{{"create", "Create"}, {"verify", phaseVerifyLabel}, {"delete", "Delete"}} {
					if strings.Contains(low, name[0]) {
						phase = name[1]
						break
					}
				}
				data, err := a.parseMetricsFile(p, filepath.Base(folder), workload, false, true)
				if err != nil {
					continue
				}
				rows := data[keySamples].([]map[string]any)
				if len(rows) == 0 {
					continue
				}
				s := rows[len(rows)-1]
				item := map[string]any{keyPhase: phase, keySourceFile: filepath.Base(p), keySuccessfulOps: s[keyCumulativeOps], metricFailed: s[keyCountFailed], metricIOPS: s[keyIntervalIOPS], metricBandwidth: s[metricBandwidth], metricP50Latency: s[metricP50Latency]}
				for _, k := range []string{keyTimestamp, "duration_seconds", "bytes_processed", "concurrency", "concurrency_mean", "node_count", metricCorrupt, "latency_mean_us", "latency_min_us", "latency_max_us", sptP99Latency} {
					item[k] = s[k]
				}
				summaries = append(summaries, item)
			}
			result[keyPhaseSummaries] = summaries
			failed := 0.0
			all := len(summaries) > 0
			for _, s := range summaries {
				v, ok := num(s, metricFailed)
				if !ok {
					all = false
					break
				}
				failed += v
			}
			if all {
				result["total_failed"] = failed
			} else {
				result["total_failed"] = nil
			}
		}
		return result
	}
	return nil
}

func validPhase(p string) bool {
	return p == phaseCreate || p == phaseVerify || p == phaseRead || p == phaseDelete
}

func (a *app) savedLiveSamples(runID string) []map[string]any {
	if !runPattern.MatchString(runID) {
		return nil
	}
	path := filepath.Join(a.stateDir, runID+".samples.json")
	info, e := os.Stat(path)
	if e != nil || info.Size() > maxSamplesBytes {
		return nil
	}
	b, e := os.ReadFile(path) // #nosec G304 -- runID matches runPattern above, so path stays inside the state directory
	if e != nil {
		return nil
	}
	var list []map[string]any
	if json.Unmarshal(b, &list) != nil {
		return nil
	}
	if len(list) > maxSamples {
		list = list[len(list)-maxSamples:]
	}
	out := []map[string]any{}
	for _, m := range list {
		if _, ok := m[keyTimestamp].(string); ok {
			out = append(out, m)
		}
	}
	return out
}

func (a *app) rememberLive(metrics map[string]any) map[string]any {
	runID := str(metrics[keyRunID])
	if a.liveRunID != runID {
		a.liveRunID = runID
		a.liveSamples = a.savedLiveSamples(runID)
		a.liveSignature = ""
	}
	changed := false
	for _, raw := range metrics[keySamples].([]map[string]any) {
		keys := []string{keyStepID, keyCumulativeOps, keyCountFailed, keyIntervalIOPS, metricBandwidth, metricP50Latency}
		vals := make([]any, 0, len(keys))
		for _, k := range keys {
			vals = append(vals, raw[k])
		}
		b, _ := json.Marshal(vals)
		sig := string(b)
		if sig != a.liveSignature {
			a.liveSamples = append(a.liveSamples, raw)
			if len(a.liveSamples) > maxSamples {
				a.liveSamples = a.liveSamples[len(a.liveSamples)-maxSamples:]
			}
			a.liveSignature = sig
			changed = true
		}
	}
	if changed && runPattern.MatchString(runID) {
		b, _ := json.Marshal(a.liveSamples)
		tmp := filepath.Join(a.stateDir, runID+".samples.tmp")
		path := filepath.Join(a.stateDir, runID+".samples.json")
		if os.WriteFile(tmp, b, 0600) == nil {
			_ = os.Chmod(tmp, 0600)
			_ = os.Rename(tmp, path)
		}
	}
	out := cloneMap(metrics)
	out[keySamples] = latestSample(a.liveSamples)
	out[keyChartSamples] = append([]map[string]any(nil), a.liveSamples...)
	return out
}

// latestSample keeps the full live history out of "samples"; "chart_samples" already carries it.
func latestSample(samples []map[string]any) []map[string]any {
	if len(samples) == 0 {
		return []map[string]any{}
	}
	return []map[string]any{samples[len(samples)-1]}
}

func (a *app) liveMetrics(state map[string]any) map[string]any {
	if a.metricsURL != "" {
		return a.liveMetricsAt(state, a.metricsURL)
	}
	hosts := a.storageSnapshot().Hosts
	var out map[string]any
	if len(hosts) > 1 {
		// With several client machines the entry's /metrics/json is only its own slice;
		// /metrics/cluster/json is SPT's merged fleet view (see METRICS.md).
		out = a.liveMetricsAt(state, a.clusterMetricsEndpoint())
	}
	if out == nil {
		out = a.liveMetricsAt(state, a.metricsEndpoint())
	}
	if out != nil && len(hosts) > 1 {
		out["nodes"] = a.liveNodes(state, hosts)
	}
	return out
}

// fetchMetricEntries reads one SPT metrics document, which may be an object or an array.
func fetchMetricEntries(client *http.Client, metricsURL string) ([]map[string]any, bool) {
	req, err := http.NewRequest(http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, false
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetricsBytes+1))
	if err != nil || len(body) > maxMetricsBytes || resp.StatusCode != 200 {
		return nil, false
	}
	var payload any
	if json.Unmarshal(body, &payload) != nil {
		return nil, false
	}
	items := []any{payload}
	if list, ok := payload.([]any); ok {
		items = list
	}
	out := []map[string]any{}
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, true
}

// freshEntries keeps running, non-terminal samples taken after the run started, newest first.
func freshEntries(entries []map[string]any, started float64) []map[string]any {
	fresh := []map[string]any{}
	for _, m := range entries {
		t, err := parseTimestamp(str(m[keySampleTS]))
		if err != nil {
			continue
		}
		stateN := numberValue(m, "test_state")
		step := str(m[keyStepID])
		if float64(t.UnixNano())/1e9 < started || stateN != 1 || m["terminal"] == true || step == "" {
			continue
		}
		fresh = append(fresh, m)
	}
	sort.Slice(fresh, func(i, j int) bool { return str(fresh[i][keySampleTS]) > str(fresh[j][keySampleTS]) })
	return fresh
}

// entryRates converts one SPT metrics sample into the dashboard's units.
func entryRates(entry map[string]any) map[string]any {
	ops := mapValue(entry["operations"])
	bandwidth := mapValue(entry["bandwidth"])
	timing := mapValue(entry["timing"])
	latency := mapValue(timing["latency"])
	success := numberValue(ops, "success_count")
	elapsed := numberValue(entry, "elapsed_time_seconds")
	bytesTotal := numberValue(bandwidth, "bytes_total")
	var iops, bw, p50, p99 any
	if v, ok := num(ops, "success_rate_last"); ok {
		iops = v
	} else if elapsed > 0 {
		iops = success / elapsed
	}
	if v, ok := num(bandwidth, "bytes_rate_last"); ok {
		bw = v / (1024 * 1024)
	} else if elapsed > 0 {
		bw = bytesTotal / elapsed / (1024 * 1024)
	}
	if numberValue(latency, "count") > 0 {
		if v, ok := num(latency, "p50_us"); ok {
			p50 = v
		}
		if v, ok := num(latency, "p99_us"); ok {
			p99 = v
		}
	}
	return map[string]any{"success": success, keyFailed: numberValue(ops, "failed_count"), "corrupt": optionalMetric(ops, "corrupt_count"), "elapsed": elapsed, "bytes_total": bytesTotal, "latency_count": numberValue(latency, "count"), metricIOPS: iops, metricBandwidth: bw, metricP50Latency: p50, metricP99Latency: p99}
}

func (a *app) liveMetricsAt(state map[string]any, metricsURL string) map[string]any {
	started, ok := num(state, keyStartedEpoch)
	if !ok {
		return nil
	}
	entries, ok := fetchMetricEntries(&http.Client{Timeout: 450 * time.Millisecond}, metricsURL)
	if !ok {
		return nil
	}
	fresh := freshEntries(entries, started)
	if len(fresh) == 0 {
		return nil
	}
	entry := map[string]any(nil)
	for _, m := range fresh {
		if m["scope"] == "fleet" || m["scope"] == "cluster" {
			entry = m
			break
		}
	}
	if entry == nil {
		for _, m := range fresh {
			if m["role"] == "entry" {
				entry = m
				break
			}
		}
	}
	if entry == nil {
		entry = fresh[0]
	}
	r := entryRates(entry)
	success, failed, elapsed := r["success"].(float64), r[keyFailed].(float64), r["elapsed"].(float64)
	ts, err := parseTimestamp(str(entry[keySampleTS]))
	if err != nil {
		return nil
	}
	stepElapsed := math.Max(0, float64(ts.UnixNano())/1e9-started)
	sample := map[string]any{keyTimestamp: ts.UTC().Format(time.RFC3339Nano), keyElapsedSeconds: stepElapsed, keyCumulativeOps: success, keyCountFailed: failed, metricCorrupt: r["corrupt"], "average_iops": any(nil), keyIntervalIOPS: r[metricIOPS], metricBandwidth: r[metricBandwidth], "bandwidth_unit": "MiB/s", keySourceFile: metricsContract, metricP50Latency: r[metricP50Latency], sptP99Latency: r[metricP99Latency], keyStepID: str(entry[keyStepID]), keyOpType: str(entry[keyOpType]), "completion_percent": optionalMetric(entry, "completion_percent")}
	if elapsed > 0 {
		sample["average_iops"] = success / elapsed
	}
	has := success > 0 || failed > 0 || r["latency_count"].(float64) > 0 || r["bytes_total"].(float64) > 0
	out := map[string]any{keyRunID: str(state[keyRunID]), keyWorkloadType: jobWorkload(state), keySourceFile: metricsContract, keyUpdatedAt: sample[keyTimestamp], keyIsActive: true, keyIsAggregate: false, "scope": firstNonempty(str(entry["scope"]), "node"), keyPhase: firstNonempty(str(sample[keyOpType]), str(sample[keyStepID])), "phase_id": str(sample[keyStepID]), keyProgressPercent: sample["completion_percent"], keySamples: []map[string]any{}}
	if partial, ok := entry["partial"].(bool); ok {
		out["fleet_partial"] = partial
	}
	if !has {
		return out
	}
	out[keySamples] = []map[string]any{sample}
	return out
}

// clusterMetricsEndpoint is the entry node's merged fleet view.
func (a *app) clusterMetricsEndpoint() string {
	return strings.TrimSuffix(a.metricsEndpoint(), "/metrics/json") + "/metrics/cluster/json"
}

// liveNodes reads each client machine's own /metrics/json so the UI can show per-node
// rows next to the fleet row. Rows are never summed with the fleet aggregate.
func (a *app) liveNodes(state map[string]any, hosts []string) []map[string]any {
	started, _ := num(state, keyStartedEpoch)
	client := &http.Client{Timeout: 400 * time.Millisecond}
	rows := make([]map[string]any, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Add(1)
		go func(i int, host string) {
			defer wg.Done()
			url := ""
			if a.nodeMetricsURL != nil {
				url = a.nodeMetricsURL(host)
			} else {
				url = "http://" + net.JoinHostPort(hostAddress(host), strconv.Itoa(constants.SptAPIPortInt)) + "/metrics/json"
			}
			row := map[string]any{keyHost: hostAddress(host), keyStatus: "unavailable"}
			entries, ok := fetchMetricEntries(client, url)
			if ok {
				for _, m := range freshEntries(entries, started) {
					if m["scope"] == "fleet" || m["scope"] == "cluster" || m["role"] == "aggregate" {
						continue
					}
					r := entryRates(m)
					row = map[string]any{keyHost: hostAddress(host), "node_id": str(m["node_id"]), "role": str(m["role"]), keyStatus: "reporting", keyOpType: str(m[keyOpType]), keySampleTS: str(m[keySampleTS]), keySuccessfulOps: r["success"], metricFailed: r[keyFailed], metricCorrupt: r["corrupt"], metricIOPS: r[metricIOPS], metricBandwidth: r[metricBandwidth], metricP50Latency: r[metricP50Latency], metricP99Latency: r[metricP99Latency]}
					break
				}
			}
			rows[i] = row
		}(i, host)
	}
	wg.Wait()
	return rows
}

func optionalMetric(m map[string]any, key string) any {
	if v, ok := num(m, key); ok {
		return v
	}
	return nil
}

func firstNonempty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

func (a *app) collectLiveHistory(runID string) {
	defer a.processes.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.mu.Lock()
		if a.activeJob == nil || a.activeJob[keyRunID] != runID || a.activeJob[keyStatus] != jobRunning {
			a.mu.Unlock()
			return
		}
		state := cloneMap(a.activeJob)
		a.mu.Unlock()
		if metrics := a.liveMetrics(state); metrics != nil {
			a.mu.Lock()
			if a.activeJob != nil && a.activeJob[keyRunID] == runID {
				_ = a.rememberLive(metrics)
			}
			a.mu.Unlock()
		}
		<-ticker.C
	}
}

// jobWorkload names the SPT workload a dashboard job runs, from its saved options.
func jobWorkload(job map[string]any) string {
	driver := ""
	switch o := job[keyOptions].(type) {
	case options:
		driver = o.Driver
	case map[string]any:
		driver = str(o["driver"])
	}
	if driver == driverMock {
		return "mock"
	}
	return driverWriteVerify
}
