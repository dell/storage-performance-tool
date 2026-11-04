package server

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (a *app) currentState() (map[string]any, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.activeJob
	// Recover completed runs too, so a service restart preserves their public
	// settings, timestamps, and outcome alongside the saved measurements.
	if state == nil && !a.lockBusy() {
		state = a.readStateFile()
		if state != nil {
			a.activeJob = state
		}
	}
	if state == nil && a.lockBusy() {
		state = a.readStateFile()
		if state != nil {
			a.activeJob = state
			state[keyStatus] = jobRunning
			if epoch, ok := num(state, "started_epoch"); ok {
				state[keyElapsedSeconds] = int(time.Now().Unix() - int64(epoch))
			}
		}
	}
	if state != nil && str(state[keyStatus]) == jobRunning && a.activeCmd == nil && !a.lockBusy() {
		state = a.markInterrupted(state)
	}
	if state != nil && str(state[keyStatus]) == jobRunning {
		state = cloneMap(state)
		if epoch, ok := num(state, "started_epoch"); ok {
			state[keyElapsedSeconds] = math.Max(0, float64(time.Now().Unix())-epoch)
		}
		state[keyPhase] = a.phaseFromOutput(str(state["results_dir"]))
		result := a.readRealRun(str(state["results_dir"]), str(state[keyRunID]), true)
		live := a.liveMetrics(state)
		if live != nil {
			result = a.rememberLive(live)
			phase := strings.ToLower(str(live[keyPhase]))
			label := "Create / write"
			if strings.Contains(phase, "verify") || strings.Contains(phase, "read") {
				label = "Verify"
			}
			if strings.Contains(phase, "delete") || strings.Contains(phase, "cleanup") {
				label = "Cleanup"
			}
			state[keyPhase] = label
			ss := live[keySamples].([]map[string]any)
			if len(ss) > 0 {
				sample := ss[0]
				state[keyMessage] = fmt.Sprintf("SPT live %s metrics: %.0f successful, %.0f failed operations.", strings.ToLower(label), numberValue(sample, keyCumulativeOps), numberValue(sample, keyCountFailed))
			} else {
				state[keyMessage] = "SPT is in the " + strings.ToLower(label) + " phase; waiting for its first measured operation."
			}
			_ = a.writeState(state)
		} else {
			retained := retainedLiveMetrics(str(state[keyRunID]), a.liveRunID, jobWorkload(state), a.liveSamples)
			if result == nil {
				if retained != nil {
					result = retained
					result[keySourceFile] = "Last SPT live metrics · waiting for next phase"
					state[keyPhase] = "Finalizing results"
					state[keyMessage] = "SPT is moving between phases; the last recorded measurements remain visible."
					_ = a.writeState(state)
				} else {
					result = map[string]any{keyRunID: state[keyRunID], keyWorkloadType: jobWorkload(state), keySourceFile: "Waiting for SPT live metrics", keyUpdatedAt: utcNow(), keyIsActive: true, keyIsAggregate: false, keySamples: []map[string]any{}, keyChartSamples: []map[string]any{}}
				}
			} else {
				result[keyIsActive] = true
				result[keyIsAggregate] = false
				chart, _ := result[keyChartSamples].([]map[string]any)
				if len(chart) == 0 && retained != nil {
					result[keyChartSamples] = retained[keyChartSamples]
					if len(result[keySamples].([]map[string]any)) == 0 {
						result[keySamples] = retained[keySamples]
					}
					result[keySourceFile] = "Last SPT live metrics · waiting for next phase"
					state[keyPhase] = "Finalizing results"
					state[keyMessage] = "SPT is moving between phases; the last recorded measurements remain visible."
					_ = a.writeState(state)
				}
			}
		}
		a.activeJob = state
		return a.stateResponse(publicJob(state), result), nil
	}
	if state != nil && str(state[keyStatus]) == jobRunning && !a.lockBusy() {
		state = a.readStateFile()
		if state == nil {
			state = a.activeJob
		}
		if state != nil {
			state = a.markInterrupted(state)
		}
	}
	if state != nil {
		return a.stateResponse(publicJob(state), a.readRealRun(str(state["results_dir"]), str(state[keyRunID]), false)), nil
	}
	return a.stateResponse(nil, a.latestSavedRun()), nil
}

func (a *app) stateResponse(job, metrics map[string]any) map[string]any {
	target := a.targetConfiguration()
	return map[string]any{keyJob: job, "metrics": metrics, "target": target, "storage": a.storageState(target), "limits": a.limitsPayload(), "branding": a.effectiveSettings().Branding}
}

// retainedLiveMetrics keeps the last live samples visible between phases, labelled
// with the job's own workload (mock runs must not read as write + verify).
func retainedLiveMetrics(runID, liveRunID, workload string, samples []map[string]any) map[string]any {
	if runID == "" || runID != liveRunID || len(samples) == 0 {
		return nil
	}
	kept := append([]map[string]any(nil), samples...)
	return map[string]any{
		keyRunID: runID, keyWorkloadType: workload, keyUpdatedAt: kept[len(kept)-1][keyTimestamp],
		keyIsActive: true, keyIsAggregate: false, keySamples: latestSample(kept),
		keyChartSamples: kept,
	}
}

func (a *app) latestSavedRun() map[string]any {
	entries, e := os.ReadDir(a.results)
	if e != nil {
		return nil
	}
	dirs := []string{}
	for _, ent := range entries {
		if ent.IsDir() && strings.HasPrefix(ent.Name(), "mt-") {
			dirs = append(dirs, filepath.Join(a.results, ent.Name()))
		}
	}
	sort.Slice(dirs, func(i, j int) bool {
		x, _ := os.Stat(dirs[i])
		y, _ := os.Stat(dirs[j])
		return x != nil && y != nil && x.ModTime().After(y.ModTime())
	})
	if len(dirs) > 200 {
		dirs = dirs[:200]
	}
	for _, dir := range dirs {
		r := a.readRealRun(dir, filepath.Base(dir), false)
		if r != nil && len(r[keySamples].([]map[string]any)) > 0 {
			return r
		}
	}
	return nil
}

func (a *app) phaseFromOutput(runDir string) string {
	files := []string{}
	_ = filepath.WalkDir(runDir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".metrics.csv") && !strings.HasSuffix(path, ".total.csv") {
			files = append(files, path)
		}
		return nil
	})
	if len(files) == 0 {
		return "SPT setup"
	}
	sort.Slice(files, func(i, j int) bool {
		x, _ := os.Stat(files[i])
		y, _ := os.Stat(files[j])
		return x != nil && y != nil && x.ModTime().After(y.ModTime())
	})
	name := strings.ToLower(filepath.Base(files[0]))
	if strings.Contains(name, "delete") || strings.Contains(name, "cleanup") {
		return "Cleanup"
	}
	if strings.Contains(name, "verify") || strings.Contains(name, "read") {
		return phaseVerifyLabel
	}
	if strings.Contains(name, "create") || strings.Contains(name, "write") {
		return "Create / write"
	}
	return "SPT metrics"
}
