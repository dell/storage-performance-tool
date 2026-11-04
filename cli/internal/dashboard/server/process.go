package server

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	jobRunning     = "running"
	jobCompleted   = "completed"
	jobFailed      = "failed"
	jobInterrupted = "interrupted"
	jobPartial     = "partial"

	msgInterruptedShutdown = "Interrupted: the dashboard was stopped (Ctrl+C) while SPT was running, so the run did not finish."
	msgInterruptedRestart  = "Interrupted: the dashboard stopped before SPT finished, so SPT's final exit status was not recorded."
)

func drainOutput(src io.Reader, path string) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600) // #nosec G304 -- state directory plus a server-generated run ID
	if err != nil {
		_, _ = io.Copy(io.Discard, src)
		return
	}
	// The run log is best-effort; a failed close cannot be reported to anyone.
	defer func() { _ = f.Close() }()
	_, _ = io.Copy(io.MultiWriter(&limitedWriter{w: f, left: maxLogBytes}, io.Discard), src)
}

func (a *app) watchProcess(runID string, cmd *exec.Cmd, lock *os.File, drained <-chan struct{}) {
	defer a.processes.Done()
	<-drained
	err := cmd.Wait()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else {
			code = -1
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	status, message := finalStatus(err, a.stopping)
	if a.activeJob != nil && a.activeJob[keyRunID] == runID {
		a.activeJob = cloneMap(a.activeJob)
		a.activeJob[keyStatus] = status
		a.activeJob["returncode"] = code
		a.activeJob["finished_at"] = utcNow()
		a.activeJob[keyMessage] = message
		_ = a.writeState(a.activeJob)
		_ = a.writeRunRecord(a.activeJob)
		a.activeCmd = nil
		a.activeLock = nil
	}
	unlockClose(lock)
}

// finalStatus names how SPT ended. When the dashboard itself is shutting down it
// interrupts SPT. SPT then exits 1, or exits 0 after a graceful shutdown; either
// way the run was interrupted, not completed or failed.
func finalStatus(err error, stopping bool) (string, string) {
	switch {
	case stopping:
		// The dashboard interrupted SPT. SPT may still exit 0 after a graceful
		// shutdown, so the exit code alone cannot say whether the run finished.
		return jobInterrupted, msgInterruptedShutdown
	case err == nil:
		return jobCompleted, "SPT finished successfully."
	default:
		return jobFailed, "SPT exited with an error. See VM-side SPT logs for details."
	}
}

// runRecordPath keeps each dashboard run's final status next to the latest-job
// state, so Run history reports the same outcome after later jobs and restarts.
func (a *app) runRecordPath(runID string) string {
	return filepath.Join(a.stateDir, "runs", runID+".json")
}

func (a *app) writeRunRecord(job map[string]any) error {
	runID := str(job[keyRunID])
	if !runPattern.MatchString(runID) || filepath.Base(runID) != runID {
		return os.ErrInvalid
	}
	dir := filepath.Join(a.stateDir, "runs")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	b, err := json.Marshal(publicJob(job))
	if err != nil {
		return err
	}
	path := a.runRecordPath(runID)
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	if err = os.Chmod(tmp, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (a *app) readRunRecord(runID string) map[string]any {
	if a.stateDir == "" || !runPattern.MatchString(runID) || filepath.Base(runID) != runID {
		return nil
	}
	b, err := os.ReadFile(a.runRecordPath(runID)) // #nosec G304 -- state directory plus a validated run ID
	if err != nil {
		return nil
	}
	var m map[string]any
	if json.Unmarshal(b, &m) != nil || str(m[keyRunID]) != runID {
		return nil
	}
	return m
}

// markInterrupted records a run whose state still says "running" although no SPT
// process holds the run lock: the dashboard stopped before SPT reported an exit.
// Callers hold a.mu.
func (a *app) markInterrupted(state map[string]any) map[string]any {
	state = cloneMap(state)
	state[keyStatus] = jobInterrupted
	state[keyMessage] = msgInterruptedRestart
	_ = a.writeState(state)
	_ = a.writeRunRecord(state)
	a.activeJob = state
	return state
}

// latestJob returns the newest dashboard job for Run history. It reads the state
// file when this process has not started or loaded a job yet, and settles a stale
// "running" state the same way /api/state does, so both views agree.
func (a *app) latestJob() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activeJob != nil {
		return cloneMap(a.activeJob)
	}
	state := a.readStateFile()
	if state == nil {
		return nil
	}
	if str(state[keyStatus]) == jobRunning && a.activeCmd == nil && !a.lockBusy() {
		state = a.markInterrupted(state)
	}
	return cloneMap(state)
}

// cloneMap copies a job or sample map. A nil map stays nil, so callers can still
// tell "no job loaded" apart from an empty job.
func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func utcNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (a *app) readStateFile() map[string]any {
	b, e := os.ReadFile(a.stateFile)
	if e != nil {
		return nil
	}
	var m map[string]any
	if json.Unmarshal(b, &m) != nil {
		return nil
	}
	return m
}

func (a *app) writeState(state map[string]any) error {
	b, e := json.Marshal(state)
	if e != nil {
		return e
	}
	tmp := a.stateFile + ".tmp"
	if e = os.WriteFile(tmp, b, 0600); e != nil {
		return e
	}
	if e = os.Chmod(tmp, 0600); e != nil {
		return e
	}
	return os.Rename(tmp, a.stateFile)
}

func publicJob(state map[string]any) map[string]any {
	keys := []string{keyRunID, keyStatus, "created_at", keyStartedAt, "finished_at", keyElapsedSeconds, "returncode", keyMessage, keyOptions, "prefix", keyPhase}
	out := map[string]any{}
	for _, k := range keys {
		if v, ok := state[k]; ok {
			out[k] = v
		}
	}
	return out
}

func (a *app) lockBusy() bool {
	f, e := os.OpenFile(a.lockFile, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	if lockFile(f) != nil {
		return true
	}
	_ = unlockFile(f)
	return false
}

func regularFile(path string) bool {
	info, e := os.Stat(path)
	return e == nil && info.Mode().IsRegular()
}
