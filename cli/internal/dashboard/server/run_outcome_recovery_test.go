package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestRunOutcomeSurvivesRestartAndLaterRun(t *testing.T) {
	for _, tt := range []struct {
		name, status string
		exitCode     int
	}{
		{"successful exit", jobCompleted, 0},
		{"failed exit", jobFailed, 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, _, _ := prepareShutdownTest(t)
			body := "#!/bin/sh\nprintf 'standard output\\n'\nprintf 'standard error\\n' >&2\nexit " + strconv.Itoa(tt.exitCode) + "\n"
			if err := os.WriteFile(a.sptBin, []byte(body), 0700); err != nil {
				t.Fatal(err)
			}
			w := submitRecoveryRun(a)
			if w.Code != http.StatusAccepted {
				t.Fatalf("run = %d %s", w.Code, w.Body.String())
			}
			waitForRecoveryRun(t, a)
			a.mu.Lock()
			job := cloneMap(a.activeJob)
			active := a.activeCmd != nil || a.activeLock != nil
			a.mu.Unlock()
			runID := str(job["run_id"])
			if job["status"] != tt.status || job["returncode"] != tt.exitCode || str(job["finished_at"]) == "" || active || a.lockBusy() {
				t.Fatalf("wrong process outcome or incomplete cleanup: %#v", job)
			}
			for name, persisted := range map[string]map[string]any{"latest state": a.readStateFile(), "run record": a.readRunRecord(runID)} {
				code, ok := num(persisted, "returncode")
				if persisted["run_id"] != runID || persisted["status"] != tt.status || !ok || code != float64(tt.exitCode) || persisted["finished_at"] != job["finished_at"] {
					t.Fatalf("%s lost outcome: %#v", name, persisted)
				}
			}
			log, err := os.ReadFile(filepath.Join(a.stateDir, runID+".log"))
			if err != nil || !strings.Contains(string(log), "standard output") || !strings.Contains(string(log), "standard error") {
				t.Fatalf("output not drained before final status: %q, %v", log, err)
			}
			// SPT-style saved measurements let history/detail resolve the run.
			// A final dashboard record must win even if the CSV looks successful.
			writeSavedRun(t, a.results, runID, "write-verify", 10, 10, true)
			restarted := &app{root: a.root, home: a.home, port: a.port, results: a.results, stateDir: a.stateDir, stateFile: a.stateFile, lockFile: a.lockFile}
			state, err := restarted.currentState()
			if err != nil {
				t.Fatal(err)
			}
			restored, _ := state["job"].(map[string]any)
			if restored["run_id"] != runID || restored["status"] != tt.status {
				t.Fatalf("restart changed terminal outcome: %#v", restored)
			}
			if got := historyStatuses(t, restarted)[runID]; got != tt.status {
				t.Fatalf("history after restart = %q; want %q", got, tt.status)
			}
			// A new accepted run proves the previous exit released the lock.
			if err := os.WriteFile(a.sptBin, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			if w := submitRecoveryRun(a); w.Code != http.StatusAccepted {
				t.Fatalf("subsequent run = %d %s", w.Code, w.Body.String())
			}
			waitForRecoveryRun(t, a)
			later := a.readStateFile()
			if later["run_id"] == runID || later["status"] != jobCompleted {
				t.Fatalf("later run did not replace latest state: %#v", later)
			}
			afterLater := &app{root: a.root, home: a.home, port: a.port, results: a.results, stateDir: a.stateDir, stateFile: a.stateFile, lockFile: a.lockFile}
			if got := historyStatuses(t, afterLater)[runID]; got != tt.status {
				t.Fatalf("history after later run = %q; want %q", got, tt.status)
			}
			detail := savedRunStatusOf(t, afterLater, runID)
			if code, ok := num(detail, "returncode"); detail["status"] != tt.status || !ok || code != float64(tt.exitCode) {
				t.Fatalf("saved detail lost original outcome: %#v", detail)
			}
		})
	}
}
