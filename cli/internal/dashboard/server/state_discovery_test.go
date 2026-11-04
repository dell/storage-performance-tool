package server

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setDiscoveryTime(t *testing.T, path string, order int) {
	t.Helper()
	stamp := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(order) * time.Hour)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

func TestStateDiscoverySelectsNewestUsableRun(t *testing.T) {
	a := newPollingApp(t)
	for _, item := range []struct {
		id         string
		order, ops int
	}{
		{"mt-zzz-older", 1, 11}, {"mt-aaa-newer", 2, 22}, {"cli-not-dashboard", 4, 44},
	} {
		writeSavedRun(t, a.results, item.id, "write-verify", 50, item.ops, false)
		setDiscoveryTime(t, filepath.Join(a.results, item.id), item.order)
	}
	// A newer, incomplete run must not hide the latest usable measurements.
	empty := filepath.Join(a.results, "mt-newest-empty")
	if err := os.Mkdir(empty, 0700); err != nil {
		t.Fatal(err)
	}
	setDiscoveryTime(t, empty, 3)
	for _, want := range []struct {
		id  string
		ops float64
	}{{"mt-aaa-newer", 22}, {"mt-zzz-older", 11}} {
		state := pollingState(t, a)
		metrics := mapValue(state["metrics"])
		rows := pollingRows(t, metrics, "samples")
		if state["job"] != nil || metrics["run_id"] != want.id || metrics["is_active"] != false || len(rows) != 1 || rows[0]["cumulative_ops"] != want.ops {
			t.Fatalf("discovery chose the wrong run: %#v", state)
		}
		if err := os.RemoveAll(filepath.Join(a.results, want.id)); err != nil {
			t.Fatal(err)
		}
	}
	state := pollingState(t, a)
	if state["job"] != nil || state["metrics"] != nil {
		t.Fatalf("invented a job or used a non-dashboard folder: %#v", state)
	}
}

func TestStateDiscoverySkipsDamagedRuns(t *testing.T) {
	for _, damage := range []struct{ name, params, csv string }{
		{"missing params", "", "valid"},
		{"malformed params", "{", "valid"},
		{"wrong params shape", "[]", "valid"},
		{"missing workload", "{}", "valid"},
		{"missing measurements", `{"workloadType":"write-verify"}`, ""},
		{"malformed measurements", `{"workloadType":"write-verify"}`, "bad"},
	} {
		t.Run(damage.name, func(t *testing.T) {
			a := newPollingApp(t)
			const good = "mt-good-discovery"
			const bad = "mt-bad-discovery"
			writeSavedRun(t, a.results, good, "write-verify", 10, 8, false)
			writeSavedRun(t, a.results, bad, "write-verify", 10, 99, false)
			dir := filepath.Join(a.results, bad, "dashboard-20261023.214759.259")
			params := filepath.Join(dir, "spt_run_params.json")
			if damage.params == "" {
				if err := os.Remove(params); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(params, []byte(damage.params), 0600); err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.csv"))
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range files {
				if damage.csv == "" {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				} else if damage.csv == "bad" {
					if err := os.WriteFile(path, []byte("not,csv,metrics\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			setDiscoveryTime(t, filepath.Join(a.results, good), 1)
			setDiscoveryTime(t, filepath.Join(a.results, bad), 2)
			state := pollingState(t, a)
			if mapValue(state["metrics"])["run_id"] != good {
				t.Fatalf("damaged run hid usable results: %#v", state)
			}
		})
	}
}

func TestStateDiscoveryCandidateLimit(t *testing.T) {
	// The limit applies to eligible run directories, before reading their data.
	// Keep the only usable run on each side of that boundary.
	for _, rank := range []int{199, 200, 201} {
		t.Run(fmt.Sprint(rank), func(t *testing.T) {
			a := newPollingApp(t)
			for i := 1; i <= 201; i++ {
				id := fmt.Sprintf("mt-candidate-%03d", i)
				path := filepath.Join(a.results, id)
				if i == rank {
					writeSavedRun(t, a.results, id, "write-verify", 10, 8, false)
				} else if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				setDiscoveryTime(t, path, 202-i)
				// Newer plain files with matching names must not consume the budget.
				if err := os.WriteFile(filepath.Join(a.results, fmt.Sprintf("mt-file-%03d", i)), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			metrics := a.latestSavedRun()
			if rank <= 200 {
				if metrics == nil || metrics["run_id"] != fmt.Sprintf("mt-candidate-%03d", rank) {
					t.Fatalf("in-budget run was missed: %#v", metrics)
				}
			} else if metrics != nil {
				t.Fatalf("discovery exceeded its candidate limit: %#v", metrics)
			}
		})
	}
}

func TestStateDiscoveryUnavailableInputs(t *testing.T) {
	for _, kind := range []string{"empty", "missing root", "root is file", "damaged latest state"} {
		t.Run(kind, func(t *testing.T) {
			a := newPollingApp(t)
			switch kind {
			case "missing root", "root is file":
				if err := os.Remove(a.results); err != nil {
					t.Fatal(err)
				}
				if kind == "root is file" {
					if err := os.WriteFile(a.results, []byte("unavailable"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "damaged latest state":
				if err := os.WriteFile(a.stateFile, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			state := pollingState(t, a)
			if state["job"] != nil || state["metrics"] != nil {
				t.Fatalf("unavailable input fabricated state: %#v", state)
			}
		})
	}
}

func TestStateOutputPhaseUsesNewestIntervalFile(t *testing.T) {
	for _, test := range []struct{ name, want string }{
		{"CREATE", "Create / write"}, {"write", "Create / write"}, {"VERIFY", "Verify"},
		{"read", "Verify"}, {"DELETE", "Cleanup"}, {"cleanup", "Cleanup"}, {"unrecognized", "SPT metrics"},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newPollingApp(t)
			dir := filepath.Join(a.results, pollingRunID, "nested")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			for i, path := range []string{
				filepath.Join(dir, "old-create.metrics.csv"),
				filepath.Join(dir, "current-"+test.name+".metrics.csv"),
				filepath.Join(dir, "newer-delete.metrics.total.csv"),
				filepath.Join(dir, "newest-cleanup.log"),
			} {
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
				setDiscoveryTime(t, path, i+1)
			}
			if got := a.phaseFromOutput(filepath.Dir(dir)); got != test.want {
				t.Fatalf("phase = %q, want %q", got, test.want)
			}
		})
	}
	for _, kind := range []string{"missing", "totals only", "empty"} {
		t.Run(kind, func(t *testing.T) {
			a := newPollingApp(t)
			dir := filepath.Join(a.results, pollingRunID)
			if kind != "missing" {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if kind == "totals only" {
					if err := os.WriteFile(filepath.Join(dir, "verify.metrics.total.csv"), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if got := a.phaseFromOutput(dir); got != "SPT setup" {
				t.Fatalf("phase = %q for %s", got, kind)
			}
		})
	}
}
