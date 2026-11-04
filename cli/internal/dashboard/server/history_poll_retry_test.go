package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHistorySnapshotRetriesWhileLiveMetricsUnavailable(t *testing.T) {
	for _, path := range []string{"state request", "background collector"} {
		t.Run(path, func(t *testing.T) {
			a := newHistoryApp(t, t.TempDir())
			if err := os.Mkdir(a.stateDir, 0700); err != nil {
				t.Fatal(err)
			}
			lock, err := a.lockRun()
			if err != nil {
				t.Fatal(err)
			}
			defer unlockClose(lock)
			a.activeJob = map[string]any{
				keyRunID: snapshotRunID, keyStatus: jobRunning,
				"started_epoch": float64(time.Now().Unix()),
				"results_dir":   filepath.Join(a.results, snapshotRunID),
			}
			a.rememberLive(snapshotMetrics(snapshotSample(1)))
			blocked := filepath.Join(a.stateDir, snapshotRunID+".samples.tmp")
			if err := os.Mkdir(blocked, 0700); err != nil {
				t.Fatal(err)
			}
			a.rememberLive(snapshotMetrics(snapshotSample(2)))
			assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 1)
			if err := os.Remove(blocked); err != nil {
				t.Fatal(err)
			}

			polls := make(chan struct{}, 2)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				select {
				case polls <- struct{}{}:
				default:
				}
			}))
			defer srv.Close()
			a.metricsURL = srv.URL

			if path == "state request" {
				state, err := a.currentState()
				if err != nil {
					t.Fatal(err)
				}
				metrics := mapValue(state["metrics"])
				assertSnapshotIndices(t, metrics[keyChartSamples].([]map[string]any), 1, 2)
				select {
				case <-polls:
				default:
					t.Fatal("state request did not poll live metrics")
				}
			} else {
				done := make(chan struct{})
				a.processes.Add(1)
				go func() {
					defer close(done)
					a.collectLiveHistory(snapshotRunID)
				}()
				defer func() {
					a.mu.Lock()
					a.activeJob = nil
					a.mu.Unlock()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("history collector did not stop")
					}
				}()
				// The second HTTP request means the collector has already
				// processed its first failed poll.
				timeout := time.After(5 * time.Second)
				for range 2 {
					select {
					case <-polls:
					case <-timeout:
						t.Fatal("history collector did not poll live metrics twice")
					}
				}
			}

			assertSnapshotIndices(t, a.savedLiveSamples(snapshotRunID), 1, 2)
		})
	}
}
