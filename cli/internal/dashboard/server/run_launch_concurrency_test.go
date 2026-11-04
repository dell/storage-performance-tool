package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

func TestConcurrentLaunchRequestsStartOneChild(t *testing.T) {
	for _, instances := range []int{1, 2} {
		name := "one dashboard"
		if instances == 2 {
			name = "independent dashboards sharing the run lock"
		}
		t.Run(name, func(t *testing.T) {
			a, marker, _, _ := newLaunchContractApp(t)
			// Persist approval so a second independent app can load exactly the
			// same configuration while retaining its own mutex and process state.
			for path, value := range map[string]any{a.storageFile: a.storageSnapshot(), a.verifyFile: a.verification.clone()} {
				if err := writeFileAtomic(path, value); err != nil {
					t.Fatal(err)
				}
			}
			apps := []*app{a}
			if instances == 2 {
				b := reopenLaunchApp(a)
				apps = append(apps, b)
				t.Cleanup(b.stopProcesses)
			}
			launches := filepath.Join(a.root, "launches")
			releasePath := filepath.Join(a.root, "release-child")
			t.Setenv("DASHBOARD_TEST_LAUNCH_COUNT", launches)
			t.Setenv("DASHBOARD_TEST_LAUNCH_RELEASE", releasePath)
			// The child stays alive until all competing requests have returned.
			// This uses an explicit release condition, not a guessed sleep time.
			script := `#!/bin/sh
set -eu
trap 'exit 0' INT TERM
printf '%s\n' "$$" >> "$DASHBOARD_TEST_LAUNCH_COUNT"
printf ready > "$DASHBOARD_TEST_READY"
while [ ! -f "$DASHBOARD_TEST_LAUNCH_RELEASE" ]; do /bin/sleep 0.01; done
exit 0
`
			if err := os.WriteFile(a.sptBin, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			// The base fixture watches <root>/ready. Its original root preceded
			// the working-directory-with-spaces fixture, so update that marker.
			t.Setenv("DASHBOARD_TEST_READY", filepath.Join(a.root, "ready"))
			var once sync.Once
			release := func() {
				once.Do(func() {
					if err := os.WriteFile(releasePath, nil, 0600); err != nil {
						t.Errorf("release child: %v", err)
					}
				})
			}
			t.Cleanup(release) // Runs before either app's process cleanup, even on failure.
			var nodeCalls atomic.Int32
			transport := &storageBoundaryTransport{}
			handlers := make([]http.Handler, len(apps))
			for i, instance := range apps {
				instance.endpointClient = &http.Client{Transport: transport}
				instance.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
					nodeCalls.Add(1)
					return &verifyapi.Report{}
				}
				handlers[i] = instance.httpServer().Handler
			}
			const competitors = 8
			type response struct {
				index int
				w     *httptest.ResponseRecorder
			}
			start, responses := make(chan struct{}), make(chan response, competitors)
			opts := make([]options, competitors)
			for i := range opts {
				opts[i] = options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60}
				if i%2 != 0 {
					opts[i].ObjectCount, opts[i].Threads = 25, 2
				}
				body, err := json.Marshal(opts[i])
				if err != nil {
					t.Fatal(err)
				}
				r := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(body))
				r.Host = "localhost:8000"
				r.Header.Set("Origin", "http://localhost:8000")
				r.Header.Set("Content-Type", "application/json")
				go func(index int, r *http.Request) {
					<-start
					w := httptest.NewRecorder()
					handlers[index%len(apps)].ServeHTTP(w, r)
					responses <- response{index, w}
				}(i, r)
			}
			close(start)
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			accepted, conflicts, winner := 0, 0, -1
			var winningJob map[string]any
			for range competitors {
				select {
				case result := <-responses:
					switch result.w.Code {
					case http.StatusAccepted:
						accepted++
						winner = result.index
						var body struct {
							Job map[string]any `json:"job"`
						}
						if err := json.Unmarshal(result.w.Body.Bytes(), &body); err != nil {
							t.Error(err)
						}
						winningJob = body.Job
					case http.StatusConflict:
						conflicts++
						assertLaunchRejection(t, result.w, http.StatusConflict)
					default:
						t.Errorf("competing request = %d %s", result.w.Code, result.w.Body.String())
					}
				case <-deadline.C:
					t.Fatal("competing requests did not finish")
				}
			}
			if accepted != 1 || conflicts != competitors-1 {
				t.Fatalf("accepted=%d conflicts=%d; want 1 and %d", accepted, conflicts, competitors-1)
			}
			waitForChild(t, a.root)
			owner := apps[winner%len(apps)]
			runID := str(winningJob[keyRunID])
			body, err := json.Marshal(opts[winner])
			if err != nil {
				t.Fatal(err)
			}
			var expectedOptions map[string]any
			if err := json.Unmarshal(body, &expectedOptions); err != nil || !reflect.DeepEqual(winningJob[keyOptions], expectedOptions) {
				t.Fatalf("winning job did not preserve its request options: %#v", winningJob)
			}
			stateBefore, err := os.ReadFile(a.stateFile)
			if err != nil {
				t.Fatal(err)
			}
			if saved := a.readStateFile(); saved[keyRunID] != runID || saved[keyStatus] != jobRunning || !reflect.DeepEqual(saved[keyOptions], winningJob[keyOptions]) {
				t.Fatalf("rejected requests replaced the winning job: %#v", saved)
			}
			entries, err := os.ReadDir(a.results)
			if err != nil || len(entries) != 1 || entries[0].Name() != runID {
				t.Fatalf("duplicate result directories: %v, %v", entries, err)
			}
			data, err := os.ReadFile(launches)
			if err != nil || len(strings.Fields(string(data))) != 1 {
				t.Fatalf("expected exactly one child start, got %q, %v", data, err)
			}
			// Verification uses the same exclusive resource, including from a
			// second instance that does not have the child's in-memory job.
			for _, instance := range apps {
				assertLaunchRejection(t, verifyRequest(instance, `{"hosts":["node-c"],"endpoint":"http://changed.invalid"}`), http.StatusConflict)
			}
			after, err := os.ReadFile(a.stateFile)
			if err != nil || string(after) != string(stateBefore) {
				t.Fatal("overlapping verification changed the running job")
			}
			release()
			for _, instance := range apps {
				waitForRecoveryRun(t, instance)
			}
			if nodeCalls.Load() != 0 || transport.calls.Load() != 0 {
				t.Error("overlapping verification reached a node or endpoint")
			}
			if owner.activeJob[keyStatus] != jobCompleted || owner.activeCmd != nil || owner.activeLock != nil || a.lockBusy() {
				t.Fatal("winning child did not finish and release its lock")
			}
			// The losing instance must be able to retry once the winner exits.
			retry := apps[(winner+1)%len(apps)]
			w := launchContractRequest(t, retry, opts[winner])
			if w.Code != http.StatusAccepted {
				t.Fatalf("retry = %d %s", w.Code, w.Body.String())
			}
			waitForRecoveryRun(t, retry)
			if retry.activeJob[keyRunID] == runID || retry.activeJob[keyStatus] != jobCompleted || retry.lockBusy() {
				t.Fatal("retry did not create and complete a new run")
			}
			data, err = os.ReadFile(launches)
			if err != nil || len(strings.Fields(string(data))) != 2 {
				t.Fatalf("retry child start count = %q, %v; want two total", data, err)
			}
			assertNotInvoked(t, marker)
		})
	}
}
