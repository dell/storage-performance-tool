package server

import (
	"net/http"
	"os"
	"reflect"
	"testing"
)

func TestRunInputRejectedBeforeLaunch(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*options)
	}{
		{"size option injection", func(o *options) { o.ObjectSize = "1MiB --threads 64" }},
		{"size command substitution", func(o *options) { o.ObjectSize = "$(id)MiB" }},
		{"size newline", func(o *options) { o.ObjectSize = "1MiB\n--help" }},
		{"size NUL", func(o *options) { o.ObjectSize = "1MiB\x00" }},
		{"size Unicode digit", func(o *options) { o.ObjectSize = "１MiB" }},
		{"size integer overflow", func(o *options) { o.ObjectSize = "9223372036854775808GiB" }},
		{"size largest canonical magnitude", func(o *options) { o.ObjectSize = "9999999GiB" }},
		{"negative count", func(o *options) { o.ObjectCount = -1 }},
		{"negative threads", func(o *options) { o.Threads = -1 }},
		{"negative runtime", func(o *options) { o.MaxRuntimeSeconds = -1 }},
		{"largest count", func(o *options) { o.ObjectCount = int(^uint(0) >> 1) }},
		{"largest threads", func(o *options) { o.Threads = int(^uint(0) >> 1) }},
		{"largest runtime", func(o *options) { o.MaxRuntimeSeconds = int(^uint(0) >> 1) }},
		{"total just above cap", func(o *options) { o.ObjectCount, o.ObjectSize = 65537, "1600KiB" }},
		{"mode option injection", func(o *options) { o.Mode = "expert --help" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker, argsPath, envPath := newLaunchContractApp(t)
			opts := options{Mode: modeExpert, ObjectCount: 65536, ObjectSize: "1600KiB", Threads: 1, MaxRuntimeSeconds: 10}
			// This is exactly 100 GiB; only the single changed field should
			// make the request invalid, including the total-data boundary.
			if err := a.validateOptions(opts); err != nil {
				t.Fatal(err)
			}
			tt.change(&opts)
			previous := map[string]any{keyRunID: "mt-dashboard-previous", keyStatus: jobCompleted, "returncode": 0}
			a.activeJob = cloneMap(previous)
			a.liveRunID, a.liveSignature = "mt-dashboard-previous", "previous-signature"
			a.liveSamples = []map[string]any{{keyCumulativeOps: float64(12)}}
			if err := a.writeState(previous); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(a.stateFile)
			if err != nil {
				t.Fatal(err)
			}
			w := launchContractRequest(t, a, opts)
			waitForRecoveryRun(t, a)
			assertLaunchRejection(t, w, http.StatusBadRequest)
			if !reflect.DeepEqual(a.activeJob, previous) || a.activeCmd != nil || a.activeLock != nil || a.lockBusy() {
				t.Error("invalid options replaced the previous job or retained a process/lock")
			}
			if a.liveRunID != "mt-dashboard-previous" || a.liveSignature != "previous-signature" || len(a.liveSamples) != 1 || a.liveSamples[0][keyCumulativeOps] != float64(12) {
				t.Error("invalid options changed previous live measurements")
			}
			after, err := os.ReadFile(a.stateFile)
			if err != nil || string(after) != string(before) {
				t.Error("invalid options changed persisted state")
			}
			for _, path := range []string{argsPath, envPath, a.results} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("rejected request created %s: %v", path, err)
				}
			}
			assertNotInvoked(t, marker)
		})
	}
}

// Environment-selected endpoints must pass the same validation as settings
// submitted through /api/storage/verify, even with a matching saved signature.
func TestRunRejectsMalformedEndpointBeforeLaunch(t *testing.T) {
	for _, endpoint := range []string{"http://storage.invalid:65536", "http://storage.invalid:", "http://::1", "http://storage..invalid"} {
		t.Run(endpoint, func(t *testing.T) {
			a, marker, argsPath, _ := newLaunchContractApp(t)
			t.Setenv("S3_ENDPOINTS", endpoint)
			a.verification.Signature = configSignature(a.storage.Hosts, endpoint)
			w := launchContractRequest(t, a, options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60})
			waitForRecoveryRun(t, a)
			assertRejectedRun(t, a, marker, w, http.StatusServiceUnavailable)
			if _, err := os.Stat(argsPath); !os.IsNotExist(err) {
				t.Fatal("invalid environment endpoint reached the child process")
			}
		})
	}
}
