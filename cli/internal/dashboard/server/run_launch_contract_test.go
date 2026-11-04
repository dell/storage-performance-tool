package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

// Capture a real exec.Command boundary using only a finite local shell fixture.
// Each argument and environment value is NUL-delimited so spaces, punctuation,
// and empty values cannot hide argument splitting or altered values.
func newLaunchContractApp(t *testing.T) (*app, string, string, string) {
	t.Helper()
	a, marker, _ := prepareShutdownTest(t)
	a.endpointClient = &http.Client{Transport: &storageBoundaryTransport{}}
	a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
		t.Error("unexpected node verification in launch fixture")
		return &verifyapi.Report{}
	}
	a.root = filepath.Join(a.root, "work directory; literal")
	if err := os.Mkdir(a.root, 0700); err != nil {
		t.Fatal(err)
	}
	a.results = filepath.Join(a.root, "results with spaces")
	a.sptBin = filepath.Join(a.root, "spt executable")
	argsPath, envPath, cwdPath := filepath.Join(a.root, "args"), filepath.Join(a.root, "env"), filepath.Join(a.root, "cwd")
	t.Setenv("DASHBOARD_TEST_LAUNCH_ARGS", argsPath)
	t.Setenv("DASHBOARD_TEST_LAUNCH_ENV", envPath)
	t.Setenv("DASHBOARD_TEST_LAUNCH_CWD", cwdPath)
	var script strings.Builder
	script.WriteString("#!/bin/sh\nset -eu\nprintf '%s\\000' \"$@\" > \"$DASHBOARD_TEST_LAUNCH_ARGS\"\npwd > \"$DASHBOARD_TEST_LAUNCH_CWD\"\n{\n")
	for _, key := range []string{
		"S3_ENDPOINT", "S3_ENDPOINTS", "S3_BUCKET", "HOSTS", "THREADS", "SPT_SERVICE_THREADS",
		"SPT_ENGINE_OVERRIDES", "SPT_S3_DRIVER", "SPT_IMAGE", "SPT_SKIP_IMAGE_PULL",
		"SPT_OBJECT_DATA_COMPRESSIBILITY", "SPT_OBJECT_DATA_DEDUPABLE", "SPT_DEFER_VERIFICATION",
		"SPT_CHECKSUM", "AWS_EC2_METADATA_DISABLED", "S3_ACCESS_KEY", "S3_SECRET_KEY",
	} {
		script.WriteString("printf '%s\\000' '" + key + "' \"${" + key + "+x}\" \"${" + key + "-}\"\n")
	}
	script.WriteString("} > \"$DASHBOARD_TEST_LAUNCH_ENV\"\nexit 0\n")
	if err := os.WriteFile(a.sptBin, []byte(script.String()), 0700); err != nil {
		t.Fatal(err)
	}
	return a, marker, argsPath, envPath
}

func launchContractRequest(t *testing.T, a *app, opts options) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(opts)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(body))
	r.Host = "localhost:8000"
	r.Header.Set("Origin", "http://localhost:8000")
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	w := httptest.NewRecorder()
	a.httpServer().Handler.ServeHTTP(w, r)
	return w
}

func readLaunchFields(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || data[len(data)-1] != 0 {
		t.Fatalf("incomplete NUL-delimited capture in %s", path)
	}
	return strings.Split(string(data[:len(data)-1]), "\x00")
}

func TestS3LaunchContract(t *testing.T) {
	for _, tt := range []struct {
		name, endpoint string
		opts           options
	}{
		{"plain HTTP expert maximum total", "http://storage.example.invalid:8333/s3/$(id)", options{Mode: modeExpert, ObjectCount: 100, ObjectSize: "1GiB", Threads: 64, MaxRuntimeSeconds: 3600}},
		{"HTTPS guided", "https://storage.example.invalid:8333", options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60}},
		{"HTTP IPv6 expert minimums", "http://[::1]:8333", options{Mode: modeExpert, ObjectCount: 1, ObjectSize: "1KiB", Threads: 1, MaxRuntimeSeconds: 10}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker, argsPath, envPath := newLaunchContractApp(t)
			// Operator environment values must not override dashboard safety flags
			// or disagree with the validated target and workload passed in argv.
			for key, value := range map[string]string{
				"HOSTS": "--help", "THREADS": "999", "SPT_SERVICE_THREADS": "999",
				"SPT_ENGINE_OVERRIDES": "--unsafe-override", "SPT_S3_DRIVER": "unexpected",
				"SPT_IMAGE": "unexpected-image", "SPT_SKIP_IMAGE_PULL": "true",
				"SPT_OBJECT_DATA_COMPRESSIBILITY": "100", "SPT_OBJECT_DATA_DEDUPABLE": "false",
				"SPT_DEFER_VERIFICATION": "true", "SPT_CHECKSUM": "unexpected",
				"AWS_EC2_METADATA_DISABLED": "false",
				"S3_ACCESS_KEY":             "ACCESS_PRIVATE_CANARY", "S3_SECRET_KEY": "SECRET_PRIVATE_CANARY;$(id)",
			} {
				t.Setenv(key, value)
			}
			a.storage = storageConfig{Hosts: []string{"root@node-a", "node-b"}, Endpoint: tt.endpoint}
			a.verification = verification{Status: verifyPassed, Signature: configSignature(a.storage.Hosts, tt.endpoint)}
			w := launchContractRequest(t, a, tt.opts)
			if w.Code != http.StatusAccepted {
				t.Fatalf("launch = %d %s", w.Code, w.Body.String())
			}
			waitForRecoveryRun(t, a)
			var response struct {
				Job map[string]any `json:"job"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			runID := str(response.Job[keyRunID])
			if !runPattern.MatchString(runID) || !strings.HasPrefix(runID, "mt-dashboard-") {
				t.Fatalf("invalid generated run id: %q", runID)
			}
			runDir := filepath.Join(a.results, runID)
			wantFlags := map[string]string{
				"--headless": "", "--endpoints": tt.endpoint, "--bucket": "dashboard-test",
				"--prefix": "dashboard/" + runID + "/", "--object-count": strconv.Itoa(tt.opts.ObjectCount),
				"--object-size": tt.opts.ObjectSize, "--threads": strconv.Itoa(tt.opts.Threads),
				"--test-hosts": "root@node-a,node-b", "--service-threads": strconv.Itoa(tt.opts.Threads),
				"--auto-terminate-seconds": strconv.Itoa(tt.opts.MaxRuntimeSeconds), "--s3-driver": "default",
				"--auth-version": "4", "--object-data-compressibility": "0", "--object-data-dedupable": "true",
				"--cleanup": "", "--label": "dashboard", "--results-dir": runDir,
			}
			gotArgs := readLaunchFields(t, argsPath)
			if gotFlags := launchFlags(t, gotArgs); !reflect.DeepEqual(gotFlags, wantFlags) {
				t.Fatalf("child flags = %#v; want %#v", gotFlags, wantFlags)
			}
			fields := readLaunchFields(t, envPath)
			if len(fields)%3 != 0 {
				t.Fatal("incomplete environment capture")
			}
			gotEnv := map[string]string{}
			for i := 0; i < len(fields); i += 3 {
				if fields[i+1] != "x" {
					t.Errorf("child environment omitted %s; an explicit empty override is different from an unset variable", fields[i])
				}
				gotEnv[fields[i]] = fields[i+2]
			}
			wantEnv := map[string]string{
				"S3_ENDPOINT": tt.endpoint, "S3_ENDPOINTS": tt.endpoint, "S3_BUCKET": "dashboard-test",
				"HOSTS": "root@node-a,node-b", "THREADS": strconv.Itoa(tt.opts.Threads), "SPT_SERVICE_THREADS": strconv.Itoa(tt.opts.Threads),
				"SPT_ENGINE_OVERRIDES": "", "SPT_S3_DRIVER": "default", "SPT_IMAGE": "", "SPT_SKIP_IMAGE_PULL": "false",
				"SPT_OBJECT_DATA_COMPRESSIBILITY": "0", "SPT_OBJECT_DATA_DEDUPABLE": "true", "SPT_DEFER_VERIFICATION": "false",
				"SPT_CHECKSUM": "", "AWS_EC2_METADATA_DISABLED": "true",
				"S3_ACCESS_KEY": "ACCESS_PRIVATE_CANARY", "S3_SECRET_KEY": "SECRET_PRIVATE_CANARY;$(id)",
			}
			if !reflect.DeepEqual(gotEnv, wantEnv) {
				t.Fatal("child environment disagrees with validated workload or safety settings")
			}
			cwd, err := os.ReadFile(filepath.Join(a.root, "cwd"))
			if err != nil || strings.TrimSpace(string(cwd)) != a.root {
				t.Fatalf("child working directory = %q, %v; want %q", cwd, err, a.root)
			}
			for _, secret := range []string{"ACCESS_PRIVATE_CANARY", "SECRET_PRIVATE_CANARY"} {
				if strings.Contains(strings.Join(gotArgs, "\x00"), secret) || strings.Contains(w.Body.String(), secret) {
					t.Error("credentials escaped into argv or the public launch response")
				}
			}
			if a.activeJob[keyStatus] != jobCompleted || a.activeCmd != nil || a.activeLock != nil || a.lockBusy() {
				t.Fatal("finite capture child did not finish cleanly")
			}
			assertNotInvoked(t, marker) // Docker must never execute in this fixture.
		})
	}
}

// Argument order is incidental; option identity, uniqueness, and values are the
// contract. In particular, a value containing punctuation stays one argument.
func launchFlags(t *testing.T, args []string) map[string]string {
	t.Helper()
	if len(args) < 2 || args[0] != "run" || args[1] != "write-verify" {
		t.Fatalf("wrong child command: %#v", args)
	}
	flags := map[string]string{}
	for i := 2; i < len(args); i++ {
		key, value, assigned := strings.Cut(args[i], "=")
		if !strings.HasPrefix(key, "--") {
			t.Fatalf("unexpected positional argument: %q", args[i])
		}
		if _, duplicate := flags[key]; duplicate {
			t.Fatalf("duplicate child option %q", key)
		}
		if !assigned && key != "--headless" && key != "--cleanup" {
			i++
			if i >= len(args) {
				t.Fatalf("missing value for %q", key)
			}
			value = args[i]
		}
		flags[key] = value
	}
	return flags
}
