package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvironmentCredentialsStayOutOfPublicState(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			a, marker := newRunHandlerTestApp(t)
			a.home = filepath.Join(a.root, "home")
			for _, key := range []string{"S3_ENDPOINTS", "S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"} {
				unsetEnvFixture(t, key)
			}
			endpoint := scheme + "://storage.example.invalid:8333"
			const homeSecret = "HOME_PRIVATE_CANARY_79db"
			const access = "ACCESS_PRIVATE_CANARY_5a31"
			const secret = "SECRET_PRIVATE_CANARY_b71a"
			const token = "TOKEN_PRIVATE_CANARY_97dc"
			const private = "INTERNAL_PRIVATE_CANARY_3f60"
			writeEnvFixture(t, a.home, "S3_ACCESS_KEY="+homeSecret+"\n")
			writeEnvFixture(t, a.root, "S3_ENDPOINTS="+endpoint+"\nS3_BUCKET=dashboard-env-test\nS3_ACCESS_KEY="+access+"\nS3_SECRET_KEY="+secret+"\nDASHBOARD_INTERNAL_SETTING="+private+"\n")
			t.Setenv("AWS_SESSION_TOKEN", token)
			// Internal process paths and arbitrary fields must stay private even
			// when present in a recovered job. Public options and outcome survive.
			a.activeJob = map[string]any{
				keyRunID: "mt-dashboard-env-public", keyStatus: jobCompleted, "returncode": 0,
				keyOptions: options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60},
				"pid":      12345, "results_dir": filepath.Join(a.results, private),
				"log_file": filepath.Join(a.stateDir, private+".log"), "S3_SECRET_KEY": secret,
			}
			request := httptest.NewRequest(http.MethodGet, "/api/state", nil)
			request.Host = "127.0.0.1:8000"
			recorder := httptest.NewRecorder()
			a.httpServer().Handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("state response status = %d, want 200", recorder.Code)
			}
			for _, value := range []string{homeSecret, access, secret, token, private, "S3_ACCESS_KEY", "S3_SECRET_KEY", "AWS_SESSION_TOKEN", "DASHBOARD_INTERNAL_SETTING"} {
				if strings.Contains(recorder.Body.String(), value) {
					t.Error("state response exposed a private configuration canary")
				}
			}
			var body struct {
				Target struct {
					Ready                 bool   `json:"ready"`
					Endpoint              string `json:"endpoint"`
					Bucket                string `json:"bucket"`
					CredentialsConfigured bool   `json:"credentials_configured"`
				} `json:"target"`
				Job map[string]any `json:"job"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if !body.Target.Ready || !body.Target.CredentialsConfigured || body.Target.Endpoint != endpoint || body.Target.Bucket != "dashboard-env-test" {
				t.Errorf("public target metadata lost the configured endpoint or readiness: %+v", body.Target)
			}
			if body.Job[keyRunID] != "mt-dashboard-env-public" || body.Job[keyStatus] != jobCompleted || body.Job["returncode"] != float64(0) {
				t.Error("public job lost its run identity or final outcome")
			}
			opts, ok := body.Job[keyOptions].(map[string]any)
			if !ok || opts["object_count"] != float64(10) || opts["object_size"] != "64KiB" {
				t.Error("public job lost its workload settings")
			}
			for _, key := range []string{"pid", "results_dir", "log_file", "S3_SECRET_KEY"} {
				if _, exists := body.Job[key]; exists {
					t.Errorf("internal job field %s was returned", key)
				}
			}
			if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Error("public state response is missing its cache or content-type protection")
			}
			assertNotInvoked(t, marker)
		})
	}
}

func TestPublicJSONEncodingFailureDoesNotExposePayload(t *testing.T) {
	const private = "ENCODING_PRIVATE_CANARY_018e"
	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusOK, map[string]any{"secret": private, "unsupported": make(chan int)})
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("encoding failure status = %d, want 500", recorder.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || body[keyError] != "Could not encode dashboard response." {
		t.Errorf("encoding failure did not produce the public error: %v", err)
	}
	if strings.Contains(recorder.Body.String(), private) {
		t.Error("encoding failure exposed private payload data")
	}
	if recorder.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") {
		t.Error("encoding failure was not returned as non-cacheable JSON")
	}
}
