/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// TestSptAPIClient_WaitForAPIReady tests the API readiness check
func TestSptAPIClient_WaitForAPIReady(t *testing.T) {
	t.Run("API ready immediately", func(t *testing.T) {
		var readyCalls, healthCalls, runHeadCalls int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/ready":
				readyCalls++
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"worker","node_id":"node-1"}`))
			case "/health":
				healthCalls++
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"ok","scope":"node","role":"worker","node_id":"node-1"}`))
			case "/run":
				if r.Method == http.MethodHead {
					runHeadCalls++
					w.WriteHeader(http.StatusNoContent)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			default:
				t.Fatalf("unexpected path: %s", r.URL.Path)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL)
		err := client.WaitForAPIReady(2 * time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if readyCalls == 0 {
			t.Error("expected readiness endpoint to be called")
		}
		if healthCalls != 1 {
			t.Errorf("expected one health probe, got %d", healthCalls)
		}
		if runHeadCalls != 1 {
			t.Errorf("expected one HEAD /run probe, got %d", runHeadCalls)
		}
	})

	t.Run("API becomes ready after delay", func(t *testing.T) {
		var readyCalls, healthCalls, runHeadCalls int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/ready":
				readyCalls++
				if readyCalls < 3 {
					w.WriteHeader(http.StatusServiceUnavailable)
					w.Write([]byte(`{"ready":false,"status":"starting"}`))
					return
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"entry-0"}`))
			case "/health":
				healthCalls++
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"ok","scope":"node","role":"entry","node_id":"entry-0"}`))
			case "/run":
				if r.Method == http.MethodHead {
					runHeadCalls++
					w.WriteHeader(http.StatusNoContent)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			default:
				t.Fatalf("unexpected path: %s", r.URL.Path)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL)
		err := client.WaitForAPIReady(3 * time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if readyCalls < 3 {
			t.Errorf("expected multiple readiness attempts, got %d", readyCalls)
		}
		if healthCalls != 1 {
			t.Errorf("expected one health probe after readiness, got %d", healthCalls)
		}
		if runHeadCalls != 1 {
			t.Errorf("expected one HEAD /run probe, got %d", runHeadCalls)
		}
	})

	t.Run("API never becomes ready", func(t *testing.T) {
		var healthCalls int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/ready":
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"ready":false,"status":"starting"}`))
			case "/health":
				healthCalls++
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"booting","scope":"node","role":"worker","node_id":"node-1"}`))
			default:
				t.Fatalf("unexpected path: %s", r.URL.Path)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL)
		err := client.WaitForAPIReady(700 * time.Millisecond)
		if err == nil {
			t.Fatal("expected timeout error but got nil")
		}
		if healthCalls != 1 {
			t.Fatalf("expected one health probe, got %d", healthCalls)
		}
	})
}

func TestSptAPIClientWaitForAPIReadyContextCancelsPromptly(t *testing.T) {
	requestSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case requestSeen <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := NewSptAPIClient(server.URL)

	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		started := time.Now()
		err := client.WaitForAPIReadyContext(ctx, time.Second)
		if !errors.Is(err, context.Canceled) || time.Since(started) > 100*time.Millisecond {
			t.Fatalf("already-canceled readiness = %v after %s", err, time.Since(started))
		}
	})

	t.Run("mid poll", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- client.WaitForAPIReadyContext(ctx, 5*time.Second) }()
		select {
		case <-requestSeen:
		case <-time.After(time.Second):
			t.Fatal("readiness probe did not start")
		}
		started := time.Now()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || time.Since(started) > 100*time.Millisecond {
			t.Fatalf("mid-poll cancellation = %v after %s", err, time.Since(started))
		}
	})
}

// TestSptAPIClient_StartTest tests the test start functionality
func TestSptAPIClient_StartTest(t *testing.T) {
	tests := []struct {
		name        string
		scenario    []byte
		defaults    []byte
		serverFunc  func() *httptest.Server
		expectError bool
		expectedID  string
	}{
		{
			name:     "successful test start",
			scenario: []byte(`Load.config({}).run();`),
			defaults: []byte(`storage: {driver: {type: "dummy-mock"}}`),
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/run" && r.Method == "POST" {
						// Verify multipart form data
						err := r.ParseMultipartForm(10 << 20)
						if err != nil {
							t.Errorf("Failed to parse multipart form: %v", err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}

						// Check for scenario file
						_, _, err = r.FormFile("scenario")
						if err != nil {
							t.Error("Missing scenario file in request")
							w.WriteHeader(http.StatusBadRequest)
							return
						}

						// Check for defaults file (optional)
						_, _, _ = r.FormFile("defaults")

						// Return success with run ID in ETag header
						w.Header().Set("ETag", "test-run-123")
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(`{"runId": "test-run-123"}`))
					}
				}))
			},
			expectError: false,
			expectedID:  "test-run-123",
		},
		{
			name:     "test start with server error",
			scenario: []byte(`Load.config({}).run();`),
			defaults: nil,
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("Internal server error"))
				}))
			},
			expectError: true,
			expectedID:  "",
		},
		{
			name:     "retry on 405 then succeed",
			scenario: []byte(`Load.config({}).run();`),
			defaults: []byte(`storage: {driver: {type: "dummy-mock"}}`),
			serverFunc: func() *httptest.Server {
				calls := 0
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.URL.Path == "/run" && r.Method == http.MethodPost:
						calls++
						if calls < 3 {
							// First two attempts: 405 Method Not Allowed (race)
							w.WriteHeader(http.StatusMethodNotAllowed)
							w.Write([]byte("Default servlet 405"))
							return
						}
						// Third attempt: succeed
						w.Header().Set("ETag", "retry-run-789")
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(`{"runId": "retry-run-789"}`))
					default:
						w.WriteHeader(http.StatusNotFound)
					}
				}))
			},
			expectError: false,
			expectedID:  "retry-run-789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tt.serverFunc()
			defer server.Close()

			client := NewSptAPIClient(server.URL)
			runID, err := client.StartTest(tt.scenario, tt.defaults)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if runID != tt.expectedID {
				t.Errorf("Expected run ID %q, got %q", tt.expectedID, runID)
			}
		})
	}
}

// TestSptAPIClient_StopTest tests the test stop functionality
func TestSptAPIClient_StopTest(t *testing.T) {
	tests := []struct {
		name        string
		serverFunc  func() *httptest.Server
		expectError bool
	}{
		{
			name: "successful test stop",
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/run" && r.Method == "DELETE" {
						w.WriteHeader(http.StatusOK)
					}
				}))
			},
			expectError: false,
		},
		{
			name: "test stop with no active test",
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/run" && r.Method == "DELETE" {
						w.WriteHeader(http.StatusNotFound)
						w.Write([]byte("No active test"))
					}
				}))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tt.serverFunc()
			defer server.Close()

			client := NewSptAPIClient(server.URL)
			err := client.StopTest()

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestSptAPIClient_StartTestContextPostsExactRealGeneratedDefaults(t *testing.T) {
	const overrideSecret = "EXACT_POST_OVERRIDE_SECRET_c421"
	params := scenario.Params{
		WorkloadType: scenario.WorkloadTypeWriteVerify,
		Endpoint:     "http://s3.example:9000",
		AccessKey:    "EXACT_POST_ACCESS_7f3a",
		SecretKey:    "EXACT_POST_SECRET_91bc",
		Bucket:       "qualification",
		Threads:      1,
		EngineOverrides: []string{
			"storage.auth.secret=" + overrideSecret,
		},
	}
	defaults, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(defaults, []byte(overrideSecret)) {
		t.Fatal("real generated defaults do not contain the exact override credential")
	}
	var postedDefaults []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/run" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if parseErr := r.ParseMultipartForm(2 << 20); parseErr != nil {
			http.Error(w, parseErr.Error(), http.StatusBadRequest)
			return
		}
		file, _, fileErr := r.FormFile("defaults")
		if fileErr != nil {
			http.Error(w, fileErr.Error(), http.StatusBadRequest)
			return
		}
		defer func() { _ = file.Close() }()
		postedDefaults, fileErr = io.ReadAll(file)
		if fileErr != nil {
			http.Error(w, fileErr.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("ETag", "4242")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	outcome, err := NewSptAPIClient(server.URL).StartTestContext(
		context.Background(), []byte("Load.run({});\n"), defaults, 4242)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Submission != SubmissionSubmitted || outcome.RunID != "4242" {
		t.Fatalf("submission outcome = %+v", outcome)
	}
	if !bytes.Equal(postedDefaults, defaults) {
		t.Fatal("POST /run defaults differ from the exact real generated bytes")
	}
}

// TestSptAPIClient_GetStatus tests the status retrieval
func TestSptAPIClient_GetStatus(t *testing.T) {
	tests := []struct {
		name           string
		serverResponse string
		expectedStatus *TestStatus
		expectError    bool
	}{
		{
			name: "running test status",
			serverResponse: `{
				"state": "RUNNING",
				"message": "Test in progress",
				"startTime": "2025-01-01T10:00:00Z",
				"runId": "test-123"
			}`,
			expectedStatus: &TestStatus{
				State:   "RUNNING",
				Message: "Test in progress",
			},
			expectError: false,
		},
		{
			name: "completed test status",
			serverResponse: `{
				"state": "COMPLETED",
				"message": "Test finished successfully",
				"startTime": "2025-01-01T10:00:00Z",
				"endTime": "2025-01-01T10:05:00Z",
				"runId": "test-123"
			}`,
			expectedStatus: &TestStatus{
				State:   "COMPLETED",
				Message: "Test finished successfully",
			},
			expectError: false,
		},
		{
			name:           "invalid JSON response",
			serverResponse: `{invalid json`,
			expectedStatus: nil,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/status" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(tt.serverResponse))
				}
			}))
			defer server.Close()

			client := NewSptAPIClient(server.URL)
			status, err := client.GetStatus()

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && status != nil {
				if status.State != tt.expectedStatus.State {
					t.Errorf("Expected state %q, got %q", tt.expectedStatus.State, status.State)
				}
				if status.Message != tt.expectedStatus.Message {
					t.Errorf("Expected message %q, got %q", tt.expectedStatus.Message, status.Message)
				}
			}
		})
	}
}

func TestSptAPIClient_ShutdownAndLinger(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shutdown":
			w.WriteHeader(http.StatusAccepted)
		case "/status":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"COMPLETED"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	if err := client.Shutdown(); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if err := client.WaitForLinger(200 * time.Millisecond); err != nil {
		t.Fatalf("linger error: %v", err)
	}
}

func TestSptAPIClient_StartTestContextCancellationLeavesUnknownWithoutRetry(t *testing.T) {
	var postCalls atomic.Int32
	postStarted := make(chan struct{})
	postCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			_, _ = io.Copy(io.Discard, r.Body)
			postCalls.Add(1)
			if postCalls.Load() == 1 {
				close(postStarted)
			}
			<-r.Context().Done()
			if postCalls.Load() == 1 {
				close(postCanceled)
			}
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"IDLE","run_id":41}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.submissionReconcileTimeout = 40 * time.Millisecond
	client.submissionReconcilePollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		out StartTestOutcome
		err error
	}, 1)
	go func() {
		out, err := client.StartTestContext(ctx, []byte("scenario"), nil, 42)
		done <- struct {
			out StartTestOutcome
			err error
		}{out, err}
	}()
	<-postStarted
	cancel()

	result := <-done
	if result.out.Submission != SubmissionUnknown {
		t.Fatalf("submission = %s, want %s", result.out.Submission, SubmissionUnknown)
	}
	var ambiguous *AmbiguousSubmissionError
	if !errors.As(result.err, &ambiguous) {
		t.Fatalf("error = %v, want AmbiguousSubmissionError", result.err)
	}
	if calls := postCalls.Load(); calls != 1 {
		t.Fatalf("POST calls = %d, want exactly one", calls)
	}
	select {
	case <-postCanceled:
	case <-time.After(time.Second):
		t.Fatal("POST handler did not observe request cancellation")
	}
}

func TestSptAPIClient_StartTestContextReconcilesMatchingRunID(t *testing.T) {
	postStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			_, _ = io.Copy(io.Discard, r.Body)
			close(postStarted)
			<-r.Context().Done()
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"RUNNING","run_id":77}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.submissionReconcileTimeout = time.Second
	client.submissionReconcilePollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		out StartTestOutcome
		err error
	}, 1)
	go func() {
		out, err := client.StartTestContext(ctx, []byte("scenario"), nil, 77)
		done <- struct {
			out StartTestOutcome
			err error
		}{out, err}
	}()
	<-postStarted
	cancel()
	result := <-done
	if result.out.Submission != SubmissionSubmitted || result.out.RunID != "77" {
		t.Fatalf("outcome = %+v, want confirmed run 77", result.out)
	}
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("error = %v, want original context cancellation", result.err)
	}
}

func TestSptAPIClient_GetStatusParsesStatusServletPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":"RUNNING","run_id":77}`))
	}))
	defer server.Close()

	status, err := NewSptAPIClient(server.URL).GetStatusContext(context.Background())
	if err != nil {
		t.Fatalf("GetStatusContext() error = %v", err)
	}
	if status.State != "RUNNING" || status.RunID != "77" {
		t.Fatalf("status = %+v, want RUNNING run 77", status)
	}
}

func TestSptAPIClient_AmbiguousSubmissionRejectsUnattributedStatus(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "different active run", payload: `{"state":"RUNNING","run_id":78}`},
		{name: "missing run id", payload: `{"state":"RUNNING"}`},
		{name: "terminal state for another run", payload: `{"state":"COMPLETED","run_id":78}`},
		{name: "malformed json", payload: `{"state":`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/status" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.payload))
			}))
			defer server.Close()

			client := NewSptAPIClient(server.URL)
			client.SetRunID("77") // Cached display state is not submission evidence.
			client.submissionReconcileTimeout = 25 * time.Millisecond
			client.submissionReconcilePollInterval = 2 * time.Millisecond
			cause := errors.New("ambiguous POST")
			outcome, err := client.reconcileAmbiguousSubmission(context.Background(), 77, cause)
			if outcome.Submission != SubmissionUnknown || outcome.RunID != "" {
				t.Fatalf("outcome = %+v, want unresolved submission", outcome)
			}
			var ambiguous *AmbiguousSubmissionError
			if !errors.As(err, &ambiguous) || !errors.Is(err, cause) {
				t.Fatalf("error = %v, want ambiguous outcome retaining cause", err)
			}
		})
	}
}

func TestSptAPIClient_StartTestContextCancellationDuring405DelayDoesNotRetry(t *testing.T) {
	var postCalls atomic.Int32
	firstResponse := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/run" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		postCalls.Add(1)
		w.WriteHeader(http.StatusMethodNotAllowed)
		if postCalls.Load() == 1 {
			close(firstResponse)
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.startRetryDelay = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		out StartTestOutcome
		err error
	}, 1)
	go func() {
		out, err := client.StartTestContext(ctx, []byte("scenario"), nil, 88)
		done <- struct {
			out StartTestOutcome
			err error
		}{out, err}
	}()
	<-firstResponse
	time.Sleep(20 * time.Millisecond)
	cancel()
	result := <-done
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", result.err)
	}
	if result.out.Submission != SubmissionNotSubmitted {
		t.Fatalf("submission = %s, want %s", result.out.Submission, SubmissionNotSubmitted)
	}
	if calls := postCalls.Load(); calls != 1 {
		t.Fatalf("POST calls = %d, want exactly one", calls)
	}
}
