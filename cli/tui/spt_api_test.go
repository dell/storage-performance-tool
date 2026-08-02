/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
