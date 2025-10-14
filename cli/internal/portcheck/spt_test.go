/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewSptAPIClient(t *testing.T) {
	baseURL := "http://localhost:9999/"
	timeout := 5 * time.Second

	client := NewSptAPIClient(baseURL, timeout)

	if client.BaseURL != "http://localhost:9999" {
		t.Errorf("BaseURL = %v, want %v", client.BaseURL, "http://localhost:9999")
	}
	if client.HTTPClient.Timeout != timeout {
		t.Errorf("HTTPClient.Timeout = %v, want %v", client.HTTPClient.Timeout, timeout)
	}
}

func TestSptAPIClient_GetStatus(t *testing.T) {
	t.Run("successful status retrieval with run and metrics", func(t *testing.T) {
		// Mock server that returns both run info and metrics
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/run":
				runResponse := sptRunResponse{
					RunID:     "test-run-123",
					Status:    "RUNNING",
					StartTime: "2025-01-15T10:00:00Z",
				}
				if err := json.NewEncoder(w).Encode(runResponse); err != nil {
					t.Fatalf("encode failed: %v", err)
				}
			case "/metrics":
				metricsResponse := sptMetricsResponse{
					LastUpdateTimeISO: "2025-01-15T10:05:00Z",
					OpType:            "create",
					SuccessCount:      1000,
					FailCount:         5,
					OpsPerSec:         150.5,
					MeanLatency:       25.7,
					Concurrency:       8.0,
				}
				if err := json.NewEncoder(w).Encode(metricsResponse); err != nil {
					t.Fatalf("encode failed: %v", err)
				}
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		status, err := client.GetStatus(ctx)
		if err != nil {
			t.Errorf("GetStatus() error = %v, want nil", err)
		}

		if status.RunID != "test-run-123" {
			t.Errorf("RunID = %v, want %v", status.RunID, "test-run-123")
		}
		if status.State != "RUNNING" {
			t.Errorf("State = %v, want %v", status.State, "RUNNING")
		}
		if status.OpsPerSec != 150 {
			t.Errorf("OpsPerSec = %v, want %v", status.OpsPerSec, 150)
		}
		if status.CompletedItems != 1000 {
			t.Errorf("CompletedItems = %v, want %v", status.CompletedItems, 1000)
		}
		if status.Duration == 0 {
			t.Errorf("Duration = %v, want > 0", status.Duration)
		}
	})

	t.Run("run info only (no metrics)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/run":
				runResponse := sptRunResponse{
					RunID:  "test-run-456",
					Status: "COMPLETED",
				}
				if err := json.NewEncoder(w).Encode(runResponse); err != nil {
					t.Fatalf("encode failed: %v", err)
				}
			case "/metrics":
				w.WriteHeader(http.StatusNotFound)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		status, err := client.GetStatus(ctx)
		if err != nil {
			t.Errorf("GetStatus() error = %v, want nil", err)
		}

		if status.RunID != "test-run-456" {
			t.Errorf("RunID = %v, want %v", status.RunID, "test-run-456")
		}
		if status.State != "COMPLETED" {
			t.Errorf("State = %v, want %v", status.State, "COMPLETED")
		}
	})

	t.Run("no run info available", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		status, err := client.GetStatus(ctx)
		if err != nil {
			t.Errorf("GetStatus() error = %v, want nil", err)
		}

		// Should still return a status object, even if minimal
		if status == nil {
			t.Errorf("GetStatus() status = nil, want non-nil")
		}
	})
}

func TestSptAPIClient_getRunInfo(t *testing.T) {
	t.Run("successful run info", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/run" {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			runResponse := sptRunResponse{
				RunID:     "test-123",
				Status:    "RUNNING",
				StartTime: "2025-01-15T10:00:00Z",
				EndTime:   "",
			}
			if err := json.NewEncoder(w).Encode(runResponse); err != nil {
				t.Fatalf("encode failed: %v", err)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		runInfo, err := client.getRunInfo(ctx)
		if err != nil {
			t.Errorf("getRunInfo() error = %v, want nil", err)
		}
		if runInfo.RunID != "test-123" {
			t.Errorf("RunID = %v, want %v", runInfo.RunID, "test-123")
		}
		if runInfo.Status != "RUNNING" {
			t.Errorf("Status = %v, want %v", runInfo.Status, "RUNNING")
		}
	})

	t.Run("no test running (404)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		runInfo, err := client.getRunInfo(ctx)
		if err == nil {
			t.Errorf("getRunInfo() error = nil, want error (404)")
		}
		if runInfo != nil {
			t.Errorf("getRunInfo() runInfo = %v, want nil", runInfo)
		}
		if !strings.Contains(err.Error(), "no test currently running") {
			t.Errorf("error message = %v, want to contain 'no test currently running'", err.Error())
		}
	})

	t.Run("unexpected status code", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		runInfo, err := client.getRunInfo(ctx)
		if err == nil {
			t.Errorf("getRunInfo() error = nil, want error (500)")
		}
		if runInfo != nil {
			t.Errorf("getRunInfo() runInfo = %v, want nil", runInfo)
		}
	})

	t.Run("invalid JSON response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := fmt.Fprintln(w, "invalid json"); err != nil {
				t.Fatalf("write failed: %v", err)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		runInfo, err := client.getRunInfo(ctx)
		if err == nil {
			t.Errorf("getRunInfo() error = nil, want JSON parsing error")
		}
		if runInfo != nil {
			t.Errorf("getRunInfo() runInfo = %v, want nil", runInfo)
		}
	})
}

func TestSptAPIClient_getMetrics(t *testing.T) {
	t.Run("successful metrics", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/metrics" {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			metricsResponse := sptMetricsResponse{
				OpType:      "create",
				OpsPerSec:   200.5,
				MeanLatency: 15.3,
			}
			if err := json.NewEncoder(w).Encode(metricsResponse); err != nil {
				t.Fatalf("encode failed: %v", err)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		metrics, err := client.getMetrics(ctx)
		if err != nil {
			t.Errorf("getMetrics() error = %v, want nil", err)
		}
		if metrics.OpType != "create" {
			t.Errorf("OpType = %v, want %v", metrics.OpType, "create")
		}
		if metrics.OpsPerSec != 200.5 {
			t.Errorf("OpsPerSec = %v, want %v", metrics.OpsPerSec, 200.5)
		}
	})

	t.Run("metrics not available (404)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		metrics, err := client.getMetrics(ctx)
		if err == nil {
			t.Errorf("getMetrics() error = nil, want error (404)")
		}
		if metrics != nil {
			t.Errorf("getMetrics() metrics = %v, want nil", metrics)
		}
	})
}

func TestSptAPIClient_StopTest(t *testing.T) {
	t.Run("successful stop", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" || r.URL.Path != "/run" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		err := client.StopTest(ctx)
		if err != nil {
			t.Errorf("StopTest() error = %v, want nil", err)
		}
	})

	t.Run("no test running (404)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		err := client.StopTest(ctx)
		if err != nil {
			t.Errorf("StopTest() error = %v, want nil (404 should be treated as success)", err)
		}
	})

	t.Run("stop command failed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 5*time.Second)
		ctx := context.Background()

		err := client.StopTest(ctx)
		if err == nil {
			t.Errorf("StopTest() error = nil, want error (500)")
		}
	})
}

func TestIsSptAPI(t *testing.T) {
	t.Run("spt-like API (returns 200)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/run" {
				w.WriteHeader(http.StatusOK)
				if err := json.NewEncoder(w).Encode(map[string]string{"runId": "test"}); err != nil {
					t.Fatalf("encode failed: %v", err)
				}
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		ctx := context.Background()
		result := IsSptAPI(ctx, server.URL)
		if !result {
			t.Errorf("IsSptAPI() = false, want true")
		}
	})

	t.Run("spt-like API (returns 404 for no test)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		ctx := context.Background()
		result := IsSptAPI(ctx, server.URL)
		if !result {
			t.Errorf("IsSptAPI() = false, want true (404 is valid for Spt)")
		}
	})

	t.Run("non-spt API", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}))
		defer server.Close()

		ctx := context.Background()
		result := IsSptAPI(ctx, server.URL)
		if result {
			t.Errorf("IsSptAPI() = true, want false")
		}
	})

	t.Run("unreachable server", func(t *testing.T) {
		ctx := context.Background()
		result := IsSptAPI(ctx, "http://localhost:1") // Should be unreachable
		if result {
			t.Errorf("IsSptAPI() = true, want false (unreachable)")
		}
	})
}

func TestWaitForAPIReady(t *testing.T) {
	t.Run("API becomes ready immediately", func(t *testing.T) {
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
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 1*time.Second)
		ctx := context.Background()

		err := client.WaitForAPIReady(ctx, 2*time.Second)
		if err != nil {
			t.Errorf("WaitForAPIReady() error = %v, want nil", err)
		}
		if readyCalls == 0 {
			t.Error("expected readiness endpoint to be called")
		}
		if healthCalls != 1 {
			t.Errorf("expected one health probe, got %d", healthCalls)
		}
		if runHeadCalls != 0 {
			t.Errorf("expected no HEAD /run probes, got %d", runHeadCalls)
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
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		client := NewSptAPIClient(server.URL, 100*time.Millisecond)
		ctx := context.Background()

		start := time.Now()
		err := client.WaitForAPIReady(ctx, 200*time.Millisecond)
		duration := time.Since(start)

		if err == nil {
			t.Errorf("WaitForAPIReady() error = nil, want timeout error")
		}

		if healthCalls != 1 {
			t.Errorf("expected one health probe, got %d", healthCalls)
		}

		// Should timeout approximately after the specified duration (allow some variance for slow systems)
		if duration < 150*time.Millisecond {
			t.Errorf("WaitForAPIReady() returned too quickly: %v", duration)
		}
		if duration > 1*time.Second {
			t.Errorf("WaitForAPIReady() took too long: %v", duration)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		// Use an unreachable URL
		client := NewSptAPIClient("http://localhost:1", 100*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel the context after a short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := client.WaitForAPIReady(ctx, 5*time.Second)
		if err != context.Canceled {
			t.Errorf("WaitForAPIReady() error = %v, want context.Canceled", err)
		}
	})
}

func TestGetSptStatus_HelperFunction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			runResponse := sptRunResponse{
				RunID:  "helper-test",
				Status: "RUNNING",
			}
			json.NewEncoder(w).Encode(runResponse)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	status, err := GetSptStatus(ctx, server.URL)

	if err != nil {
		t.Errorf("GetSptStatus() error = %v, want nil", err)
	}
	if status.RunID != "helper-test" {
		t.Errorf("RunID = %v, want %v", status.RunID, "helper-test")
	}
}

func TestStopSptTest_HelperFunction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && r.URL.Path == "/run" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	err := StopSptTest(ctx, server.URL)

	if err != nil {
		t.Errorf("StopSptTest() error = %v, want nil", err)
	}
}
