/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestNewPortChecker(t *testing.T) {
	checker := NewPortChecker(constants.SptAPIPort)

	if checker.Port != constants.SptAPIPort {
		t.Errorf("Port = %v, want %v", checker.Port, constants.SptAPIPort)
	}
	if checker.Host != "localhost" {
		t.Errorf("Host = %v, want %v", checker.Host, "localhost")
	}
	if checker.ConnectTimeout != DefaultConnectTimeout {
		t.Errorf("ConnectTimeout = %v, want %v", checker.ConnectTimeout, DefaultConnectTimeout)
	}
	if checker.HTTPTimeout != DefaultHTTPTimeout {
		t.Errorf("HTTPTimeout = %v, want %v", checker.HTTPTimeout, DefaultHTTPTimeout)
	}
	if checker.RetryAttempts != DefaultRetryAttempts {
		t.Errorf("RetryAttempts = %v, want %v", checker.RetryAttempts, DefaultRetryAttempts)
	}
}

func TestNewPortCheckerWithOptions(t *testing.T) {
	connectTimeout := 1 * time.Second
	httpTimeout := 3 * time.Second
	retryAttempts := 5

	checker := NewPortCheckerWithOptions("8080", "example.com", connectTimeout, httpTimeout, retryAttempts)

	if checker.Port != "8080" {
		t.Errorf("Port = %v, want %v", checker.Port, "8080")
	}
	if checker.Host != "example.com" {
		t.Errorf("Host = %v, want %v", checker.Host, "example.com")
	}
	if checker.ConnectTimeout != connectTimeout {
		t.Errorf("ConnectTimeout = %v, want %v", checker.ConnectTimeout, connectTimeout)
	}
	if checker.HTTPTimeout != httpTimeout {
		t.Errorf("HTTPTimeout = %v, want %v", checker.HTTPTimeout, httpTimeout)
	}
	if checker.RetryAttempts != retryAttempts {
		t.Errorf("RetryAttempts = %v, want %v", checker.RetryAttempts, retryAttempts)
	}
}

func TestPortChecker_IsPortInUse(t *testing.T) {
	// Start a test server to occupy a port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test listener: %v", err)
	}
	defer func() { _ = listener.Close() }()

	// Extract the port number
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to extract port: %v", err)
	}

	t.Run("port in use", func(t *testing.T) {
		checker := NewPortCheckerWithOptions(
			port,
			"127.0.0.1",
			DefaultConnectTimeout,
			DefaultHTTPTimeout,
			DefaultRetryAttempts,
		)
		if !checker.IsPortInUse() {
			t.Errorf("IsPortInUse() = false, want true (port should be in use)")
		}
	})

	t.Run("port available", func(t *testing.T) {
		// Use a port that should be available
		checker := NewPortChecker("0") // Port 0 should not be in use
		if checker.IsPortInUse() {
			t.Errorf("IsPortInUse() = true, want false (port 0 should be available)")
		}
	})

	t.Run("invalid host", func(t *testing.T) {
		checker := NewPortCheckerWithOptions(constants.SptAPIPort, "invalid-host-that-should-not-exist", 1*time.Second, 1*time.Second, 1)
		// Should return false (not in use) because connection fails
		if checker.IsPortInUse() {
			t.Errorf("IsPortInUse() = true, want false (invalid host should fail)")
		}
	})
}

func TestPortChecker_CheckService(t *testing.T) {
	t.Run("port not in use", func(t *testing.T) {
		checker := NewPortChecker("0") // Port 0 should not be in use
		ctx := context.Background()

		info, err := checker.CheckService(ctx)
		if err != nil {
			t.Errorf("CheckService() error = %v, want nil", err)
		}
		if info.IsListening {
			t.Errorf("IsListening = true, want false")
		}
		if info.IsSpt {
			t.Errorf("IsSpt = true, want false")
		}
	})

	t.Run("port in use - non-HTTP service", func(t *testing.T) {
		// Create a non-HTTP TCP server
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to create test listener: %v", err)
		}
		defer func() { _ = listener.Close() }()

		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		checker := NewPortCheckerWithOptions(port, "127.0.0.1", 100*time.Millisecond, 150*time.Millisecond, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		info, err := checker.CheckService(ctx)
		if err != nil {
			t.Errorf("CheckService() error = %v, want nil", err)
		}
		if !info.IsListening {
			t.Errorf("IsListening = false, want true")
		}
		if info.IsSpt {
			t.Errorf("IsSpt = true, want false (non-HTTP service)")
		}
	})

	t.Run("port in use - HTTP service (non-Spt)", func(t *testing.T) {
		// Create a test HTTP server that doesn't look like Spt
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Only respond to root path, not Spt endpoints
			if r.URL.Path == "/" {
				w.WriteHeader(http.StatusOK)
				if _, err := fmt.Fprintln(w, "Hello World"); err != nil {
					t.Fatalf("write failed: %v", err)
				}
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		// Extract port from server URL
		_, port, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		checker := NewPortCheckerWithOptions(port, "127.0.0.1", 100*time.Millisecond, 150*time.Millisecond, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		info, err := checker.CheckService(ctx)
		if err != nil {
			t.Errorf("CheckService() error = %v, want nil", err)
		}
		if !info.IsListening {
			t.Errorf("IsListening = false, want true")
		}
		// Should not be detected as Spt since it doesn't match Spt patterns
		if info.IsSpt {
			t.Errorf("IsSpt = true, want false (generic HTTP service)")
		}
	})
}

func TestPortChecker_detectSptService(t *testing.T) {
	t.Run("spt-like responses", func(t *testing.T) {
		// Create a test server that mimics actual Spt responses
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/metrics/json", "/metrics":
				w.WriteHeader(http.StatusOK)
				// Provide a realistic Spt metrics JSON response
				sptJSON := `{
					"lastUpdateTimeISO": "2025-08-14T15:30:00.000Z",
					"opType": "create",
					"opsPerSec": 150.5,
					"meanLatencyMicros": 25.7,
					"concurrency": 8.0,
					"successCount": 1000,
					"failCount": 2,
					"bytesCount": 1048576
				}`
				if _, err := fmt.Fprintln(w, sptJSON); err != nil {
					t.Fatalf("write failed: %v", err)
				}
			case "/run":
				w.WriteHeader(http.StatusNotFound) // No test running
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		_, port, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		checker := NewPortCheckerWithOptions(port, "127.0.0.1", 100*time.Millisecond, 150*time.Millisecond, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		status, err := checker.detectSptService(ctx)
		if err != nil {
			t.Errorf("detectSptService() error = %v, want nil", err)
		}
		if status == nil {
			t.Errorf("detectSptService() status = nil, want non-nil")
		}
	})

	t.Run("non-spt service", func(t *testing.T) {
		// Create a test server that doesn't look like Spt
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		_, port, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		checker := NewPortCheckerWithOptions(port, "127.0.0.1", 100*time.Millisecond, 150*time.Millisecond, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		status, err := checker.detectSptService(ctx)
		if err == nil {
			t.Errorf("detectSptService() error = nil, want error")
		}
		if status != nil {
			t.Errorf("detectSptService() status = %v, want nil", status)
		}
	})
}

func TestPortChecker_validateSptMetricsResponse(t *testing.T) {
	checker := NewPortChecker(constants.SptAPIPort)

	tests := []struct {
		name     string
		jsonBody string
		expected bool
	}{
		{
			name: "valid spt metrics JSON",
			jsonBody: `{
				"lastUpdateTimeISO": "2025-08-14T15:30:00.000Z",
				"opType": "create",
				"opsPerSec": 150.5,
				"meanLatencyMicros": 25.7,
				"concurrency": 8.0
			}`,
			expected: true,
		},
		{
			name: "spt metrics with alternative fields",
			jsonBody: `{
				"successCount": 1000,
				"failCount": 2,
				"concurrency": 8.0,
				"bytesCount": 1048576
			}`,
			expected: true,
		},
		{
			name: "minimal spt metrics (just enough fields)",
			jsonBody: `{
				"opType": "create",
				"opsPerSec": 150.5
			}`,
			expected: true,
		},
		{
			name: "not enough spt fields",
			jsonBody: `{
				"opType": "create"
			}`,
			expected: false,
		},
		{
			name: "generic JSON (not spt)",
			jsonBody: `{
				"status": "ok",
				"message": "hello world",
				"data": {}
			}`,
			expected: false,
		},
		{
			name:     "invalid JSON",
			jsonBody: `{"invalid": json}`,
			expected: false,
		},
		{
			name:     "empty JSON",
			jsonBody: `{}`,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP response
			resp := &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(tt.jsonBody)),
			}

			result := checker.validateSptMetricsResponse(resp)
			if result != tt.expected {
				t.Errorf("validateSptMetricsResponse() = %v, want %v for JSON: %s", result, tt.expected, tt.jsonBody)
			}
		})
	}
}

func TestPortChecker_validateSptRunResponse(t *testing.T) {
	checker := NewPortChecker(constants.SptAPIPort)

	tests := []struct {
		name     string
		jsonBody string
		expected bool
	}{
		{
			name: "valid spt run response",
			jsonBody: `{
				"runId": "test-run-123",
				"status": "RUNNING",
				"startTimeISO": "2025-08-14T15:30:00Z"
			}`,
			expected: true,
		},
		{
			name: "spt run with alternative fields",
			jsonBody: `{
				"runId": "test-456", 
				"startTime": "2025-08-14T15:30:00Z"
			}`,
			expected: true,
		},
		{
			name: "incomplete spt run info",
			jsonBody: `{
				"runId": "test-789"
			}`,
			expected: false,
		},
		{
			name: "generic JSON response",
			jsonBody: `{
				"message": "Hello World",
				"status": "ok"
			}`,
			expected: false,
		},
		{
			name:     "invalid JSON",
			jsonBody: `{"invalid": response}`,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(tt.jsonBody)),
			}

			result := checker.validateSptRunResponse(resp)
			if result != tt.expected {
				t.Errorf("validateSptRunResponse() = %v, want %v for JSON: %s", result, tt.expected, tt.jsonBody)
			}
		})
	}
}

func TestPortChecker_isSptResponse(t *testing.T) {
	checker := NewPortChecker(constants.SptAPIPort)

	tests := []struct {
		name       string
		endpoint   string
		statusCode int
		expected   bool
	}{
		{"/metrics with 200", "/metrics", http.StatusOK, true},
		{"/metrics with 404", "/metrics", http.StatusNotFound, false},
		{"/run with 200", "/run", http.StatusOK, true},
		{"/run with 404", "/run", http.StatusNotFound, true}, // 404 is valid for no test running
		{"/run with 500", "/run", http.StatusInternalServerError, false},
		{"/ with 200", "/", http.StatusOK, false},       // Changed to be more conservative
		{"/ with 404", "/", http.StatusNotFound, false}, // Changed to be more conservative
		{"/ with 500", "/", http.StatusInternalServerError, false},
		{"/unknown with 200", "/unknown", http.StatusOK, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock response
			resp := &http.Response{
				StatusCode: tt.statusCode,
			}

			result := checker.isSptResponse(resp, tt.endpoint)
			if result != tt.expected {
				t.Errorf("isSptResponse(%s, %d) = %v, want %v", tt.endpoint, tt.statusCode, result, tt.expected)
			}
		})
	}
}

func TestValidatePort(t *testing.T) {
	tests := []struct {
		name      string
		port      string
		wantError bool
	}{
		{"valid port", constants.SptAPIPort, false},
		{"valid port with whitespace", "  8080  ", false},
		{"port 80", "80", false},
		{"port 0", "0", false},
		{"empty string", "", true},
		{"only whitespace", "   ", true},
		{"negative port", "-1", true},
		{"port too high", "65536", true},
		{"non-numeric", "abc", true},
		{"port with colon", ":9999", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePort(tt.port)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidatePort(%q) error = %v, wantError %v", tt.port, err, tt.wantError)
			}
		})
	}
}

func TestPortChecker_Close(t *testing.T) {
	checker := NewPortChecker(constants.SptAPIPort)
	err := checker.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestServiceInfo_GetServiceDescription_Integration(t *testing.T) {
	// This test ensures the port checker properly populates ServiceInfo
	// Create a simple HTTP server that doesn't look like Spt
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to extract port: %v", err)
	}

	checker := NewPortCheckerWithOptions(port, "127.0.0.1", DefaultConnectTimeout, DefaultHTTPTimeout, DefaultRetryAttempts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	info, err := checker.CheckService(ctx)
	if err != nil {
		t.Errorf("CheckService() error = %v, want nil", err)
	}

	description := info.GetServiceDescription()
	if description == "" {
		t.Errorf("GetServiceDescription() = empty string, want non-empty description")
	}

	// Should detect that it's listening
	if !info.IsListening {
		t.Errorf("IsListening = false, want true")
	}
}

// Tests for enhanced detection functionality
func TestPortChecker_EnhancedDetection(t *testing.T) {
	t.Run("empty array metrics response", func(t *testing.T) {
		checker := NewPortChecker(constants.SptAPIPort)

		// Create response with empty array
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("[]")),
		}

		result := checker.validateSptMetricsResponse(resp)
		if !result {
			t.Errorf("validateSptMetricsResponse([]) = false, want true")
		}
	})

	t.Run("non-empty array metrics response", func(t *testing.T) {
		checker := NewPortChecker(constants.SptAPIPort)

		// Create response with array containing Spt fields
		metricsArray := `[{
			"lastUpdateTimeISO": "2025-08-14T15:30:00.000Z",
			"opType": "create",
			"opsPerSec": 150.5,
			"meanLatencyMicros": 25.7
		}]`

		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(metricsArray)),
		}

		result := checker.validateSptMetricsResponse(resp)
		if !result {
			t.Errorf("validateSptMetricsResponse(spt array) = false, want true")
		}
	})

	t.Run("spt fields validation", func(t *testing.T) {
		checker := NewPortChecker(constants.SptAPIPort)

		tests := []struct {
			name     string
			fields   map[string]interface{}
			expected bool
		}{
			{
				name: "sufficient spt fields",
				fields: map[string]interface{}{
					"opType":            "create",
					"opsPerSec":         150.5,
					"meanLatencyMicros": 25.7,
				},
				expected: true,
			},
			{
				name: "insufficient spt fields",
				fields: map[string]interface{}{
					"opType": "create",
				},
				expected: false,
			},
			{
				name: "alternative indicators sufficient",
				fields: map[string]interface{}{
					"concurrency":  8.0,
					"successCount": 1000,
					"failCount":    2,
				},
				expected: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := checker.validateSptFields(tt.fields)
				if result != tt.expected {
					t.Errorf("validateSptFields() = %v, want %v", result, tt.expected)
				}
			})
		}
	})

	t.Run("spt bad request error detection", func(t *testing.T) {
		checker := NewPortChecker(constants.SptAPIPort)

		tests := []struct {
			name     string
			body     string
			expected bool
		}{
			{
				name: "missing if-match header error",
				body: `<html>
<head>
<title>Error 400 Missing header: If-Match</title>
</head>
<body><h2>HTTP ERROR 400 Missing header: If-Match</h2>
<servlet>com.dell.spt.base.control.run.RunServlet-1869f114</servlet>
</body>
</html>`,
				expected: true,
			},
			{
				name:     "jetty powered error",
				body:     `<html><body><h1>Error</h1><hr/><a href="https://jetty.org/">Powered by Jetty:// 9.4.56.v20240826</a><hr/></body></html>`,
				expected: true,
			},
			{
				name:     "generic 400 error",
				body:     `{"error": "Bad Request", "message": "Invalid parameter"}`,
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp := &http.Response{
					StatusCode: 400,
					Body:       io.NopCloser(strings.NewReader(tt.body)),
				}

				result := checker.isSptBadRequestError(resp)
				if result != tt.expected {
					t.Errorf("isSptBadRequestError() = %v, want %v", result, tt.expected)
				}
			})
		}
	})
}

func TestPortChecker_DockerFallbackIntegration(t *testing.T) {
	t.Run("HTTP fails but Docker succeeds", func(t *testing.T) {
		// Skip if Docker not available
		dockerFinder, err := NewDockerContainerFinder()
		if err != nil {
			t.Skipf("Docker not available: %v", err)
		}
		defer func() { _ = dockerFinder.Close() }()

		// Create a non-HTTP server (just TCP listener)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to create test listener: %v", err)
		}
		defer func() { _ = listener.Close() }()

		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		checker := NewPortCheckerWithOptions(port, "127.0.0.1", DefaultConnectTimeout, DefaultHTTPTimeout, DefaultRetryAttempts)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		info, err := checker.CheckService(ctx)
		if err != nil {
			t.Errorf("CheckService() error = %v, want nil", err)
		}

		// Should detect port is in use
		if !info.IsListening {
			t.Errorf("IsListening = false, want true")
		}

		// HTTP detection should fail, but this test doesn't guarantee Docker will find anything
		// This is more of a regression test to ensure the code path works
	})
}

func TestPortChecker_EnhancedRunEndpointDetection(t *testing.T) {
	t.Run("run endpoint with If-Match header", func(t *testing.T) {
		headerReceived := false
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/run" {
				if r.Header.Get("If-Match") == "*" {
					headerReceived = true
					w.WriteHeader(http.StatusNotFound) // No test running
				} else {
					w.WriteHeader(http.StatusBadRequest)
					if _, err := fmt.Fprintln(w, "Missing header: If-Match"); err != nil {
						t.Fatalf("write failed: %v", err)
					}
				}
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		_, port, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		checker := NewPortCheckerWithOptions(port, "127.0.0.1", 100*time.Millisecond, 150*time.Millisecond, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		info, err := checker.CheckService(ctx)
		if err != nil {
			t.Errorf("CheckService() error = %v, want nil", err)
		}

		if !headerReceived {
			t.Errorf("If-Match header was not sent to /run endpoint")
		}

		// Header was sent correctly, but 404 alone isn't enough to confirm Spt
		// This is expected behavior - we need more evidence than just 404
		// The test verifies the If-Match header was sent properly
		if info.IsSpt {
			t.Errorf("IsSpt = true, want false (404 alone isn't sufficient evidence)")
		}
	})

	t.Run("run endpoint status codes", func(t *testing.T) {
		tests := []struct {
			name           string
			statusCode     int
			responseBody   string
			expectDetected bool
		}{
			{
				name:           "404 no test running",
				statusCode:     404,
				responseBody:   "",
				expectDetected: false, // Changed to be more conservative - 404 alone isn't sufficient evidence
			},
			{
				name:           "400 missing header",
				statusCode:     400,
				responseBody:   "Missing header: If-Match",
				expectDetected: true,
			},
			{
				name:           "500 server error",
				statusCode:     500,
				responseBody:   "Internal Server Error",
				expectDetected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/run" {
						w.WriteHeader(tt.statusCode)
						if _, err := fmt.Fprintln(w, tt.responseBody); err != nil {
							t.Fatalf("write failed: %v", err)
						}
					} else {
						w.WriteHeader(http.StatusNotFound)
					}
				}))
				defer server.Close()

				_, port, err := net.SplitHostPort(server.Listener.Addr().String())
				if err != nil {
					t.Fatalf("Failed to extract port: %v", err)
				}

				checker := NewPortCheckerWithOptions(port, "127.0.0.1", 100*time.Millisecond, 500*time.Millisecond, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				info, err := checker.CheckService(ctx)
				if err != nil {
					t.Errorf("CheckService() error = %v, want nil", err)
				}

				if info.IsSpt != tt.expectDetected {
					t.Errorf("IsSpt = %v, want %v for status %d", info.IsSpt, tt.expectDetected, tt.statusCode)
				}
			})
		}
	})
}
