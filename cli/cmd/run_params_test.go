package cmd

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestFormatScenarioParams(t *testing.T) {
	tests := []struct {
		name     string
		params   scenario.ScenarioParams
		expected []string // Lines that must be present in output
	}{
		{
			name: "All parameters set for write workload",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://s3.example.com",
				Endpoints:    []string{"http://s3.example.com"},
				AccessKey:    "AKIAIOSFODNN7EXAMPLE",
				SecretKey:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Bucket:       "test-bucket",
				Threads:      16,
				ObjectSize:   "10MB",
				ObjectCount:  1000,
				Duration:     "5m",
				Cleanup:      true,
				KeepScenario: true,
			},
			expected: []string{
				"Workload Type: write",
				"Endpoint: http://s3.example.com",
				"Bucket: test-bucket",
				"Auth Version: 4",
				"Access Key: AKI***",
				"Secret Key: wJa***",
				"Threads: 16",
				"Object Size: 10MB",
				"Object Count: 1000",
				"Duration: 5m",
				"Cleanup: Yes (delete objects after creation)",
				"Keep Scenario: Yes",
			},
		},
		{
			name: "Minimal parameters for write workload",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "",
				Endpoints:    nil,
				AccessKey:    "",
				SecretKey:    "",
				Bucket:       "",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  0,
				Duration:     "",
				Cleanup:      false,
				KeepScenario: false,
			},
			expected: []string{
				"Workload Type: write",
				"Endpoint: (not set)",
				"Bucket: (not set)",
				"Auth Version: 4",
				"Access Key: (not set)",
				"Secret Key: (not set)",
				"Threads: 1",
				"Object Size: 1MB",
				"Object Count: (not set)",
				"Duration: (not set)",
				"Cleanup: No",
				"Keep Scenario: No",
			},
		},
		{
			name: "Multi-endpoint summary shows count",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "",
				Endpoints:    []string{"http://s3a:9000", "http://s3b:9000", "http://s3c:9000"},
				AccessKey:    "AKI",
				SecretKey:    "sec",
				Bucket:       "b",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  0,
				Duration:     "",
				Cleanup:      false,
				KeepScenario: false,
			},
			expected: []string{
				"Workload Type: write",
				"Endpoints: 3 total",
				"Bucket: b",
				"Auth Version: 4",
				"Access Key: ***",
				"Secret Key: ***",
			},
		},
		{
			name: "Mock workload type (no S3 parameters)",
			params: scenario.ScenarioParams{
				WorkloadType: "mock",
				Endpoint:     "", // Should be ignored for mock
				AccessKey:    "", // Should be ignored for mock
				SecretKey:    "", // Should be ignored for mock
				Bucket:       "", // Should be ignored for mock
				Threads:      8,
				ObjectSize:   "512KB",
				ObjectCount:  500,
				Duration:     "",
				Cleanup:      false,
				KeepScenario: true,
			},
			expected: []string{
				"Workload Type: mock",
				"Threads: 8",
				"Object Size: 512KB",
				"Object Count: 500",
				"Duration: (not set)",
				"Cleanup: No",
				"Keep Scenario: Yes",
			},
		},
		{
			name: "Short credentials (edge case)",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://localhost:9000",
				Endpoints:    []string{"http://localhost:9000"},
				AccessKey:    "AB",
				SecretKey:    "XY",
				Bucket:       "b",
				Threads:      2,
				ObjectSize:   "1KB",
				ObjectCount:  0,
				Duration:     "30s",
				Cleanup:      false,
				KeepScenario: false,
			},
			expected: []string{
				"Workload Type: write",
				"Endpoint: http://localhost:9000",
				"Bucket: b",
				"Auth Version: 4",
				"Access Key: ***", // Too short to show prefix
				"Secret Key: ***", // Too short to show prefix
				"Threads: 2",
				"Object Size: 1KB",
				"Object Count: (not set)",
				"Duration: 30s",
				"Cleanup: No",
				"Keep Scenario: No",
			},
		},
		{
			name: "Duration only (no object count)",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "https://s3.amazonaws.com",
				Endpoints:    []string{"https://s3.amazonaws.com"},
				AccessKey:    "test",
				SecretKey:    "secret",
				Bucket:       "production",
				Threads:      4,
				ObjectSize:   "100MB",
				ObjectCount:  0,
				Duration:     "1h",
				Cleanup:      true,
				KeepScenario: false,
			},
			expected: []string{
				"Workload Type: write",
				"Endpoint: https://s3.amazonaws.com",
				"Bucket: production",
				"Auth Version: 4",
				"Access Key: tes***",
				"Secret Key: sec***",
				"Threads: 4",
				"Object Size: 100MB",
				"Object Count: (not set)",
				"Duration: 1h",
				"Cleanup: Yes (delete objects after creation)",
				"Keep Scenario: No",
			},
		},
		{
			name: "Object count only (no duration)",
			params: scenario.ScenarioParams{
				WorkloadType: "read",
				Endpoint:     "http://minio:9000",
				Endpoints:    []string{"http://minio:9000"},
				AccessKey:    "minioadmin",
				SecretKey:    "minioadmin",
				Bucket:       "test",
				Threads:      32,
				ObjectSize:   "64KB",
				ObjectCount:  10000,
				Duration:     "",
				Cleanup:      false,
				KeepScenario: true,
			},
			expected: []string{
				"Workload Type: read",
				"Endpoint: http://minio:9000",
				"Bucket: test",
				"Auth Version: 4",
				"Access Key: min***",
				"Secret Key: min***",
				"Threads: 32",
				"Object Size: 64KB",
				"Object Count: 10000",
				"Duration: (not set)",
				"Cleanup: No",
				"Keep Scenario: Yes",
			},
		},
		{
			name: "List workload summarizes prefix",
			params: scenario.ScenarioParams{
				WorkloadType: "list",
				Endpoint:     "http://s3.example.com",
				Endpoints:    []string{"http://s3.example.com"},
				AccessKey:    "access123",
				SecretKey:    "secret456",
				Bucket:       "reports",
				Prefix:       "projects/",
				Threads:      4,
				ObjectSize:   "",
				ObjectCount:  0,
				Duration:     "",
				Cleanup:      false,
				KeepScenario: false,
			},
			expected: []string{
				"Workload Type: list",
				"Endpoint: http://s3.example.com",
				"Bucket: reports",
				"Prefix: projects/",
				"Auth Version: 4",
				"Object Size: (not applicable)",
				"Cleanup: No",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := formatScenarioParams(tt.params)

			// Check that all expected lines are present
			for _, expectedLine := range tt.expected {
				if !strings.Contains(output, expectedLine) {
					t.Errorf("Expected line not found in output:\nExpected: %s\nFull output:\n%s", expectedLine, output)
				}
			}

			// For mock workload, ensure S3 parameters are NOT in output
			if tt.params.WorkloadType == "mock" {
				unexpectedLines := []string{"Endpoint:", "Bucket:", "Access Key:", "Secret Key:"}
				for _, unexpectedLine := range unexpectedLines {
					if strings.Contains(output, unexpectedLine) {
						t.Errorf("Mock workload should not contain S3 parameter: %s\nFull output:\n%s", unexpectedLine, output)
					}
				}
			}

			// Verify the output is properly formatted (each parameter on its own line)
			lines := strings.Split(output, "\n")
			if len(lines) == 0 {
				t.Error("Output should contain multiple lines")
			}

			// For non-mock workloads, count expected number of lines
			// Note: strings.Split includes an empty string after the last \n
			if tt.params.WorkloadType != "mock" {
				expectedLineCount := 12 // Baseline for non-mock workloads (includes auth version)
				if tt.params.WorkloadType == WorkloadTypeList {
					expectedLineCount++ // Prefix line for list workloads
				}
				if len(lines) != expectedLineCount {
					t.Errorf("Expected %d lines for non-mock workload, got %d\nOutput:\n%s", expectedLineCount, len(lines), output)
				}
			} else {
				expectedLineCount := 7 // Fewer parameters for mock (6 lines + 1 empty)
				if len(lines) != expectedLineCount {
					t.Errorf("Expected %d lines for mock workload, got %d\nOutput:\n%s", expectedLineCount, len(lines), output)
				}
			}
		})
	}
}
