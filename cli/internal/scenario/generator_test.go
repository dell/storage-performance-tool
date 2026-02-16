package scenario

import (
	"regexp"
	"strings"
	"testing"
)

// removed unused helper 'min' (no longer needed)

func TestGenerateWriteScenario(t *testing.T) {
	tests := []struct {
		name       string
		params     Params
		wantErr    bool
		checkLines []string // Key lines that should be present in the output
	}{
		{
			name: "basic write scenario with count limit",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testaccess",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  1000,
			},
			wantErr: false,
			checkLines: []string{
				`var concurrency = 4`,
				`var itemSize = "1MB"`,
				`var outputPath = "/testbucket"`,
				`var itemCount = 1000`,
				`"type": "s3"`,
				`"type": "create"`,
				`"op": {`,
				`"limit": {`,
				`"count": itemCount`,
			},
		},
		{
			name: "write scenario with duration limit",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "https://s3.amazonaws.com",
				AccessKey:    "AWS_ACCESS_KEY_ID_EXAMPLE",
				SecretKey:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Bucket:       "my-bucket",
				Threads:      8,
				ObjectSize:   "5MB",
				Duration:     "5m",
			},
			wantErr: false,
			checkLines: []string{
				`var concurrency = 8`,
				`var itemSize = "5MB"`,
				`var outputPath = "/my-bucket"`,
				`var duration = "5m"`,
				`"type": "s3"`,
				`"type": "create"`,
				`"step": {`,
				`"time": duration`,
			},
		},
		{
			name: "write scenario with no limits (should default to 100)",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://storage:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "data",
				Threads:      2,
				ObjectSize:   "512KB",
				// No ObjectCount or Duration specified
			},
			wantErr: false,
			checkLines: []string{
				`var concurrency = 2`,
				`var itemSize = "512KB"`,
				`var itemCount = 100`, // Should default to 100
				`"type": "s3"`,
				`"type": "create"`,
				`"op": {`,
				`"limit": {`,
				`"count": itemCount`,
			},
		},
		{
			name: "custom port",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://storage:8080",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "data",
				Threads:      1,
				ObjectSize:   "256KB",
				ObjectCount:  100,
			},
			wantErr: false,
			checkLines: []string{
				`var concurrency = 1`,
				`"type": "s3"`,
				`"op": {`,
			},
		},
		{
			name: "bucket with leading slash",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://localhost:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "/mybucket",
				Threads:      2,
				ObjectSize:   "1GB",
				ObjectCount:  10,
			},
			wantErr: false,
			checkLines: []string{
				`var concurrency = 2`,
				`"type": "s3"`,
			},
		},
		{
			name: "write scenario with cleanup (should use sequential CreateLoad and DeleteLoad)",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testaccess",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  100,
				Cleanup:      true,
			},
			wantErr: false,
			checkLines: []string{
				`CreateLoad`, // Should use CreateLoad for create phase
				`DeleteLoad`, // Should use DeleteLoad for delete phase
				`var concurrency = 4`,
				`var itemSize = "1MB"`,
				`var outputPath = "/testbucket"`,
				`var itemCount = 100`,
				`"type": "s3"`,
				`"type": "create"`,
				`"type": "delete"`,
				`function pause`, // Should have pause function
				`itemsFile`,      // Should use file-based item passing
			},
		},
		{
			name: "write scenario with cleanup and duration",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "https://s3.example.com",
				AccessKey:    "access",
				SecretKey:    "secret",
				Bucket:       "cleanup-test",
				Threads:      2,
				ObjectSize:   "512KB",
				Duration:     "5m",
				Cleanup:      true,
			},
			wantErr: false,
			checkLines: []string{
				`var concurrency = 2`,
				`var itemSize = "512KB"`,
				`var outputPath = "/cleanup-test"`,
				`"type": "s3"`,
			},
		},
		{
			name: "write scenario with any endpoint string",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "not-a-valid-url", // URL validation now happens in BuildEndpointArgs
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false,
			checkLines: []string{
				`var concurrency = 1`,
				`var itemSize = "1MB"`,
				`var outputPath = "/bucket"`,
				`"type": "s3"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateWriteScenario(tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateWriteScenario() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Check that all expected lines are present
			for _, line := range tt.checkLines {
				if !strings.Contains(got, line) {
					t.Errorf("GenerateWriteScenario() missing expected line: %s\nGot:\n%s", line, got)
				}
			}

			// Verify it's valid JavaScript structure
			// New format uses CreateLoad/Load for writes, DeleteLoad for cleanup
			if tt.params.Cleanup {
				// Cleanup scenarios should have CreateLoad and DeleteLoad
				if !strings.Contains(got, "CreateLoad") {
					t.Errorf("GenerateWriteScenario() with cleanup should contain 'CreateLoad'")
				}
				if !strings.Contains(got, "DeleteLoad") {
					t.Errorf("GenerateWriteScenario() with cleanup should contain 'DeleteLoad'")
				}
				// Should have pause functions
				if !strings.Contains(got, "function pause") {
					t.Errorf("GenerateWriteScenario() with cleanup should contain pause function")
				}
				// Should use file-based item passing
				if !strings.Contains(got, "itemsFile") {
					t.Errorf("GenerateWriteScenario() with cleanup should use itemsFile for passing items")
				}
			} else {
				// Non-cleanup scenarios should just have Load
				if !strings.Contains(got, "Load") {
					t.Errorf("GenerateWriteScenario() without cleanup should contain 'Load'")
				}
			}
		})
	}
}

func TestGenerateListScenario(t *testing.T) {
	tests := []struct {
		name       string
		params     Params
		wantErr    bool
		contains   []string
		notContain []string
	}{
		{
			name: "basic list scenario with prefix and duration",
			params: Params{
				WorkloadType: "list",
				Bucket:       "reports",
				Threads:      4,
				Prefix:       "2025/",
				Duration:     "2m",
			},
			contains: []string{
				"var concurrency = 4;",
				"var bucketPath = \"/reports\";",
				"\"type\": \"list\"",
				"\"fetch_metadata\": false",
				"\"include_versions\": false",
				"\"max_keys\": 1000",
				"\"rate\": 0",
				"\"prefix\": \"2025/\"",
				"\"time\": \"2m\"",
				"\"type\": \"path\"",
				"\"type\": \"random\"",
				"-list\"",
			},
		},
		{
			name: "list scenario with count limit and no prefix",
			params: Params{
				WorkloadType: "list",
				Bucket:       "data",
				Threads:      1,
				ObjectCount:  500,
			},
			contains: []string{
				"var concurrency = 1;",
				"var bucketPath = \"/data\";",
				"\"type\": \"list\"",
				"\"max_keys\": 1000",
				"\"count\": 500",
			},
			notContain: []string{
				"\"prefix\"",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := GenerateListScenario(tt.params)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GenerateListScenario() error = %v, wantErr %v", err, tt.wantErr)
			}
			for _, substr := range tt.contains {
				if !strings.Contains(s, substr) {
					t.Fatalf("expected scenario to contain %q\nScenario:\n%s", substr, s)
				}
			}
			for _, substr := range tt.notContain {
				if strings.Contains(s, substr) {
					t.Fatalf("expected scenario to NOT contain %q\nScenario:\n%s", substr, s)
				}
			}
		})
	}
}

func TestGenerateMockScenario(t *testing.T) {
	tests := []struct {
		name       string
		params     Params
		checkLines []string
	}{
		{
			name: "mock with count limit",
			params: Params{
				WorkloadType: "mock",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  500,
			},
			checkLines: []string{
				`var concurrency = 4`,
				`var itemSize = "1MB"`,
				`var itemCount = 500`,
				`"type": "dummy-mock"`,
				`"op": {`,
				`"type": "create"`,
				`"limit": {`,
				`"count": itemCount`,
			},
		},
		{
			name: "mock with duration limit",
			params: Params{
				WorkloadType: "mock",
				Threads:      8,
				ObjectSize:   "512KB",
				Duration:     "30s",
			},
			checkLines: []string{
				`var concurrency = 8`,
				`var itemSize = "512KB"`,
				`var duration = "30s"`,
				`"type": "dummy-mock"`,
				`"op": {`,
				`"type": "create"`,
				`"step": {`,
				`"time": duration`,
			},
		},
		{
			name: "mock with cleanup and count (should use PipelineLoad)",
			params: Params{
				WorkloadType: "mock",
				Threads:      2,
				ObjectSize:   "2MB",
				ObjectCount:  100,
				Cleanup:      true,
			},
			checkLines: []string{
				`PipelineLoad`,
				`var concurrency = 2`,
				`var itemSize = "2MB"`,
				`var itemCount = 100`,
				`"type": "dummy-mock"`,
				`"type": "create"`,
				`.append(createConfig)`,
				`.append(deleteConfig)`,
				`"type": "delete"`,
				`"limit": {`,
				`"count": itemCount`,
			},
		},
		{
			name: "mock with cleanup and duration (cleanup ignored for duration)",
			params: Params{
				WorkloadType: "mock",
				Threads:      4,
				ObjectSize:   "256KB",
				Duration:     "1m",
				Cleanup:      true, // Cleanup only works with object count
			},
			checkLines: []string{
				`Load`, // Should use Load, not PipelineLoad (duration doesn't support cleanup)
				`var concurrency = 4`,
				`var itemSize = "256KB"`,
				`var duration = "1m"`,
				`"type": "dummy-mock"`,
				`"op": {`,
				`"type": "create"`,
				`"step": {`,
				`"time": duration`,
			},
		},
		{
			name: "mock with no limits (should default to 100)",
			params: ScenarioParams{
				WorkloadType: "mock",
				Threads:      1,
				ObjectSize:   "1MB",
				// No ObjectCount or Duration
			},
			checkLines: []string{
				`var concurrency = 1`,
				`var itemSize = "1MB"`,
				`var itemCount = 100`, // Should default to 100
				`"type": "dummy-mock"`,
				`"op": {`,
				`"type": "create"`,
				`"limit": {`,
				`"count": itemCount`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateMockScenario(tt.params)
			if err != nil {
				t.Errorf("GenerateMockScenario() error = %v", err)
				return
			}

			// Check that all expected lines are present
			for _, line := range tt.checkLines {
				if !strings.Contains(got, line) {
					t.Errorf("GenerateMockScenario() missing expected line: %s\nGot:\n%s", line, got)
				}
			}

			// Verify it's valid JavaScript structure (Load or PipelineLoad)
			if !strings.Contains(got, "Load") && !strings.Contains(got, "PipelineLoad") {
				t.Errorf("GenerateMockScenario() should contain 'Load' or 'PipelineLoad'")
			}
			if !strings.HasSuffix(strings.TrimSpace(got), ".run();") {
				t.Errorf("GenerateMockScenario() should end with '.run();'")
			}
		})
	}
}

func TestGenerateScenario(t *testing.T) {
	tests := []struct {
		name    string
		params  ScenarioParams
		wantErr bool
		errMsg  string
	}{
		{
			name: "write workload",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false,
		},
		{
			name: "mock workload",
			params: ScenarioParams{
				WorkloadType: "mock",
				Threads:      1,
				ObjectSize:   "1MB",
			},
			wantErr: false,
		},
		{
			name: "read workload",
			params: ScenarioParams{
				WorkloadType: "read",
				Endpoint:     "http://test:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
			},
			wantErr: false,
		},
		{
			name: "unsupported workload",
			params: ScenarioParams{
				WorkloadType: "delete",
			},
			wantErr: true,
			errMsg:  "unsupported workload type: delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := GenerateScenario(tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateScenario() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil && tt.errMsg != "" {
				if err.Error() != tt.errMsg {
					t.Errorf("GenerateScenario() error = %v, want %v", err.Error(), tt.errMsg)
				}
			}
		})
	}
}

func TestGenerateScenarioSecurity(t *testing.T) {
	tests := []struct {
		name         string
		params       Params
		wantErr      bool
		checkNoInj   []string // Strings that should NOT appear (potential injections)
		checkEscaped []string // Strings that should appear (properly escaped)
		description  string
	}{
		{
			name: "bucket with quotes",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    "access",
				SecretKey:    "secret",
				Bucket:       `test"bucket"with"quotes`,
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:      false,
			checkNoInj:   []string{`"test"bucket"with"quotes"`},                      // Should not have unescaped quotes
			checkEscaped: []string{`var outputPath = "/test\"bucket\"with\"quotes"`}, // Should have escaped quotes in variable
			description:  "Bucket with quotes should be properly escaped",
		},
		{
			name: "secret key with single quotes",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    "test",
				SecretKey:    `secret'with'quotes`,
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:     false,
			description: "Secret key with single quotes should be handled safely",
		},
		{
			name: "bucket name with JavaScript injection attempt",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    "test",
				SecretKey:    "secret",
				Bucket:       `bucket"}); malicious.code(); Load.config({"x":"`,
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:     false,
			checkNoInj:  []string{`"); malicious.code();`}, // Check for unescaped quote + code
			description: "Bucket name with JS injection should be escaped",
		},
		{
			name: "access key with newlines",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    "test\nwith\nnewlines",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:      false,
			checkNoInj:   []string{"test\nwith\nnewlines"}, // Should not have literal newlines in JSON
			checkEscaped: []string{`var itemSize = "1MB"`}, // Object size should still appear as variable
			description:  "Access key validation moved to CLI flags, scenario should still generate",
		},
		{
			name: "bucket with backslashes",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    "test",
				SecretKey:    "secret",
				Bucket:       `bucket\with\backslashes`,
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:     false,
			description: "Bucket with backslashes should be handled properly",
		},
		{
			name: "unicode characters in parameters",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    "test",
				SecretKey:    "secret",
				Bucket:       "bucket-测试-🚀",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:     false,
			description: "Unicode characters should be preserved",
		},
		{
			name: "very long access key",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://test:9000",
				AccessKey:    strings.Repeat("A", 1000),
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:     false,
			description: "Very long parameters should be handled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateScenario(tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateScenario() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check that potentially dangerous strings are not present
			for _, dangerous := range tt.checkNoInj {
				if strings.Contains(got, dangerous) {
					t.Errorf("GenerateScenario() contains potential injection: %s\nDescription: %s\nGot:\n%s",
						dangerous, tt.description, got)
				}
			}

			// Check that properly escaped strings are present
			for _, escaped := range tt.checkEscaped {
				if !strings.Contains(got, escaped) {
					t.Errorf("GenerateScenario() missing properly escaped value: %s\nDescription: %s\nGot:\n%s",
						escaped, tt.description, got)
				}
			}

			// Verify the scenario still has valid structure
			// New templates use Load, CreateLoad, or DeleteLoad
			hasLoadStructure := strings.Contains(got, "Load") || strings.Contains(got, "CreateLoad") || strings.Contains(got, "DeleteLoad")
			hasRunCall := strings.Contains(got, ".run()")
			if !hasLoadStructure || !hasRunCall {
				t.Errorf("GenerateScenario() produced invalid structure for %s", tt.name)
			}
		})
	}
}

func TestBuildEndpointArgs_Deprecated(t *testing.T) {
	// NOTE: BuildEndpointArgs is DEPRECATED in favor of GenerateDefaults which creates
	// a defaults.yaml configuration file for the API-based approach.
	// This test verifies the deprecated function returns empty args as expected.
	// See TestGenerateDefaults for the new configuration approach.

	// The function should always return empty args now
	tests := []struct {
		name   string
		params ScenarioParams
	}{
		{
			name: "mock workload type",
			params: ScenarioParams{
				WorkloadType: "mock",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
			},
		},
		{
			name: "write workload with HTTP endpoint",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
			},
		},
		{
			name: "write workload with HTTPS endpoint",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "https://s3.example.com:8443",
				AccessKey:    "AWS_ACCESS_KEY_ID_EXAMPLE",
				SecretKey:    "secretkey123",
			},
		},
		{
			name: "read workload type",
			params: ScenarioParams{
				WorkloadType: "read",
				Endpoint:     "http://storage.local",
				AccessKey:    "admin",
				SecretKey:    "password",
			},
		},
		{
			name: "delete workload type",
			params: ScenarioParams{
				WorkloadType: "delete",
				Endpoint:     "https://s3.amazonaws.com",
				AccessKey:    "AWS_ACCESS_KEY_ID_EXAMPLE",
				SecretKey:    "verylongsecretkey",
			},
		},
		{
			name: "invalid URL should still return empty args",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "://invalid",
				AccessKey:    "key",
				SecretKey:    "secret",
			},
		},
		{
			name: "empty endpoint should still return empty args",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "",
				AccessKey:    "key",
				SecretKey:    "secret",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildEndpointArgs(tt.params)

			// Function should never return an error
			if err != nil {
				t.Errorf("BuildEndpointArgs() unexpected error = %v", err)
				return
			}

			// Function should always return empty args (deprecated)
			if len(got) != 0 {
				t.Errorf("BuildEndpointArgs() got %d args, want 0 args (deprecated function)\nGot: %v", len(got), got)
			}
		})
	}
}

func TestEscapeJSONString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "string with quotes",
			input:    `hello"world"test`,
			expected: `hello\"world\"test`,
		},
		{
			name:     "string with backslashes",
			input:    `hello\world\test`,
			expected: `hello\\world\\test`,
		},
		{
			name:     "string with newlines",
			input:    "hello\nworld\ntest",
			expected: `hello\nworld\ntest`,
		},
		{
			name:     "string with tabs",
			input:    "hello\tworld\ttest",
			expected: `hello\tworld\ttest`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "string with unicode",
			input:    "测试🚀",
			expected: "测试🚀",
		},
		{
			name:     "complex string with multiple escapes",
			input:    `bucket"}); malicious.code(); Load.config({"x":"`,
			expected: `bucket\"}); malicious.code(); Load.config({\"x\":\"`,
		},
		{
			name:     "string with single quotes",
			input:    "hello'world'test",
			expected: "hello'world'test", // Single quotes don't need escaping in JSON
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeJSONString(tt.input)
			if got != tt.expected {
				t.Errorf("escapeJSONString() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestGenerateScenarioEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		params      ScenarioParams
		wantErr     bool
		errContains string
	}{
		{
			name: "empty endpoint URL",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // URL validation moved to BuildEndpointArgs
		},
		{
			name: "URL without scheme",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "example.com:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr:     false, // Go's url.Parse accepts this
			errContains: "",
		},
		{
			name: "URL with only scheme",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // URL parsing allows this
		},
		{
			name: "URL with empty hostname and port",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // URL parsing allows this
		},
		{
			name: "URL with invalid port",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com:abc",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // URL validation moved to BuildEndpointArgs
		},
		{
			name: "URL with negative port",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com:-1",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // URL validation moved to BuildEndpointArgs
		},
		{
			name: "URL with port too large",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com:99999",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // strconv.Atoi allows this, but it's invalid port range
		},
		{
			name: "empty bucket name",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // Currently allowed, creates path "/"
		},
		{
			name: "empty access key",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // Currently allowed
		},
		{
			name: "empty secret key",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // Currently allowed
		},
		{
			name: "zero threads",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      0,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // Currently allowed
		},
		{
			name: "negative threads",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      -1,
				ObjectSize:   "1MB",
				ObjectCount:  1,
			},
			wantErr: false, // Currently allowed
		},
		{
			name: "empty object size",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "",
				ObjectCount:  1,
			},
			wantErr: false, // Currently allowed
		},
		{
			name: "zero object count and no duration",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  0,
				Duration:     "",
			},
			wantErr: false, // Currently generates scenario without limit
		},
		{
			name: "both object count and duration specified",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  100,
				Duration:     "5m",
			},
			wantErr: false, // Currently uses object count
		},
		{
			name: "mock with empty object size",
			params: ScenarioParams{
				WorkloadType: "mock",
				Threads:      1,
				ObjectSize:   "",
			},
			wantErr: false, // Currently allowed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateScenario(tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateScenario() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("GenerateScenario() error = %v, want error containing %v", err.Error(), tt.errContains)
				}
			}
			if err == nil && got == "" {
				t.Errorf("GenerateScenario() returned empty scenario without error")
			}
		})
	}
}

// Ensure generated scenarios use op.limit.count (valid) and not step.limit.count (invalid)
func TestNoOpLimitInGeneratedScenarios(t *testing.T) {
	reStepCount := regexp.MustCompile(`"step"\s*:\s*\{[^}]*"limit"\s*:\s*\{[^}]*"count"`)

	cases := []struct {
		name string
		gen  func() (string, error)
	}{
		{
			name: "write_count",
			gen: func() (string, error) {
				return GenerateWriteScenario(ScenarioParams{
					WorkloadType: "write",
					Endpoint:     "http://minio:9000",
					AccessKey:    "a",
					SecretKey:    "s",
					Bucket:       "b",
					Threads:      2,
					ObjectSize:   "1MB",
					ObjectCount:  10,
				})
			},
		},
		{
			name: "write_duration",
			gen: func() (string, error) {
				return GenerateWriteScenario(ScenarioParams{
					WorkloadType: "write",
					Endpoint:     "http://minio:9000",
					AccessKey:    "a",
					SecretKey:    "s",
					Bucket:       "b",
					Threads:      2,
					ObjectSize:   "1MB",
					Duration:     "5s",
				})
			},
		},
		{
			name: "mock_count",
			gen: func() (string, error) {
				return GenerateMockScenario(ScenarioParams{
					WorkloadType: "mock",
					Threads:      2,
					ObjectSize:   "1MB",
					ObjectCount:  10,
				})
			},
		},
		{
			name: "mock_duration",
			gen: func() (string, error) {
				return GenerateMockScenario(ScenarioParams{
					WorkloadType: "mock",
					Threads:      2,
					ObjectSize:   "1MB",
					Duration:     "5s",
				})
			},
		},
		{
			name: "mock_cleanup_count",
			gen: func() (string, error) {
				return GenerateMockScenario(ScenarioParams{
					WorkloadType: "mock",
					Threads:      2,
					ObjectSize:   "1MB",
					ObjectCount:  10,
					Cleanup:      true,
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.gen()
			if err != nil {
				t.Fatalf("unexpected error generating scenario: %v", err)
			}
			if reStepCount.MatchString(got) {
				t.Errorf("scenario contains invalid step.limit.count path:\n%s", got)
			}
			// Duration scenarios should include step-level limit.time, which is still valid
		})
	}
}

// TestStorageDriverType verifies the correct storage driver type (s3 vs s3-rdma)
// is generated in the JavaScript scenario based on UseRdma flag.
func TestStorageDriverType(t *testing.T) {
	tests := []struct {
		name           string
		params         Params
		wantDriverType string // expected "type": "..." in output
		notDriverType  string // must NOT appear
	}{
		{
			name: "write count - default s3 driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  100,
			},
			wantDriverType: `"type": "s3"`,
			notDriverType:  `"type": "s3-rdma"`,
		},
		{
			name: "write count - RDMA driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  100,
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
		{
			name: "write duration - default s3 driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      8,
				ObjectSize:   "1MB",
				Duration:     "30s",
			},
			wantDriverType: `"type": "s3"`,
			notDriverType:  `"type": "s3-rdma"`,
		},
		{
			name: "write duration - RDMA driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      8,
				ObjectSize:   "1MB",
				Duration:     "30s",
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
		{
			name: "write default (no count/duration) - s3 driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      2,
				ObjectSize:   "1MB",
			},
			wantDriverType: `"type": "s3"`,
			notDriverType:  `"type": "s3-rdma"`,
		},
		{
			name: "write default (no count/duration) - RDMA driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      2,
				ObjectSize:   "1MB",
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
		{
			name: "write with cleanup count - s3 driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  50,
				Cleanup:      true,
			},
			wantDriverType: `"type": "s3"`,
			notDriverType:  `"type": "s3-rdma"`,
		},
		{
			name: "write with cleanup count - RDMA driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  50,
				Cleanup:      true,
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
		{
			name: "write with cleanup duration - s3 driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				Duration:     "1m",
				Cleanup:      true,
			},
			wantDriverType: `"type": "s3"`,
			notDriverType:  `"type": "s3-rdma"`,
		},
		{
			name: "write with cleanup duration - RDMA driver",
			params: Params{
				WorkloadType: "write",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				Duration:     "1m",
				Cleanup:      true,
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateWriteScenario(tt.params)
			if err != nil {
				t.Fatalf("GenerateWriteScenario() error = %v", err)
			}

			if !strings.Contains(got, tt.wantDriverType) {
				t.Errorf("expected scenario to contain %q\nGot:\n%s", tt.wantDriverType, got)
			}

			if tt.notDriverType != "" && strings.Contains(got, tt.notDriverType) {
				t.Errorf("expected scenario to NOT contain %q\nGot:\n%s", tt.notDriverType, got)
			}
		})
	}
}

func TestRdmaDoesNotAffectListScenario(t *testing.T) {
	// List scenarios always use "s3" driver regardless of UseRdma
	params := Params{
		WorkloadType: "list",
		Bucket:       "testbucket",
		Threads:      4,
		ObjectCount:  100,
		UseRdma:      true, // should be ignored for list workloads
	}

	got, err := GenerateListScenario(params)
	if err != nil {
		t.Fatalf("GenerateListScenario() error = %v", err)
	}

	if !strings.Contains(got, `"type": "s3"`) {
		t.Error("List scenario should always use s3 driver type")
	}
	if strings.Contains(got, `"type": "s3-rdma"`) {
		t.Error("List scenario should never use s3-rdma driver type")
	}
}

func TestRdmaDoesNotAffectMockScenario(t *testing.T) {
	// Mock scenarios always use "dummy-mock" driver regardless of UseRdma
	params := Params{
		WorkloadType: "mock",
		Threads:      2,
		ObjectSize:   "1KB",
		ObjectCount:  50,
		UseRdma:      true, // should be ignored for mock workloads
	}

	got, err := GenerateMockScenario(params)
	if err != nil {
		t.Fatalf("GenerateMockScenario() error = %v", err)
	}

	if !strings.Contains(got, `"type": "dummy-mock"`) {
		t.Error("Mock scenario should always use dummy-mock driver type")
	}
	if strings.Contains(got, `"type": "s3-rdma"`) {
		t.Error("Mock scenario should never use s3-rdma driver type")
	}
}

func TestGenerateScenario_RdmaRouting(t *testing.T) {
	// Verify GenerateScenario routes correctly and UseRdma only affects write
	tests := []struct {
		name          string
		params        Params
		wantDriver    string
		notWantDriver string
	}{
		{
			name: "write with RDMA routes to s3-rdma",
			params: Params{
				WorkloadType: "write",
				Bucket:       "b",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  10,
				UseRdma:      true,
			},
			wantDriver: `"type": "s3-rdma"`,
		},
		{
			name: "write without RDMA routes to s3",
			params: Params{
				WorkloadType: "write",
				Bucket:       "b",
				Threads:      1,
				ObjectSize:   "1MB",
				ObjectCount:  10,
			},
			wantDriver:    `"type": "s3"`,
			notWantDriver: `"type": "s3-rdma"`,
		},
		{
			name: "list ignores UseRdma",
			params: Params{
				WorkloadType: "list",
				Bucket:       "b",
				Threads:      1,
				ObjectCount:  10,
				UseRdma:      true,
			},
			wantDriver:    `"type": "s3"`,
			notWantDriver: `"type": "s3-rdma"`,
		},
		{
			name: "mock ignores UseRdma",
			params: Params{
				WorkloadType: "mock",
				Threads:      1,
				ObjectSize:   "1KB",
				ObjectCount:  10,
				UseRdma:      true,
			},
			wantDriver:    `"type": "dummy-mock"`,
			notWantDriver: `"type": "s3-rdma"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateScenario(tt.params)
			if err != nil {
				t.Fatalf("GenerateScenario() error = %v", err)
			}

			if !strings.Contains(got, tt.wantDriver) {
				t.Errorf("expected %q in scenario output", tt.wantDriver)
			}
			if tt.notWantDriver != "" && strings.Contains(got, tt.notWantDriver) {
				t.Errorf("did not expect %q in scenario output", tt.notWantDriver)
			}
		})
	}
}

func TestGenerateReadScenario(t *testing.T) {
	tests := []struct {
		name       string
		params     Params
		wantErr    bool
		checkLines []string // lines that should be present
		notLines   []string // lines that should NOT be present
	}{
		{
			name: "read with cleanup and duration",
			params: Params{
				WorkloadType: "read",
				Bucket:       "testbucket",
				Threads:      8,
				ObjectSize:   "1MB",
				Duration:     "2m",
				Cleanup:      true,
				SeedCount:    500,
			},
			checkLines: []string{
				`var concurrency = 8`,
				`var itemSize = "1MB"`,
				`var seedCount = 500`,
				`var duration = "2m"`,
				`var outputPath = "/testbucket"`,
				`"type": "s3"`,
				`"type": "create"`,
				`"type": "read"`,
				`"type": "delete"`,
				`"mode": true`,
				`PreconditionLoad`,
				`ReadLoad`,
				`DeleteLoad`,
				`itemsFile`,
				`"count": seedCount`,
				`"time": duration`,
				`function pause`,
			},
		},
		{
			name: "read with cleanup and count",
			params: Params{
				WorkloadType: "read",
				Bucket:       "testbucket",
				Threads:      4,
				ObjectSize:   "512KB",
				ObjectCount:  10000,
				Cleanup:      true,
				SeedCount:    1000,
			},
			checkLines: []string{
				`var concurrency = 4`,
				`var itemSize = "512KB"`,
				`var seedCount = 1000`,
				`var readCount = 10000`,
				`PreconditionLoad`,
				`ReadLoad`,
				`DeleteLoad`,
				`"type": "read"`,
				`"count": readCount`,
			},
			notLines: []string{
				`"mode": true`, // count-based should not recycle
				`"time":`,      // no duration limit
			},
		},
		{
			name: "read without cleanup and duration",
			params: Params{
				WorkloadType: "read",
				Bucket:       "testbucket",
				Threads:      16,
				ObjectSize:   "4MB",
				Duration:     "5m",
				Cleanup:      false,
				SeedCount:    2000,
			},
			checkLines: []string{
				`var concurrency = 16`,
				`var seedCount = 2000`,
				`PreconditionLoad`,
				`ReadLoad`,
				`"mode": true`,
				`"time": duration`,
				`seed objects preserved`,
			},
			notLines: []string{
				`DeleteLoad`, // no cleanup
			},
		},
		{
			name: "read without cleanup and count",
			params: Params{
				WorkloadType: "read",
				Bucket:       "testbucket",
				Threads:      2,
				ObjectSize:   "256KB",
				ObjectCount:  500,
				Cleanup:      false,
				SeedCount:    100,
			},
			checkLines: []string{
				`var concurrency = 2`,
				`var seedCount = 100`,
				`var readCount = 500`,
				`PreconditionLoad`,
				`ReadLoad`,
				`"count": readCount`,
				`seed objects preserved`,
			},
			notLines: []string{
				`DeleteLoad`,
				`"mode": true`,
			},
		},
		{
			name: "read with defaults (no duration, no count, no cleanup flag)",
			params: Params{
				WorkloadType: "read",
				Bucket:       "default-bucket",
				Threads:      1,
				ObjectSize:   "1MB",
			},
			checkLines: []string{
				`var seedCount = 2500`,
				`var duration = "60s"`,
				`PreconditionLoad`,
				`ReadLoad`,
				`DeleteLoad`,
				`"mode": true`,
			},
		},
		{
			name: "read with default seed count",
			params: Params{
				WorkloadType: "read",
				Bucket:       "testbucket",
				Threads:      4,
				ObjectSize:   "1MB",
				Duration:     "1m",
				Cleanup:      true,
				// SeedCount = 0 should default to 2500
			},
			checkLines: []string{
				`var seedCount = 2500`,
				`PreconditionLoad`,
				`ReadLoad`,
				`DeleteLoad`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateReadScenario(tt.params)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GenerateReadScenario() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			for _, line := range tt.checkLines {
				if !strings.Contains(got, line) {
					t.Errorf("GenerateReadScenario() missing expected line: %s\nGot:\n%s", line, got)
				}
			}

			for _, line := range tt.notLines {
				if strings.Contains(got, line) {
					t.Errorf("GenerateReadScenario() should NOT contain: %s\nGot:\n%s", line, got)
				}
			}

			// All read scenarios must have PreconditionLoad and ReadLoad
			if !strings.Contains(got, "PreconditionLoad") {
				t.Error("Read scenario must contain PreconditionLoad")
			}
			if !strings.Contains(got, "ReadLoad") {
				t.Error("Read scenario must contain ReadLoad")
			}
		})
	}
}

func TestGenerateReadScenario_RDMA(t *testing.T) {
	tests := []struct {
		name           string
		params         Params
		wantDriverType string
		notDriverType  string
	}{
		{
			name: "read with s3 driver",
			params: Params{
				WorkloadType: "read",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				Duration:     "1m",
				Cleanup:      true,
			},
			wantDriverType: `"type": "s3"`,
			notDriverType:  `"type": "s3-rdma"`,
		},
		{
			name: "read with RDMA driver",
			params: Params{
				WorkloadType: "read",
				Bucket:       "bucket",
				Threads:      4,
				ObjectSize:   "1MB",
				Duration:     "1m",
				Cleanup:      true,
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
		{
			name: "read count with RDMA driver",
			params: Params{
				WorkloadType: "read",
				Bucket:       "bucket",
				Threads:      8,
				ObjectSize:   "1MB",
				ObjectCount:  5000,
				Cleanup:      true,
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
		{
			name: "read no cleanup with RDMA",
			params: Params{
				WorkloadType: "read",
				Bucket:       "bucket",
				Threads:      2,
				ObjectSize:   "4MB",
				Duration:     "30s",
				Cleanup:      false,
				UseRdma:      true,
			},
			wantDriverType: `"type": "s3-rdma"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateReadScenario(tt.params)
			if err != nil {
				t.Fatalf("GenerateReadScenario() error = %v", err)
			}

			if !strings.Contains(got, tt.wantDriverType) {
				t.Errorf("expected scenario to contain %q\nGot:\n%s", tt.wantDriverType, got)
			}

			if tt.notDriverType != "" && strings.Contains(got, tt.notDriverType) {
				t.Errorf("expected scenario to NOT contain %q\nGot:\n%s", tt.notDriverType, got)
			}
		})
	}
}

func TestGenerateReadScenario_StepIDs(t *testing.T) {
	params := Params{
		WorkloadType: "read",
		Bucket:       "testbucket",
		Threads:      4,
		ObjectSize:   "1MB",
		Duration:     "2m",
		Cleanup:      true,
		SeedCount:    100,
	}

	got, err := GenerateReadScenario(params)
	if err != nil {
		t.Fatalf("GenerateReadScenario() error = %v", err)
	}

	// Read scenarios must have 3 step IDs: seed, read, delete
	seedIDPattern := regexp.MustCompile(`mt-001-\d{8}\.\d{6}\.\d{3}-seed`)
	readIDPattern := regexp.MustCompile(`mt-002-\d{8}\.\d{6}\.\d{3}-read`)
	deleteIDPattern := regexp.MustCompile(`mt-003-\d{8}\.\d{6}\.\d{3}-delete`)

	if !seedIDPattern.MatchString(got) {
		t.Errorf("Read scenario missing seed step ID (mt-001-...-seed)")
	}
	if !readIDPattern.MatchString(got) {
		t.Errorf("Read scenario missing read step ID (mt-002-...-read)")
	}
	if !deleteIDPattern.MatchString(got) {
		t.Errorf("Read scenario missing delete step ID (mt-003-...-delete)")
	}
}

func TestGenerateReadScenario_NoCleanupStepIDs(t *testing.T) {
	params := Params{
		WorkloadType: "read",
		Bucket:       "testbucket",
		Threads:      4,
		ObjectSize:   "1MB",
		Duration:     "2m",
		Cleanup:      false,
		SeedCount:    100,
	}

	got, err := GenerateReadScenario(params)
	if err != nil {
		t.Fatalf("GenerateReadScenario() error = %v", err)
	}

	seedIDPattern := regexp.MustCompile(`mt-001-\d{8}\.\d{6}\.\d{3}-seed`)
	readIDPattern := regexp.MustCompile(`mt-002-\d{8}\.\d{6}\.\d{3}-read`)
	deleteIDPattern := regexp.MustCompile(`mt-003-\d{8}\.\d{6}\.\d{3}-delete`)

	if !seedIDPattern.MatchString(got) {
		t.Errorf("Read scenario missing seed step ID")
	}
	if !readIDPattern.MatchString(got) {
		t.Errorf("Read scenario missing read step ID")
	}
	// Delete step ID is generated but not used in no-cleanup template
	if deleteIDPattern.MatchString(got) {
		t.Errorf("No-cleanup read scenario should NOT contain delete step ID")
	}
}

func TestReadNoOpLimitInGeneratedScenarios(t *testing.T) {
	reStepCount := regexp.MustCompile(`"step"\s*:\s*\{[^}]*"limit"\s*:\s*\{[^}]*"count"`)

	cases := []struct {
		name string
		gen  func() (string, error)
	}{
		{
			name: "read_cleanup_duration",
			gen: func() (string, error) {
				return GenerateReadScenario(Params{
					WorkloadType: "read",
					Bucket:       "b",
					Threads:      2,
					ObjectSize:   "1MB",
					Duration:     "30s",
					Cleanup:      true,
					SeedCount:    100,
				})
			},
		},
		{
			name: "read_cleanup_count",
			gen: func() (string, error) {
				return GenerateReadScenario(Params{
					WorkloadType: "read",
					Bucket:       "b",
					Threads:      2,
					ObjectSize:   "1MB",
					ObjectCount:  500,
					Cleanup:      true,
					SeedCount:    100,
				})
			},
		},
		{
			name: "read_nocleanup_duration",
			gen: func() (string, error) {
				return GenerateReadScenario(Params{
					WorkloadType: "read",
					Bucket:       "b",
					Threads:      2,
					ObjectSize:   "1MB",
					Duration:     "1m",
					Cleanup:      false,
					SeedCount:    50,
				})
			},
		},
		{
			name: "read_default",
			gen: func() (string, error) {
				return GenerateReadScenario(Params{
					WorkloadType: "read",
					Bucket:       "b",
					Threads:      2,
					ObjectSize:   "1MB",
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.gen()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if reStepCount.MatchString(got) {
				t.Errorf("scenario contains invalid step.limit.count:\n%s", got)
			}
		})
	}
}

func TestGenerateScenario_ReadRdmaRouting(t *testing.T) {
	tests := []struct {
		name          string
		params        Params
		wantDriver    string
		notWantDriver string
	}{
		{
			name: "read with RDMA routes to s3-rdma",
			params: Params{
				WorkloadType: "read",
				Bucket:       "b",
				Threads:      1,
				ObjectSize:   "1MB",
				Duration:     "30s",
				UseRdma:      true,
			},
			wantDriver: `"type": "s3-rdma"`,
		},
		{
			name: "read without RDMA routes to s3",
			params: Params{
				WorkloadType: "read",
				Bucket:       "b",
				Threads:      1,
				ObjectSize:   "1MB",
				Duration:     "30s",
			},
			wantDriver:    `"type": "s3"`,
			notWantDriver: `"type": "s3-rdma"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateScenario(tt.params)
			if err != nil {
				t.Fatalf("GenerateScenario() error = %v", err)
			}

			if !strings.Contains(got, tt.wantDriver) {
				t.Errorf("expected %q in scenario output", tt.wantDriver)
			}
			if tt.notWantDriver != "" && strings.Contains(got, tt.notWantDriver) {
				t.Errorf("did not expect %q in scenario output", tt.notWantDriver)
			}
		})
	}
}
