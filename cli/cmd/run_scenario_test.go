package cmd

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func TestBuildScenarioParams(t *testing.T) {
	tests := []struct {
		name         string
		workloadType string
		flags        map[string]interface{}
		expected     scenario.Params
		wantErr      bool
	}{
		{
			name:         "write workload with all parameters",
			workloadType: "write",
			flags: map[string]interface{}{
				"endpoint":     "http://s3.example.com",
				"access-key":   "access123",
				"secret-key":   "secret456",
				"bucket":       "mybucket",
				"threads":      8,
				"object-size":  "10MB",
				"object-count": 1000,
				"duration":     "",
			},
			expected: scenario.Params{
				WorkloadType:        "write",
				Endpoint:            "http://s3.example.com",
				Endpoints:           []string{"http://s3.example.com"},
				AccessKey:           "access123",
				SecretKey:           "secret456",
				Bucket:              "mybucket",
				AuthVersion:         4,
				Threads:             8,
				ObjectSize:          "10MB",
				ObjectCount:         1000,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "write workload with multi-endpoints and slicing",
			workloadType: "write",
			flags: map[string]interface{}{
				"endpoints":       "http://s1:9000,http://s2:9000",
				"slice-endpoints": true,
				"access-key":      "ak",
				"secret-key":      "sk",
				"bucket":          "bkt",
				"threads":         8,
				"object-size":     "1MB",
				"object-count":    100,
				"duration":        "",
			},
			expected: scenario.Params{
				WorkloadType:        "write",
				Endpoint:            "",
				Endpoints:           []string{"http://s1:9000", "http://s2:9000"},
				SliceEndpoints:      true,
				AccessKey:           "ak",
				SecretKey:           "sk",
				Bucket:              "bkt",
				AuthVersion:         4,
				Threads:             8,
				ObjectSize:          "1MB",
				ObjectCount:         100,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "write workload with duration instead of count",
			workloadType: "write",
			flags: map[string]interface{}{
				"endpoint":     "https://s3.amazonaws.com",
				"access-key":   "AKIAIOSFODNN7EXAMPLE",
				"secret-key":   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"bucket":       "test-bucket",
				"threads":      4,
				"object-size":  "512KB",
				"object-count": 0,
				"duration":     "5m",
			},
			expected: scenario.Params{
				WorkloadType:        "write",
				Endpoint:            "https://s3.amazonaws.com",
				Endpoints:           []string{"https://s3.amazonaws.com"},
				AccessKey:           "AKIAIOSFODNN7EXAMPLE",
				SecretKey:           "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Bucket:              "test-bucket",
				AuthVersion:         4,
				Threads:             4,
				ObjectSize:          "512KB",
				ObjectCount:         0,
				Duration:            "5m",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "write workload with default object size",
			workloadType: "write",
			flags: map[string]interface{}{
				"endpoint":     "http://minio:9000",
				"access-key":   "minioadmin",
				"secret-key":   "minioadmin",
				"bucket":       "test",
				"threads":      1,
				"object-size":  "",
				"object-count": 100,
				"duration":     "",
			},
			expected: scenario.Params{
				WorkloadType:        "write",
				Endpoint:            "http://minio:9000",
				Endpoints:           []string{"http://minio:9000"},
				AccessKey:           "minioadmin",
				SecretKey:           "minioadmin",
				Bucket:              "test",
				AuthVersion:         4,
				Threads:             1,
				ObjectSize:          "1MB",
				ObjectCount:         100,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "list workload with prefix",
			workloadType: "list",
			flags: map[string]interface{}{
				"endpoint":     "http://s3.example.com",
				"access-key":   "access123",
				"secret-key":   "secret456",
				"bucket":       "list-bucket",
				"threads":      4,
				"prefix":       "reports/",
				"object-size":  "",
				"object-count": 0,
				"duration":     "",
			},
			expected: scenario.Params{
				WorkloadType:        "list",
				Endpoint:            "http://s3.example.com",
				Endpoints:           []string{"http://s3.example.com"},
				AccessKey:           "access123",
				SecretKey:           "secret456",
				Bucket:              "list-bucket",
				Prefix:              "reports/",
				AuthVersion:         4,
				Threads:             4,
				ObjectSize:          "",
				ObjectCount:         0,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "list workload overrides auth version",
			workloadType: "list",
			flags: map[string]interface{}{
				"endpoint":     "http://s3.example.com",
				"access-key":   "ak",
				"secret-key":   "sk",
				"bucket":       "list-bucket",
				"threads":      2,
				"auth-version": 2,
			},
			expected: scenario.Params{
				WorkloadType:        "list",
				Endpoint:            "http://s3.example.com",
				Endpoints:           []string{"http://s3.example.com"},
				AccessKey:           "ak",
				SecretKey:           "sk",
				Bucket:              "list-bucket",
				AuthVersion:         2,
				Threads:             2,
				ObjectSize:          "",
				ObjectCount:         0,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "mock workload ignores S3 parameters",
			workloadType: "mock",
			flags: map[string]interface{}{
				"endpoint":     "http://dummy",
				"access-key":   "dummy",
				"secret-key":   "dummy",
				"bucket":       "dummy",
				"threads":      2,
				"object-size":  "2MB",
				"object-count": 500,
				"duration":     "",
			},
			expected: scenario.Params{
				WorkloadType:        "mock",
				Endpoint:            "",
				AccessKey:           "",
				SecretKey:           "",
				Bucket:              "",
				Threads:             2,
				ObjectSize:          "2MB",
				ObjectCount:         500,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "mock workload with duration",
			workloadType: "mock",
			flags: map[string]interface{}{
				"endpoint":     "",
				"access-key":   "",
				"secret-key":   "",
				"bucket":       "",
				"threads":      16,
				"object-size":  "",
				"object-count": 0,
				"duration":     "30s",
			},
			expected: scenario.Params{
				WorkloadType:        "mock",
				Endpoint:            "",
				AccessKey:           "",
				SecretKey:           "",
				Bucket:              "",
				Threads:             16,
				ObjectSize:          "1MB",
				ObjectCount:         0,
				Duration:            "30s",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "write workload with cleanup flag",
			workloadType: "write",
			flags: map[string]interface{}{
				"endpoint":     "http://s3.example.com",
				"access-key":   "access123",
				"secret-key":   "secret456",
				"bucket":       "testbucket",
				"threads":      4,
				"object-size":  "1MB",
				"object-count": 100,
				"duration":     "",
				"cleanup":      true,
			},
			expected: scenario.Params{
				WorkloadType:        "write",
				Endpoint:            "http://s3.example.com",
				Endpoints:           []string{"http://s3.example.com"},
				AccessKey:           "access123",
				SecretKey:           "secret456",
				Bucket:              "testbucket",
				AuthVersion:         4,
				Threads:             4,
				ObjectSize:          "1MB",
				ObjectCount:         100,
				Duration:            "",
				Cleanup:             true,
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "read workload",
			workloadType: "read",
			flags: map[string]interface{}{
				"endpoint":     "http://localhost:9000",
				"access-key":   "test",
				"secret-key":   "test123",
				"bucket":       "read-bucket",
				"threads":      4,
				"object-size":  "100KB",
				"object-count": 2000,
				"duration":     "",
			},
			expected: scenario.Params{
				WorkloadType:        "read",
				Endpoint:            "http://localhost:9000",
				Endpoints:           []string{"http://localhost:9000"},
				AccessKey:           "test",
				SecretKey:           "test123",
				Bucket:              "read-bucket",
				AuthVersion:         4,
				Threads:             4,
				ObjectSize:          "100KB",
				ObjectCount:         2000,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "read workload with shuffle override",
			workloadType: "read",
			flags: map[string]interface{}{
				"endpoint":               "http://localhost:9000",
				"access-key":             "test",
				"secret-key":             "test123",
				"bucket":                 "read-bucket",
				"threads":                4,
				"object-size":            "100KB",
				"duration":               "5m",
				flagReadShuffle:          true,
				flagReadShuffleBatchSize: 1000000,
			},
			expected: scenario.Params{
				WorkloadType:         "read",
				Endpoint:             "http://localhost:9000",
				Endpoints:            []string{"http://localhost:9000"},
				AccessKey:            "test",
				SecretKey:            "test123",
				Bucket:               "read-bucket",
				AuthVersion:          4,
				Threads:              4,
				ObjectSize:           "100KB",
				ObjectCount:          0,
				Duration:             "5m",
				ReadShuffle:          true,
				ReadShuffleBatchSize: 1000000,
				ObjectDataDedupable:  true,
			},
			wantErr: false,
		},
		{
			name:         "mixed workload",
			workloadType: "mixed",
			flags: map[string]interface{}{
				"endpoint":     "https://s3.us-west-2.amazonaws.com",
				"access-key":   "AKIAEXAMPLE",
				"secret-key":   "secretEXAMPLE",
				"bucket":       "mixed-bucket",
				"threads":      8,
				"object-size":  "5MB",
				"object-count": 0,
				"duration":     "10m",
			},
			expected: scenario.Params{
				WorkloadType:        "mixed",
				Endpoint:            "https://s3.us-west-2.amazonaws.com",
				Endpoints:           []string{"https://s3.us-west-2.amazonaws.com"},
				AccessKey:           "AKIAEXAMPLE",
				SecretKey:           "secretEXAMPLE",
				Bucket:              "mixed-bucket",
				AuthVersion:         4,
				Threads:             8,
				ObjectSize:          "5MB",
				ObjectCount:         0,
				Duration:            "10m",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "delete workload",
			workloadType: "delete",
			flags: map[string]interface{}{
				"endpoint":     "http://ceph-s3:7480",
				"access-key":   "cephuser",
				"secret-key":   "cephpass",
				"bucket":       "delete-bucket",
				"threads":      2,
				"object-size":  "256KB",
				"object-count": 50,
				"duration":     "",
			},
			expected: scenario.ScenarioParams{
				WorkloadType:        "delete",
				Endpoint:            "http://ceph-s3:7480",
				Endpoints:           []string{"http://ceph-s3:7480"},
				AccessKey:           "cephuser",
				SecretKey:           "cephpass",
				Bucket:              "delete-bucket",
				AuthVersion:         4,
				Threads:             2,
				ObjectSize:          "256KB",
				ObjectCount:         50,
				Duration:            "",
				SelectionOrder:      scenario.SelectionOrderCanonical,
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "zero threads defaults to zero (not overridden)",
			workloadType: "write",
			flags: map[string]interface{}{
				"endpoint":     "http://s3.example.com",
				"access-key":   "key",
				"secret-key":   "secret",
				"bucket":       "bucket",
				"threads":      0,
				"object-size":  "1KB",
				"object-count": 10,
				"duration":     "",
			},
			expected: scenario.ScenarioParams{
				WorkloadType:        "write",
				Endpoint:            "http://s3.example.com",
				Endpoints:           []string{"http://s3.example.com"},
				AccessKey:           "key",
				SecretKey:           "secret",
				Bucket:              "bucket",
				AuthVersion:         4,
				Threads:             0,
				ObjectSize:          "1KB",
				ObjectCount:         10,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
		{
			name:         "special characters in credentials",
			workloadType: "write",
			flags: map[string]interface{}{
				"endpoint":     "http://s3.example.com",
				"access-key":   "key+with/special=chars",
				"secret-key":   "secret@with#special$chars%",
				"bucket":       "bucket-with-dash",
				"threads":      1,
				"object-size":  "1GB",
				"object-count": 1,
				"duration":     "",
			},
			expected: scenario.ScenarioParams{
				WorkloadType:        "write",
				Endpoint:            "http://s3.example.com",
				Endpoints:           []string{"http://s3.example.com"},
				AccessKey:           "key+with/special=chars",
				SecretKey:           "secret@with#special$chars%",
				Bucket:              "bucket-with-dash",
				AuthVersion:         4,
				Threads:             1,
				ObjectSize:          "1GB",
				ObjectCount:         1,
				Duration:            "",
				ObjectDataDedupable: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock command with flags
			cmd := &cobra.Command{}

			// Add all the flags that the run command would have
			cmd.Flags().String("endpoint", "", "")
			cmd.Flags().StringSlice("endpoints", []string{}, "")
			cmd.Flags().Bool("slice-endpoints", false, "")
			cmd.Flags().String("access-key", "", "")
			cmd.Flags().String("secret-key", "", "")
			cmd.Flags().String("bucket", "", "")
			cmd.Flags().String("prefix", "", "")
			cmd.Flags().Int("auth-version", 4, "")
			cmd.Flags().Int("threads", 0, "")
			cmd.Flags().String("object-size", "", "")
			cmd.Flags().Int("object-count", 0, "")
			cmd.Flags().String("duration", "", "")
			cmd.Flags().Bool("cleanup", false, "")
			cmd.Flags().Bool(flagReadShuffle, false, "")
			cmd.Flags().Int(flagReadShuffleBatchSize, 0, "")
			cmd.Flags().Bool("create-prefix", false, "")
			cmd.Flags().Int("service-threads", 0, "")
			cmd.Flags().String("s3-driver", "default", "")
			cmd.Flags().Bool("use-rdma", false, "")
			cmd.Flags().String("rdma-local-ip", "", "")
			cmd.Flags().String("rdma-threshold", "1MB", "")
			cmd.Flags().Bool("rdma-fallback", false, "")
			cmd.Flags().String("rdma-device", "auto", "")
			cmd.Flags().String("rdma-log-level", "WARN", "")
			cmd.Flags().Int64("rdma-timeout-ms", 30000, "")
			cmd.Flags().String("checksum", "", "")
			cmd.Flags().Float64("object-data-compressibility", 0.0, "")
			cmd.Flags().Bool("object-data-dedupable", true, "")

			// Set the flag values from the test case
			for flagName, value := range tt.flags {
				switch v := value.(type) {
				case string:
					if err := cmd.Flags().Set(flagName, v); err != nil {
						t.Fatalf("failed to set flag %s: %v", flagName, err)
					}
				case int:
					cmd.Flags().Set(flagName, fmt.Sprintf("%d", v))
				case bool:
					cmd.Flags().Set(flagName, fmt.Sprintf("%t", v))
				}
			}

			// Call the function
			got, err := buildScenarioParams(tt.workloadType, cmd)
			if err != nil {
				t.Fatalf("buildScenarioParams() unexpected error: %v", err)
			}

			// Check result — fill in S3Driver default if the test didn't set it
			if tt.expected.S3Driver == "" {
				tt.expected.S3Driver = scenario.S3DriverDefault
			}
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("buildScenarioParams() = %+v, want %+v", got, tt.expected)
			}
		})
	}
}

func TestBuildScenarioParamsIncludesAllVersions(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(flagVersions, scenario.VersionsCurrent, "")
	if err := cmd.Flags().Set(flagVersions, scenario.VersionsAll); err != nil {
		t.Fatal(err)
	}
	params, err := buildScenarioParams(WorkloadTypeReadVerify, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if params.Versions != scenario.VersionsAll {
		t.Fatalf("buildScenarioParams versions = %q, want %q", params.Versions, scenario.VersionsAll)
	}
}

func TestBuildScenarioParamsIncludesDeferredVerification(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Bool(flagDeferVerification, false, "")
	if err := cmd.Flags().Set(flagDeferVerification, "true"); err != nil {
		t.Fatal(err)
	}
	params, err := buildScenarioParams(WorkloadTypeWriteVerify, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !params.DeferVerification {
		t.Fatal("buildScenarioParams dropped --defer-verification")
	}
}

func TestBuildScenarioParamsEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		workloadType string
		setupFlags   func(*cobra.Command)
		validate     func(t *testing.T, params scenario.Params)
	}{
		{
			name:         "empty strings for all parameters",
			workloadType: "write",
			setupFlags: func(cmd *cobra.Command) {
				// All flags will have empty string defaults
			},
			validate: func(t *testing.T, params scenario.Params) {
				t.Helper()
				if params.WorkloadType != "write" {
					t.Errorf("Expected workloadType 'write', got %s", params.WorkloadType)
				}
				if params.ObjectSize != "1MB" {
					t.Errorf("Expected default ObjectSize '1MB', got %s", params.ObjectSize)
				}
			},
		},
		{
			name:         "very long strings",
			workloadType: "write",
			setupFlags: func(cmd *cobra.Command) {
				longString := ""
				for i := 0; i < 1000; i++ {
					longString += "a"
				}
				cmd.Flags().Set("endpoint", "http://"+longString+".example.com")
				cmd.Flags().Set("access-key", longString)
				cmd.Flags().Set("secret-key", longString)
				cmd.Flags().Set("bucket", longString)
			},
			validate: func(t *testing.T, params scenario.Params) {
				t.Helper()
				if len(params.AccessKey) != 1000 {
					t.Errorf("Expected long access key to be preserved")
				}
			},
		},
		{
			name:         "unicode in parameters",
			workloadType: "write",
			setupFlags: func(cmd *cobra.Command) {
				cmd.Flags().Set("endpoint", "http://s3.example.com")
				cmd.Flags().Set("access-key", "用户密钥")
				cmd.Flags().Set("secret-key", "秘密🔐")
				cmd.Flags().Set("bucket", "bucket-测试")
			},
			validate: func(t *testing.T, params scenario.ScenarioParams) {
				t.Helper()
				if params.AccessKey != "用户密钥" {
					t.Errorf("Unicode access key not preserved")
				}
				if params.SecretKey != "秘密🔐" {
					t.Errorf("Unicode secret key not preserved")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock command with flags
			cmd := &cobra.Command{}

			// Add all the flags
			cmd.Flags().String("endpoint", "", "")
			cmd.Flags().String("access-key", "", "")
			cmd.Flags().String("secret-key", "", "")
			cmd.Flags().String("bucket", "", "")
			cmd.Flags().Int("auth-version", 4, "")
			cmd.Flags().Int("threads", 0, "")
			cmd.Flags().String("object-size", "", "")
			cmd.Flags().Int("object-count", 0, "")
			cmd.Flags().String("duration", "", "")
			cmd.Flags().Bool(flagReadShuffle, false, "")
			cmd.Flags().Int(flagReadShuffleBatchSize, 0, "")
			cmd.Flags().String("s3-driver", "default", "")
			cmd.Flags().Bool("use-rdma", false, "")
			cmd.Flags().String("rdma-local-ip", "", "")
			cmd.Flags().String("rdma-threshold", "1MB", "")
			cmd.Flags().Bool("rdma-fallback", false, "")
			cmd.Flags().String("rdma-device", "auto", "")
			cmd.Flags().String("rdma-log-level", "WARN", "")
			cmd.Flags().Int64("rdma-timeout-ms", 30000, "")
			cmd.Flags().String("checksum", "", "")
			cmd.Flags().Float64("object-data-compressibility", 0.0, "")
			cmd.Flags().Bool("object-data-dedupable", true, "")

			// Setup flags for this test
			tt.setupFlags(cmd)

			// Call the function
			params, err := buildScenarioParams(tt.workloadType, cmd)
			if err != nil {
				t.Fatalf("buildScenarioParams() unexpected error: %v", err)
			}

			// Validate the result
			tt.validate(t, params)
		})
	}
}

func TestBuildScenarioParamsServiceThreads(t *testing.T) {
	tests := []struct {
		name     string
		value    int
		expected int
	}{
		{"zero keeps default", 0, 0},
		{"positive value passed through", 32, 32},
		{"small value passed through", 2, 2},
		{"large value passed through", 256, 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().String("endpoint", "", "")
			cmd.Flags().String("access-key", "", "")
			cmd.Flags().String("secret-key", "", "")
			cmd.Flags().String("bucket", "", "")
			cmd.Flags().Int("auth-version", 4, "")
			cmd.Flags().Int("threads", 0, "")
			cmd.Flags().String("object-size", "", "")
			cmd.Flags().Int("object-count", 0, "")
			cmd.Flags().String("duration", "", "")
			cmd.Flags().Bool(flagReadShuffle, false, "")
			cmd.Flags().Int(flagReadShuffleBatchSize, 0, "")
			cmd.Flags().Int("service-threads", 0, "")
			cmd.Flags().String("s3-driver", "default", "")
			cmd.Flags().Bool("use-rdma", false, "")
			cmd.Flags().String("rdma-local-ip", "", "")
			cmd.Flags().String("rdma-threshold", "1MB", "")
			cmd.Flags().Bool("rdma-fallback", false, "")
			cmd.Flags().String("rdma-device", "auto", "")
			cmd.Flags().String("rdma-log-level", "WARN", "")
			cmd.Flags().Int64("rdma-timeout-ms", 30000, "")
			cmd.Flags().String("checksum", "", "")

			_ = cmd.Flags().Set("service-threads", fmt.Sprintf("%d", tt.value))

			params, err := buildScenarioParams("mock", cmd)
			if err != nil {
				t.Fatalf("buildScenarioParams() unexpected error: %v", err)
			}
			if params.ServiceThreads != tt.expected {
				t.Errorf("ServiceThreads = %d, want %d", params.ServiceThreads, tt.expected)
			}
		})
	}
}

func TestBuildScenarioParams_S3DriverFlag(t *testing.T) {
	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().String("endpoint", "", "")
		cmd.Flags().StringSlice("endpoints", []string{}, "")
		cmd.Flags().Bool("slice-endpoints", false, "")
		cmd.Flags().String("access-key", "", "")
		cmd.Flags().String("secret-key", "", "")
		cmd.Flags().String("bucket", "", "")
		cmd.Flags().String("prefix", "", "")
		cmd.Flags().Int("auth-version", 4, "")
		cmd.Flags().Int("threads", 0, "")
		cmd.Flags().String("object-size", "", "")
		cmd.Flags().Int("object-count", 0, "")
		cmd.Flags().String("duration", "", "")
		cmd.Flags().Bool(flagReadShuffle, false, "")
		cmd.Flags().Int(flagReadShuffleBatchSize, 0, "")
		cmd.Flags().Bool("cleanup", false, "")
		cmd.Flags().Int("service-threads", 0, "")
		cmd.Flags().String("s3-driver", "default", "")
		cmd.Flags().Bool("use-rdma", false, "")
		cmd.Flags().String("rdma-local-ip", "", "")
		cmd.Flags().String("rdma-threshold", "1MB", "")
		cmd.Flags().Bool("rdma-fallback", false, "")
		cmd.Flags().String("rdma-device", "auto", "")
		cmd.Flags().String("rdma-log-level", "WARN", "")
		cmd.Flags().Int64("rdma-timeout-ms", 30000, "")
		cmd.Flags().String("checksum", "", "")
		cmd.Flags().Float64("object-data-compressibility", 0.0, "")
		cmd.Flags().Bool("object-data-dedupable", true, "")
		return cmd
	}

	t.Run("--s3-driver aws sets S3Driver", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("s3-driver", "aws")
		p, err := buildScenarioParams("mock", cmd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.S3Driver != scenario.S3DriverAws {
			t.Errorf("S3Driver = %q, want %q", p.S3Driver, scenario.S3DriverAws)
		}
	})

	t.Run("--use-rdma alone sets S3Driver to rdma", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("use-rdma", "true")
		p, err := buildScenarioParams("mock", cmd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.S3Driver != scenario.S3DriverRdma {
			t.Errorf("S3Driver = %q, want %q", p.S3Driver, scenario.S3DriverRdma)
		}
	})

	t.Run("--s3-driver rdma --use-rdma is not a conflict", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("s3-driver", "rdma")
		_ = cmd.Flags().Set("use-rdma", "true")
		_, err := buildScenarioParams("mock", cmd)
		if err != nil {
			t.Fatalf("expected no error for consistent flags, got: %v", err)
		}
	})

	t.Run("--s3-driver aws --use-rdma conflicts", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("s3-driver", "aws")
		_ = cmd.Flags().Set("use-rdma", "true")
		_, err := buildScenarioParams("mock", cmd)
		if err == nil {
			t.Fatal("expected conflict error")
		}
		if !strings.Contains(err.Error(), "conflicting") {
			t.Errorf("expected 'conflicting' in error, got: %v", err)
		}
	})

	t.Run("invalid --s3-driver value", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("s3-driver", "bogus")
		_, err := buildScenarioParams("mock", cmd)
		if err == nil {
			t.Fatal("expected validation error")
		}
		if !strings.Contains(err.Error(), "invalid --s3-driver") {
			t.Errorf("expected validation message, got: %v", err)
		}
	})

	t.Run("--checksum crc64-nvme with --s3-driver aws is accepted", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("s3-driver", "aws")
		_ = cmd.Flags().Set("checksum", "crc64-nvme")
		p, err := buildScenarioParams("mock", cmd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Checksum != "crc64-nvme" {
			t.Errorf("Checksum = %q, want %q", p.Checksum, "crc64-nvme")
		}
	})

	t.Run("--checksum crc64-nvme with default driver is accepted", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("checksum", "crc64-nvme")
		p, err := buildScenarioParams("mock", cmd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Checksum != "crc64-nvme" {
			t.Errorf("Checksum = %q, want %q", p.Checksum, "crc64-nvme")
		}
	})

	t.Run("--checksum crc64-nvme with rdma is accepted", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("s3-driver", "rdma")
		_ = cmd.Flags().Set("checksum", "crc64-nvme")
		p, err := buildScenarioParams("mock", cmd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Checksum != "crc64-nvme" {
			t.Errorf("Checksum = %q, want %q", p.Checksum, "crc64-nvme")
		}
	})

	t.Run("invalid --checksum value lists crc64-nvme", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("checksum", "blake3")
		_, err := buildScenarioParams("mock", cmd)
		if err == nil {
			t.Fatal("expected validation error")
		}
		if !strings.Contains(err.Error(), "crc64-nvme") {
			t.Errorf("expected crc64-nvme in validation list, got: %v", err)
		}
	})

	t.Run("object data knobs are parsed", func(t *testing.T) {
		cmd := newCmd()
		_ = cmd.Flags().Set("object-data-compressibility", "75")
		_ = cmd.Flags().Set("object-data-dedupable", "false")
		p, err := buildScenarioParams("mock", cmd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.ObjectDataCompressibility != 75 {
			t.Errorf("ObjectDataCompressibility = %v, want 75", p.ObjectDataCompressibility)
		}
		if p.ObjectDataDedupable {
			t.Errorf("ObjectDataDedupable = true, want false")
		}
	})
}
