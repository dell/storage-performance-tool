package scenario

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGenerateDefaults(t *testing.T) {
	tests := []struct {
		name        string
		params      Params
		wantErr     bool
		checkOutput func(t *testing.T, data []byte)
	}{
		{
			name: "mock workload",
			params: Params{
				WorkloadType: "mock",
				Threads:      8,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "dummy-mock" {
					t.Errorf("Expected driver type 'dummy-mock', got %s", config.Storage.Driver.Type)
				}
				if config.Storage.Driver.Limit.Concurrency != 8 {
					t.Errorf("Expected concurrency 8, got %d", config.Storage.Driver.Limit.Concurrency)
				}
				if config.Output.Metrics.Average.Period != "1s" {
					t.Errorf("Expected metrics period '1s', got %s", config.Output.Metrics.Average.Period)
				}
			},
		},
		{
			name: "S3 write workload with HTTP",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if len(config.Storage.Net.Node.Addrs) != 1 || config.Storage.Net.Node.Addrs[0] != "minio" {
					t.Errorf("Expected node address 'minio', got %v", config.Storage.Net.Node.Addrs)
				}
				if config.Storage.Net.Node.Port != 9000 {
					t.Errorf("Expected port 9000, got %d", config.Storage.Net.Node.Port)
				}
				if config.Storage.Auth.UID != "testkey" {
					t.Errorf("Expected UID 'testkey', got %s", config.Storage.Auth.UID)
				}
				if config.Storage.Auth.Secret != "testsecret" {
					t.Errorf("Expected secret 'testsecret', got %s", config.Storage.Auth.Secret)
				}
				if config.Storage.Auth.Version != 4 {
					t.Errorf("Expected auth version 4, got %d", config.Storage.Auth.Version)
				}
				if config.Storage.Net.SSL.Enabled {
					t.Error("Expected SSL to be disabled for HTTP")
				}
			},
		},
		{
			name: "S3 write workload with HTTPS",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "https://s3.amazonaws.com",
				AccessKey:    "awskey",
				SecretKey:    "awssecret",
				Bucket:       "mybucket",
				Threads:      16,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Net.Node.Port != 443 {
					t.Errorf("Expected port 443 for HTTPS, got %d", config.Storage.Net.Node.Port)
				}
				if !config.Storage.Net.SSL.Enabled {
					t.Error("Expected SSL to be enabled for HTTPS")
				}
				if config.Storage.Auth.Version != 4 {
					t.Errorf("Expected auth version 4, got %d", config.Storage.Auth.Version)
				}
			},
		},
		{
			name: "S3 list workload uses S3 driver",
			params: Params{
				WorkloadType: "list",
				Endpoint:     "http://minio:9000",
				AccessKey:    "listaccess",
				SecretKey:    "listsecret",
				Bucket:       "listbucket",
				Threads:      3,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Limit.Concurrency != 3 {
					t.Errorf("Expected concurrency 3, got %d", config.Storage.Driver.Limit.Concurrency)
				}
				if len(config.Storage.Net.Node.Addrs) != 1 || config.Storage.Net.Node.Addrs[0] != "minio" {
					t.Errorf("Expected node address 'minio', got %v", config.Storage.Net.Node.Addrs)
				}
				if config.Storage.Auth.UID != "listaccess" {
					t.Errorf("Expected UID 'listaccess', got %s", config.Storage.Auth.UID)
				}
				if config.Storage.Auth.Secret != "listsecret" {
					t.Errorf("Expected secret 'listsecret', got %s", config.Storage.Auth.Secret)
				}
				if config.Storage.Auth.Version != 4 {
					t.Errorf("Expected auth version 4, got %d", config.Storage.Auth.Version)
				}
			},
		},
		{
			name: "S3 workload without endpoint",
			params: Params{
				WorkloadType: "read",
				AccessKey:    "key",
				SecretKey:    "secret",
			},
			wantErr: true,
		},
		{
			name: "Invalid workload type",
			params: Params{
				WorkloadType: "invalid",
			},
			wantErr: true,
		},
		{
			name: "S3 with custom port",
			params: ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://localhost:8080",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      2,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Net.Node.Port != 8080 {
					t.Errorf("Expected port 8080, got %d", config.Storage.Net.Node.Port)
				}
			},
		},
		{
			name: "S3 write with multiple HTTP endpoints (same port)",
			params: Params{
				WorkloadType:   "write",
				Endpoints:      []string{"http://s3a:9000", "http://s3b:9000", "http://s3a:9000"}, // includes dup
				AccessKey:      "k",
				SecretKey:      "s",
				Bucket:         "b",
				Threads:        4,
				SliceEndpoints: false,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var cfg DefaultsConfig
				if err := yaml.Unmarshal(data, &cfg); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if got := cfg.Storage.Net.SSL.Enabled; got {
					t.Errorf("expected ssl.enabled=false, got true")
				}
				if cfg.Storage.Net.Node.Port != 9000 {
					t.Errorf("expected node.port=9000, got %d", cfg.Storage.Net.Node.Port)
				}
				if len(cfg.Storage.Net.Node.Addrs) != 2 {
					t.Fatalf("expected 2 unique addrs, got %v", cfg.Storage.Net.Node.Addrs)
				}
				if cfg.Storage.Net.Node.Addrs[0] != "s3a" || cfg.Storage.Net.Node.Addrs[1] != "s3b" {
					t.Errorf("unexpected addrs: %v", cfg.Storage.Net.Node.Addrs)
				}
				if cfg.Storage.Auth.Version != 4 {
					t.Errorf("expected auth version 4, got %d", cfg.Storage.Auth.Version)
				}
			},
		},
		{
			name: "S3 write with multiple HTTP endpoints (mixed ports)",
			params: Params{
				WorkloadType: "write",
				Endpoints:    []string{"http://s3a:9000", "http://s3b:9001"},
				AccessKey:    "k",
				SecretKey:    "s",
				Bucket:       "b",
				Threads:      4,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var cfg DefaultsConfig
				if err := yaml.Unmarshal(data, &cfg); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if cfg.Storage.Net.Node.Port != 0 {
					t.Errorf("expected no node.port, got %d", cfg.Storage.Net.Node.Port)
				}
				if len(cfg.Storage.Net.Node.Addrs) != 2 {
					t.Fatalf("expected 2 addrs, got %v", cfg.Storage.Net.Node.Addrs)
				}
				if cfg.Storage.Net.Node.Addrs[0] != "s3a:9000" || cfg.Storage.Net.Node.Addrs[1] != "s3b:9001" {
					t.Errorf("unexpected addrs: %v", cfg.Storage.Net.Node.Addrs)
				}
				if cfg.Storage.Auth.Version != 4 {
					t.Errorf("expected auth version 4, got %d", cfg.Storage.Auth.Version)
				}
			},
		},
		{
			name: "S3 with mixed schemes should error",
			params: Params{
				WorkloadType: "write",
				Endpoints:    []string{"http://s3a:9000", "https://s3b"},
				AccessKey:    "k",
				SecretKey:    "s",
				Bucket:       "b",
				Threads:      1,
			},
			wantErr: true,
		},
		{
			name: "SliceEndpoints enabled",
			params: Params{
				WorkloadType:   "write",
				Endpoints:      []string{"http://s3a:9000", "http://s3b:9000"},
				AccessKey:      "k",
				SecretKey:      "s",
				Bucket:         "b",
				Threads:        1,
				SliceEndpoints: true,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var cfg DefaultsConfig
				if err := yaml.Unmarshal(data, &cfg); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if !cfg.Storage.Net.Node.Slice {
					t.Errorf("expected node.slice=true")
				}
				if cfg.Storage.Auth.Version != 4 {
					t.Errorf("expected auth version 4, got %d", cfg.Storage.Auth.Version)
				}
			},
		},
		{
			name: "Auth version overrides to v2 when requested",
			params: Params{
				WorkloadType: "list",
				Endpoint:     "http://s3.example.com:9000",
				AccessKey:    "ak",
				SecretKey:    "sk",
				Bucket:       "bucket",
				Threads:      2,
				AuthVersion:  2,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var cfg DefaultsConfig
				if err := yaml.Unmarshal(data, &cfg); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if cfg.Storage.Auth.Version != 2 {
					t.Errorf("expected auth version 2, got %d", cfg.Storage.Auth.Version)
				}
			},
		},
		{
			name: "S3 write with RDMA enabled (defaults)",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
				UseRdma:      true,
				RdmaFallback: true,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "s3-rdma" {
					t.Errorf("Expected driver type 's3-rdma', got %s", config.Storage.Driver.Type)
				}
				if config.Storage.Rdma == nil {
					t.Fatal("Expected storage.rdma section to be present")
				}
				if config.Storage.Rdma.Threshold != 1048576 {
					t.Errorf("Expected default threshold 1048576, got %d", config.Storage.Rdma.Threshold)
				}
				if !config.Storage.Rdma.Fallback {
					t.Error("Expected fallback to be true")
				}
				if config.Storage.Rdma.Device != "auto" {
					t.Errorf("Expected default device 'auto', got %s", config.Storage.Rdma.Device)
				}
				if config.Storage.Rdma.LogLevel != "WARN" {
					t.Errorf("Expected default log level 'WARN', got %s", config.Storage.Rdma.LogLevel)
				}
				if config.Storage.Rdma.TimeoutMs != 30000 {
					t.Errorf("Expected default timeout 30000, got %d", config.Storage.Rdma.TimeoutMs)
				}
			},
		},
		{
			name: "S3 write with RDMA custom settings",
			params: Params{
				WorkloadType:       "write",
				Endpoint:           "http://minio:9000",
				AccessKey:          "testkey",
				SecretKey:          "testsecret",
				Bucket:             "testbucket",
				Threads:            16,
				UseRdma:            true,
				RdmaLocalIp:        "10.247.128.125",
				RdmaThresholdBytes: 4194304,
				RdmaFallback:       false,
				RdmaDevice:         "mlx5_0",
				RdmaLogLevel:       "DEBUG",
				RdmaTimeoutMs:      60000,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "s3-rdma" {
					t.Errorf("Expected driver type 's3-rdma', got %s", config.Storage.Driver.Type)
				}
				if config.Storage.Rdma == nil {
					t.Fatal("Expected storage.rdma section to be present")
				}
				if config.Storage.Rdma.LocalIp != "10.247.128.125" {
					t.Errorf("Expected localIp '10.247.128.125', got %s", config.Storage.Rdma.LocalIp)
				}
				if config.Storage.Rdma.Threshold != 4194304 {
					t.Errorf("Expected threshold 4194304, got %d", config.Storage.Rdma.Threshold)
				}
				if config.Storage.Rdma.Fallback {
					t.Error("Expected fallback to be false")
				}
				if config.Storage.Rdma.Device != "mlx5_0" {
					t.Errorf("Expected device 'mlx5_0', got %s", config.Storage.Rdma.Device)
				}
				if config.Storage.Rdma.LogLevel != "DEBUG" {
					t.Errorf("Expected log level 'DEBUG', got %s", config.Storage.Rdma.LogLevel)
				}
				if config.Storage.Rdma.TimeoutMs != 60000 {
					t.Errorf("Expected timeout 60000, got %d", config.Storage.Rdma.TimeoutMs)
				}
			},
		},
		{
			name: "S3 write without RDMA has no rdma section",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "" {
					t.Errorf("Expected no driver type for non-RDMA S3, got %s", config.Storage.Driver.Type)
				}
				if config.Storage.Rdma != nil {
					t.Error("Expected no storage.rdma section when RDMA is not enabled")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := GenerateDefaults(tt.params)

			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateDefaults() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil && tt.checkOutput != nil {
				// Check that it's valid YAML
				yamlStr := string(data)
				if !strings.Contains(yamlStr, "storage:") && !strings.Contains(yamlStr, "output:") {
					t.Error("Generated YAML missing expected top-level keys")
				}

				// Run specific checks
				tt.checkOutput(t, data)
			}
		})
	}
}
