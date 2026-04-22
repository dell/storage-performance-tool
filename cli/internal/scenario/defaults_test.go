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
				MpuObjects:   2,
				MpuParts:     3,
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
				if config.Storage.Driver.Limit.Concurrency != 4 {
					t.Errorf("Expected concurrency 4, got %d", config.Storage.Driver.Limit.Concurrency)
				}
				if config.Storage.Driver.Limit.Multipart == nil {
					t.Fatal("Expected multipart limits to be set, got nil")
				}
				if config.Storage.Driver.Limit.Multipart.Objects != 2 {
					t.Errorf("Expected MPU objects 2, got %d", config.Storage.Driver.Limit.Multipart.Objects)
				}
				if config.Storage.Driver.Limit.Multipart.Parts != 3 {
					t.Errorf("Expected MPU parts 3, got %d", config.Storage.Driver.Limit.Multipart.Parts)
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
			name: "S3 write with RDMA enabled (Cobra defaults)",
			params: Params{
				WorkloadType:       "write",
				Endpoint:           "http://minio:9000",
				AccessKey:          "testkey",
				SecretKey:          "testsecret",
				Bucket:             "testbucket",
				Threads:            4,
				S3Driver:           S3DriverRdma,
				RdmaFallback:       true,
				RdmaThresholdBytes: 1048576, // Cobra default
				RdmaTimeoutMs:      30000,   // Cobra default
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
			name: "S3 write with RDMA threshold zero",
			params: Params{
				WorkloadType:       "write",
				Endpoint:           "http://minio:9000",
				AccessKey:          "testkey",
				SecretKey:          "testsecret",
				Bucket:             "testbucket",
				Threads:            4,
				S3Driver:           S3DriverRdma,
				RdmaFallback:       true,
				RdmaThresholdBytes: 0,     // explicit zero — force RDMA for all sizes
				RdmaTimeoutMs:      30000, // Cobra default
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Rdma == nil {
					t.Fatal("Expected storage.rdma section to be present")
				}
				if config.Storage.Rdma.Threshold != 0 {
					t.Errorf("Expected threshold 0, got %d", config.Storage.Rdma.Threshold)
				}
				// Verify "thresholdBytes: 0" appears in raw YAML (not dropped by omitempty)
				if !strings.Contains(string(data), "thresholdBytes: 0") {
					t.Error("Expected raw YAML to contain 'thresholdBytes: 0' but it was omitted")
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
				S3Driver:           S3DriverRdma,
				RdmaLocalIP:        "10.247.128.125",
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
				if config.Storage.Rdma.LocalIP != "10.247.128.125" {
					t.Errorf("Expected localIp '10.247.128.125', got %s", config.Storage.Rdma.LocalIP)
				}
				if config.Storage.Rdma.Threshold != 4194304 {
					t.Errorf("Expected threshold 4194304, got %d", config.Storage.Rdma.Threshold)
				}
				if config.Storage.Rdma.Fallback {
					t.Error("Expected fallback to be false")
				}
				// Verify "fallback: false" is present in raw YAML (not omitted by omitempty)
				if !strings.Contains(string(data), "fallback: false") {
					t.Error("Expected raw YAML to contain 'fallback: false' but it was omitted")
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
		{
			name: "S3 list workload with RDMA sets s3-rdma driver in defaults",
			params: Params{
				WorkloadType: "list",
				Endpoint:     "http://minio:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "listbucket",
				Threads:      4,
				S3Driver:     S3DriverRdma,
				RdmaFallback: true,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				// defaults.yaml configures the engine driver; s3-rdma handles all S3 ops
				if config.Storage.Driver.Type != "s3-rdma" {
					t.Errorf("Expected driver type 's3-rdma' for list+RDMA defaults, got %s", config.Storage.Driver.Type)
				}
				if config.Storage.Rdma == nil {
					t.Fatal("Expected storage.rdma section in defaults when RDMA enabled")
				}
			},
		},
		{
			name: "Mock workload ignores S3Driver",
			params: Params{
				WorkloadType: "mock",
				Threads:      4,
				S3Driver:     S3DriverRdma, // should be ignored for mock workloads
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "dummy-mock" {
					t.Errorf("Mock workload should use dummy-mock driver, got %s", config.Storage.Driver.Type)
				}
				if config.Storage.Rdma != nil {
					t.Error("Mock workload should not have storage.rdma section")
				}
			},
		},
		{
			name: "S3 write with AWS driver",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
				S3Driver:     S3DriverAws,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "s3-aws" {
					t.Errorf("Expected driver type 's3-aws', got %q", config.Storage.Driver.Type)
				}
				if config.Storage.Rdma != nil {
					t.Error("AWS driver should not have storage.rdma section")
				}
			},
		},
		{
			name: "S3 write with default driver omits driver type",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
				S3Driver:     S3DriverDefault,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "" {
					t.Errorf("Default driver should omit type, got %q", config.Storage.Driver.Type)
				}
			},
		},
		{
			name: "S3 write with ServiceThreads sets load.service.threads",
			params: Params{
				WorkloadType:   "write",
				Endpoint:       "http://minio:9000",
				AccessKey:      "testkey",
				SecretKey:      "testsecret",
				Bucket:         "testbucket",
				Threads:        4,
				ServiceThreads: 32,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Load == nil {
					t.Fatal("Expected load section to be present when ServiceThreads > 0")
				}
				if config.Load.Service.Threads != 32 {
					t.Errorf("Expected load.service.threads 32, got %d", config.Load.Service.Threads)
				}
				// Verify the raw YAML path matches engine schema
				if !strings.Contains(string(data), "load:") {
					t.Error("Expected raw YAML to contain 'load:' top-level key")
				}
				if !strings.Contains(string(data), "service:") {
					t.Error("Expected raw YAML to contain 'service:' key under load")
				}
				if !strings.Contains(string(data), "threads: 32") {
					t.Error("Expected raw YAML to contain 'threads: 32'")
				}
			},
		},
		{
			name: "ObjectDataCompressibility writes item.data.input.compressibility",
			params: Params{
				WorkloadType:             "write",
				Endpoint:                 "http://minio:9000",
				AccessKey:                "testkey",
				SecretKey:                "testsecret",
				Bucket:                   "testbucket",
				Threads:                  4,
				ObjectDataCompressibility: 72.5,
				ObjectDataDedupable:      true,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				raw := string(data)
				if !strings.Contains(raw, "item:") {
					t.Error("expected item section in defaults output")
				}
				if !strings.Contains(raw, "compressibility: 72.5") {
					t.Error("expected item.data.input.compressibility: 72.5")
				}
			},
		},
		{
			name: "ObjectDataDedupable false writes item.data.dedupable false",
			params: Params{
				WorkloadType:        "write",
				Endpoint:            "http://minio:9000",
				AccessKey:           "testkey",
				SecretKey:           "testsecret",
				Bucket:              "testbucket",
				Threads:             4,
				ObjectDataDedupable: false,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				raw := string(data)
				if !strings.Contains(raw, "dedupable: false") {
					t.Error("expected item.data.dedupable: false")
				}
			},
		},
		{
			name: "ServiceThreads zero omits load section",
			params: Params{
				WorkloadType:   "write",
				Endpoint:       "http://minio:9000",
				AccessKey:      "testkey",
				SecretKey:      "testsecret",
				Bucket:         "testbucket",
				Threads:        4,
				ServiceThreads: 0,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Load != nil {
					t.Error("Expected no load section when ServiceThreads is 0")
				}
				if strings.Contains(string(data), "load:") {
					t.Error("Raw YAML should not contain 'load:' when ServiceThreads is 0")
				}
			},
		},
		{
			name: "ServiceThreads negative omits load section",
			params: Params{
				WorkloadType:   "write",
				Endpoint:       "http://minio:9000",
				AccessKey:      "testkey",
				SecretKey:      "testsecret",
				Bucket:         "testbucket",
				Threads:        4,
				ServiceThreads: -1,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Load != nil {
					t.Error("Expected no load section when ServiceThreads is negative")
				}
			},
		},
		{
			name: "Mock workload with ServiceThreads still sets load section",
			params: Params{
				WorkloadType:   "mock",
				Threads:        8,
				ServiceThreads: 16,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "dummy-mock" {
					t.Errorf("Expected dummy-mock driver, got %s", config.Storage.Driver.Type)
				}
				if config.Load == nil {
					t.Fatal("Expected load section for mock workload when ServiceThreads > 0")
				}
				if config.Load.Service.Threads != 16 {
					t.Errorf("Expected load.service.threads 16, got %d", config.Load.Service.Threads)
				}
			},
		},
	}

	// S3 Tables cases
	tablesCases := []struct {
		name        string
		params      Params
		wantErr     bool
		checkOutput func(t *testing.T, data []byte)
	}{
		{
			name: "tables workload HTTP",
			params: Params{
				WorkloadType: "tables",
				Endpoint:     "http://172.17.0.2:4566",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Tables: TablesParams{
					ConcurrentWriters: 8,
				},
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "s3-tables" {
					t.Errorf("Expected driver type 's3-tables', got %q", config.Storage.Driver.Type)
				}
				if config.Storage.Driver.Limit.Concurrency != 8 {
					t.Errorf("Expected concurrency 8, got %d", config.Storage.Driver.Limit.Concurrency)
				}
				if config.Storage.Net.SSL.Enabled {
					t.Error("HTTP endpoint should have SSL disabled")
				}
				if config.Storage.Net.Node.Port != 4566 {
					t.Errorf("Expected port 4566, got %d", config.Storage.Net.Node.Port)
				}
				if len(config.Storage.Net.Node.Addrs) != 1 || config.Storage.Net.Node.Addrs[0] != "172.17.0.2" {
					t.Errorf("Expected addr '172.17.0.2', got %v", config.Storage.Net.Node.Addrs)
				}
				if config.Storage.Auth.UID != "testkey" {
					t.Errorf("Expected uid 'testkey', got %q", config.Storage.Auth.UID)
				}
				if config.Storage.Auth.Version != 4 {
					t.Errorf("Expected auth version 4, got %d", config.Storage.Auth.Version)
				}
			},
		},
		{
			name: "tables workload HTTPS",
			params: Params{
				WorkloadType: "tables",
				Endpoint:     "https://s3tables.us-east-1.amazonaws.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Tables: TablesParams{
					ConcurrentWriters: 4,
				},
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if !config.Storage.Net.SSL.Enabled {
					t.Error("HTTPS endpoint should have SSL enabled")
				}
				if config.Storage.Net.Node.Port != 443 {
					t.Errorf("Expected port 443, got %d", config.Storage.Net.Node.Port)
				}
			},
		},
		{
			name: "tables workload missing endpoint",
			params: Params{
				WorkloadType: "tables",
				AccessKey:    "key",
				SecretKey:    "secret",
			},
			wantErr: true,
		},
	}
	for _, tt := range tablesCases {
		tests = append(tests, tt)
	}

	// Multipart upload (part-size) test cases
	mpuCases := []struct {
		name        string
		params      Params
		wantErr     bool
		checkOutput func(t *testing.T, data []byte)
	}{
		{
			name: "write with part-size sets batch size 1",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://s3.example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
				PartSize:     "64MB",
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Load == nil {
					t.Fatal("Expected Load config to be set")
				}
				if config.Load.Batch == nil {
					t.Fatal("Expected Load.Batch config to be set for MPU")
				}
				if config.Load.Batch.Size != 1 {
					t.Errorf("Expected batch size 1 for MPU, got %d", config.Load.Batch.Size)
				}
			},
		},
		{
			name: "write without part-size has no batch override",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://s3.example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				yamlStr := string(data)
				if strings.Contains(yamlStr, "batch:") {
					t.Error("Expected no batch config when part-size is not set")
				}
			},
		},
		{
			name: "part-size with service-threads sets both",
			params: Params{
				WorkloadType:   "write",
				Endpoint:       "http://s3.example.com",
				AccessKey:      "key",
				SecretKey:      "secret",
				Bucket:         "bucket",
				Threads:        4,
				PartSize:       "64MB",
				ServiceThreads: 8,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Load == nil {
					t.Fatal("Expected Load config")
				}
				if config.Load.Batch == nil || config.Load.Batch.Size != 1 {
					t.Error("Expected batch size 1 for MPU")
				}
				if config.Load.Service == nil || config.Load.Service.Threads != 8 {
					t.Error("Expected service threads 8")
				}
			},
		},
	}
	for _, tt := range mpuCases {
		tests = append(tests, tt)
	}

	// Checksum test cases
	checksumCases := []struct {
		name        string
		params      Params
		wantErr     bool
		checkOutput func(t *testing.T, data []byte)
	}{
		{
			name: "checksum crc32 enables checksum section",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://s3.example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
				Checksum:     "crc32",
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Checksum == nil {
					t.Fatal("Expected storage.checksum section to be present")
				}
				if !config.Storage.Checksum.Enabled {
					t.Error("Expected checksum.enabled to be true")
				}
				if config.Storage.Checksum.Algorithm != "crc32" {
					t.Errorf("Expected algorithm 'crc32', got %q", config.Storage.Checksum.Algorithm)
				}
			},
		},
		{
			name: "checksum sha256",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://s3.example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
				Checksum:     "sha256",
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Checksum == nil {
					t.Fatal("Expected storage.checksum section")
				}
				if config.Storage.Checksum.Algorithm != "sha256" {
					t.Errorf("Expected algorithm 'sha256', got %q", config.Storage.Checksum.Algorithm)
				}
			},
		},
		{
			name: "no checksum omits checksum section",
			params: Params{
				WorkloadType: "write",
				Endpoint:     "http://s3.example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Checksum != nil {
					t.Error("Expected no storage.checksum section when checksum is empty")
				}
				if strings.Contains(string(data), "checksum:") {
					t.Error("Raw YAML should not contain 'checksum:' when disabled")
				}
			},
		},
		{
			name: "checksum with RDMA both present",
			params: Params{
				WorkloadType:       "write",
				Endpoint:           "http://s3.example.com",
				AccessKey:          "key",
				SecretKey:          "secret",
				Bucket:             "bucket",
				Threads:            4,
				S3Driver:           S3DriverRdma,
				RdmaThresholdBytes: 0,
				RdmaTimeoutMs:      30000,
				Checksum:           "crc32c",
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Checksum == nil {
					t.Fatal("Expected checksum section with RDMA")
				}
				if config.Storage.Checksum.Algorithm != "crc32c" {
					t.Errorf("Expected algorithm 'crc32c', got %q", config.Storage.Checksum.Algorithm)
				}
				if config.Storage.Rdma == nil {
					t.Fatal("Expected RDMA section with checksum")
				}
			},
		},
	}
	for _, tt := range checksumCases {
		tests = append(tests, tt)
	}

	// Mixed workload cases
	mixedCases := []struct {
		name        string
		params      Params
		wantErr     bool
		checkOutput func(t *testing.T, data []byte)
	}{
		{
			name: "mixed workload uses S3 driver and concurrency from threads",
			params: Params{
				WorkloadType: "mixed",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      10,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Limit.Concurrency != 10 {
					t.Errorf("Expected concurrency 10, got %d", config.Storage.Driver.Limit.Concurrency)
				}
				if config.Storage.Net.Node.Port != 9000 {
					t.Errorf("Expected port 9000, got %d", config.Storage.Net.Node.Port)
				}
				if len(config.Storage.Net.Node.Addrs) != 1 || config.Storage.Net.Node.Addrs[0] != "minio" {
					t.Errorf("Expected node address 'minio', got %v", config.Storage.Net.Node.Addrs)
				}
				if config.Storage.Auth.UID != "testkey" {
					t.Errorf("Expected UID 'testkey', got %s", config.Storage.Auth.UID)
				}
				if config.Storage.Auth.Version != 4 {
					t.Errorf("Expected auth version 4, got %d", config.Storage.Auth.Version)
				}
			},
		},
		{
			name: "mixed workload with AWS driver sets s3-aws type",
			params: Params{
				WorkloadType: "mixed",
				Endpoint:     "http://minio:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
				S3Driver:     S3DriverAws,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "s3-aws" {
					t.Errorf("Expected driver type 's3-aws', got %q", config.Storage.Driver.Type)
				}
			},
		},
		{
			name: "mixed workload with RDMA driver sets rdma config",
			params: Params{
				WorkloadType:       "mixed",
				Endpoint:           "http://minio:9000",
				AccessKey:          "key",
				SecretKey:          "secret",
				Bucket:             "bucket",
				Threads:            8,
				S3Driver:           S3DriverRdma,
				RdmaFallback:       true,
				RdmaThresholdBytes: 1048576,
				RdmaTimeoutMs:      30000,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Driver.Type != "s3-rdma" {
					t.Errorf("Expected driver type 's3-rdma', got %q", config.Storage.Driver.Type)
				}
				if config.Storage.Rdma == nil {
					t.Fatal("Expected storage.rdma section for mixed+RDMA")
				}
				if !config.Storage.Rdma.Fallback {
					t.Error("Expected RDMA fallback to be true")
				}
			},
		},
		{
			name: "mixed workload with checksum sets checksum config",
			params: Params{
				WorkloadType: "mixed",
				Endpoint:     "http://minio:9000",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
				Checksum:     "crc32c",
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if config.Storage.Checksum == nil {
					t.Fatal("Expected checksum section for mixed workload")
				}
				if !config.Storage.Checksum.Enabled {
					t.Error("Expected checksum.enabled to be true")
				}
				if config.Storage.Checksum.Algorithm != "crc32c" {
					t.Errorf("Expected algorithm 'crc32c', got %q", config.Storage.Checksum.Algorithm)
				}
			},
		},
		{
			name: "mixed workload with HTTPS enables SSL",
			params: Params{
				WorkloadType: "mixed",
				Endpoint:     "https://s3.example.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      4,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if !config.Storage.Net.SSL.Enabled {
					t.Error("Expected SSL enabled for HTTPS endpoint")
				}
				if config.Storage.Net.Node.Port != 443 {
					t.Errorf("Expected port 443, got %d", config.Storage.Net.Node.Port)
				}
			},
		},
		{
			name: "mixed workload with multiple endpoints",
			params: Params{
				WorkloadType: "mixed",
				Endpoints:    []string{"http://s3a:9000", "http://s3b:9000"},
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
				Threads:      8,
			},
			wantErr: false,
			checkOutput: func(t *testing.T, data []byte) {
				t.Helper()
				var config DefaultsConfig
				if err := yaml.Unmarshal(data, &config); err != nil {
					t.Fatalf("Failed to unmarshal YAML: %v", err)
				}
				if len(config.Storage.Net.Node.Addrs) != 2 {
					t.Fatalf("Expected 2 addrs, got %v", config.Storage.Net.Node.Addrs)
				}
				if config.Storage.Net.Node.Addrs[0] != "s3a" || config.Storage.Net.Node.Addrs[1] != "s3b" {
					t.Errorf("Unexpected addrs: %v", config.Storage.Net.Node.Addrs)
				}
			},
		},
		{
			name: "mixed workload without endpoint fails",
			params: Params{
				WorkloadType: "mixed",
				AccessKey:    "key",
				SecretKey:    "secret",
			},
			wantErr: true,
		},
	}
	for _, tt := range mixedCases {
		tests = append(tests, tt)
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
