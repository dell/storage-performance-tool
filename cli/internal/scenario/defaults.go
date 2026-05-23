// Package scenario generates Spt scenarios and defaults from user params.
package scenario

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"gopkg.in/yaml.v3"
)

const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// DefaultsConfig represents the Spt defaults configuration
type DefaultsConfig struct {
	Storage StorageConfig `yaml:"storage"`
	Item    *ItemConfig   `yaml:"item,omitempty"` // pointer so omitted when nil
	Output  OutputConfig  `yaml:"output"`
	Load    *LoadConfig   `yaml:"load,omitempty"` // pointer so omitted when nil
}

// StorageConfig represents storage configuration
type StorageConfig struct {
	Driver   DriverConfig    `yaml:"driver,omitempty"`
	Net      NetConfig       `yaml:"net,omitempty"`
	Auth     AuthConfig      `yaml:"auth,omitempty"`
	Checksum *ChecksumConfig `yaml:"checksum,omitempty"` // pointer so omitted when nil
	Rdma     *RdmaConfig     `yaml:"rdma,omitempty"`     // pointer so omitted when nil
}

// ItemConfig represents item-level configuration knobs.
type ItemConfig struct {
	Data *ItemDataConfig `yaml:"data,omitempty"`
}

// ItemDataConfig represents item.data configuration.
type ItemDataConfig struct {
	Input     *ItemDataInputConfig `yaml:"input,omitempty"`
	Dedupable *bool                `yaml:"dedupable,omitempty"`
}

// ItemDataInputConfig represents item.data.input configuration.
type ItemDataInputConfig struct {
	Compressibility float64 `yaml:"compressibility,omitempty"`
}

// ChecksumConfig represents storage checksum configuration
type ChecksumConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Algorithm string `yaml:"algorithm"`
}

// RdmaConfig represents RDMA acceleration configuration for the s3-rdma driver
type RdmaConfig struct {
	Threshold int64  `yaml:"thresholdBytes"`
	Fallback  bool   `yaml:"fallback"`
	Device    string `yaml:"device,omitempty"`
	LocalIP   string `yaml:"localIp,omitempty"`
	LogLevel  string `yaml:"logLevel,omitempty"`
	TimeoutMs int64  `yaml:"timeoutMs"`
}

// DriverConfig represents storage driver configuration
type DriverConfig struct {
	Type  string       `yaml:"type,omitempty"` // For mock workloads
	Limit DriverLimits `yaml:"limit,omitempty"`
}

// DriverLimits represents driver limit configuration
type DriverLimits struct {
	Concurrency int              `yaml:"concurrency,omitempty"`
	Multipart   *MultipartLimits `yaml:"multipart,omitempty"`
}

// MultipartLimits represents multipart upload concurrency limits
type MultipartLimits struct {
	Objects int `yaml:"objects"`
	Parts   int `yaml:"parts"`
}

// NetConfig represents network configuration
type NetConfig struct {
	Node NodeConfig `yaml:"node,omitempty"`
	SSL  SSLConfig  `yaml:"ssl,omitempty"`
}

// NodeConfig represents node configuration
type NodeConfig struct {
	Addrs []string `yaml:"addrs,omitempty"`
	Port  int      `yaml:"port,omitempty"`
	Slice bool     `yaml:"slice,omitempty"`
}

// SSLConfig represents SSL configuration
type SSLConfig struct {
	Enabled bool `yaml:"enabled,omitempty"`
}

// AuthConfig represents authentication configuration
type AuthConfig struct {
	UID     string `yaml:"uid,omitempty"`
	Secret  string `yaml:"secret,omitempty"`
	Version int    `yaml:"version,omitempty"`
}

// OutputConfig represents output configuration
type OutputConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
}

// MetricsConfig represents metrics configuration
type MetricsConfig struct {
	Average AverageConfig `yaml:"average"`
}

// AverageConfig represents average metrics configuration
type AverageConfig struct {
	Period string `yaml:"period"`
}

// LoadConfig represents engine load configuration
type LoadConfig struct {
	Service *ServiceConfig `yaml:"service,omitempty"`
}

// ServiceConfig represents the service thread pool configuration
type ServiceConfig struct {
	Threads int `yaml:"threads"`
}

// GenerateDefaults creates a defaults.yaml configuration from scenario parameters
func GenerateDefaults(params Params) ([]byte, error) {
	config := DefaultsConfig{
		Output: OutputConfig{
			Metrics: MetricsConfig{
				Average: AverageConfig{
					Period: "1s", // Our Phase 1 improvement for faster metrics
				},
			},
		},
	}

	// Configure based on workload type
	switch params.WorkloadType {
	case "mock":
		// For mock workloads, use dummy-mock driver
		config.Storage.Driver = DriverConfig{
			Type: "dummy-mock",
			Limit: DriverLimits{
				Concurrency: params.Threads,
			},
		}

	case "tables":
		// For tables workloads, endpoint/auth are configured identically to S3 workloads.
		// The s3-tables driver extends S3StorageDriver and uses the same net config.
		authVersion := params.AuthVersion
		if authVersion == 0 {
			authVersion = 4
		}
		eps := make([]string, 0, len(params.Endpoints))
		for _, e := range params.Endpoints {
			if s := strings.TrimSpace(e); s != "" {
				eps = append(eps, s)
			}
		}
		if len(eps) == 0 && strings.TrimSpace(params.Endpoint) != "" {
			eps = []string{strings.TrimSpace(params.Endpoint)}
		}
		if len(eps) == 0 {
			return nil, fmt.Errorf("endpoint is required for tables workload")
		}
		u, err := url.Parse(eps[0])
		if err != nil {
			return nil, fmt.Errorf("invalid endpoint URL: %w", err)
		}
		portStr := u.Port()
		if portStr == "" {
			if u.Scheme == schemeHTTPS {
				portStr = constants.DefaultHTTPSPort
			} else {
				portStr = constants.DefaultHTTPPort
			}
		}
		var portInt int
		if _, scanErr := fmt.Sscanf(portStr, "%d", &portInt); scanErr != nil {
			return nil, fmt.Errorf("invalid port in endpoint %q", eps[0])
		}
		config.Storage = StorageConfig{
			Driver: DriverConfig{
				Type:  storageDriverTypeS3Tables,
				Limit: DriverLimits{Concurrency: params.Tables.ConcurrentWriters},
			},
			Net: NetConfig{
				Node: NodeConfig{
					Addrs: []string{u.Hostname()},
					Port:  portInt,
				},
				SSL: SSLConfig{Enabled: u.Scheme == schemeHTTPS},
			},
			Auth: AuthConfig{UID: params.AccessKey, Secret: params.SecretKey, Version: authVersion},
		}

	case "write", "read", "mixed", "delete", "list":
		// For S3 workloads, parse one or more endpoints and configure accordingly
		authVersion := params.AuthVersion
		if authVersion == 0 {
			authVersion = 4
		}
		// Build working set of endpoints
		eps := make([]string, 0, len(params.Endpoints))
		for _, e := range params.Endpoints {
			if s := strings.TrimSpace(e); s != "" {
				eps = append(eps, s)
			}
		}
		// Migration aid: allow comma-separated single Endpoint
		if len(eps) == 0 && strings.TrimSpace(params.Endpoint) != "" {
			if strings.Contains(params.Endpoint, ",") {
				parts := strings.Split(params.Endpoint, ",")
				for _, p := range parts {
					if s := strings.TrimSpace(p); s != "" {
						eps = append(eps, s)
					}
				}
			} else {
				eps = []string{strings.TrimSpace(params.Endpoint)}
			}
		}
		if len(eps) == 0 {
			return nil, fmt.Errorf("endpoint is required for %s workload", params.WorkloadType)
		}

		type hostPort struct {
			host string
			port int
		}
		parsed := make([]hostPort, 0, len(eps))
		scheme := ""

		for _, ep := range eps {
			u, err := url.Parse(ep)
			if err != nil {
				return nil, fmt.Errorf("invalid endpoint URL: %w", err)
			}
			if u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS {
				return nil, fmt.Errorf("invalid endpoint URL: unsupported scheme %q (must be http or https)", u.Scheme)
			}
			if scheme == "" {
				scheme = u.Scheme
			} else if scheme != u.Scheme {
				return nil, fmt.Errorf("all endpoints must use the same scheme; found %q and %q", scheme, u.Scheme)
			}

			h := u.Hostname()
			p := u.Port()
			if p == "" {
				if u.Scheme == schemeHTTPS {
					p = constants.DefaultHTTPSPort
				} else {
					p = constants.DefaultHTTPPort
				}
			}
			var pi int
			if _, err := fmt.Sscanf(p, "%d", &pi); err != nil {
				return nil, fmt.Errorf("invalid port: %s", p)
			}
			parsed = append(parsed, hostPort{host: h, port: pi})
		}

		// Deduplicate while preserving order
		seen := make(map[string]struct{}, len(parsed))
		uniq := make([]hostPort, 0, len(parsed))
		for _, hp := range parsed {
			key := fmt.Sprintf("%s:%d", hp.host, hp.port)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			uniq = append(uniq, hp)
		}

		// Determine if ports are uniform
		uniformPort := true
		refPort := uniq[0].port
		for _, hp := range uniq[1:] {
			if hp.port != refPort {
				uniformPort = false
				break
			}
		}

		// Build node.addrs and optional node.port
		addrs := make([]string, 0, len(uniq))
		if uniformPort {
			for _, hp := range uniq {
				addrs = append(addrs, hp.host)
			}
		} else {
			for _, hp := range uniq {
				addrs = append(addrs, fmt.Sprintf("%s:%d", hp.host, hp.port))
			}
		}

		var multipartLimits *MultipartLimits
		if params.MpuObjects > 0 || params.MpuParts > 0 {
			multipartLimits = &MultipartLimits{
				Objects: params.MpuObjects,
				Parts:   params.MpuParts,
			}
		}

		config.Storage = StorageConfig{
			Driver: DriverConfig{
				Limit: DriverLimits{
					Concurrency: params.Threads,
					Multipart:   multipartLimits,
				},
			},
			Net: NetConfig{
				Node: NodeConfig{
					Addrs: addrs,
					Port: func() int {
						if uniformPort {
							return refPort
						}
						return 0
					}(),
					Slice: params.SliceEndpoints,
				},
				SSL: SSLConfig{Enabled: scheme == schemeHTTPS},
			},
			Auth: AuthConfig{UID: params.AccessKey, Secret: params.SecretKey, Version: authVersion},
		}

		// S3 driver selection: set driver type for non-default drivers
		driverType := resolveStorageDriverType(params.S3Driver)
		if driverType != storageDriverTypeS3 {
			config.Storage.Driver.Type = driverType
		}

		// RDMA acceleration: populate rdma config section when using s3-rdma driver
		if params.S3Driver == S3DriverRdma {
			threshold := params.RdmaThresholdBytes
			device := params.RdmaDevice
			if device == "" {
				device = "auto"
			}
			logLevel := params.RdmaLogLevel
			if logLevel == "" {
				logLevel = "WARN"
			}
			timeoutMs := params.RdmaTimeoutMs

			config.Storage.Rdma = &RdmaConfig{
				Threshold: threshold,
				Fallback:  params.RdmaFallback,
				Device:    device,
				LocalIP:   params.RdmaLocalIP,
				LogLevel:  logLevel,
				TimeoutMs: timeoutMs,
			}
		}

		// Checksum validation: populate checksum config when algorithm is specified
		if params.Checksum != "" {
			config.Storage.Checksum = &ChecksumConfig{
				Enabled:   true,
				Algorithm: params.Checksum,
			}
		}

	default:
		return nil, fmt.Errorf("unsupported workload type: %s", params.WorkloadType)
	}

	needsItemData := params.ObjectDataCompressibility > 0.0 || !params.ObjectDataDedupable
	if needsItemData {
		if config.Item == nil {
			config.Item = &ItemConfig{}
		}
		if config.Item.Data == nil {
			config.Item.Data = &ItemDataConfig{}
		}
		if params.ObjectDataCompressibility > 0.0 {
			if config.Item.Data.Input == nil {
				config.Item.Data.Input = &ItemDataInputConfig{}
			}
			config.Item.Data.Input.Compressibility = params.ObjectDataCompressibility
		}
		if !params.ObjectDataDedupable {
			dedupable := false
			config.Item.Data.Dedupable = &dedupable
		}
	}

	// Engine tuning: VT carrier parallelism
	if params.ServiceThreads > 0 {
		if config.Load == nil {
			config.Load = &LoadConfig{}
		}
		config.Load.Service = &ServiceConfig{Threads: params.ServiceThreads}
	}

	// Marshal to YAML
	data, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal defaults config: %w", err)
	}

	return data, nil
}
