// Package scenario generates Spt scenarios and defaults from user params.
package scenario

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
	"gopkg.in/yaml.v3"
)

const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// DefaultsConfig represents the Spt defaults configuration
type DefaultsConfig struct {
	Storage StorageConfig `yaml:"storage"`
	Run     *RunConfig    `yaml:"run,omitempty"`
	Item    *ItemConfig   `yaml:"item,omitempty"` // pointer so omitted when nil
	Output  OutputConfig  `yaml:"output"`
	Load    *LoadConfig   `yaml:"load,omitempty"` // pointer so omitted when nil
}

// RunConfig carries a preallocated verification run identity.
type RunConfig struct {
	ID      int64            `yaml:"id"`
	Cluster RunClusterConfig `yaml:"cluster"`
}

// RunClusterConfig carries the stable identity shared by all participants in one run.
type RunClusterConfig struct {
	ID string `yaml:"id"`
}

// StorageConfig represents storage configuration
type StorageConfig struct {
	Region       string              `yaml:"region,omitempty"`
	TargetBucket *TargetBucketConfig `yaml:"targetBucket,omitempty"` // emitted only for a bucket set or to disable creation
	Driver       DriverConfig        `yaml:"driver,omitempty"`
	Net          NetConfig           `yaml:"net,omitempty"`
	Auth         AuthConfig          `yaml:"auth,omitempty"`
	Checksum     *ChecksumConfig     `yaml:"checksum,omitempty"` // pointer so omitted when nil
	Rdma         *RdmaConfig         `yaml:"rdma,omitempty"`     // pointer so omitted when nil
}

// TargetBucketConfig represents storage.targetBucket. The engine ships count: 1, names: [] and
// provision: true.
type TargetBucketConfig struct {
	Count     int      `yaml:"count,omitempty"`
	Names     []string `yaml:"names,omitempty"`
	Provision *bool    `yaml:"provision,omitempty"`
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
	// Emitted only when enabled so engines without this schema key still accept the defaults.
	AllowMissingBytesHeader bool `yaml:"allowMissingBytesHeader,omitempty"`
	// Emitted only when the pool is disabled; the engine default reuses registered buffers.
	BufferPool *bool `yaml:"bufferPool,omitempty"`
	// Emitted only above 1; the engine default copies a PUT payload on the dispatcher alone.
	CopyThreads int `yaml:"copyThreads,omitempty"`
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
	Node     NodeConfig      `yaml:"node,omitempty"`
	SSL      SSLConfig       `yaml:"ssl,omitempty"`
	Endpoint *EndpointConfig `yaml:"endpoint,omitempty"` // pointer so omitted in the default mode
}

// EndpointConfig represents storage.net.endpoint (opt-in endpoint selection).
type EndpointConfig struct {
	Selection string                 `yaml:"selection"`
	Hostname  string                 `yaml:"hostname,omitempty"`
	DNS       *EndpointDNSConfig     `yaml:"dns,omitempty"`
	Connect   *EndpointConnectConfig `yaml:"connect,omitempty"`
}

// EndpointDNSConfig represents storage.net.endpoint.dns.
type EndpointDNSConfig struct {
	Server          string `yaml:"server,omitempty"`
	TimeoutMilliSec int    `yaml:"timeoutMilliSec,omitempty"`
}

// EndpointConnectConfig represents storage.net.endpoint.connect.
type EndpointConnectConfig struct {
	TimeoutMilliSec int `yaml:"timeoutMilliSec,omitempty"`
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
	if params.RunID > 0 {
		config.Run = &RunConfig{
			ID: params.RunID,
			Cluster: RunClusterConfig{
				ID: constants.RunClusterID(params.RunID),
			},
		}
	}

	// Configure based on workload type
	switch params.WorkloadType {
	case workload.Mock:
		// For mock workloads, use dummy-mock driver
		config.Storage.Driver = DriverConfig{
			Type: "dummy-mock",
			Limit: DriverLimits{
				Concurrency: params.Threads,
			},
		}

	case workload.Tables:
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
			Region: params.Region,
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

	case workload.Write, workload.Read, workload.WriteVerify, workload.ReadVerify, workload.Mixed, workload.Delete, workload.List:
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

		nodes, err := ParseNodeEndpoints(eps)
		if err != nil {
			return nil, err
		}

		var multipartLimits *MultipartLimits
		if params.MpuObjects > 0 || params.MpuParts > 0 {
			multipartLimits = &MultipartLimits{
				Objects: params.MpuObjects,
				Parts:   params.MpuParts,
			}
		}

		config.Storage = StorageConfig{
			Region:       params.Region,
			TargetBucket: targetBucketConfig(params),
			Driver: DriverConfig{
				Limit: DriverLimits{
					Concurrency: params.Threads,
					Multipart:   multipartLimits,
				},
			},
			Net: NetConfig{
				Node: NodeConfig{
					Addrs: nodes.Addrs,
					Port:  nodes.Port,
					Slice: params.SliceEndpoints,
				},
				SSL:      SSLConfig{Enabled: nodes.Scheme == schemeHTTPS},
				Endpoint: endpointConfig(params.EndpointSelection),
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

				AllowMissingBytesHeader: params.RdmaAllowMissingBytesHeader,
			}
			if params.RdmaDisableBufferPool {
				bufferPool := false
				config.Storage.Rdma.BufferPool = &bufferPool
			}
			if params.RdmaCopyThreads > 1 {
				config.Storage.Rdma.CopyThreads = params.RdmaCopyThreads
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

	// Keep this in /run defaults for effective-config attribution even though
	// startup argument delivery is what activates VT parallelism for the node JVM.
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

	if len(params.EngineOverrides) > 0 {
		data, err = applyEngineOverrides(data, params.EngineOverrides)
		if err != nil {
			return nil, err
		}
		if data, err = restoreTargetBucketSet(data, params); err != nil {
			return nil, err
		}
	}

	if err := validateRangeDefaults(params, data); err != nil {
		return nil, err
	}

	return data, nil
}

// restoreTargetBucketSet writes the validated bucket set back after engine overrides. An override
// that replaces storage.targetBucket or storage as a whole, such as
// storage.targetBucket={provision: false}, would drop the count or names the CLI validated.
// Overrides that set them are rejected earlier, so the flags' values are final.
func restoreTargetBucketSet(data []byte, params Params) ([]byte, error) {
	var err error
	if params.BucketCount > 1 {
		if data, err = applyEngineOverrides(data, []string{fmt.Sprintf("storage.targetBucket.count=%d", params.BucketCount)}); err != nil {
			return nil, err
		}
	}
	// Names are set as a value, not parsed from an override string, because a name used as written
	// may hold characters that YAML would interpret.
	if len(params.BucketNames) > 1 {
		return setDefaultsValue(data, append([]string(nil), params.BucketNames...), "storage", "targetBucket", "names")
	}
	return data, nil
}

// targetBucketConfig returns the storage.targetBucket defaults only for a bucket set or a
// provisioning opt-out, so the generated defaults are unchanged for one bucket with provisioning on.
func targetBucketConfig(params Params) *TargetBucketConfig {
	var config TargetBucketConfig
	if params.BucketCount > 1 {
		config.Count = params.BucketCount
	}
	if len(params.BucketNames) > 1 {
		config.Names = params.BucketNames
	}
	if params.TargetBucketProvisionDisabled {
		provision := false
		config.Provision = &provision
	}
	if config.Count == 0 && config.Names == nil && config.Provision == nil {
		return nil
	}
	return &config
}

// setDefaultsValue sets one path of generated defaults to a value, creating missing maps. It fails
// when an ancestor holds a scalar, as an engine override of that path would.
func setDefaultsValue(data []byte, value any, path ...string) ([]byte, error) {
	var root map[string]any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("failed to parse defaults before setting %s: %w", strings.Join(path, "."), err)
	}
	if root == nil {
		root = map[string]any{}
	}
	node := root
	for _, part := range path[:len(path)-1] {
		next, ok := node[part]
		if !ok || next == nil {
			child := map[string]any{}
			node[part] = child
			node = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("cannot set %s: %s already contains a scalar value", strings.Join(path, "."), part)
		}
		node = child
	}
	node[path[len(path)-1]] = value
	merged, err := yaml.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal defaults after setting %s: %w", strings.Join(path, "."), err)
	}
	return merged, nil
}

func applyEngineOverrides(data []byte, overrides []string) ([]byte, error) {
	var root map[string]any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("failed to parse defaults before applying engine overrides: %w", err)
	}

	for _, override := range overrides {
		if err := applyEngineOverride(root, override); err != nil {
			return nil, err
		}
	}

	merged, err := yaml.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal defaults after applying engine overrides: %w", err)
	}
	return merged, nil
}

func applyEngineOverride(root map[string]any, override string) error {
	rawPath, rawValue, ok := strings.Cut(override, "=")
	if !ok {
		return engineOverrideError(override, "expected path=value")
	}

	parts := splitEngineOverridePath(rawPath)
	if len(parts) == 0 {
		return engineOverrideError(override, "path is empty")
	}

	value, err := parseEngineOverrideValue(rawValue)
	if err != nil {
		return engineOverrideError(override, "value is not valid YAML")
	}

	node := root
	for _, part := range parts[:len(parts)-1] {
		next, ok := node[part]
		if !ok || next == nil {
			child := map[string]any{}
			node[part] = child
			node = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			return engineOverrideError(
				override, fmt.Sprintf("%s already contains a scalar value", part))
		}
		node = child
	}

	node[parts[len(parts)-1]] = value
	return nil
}

// EngineOverrideSets reports whether an engine override sets the configuration path, directly or
// through a map value of one of its ancestors, such as storage.targetBucket={count: 4}. An
// override that does not parse sets nothing; GenerateDefaults reports it.
func EngineOverrideSets(override string, path ...string) bool {
	root := map[string]any{}
	if err := applyEngineOverride(root, override); err != nil {
		return false
	}
	var node any = root
	for _, part := range path {
		children, ok := node.(map[string]any)
		if !ok {
			return false
		}
		if node, ok = children[part]; !ok {
			return false
		}
	}
	return true
}

func engineOverrideError(override, reason string) error {
	return fmt.Errorf(
		"invalid engine override %q: %s",
		secretmask.EngineOverride(override), reason)
}

func splitEngineOverridePath(path string) []string {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	separator := "."
	if !strings.Contains(path, ".") {
		separator = "-"
	}

	rawParts := strings.Split(path, separator)
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil
		}
		parts = append(parts, part)
	}
	return parts
}

func parseEngineOverrideValue(raw string) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}

	var value any
	if err := yaml.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}
	return value, nil
}

const loadServiceThreadsStartupArg = "--load-service-threads="

// BuildEngineStartupArgs resolves engine settings that must be present before
// the node API starts. They cannot be activated by defaults posted later to /run.
func BuildEngineStartupArgs(params Params) ([]string, error) {
	if params.ServiceThreads < 0 {
		return nil, fmt.Errorf("service threads must be non-negative, got %d", params.ServiceThreads)
	}

	effectiveThreads := params.ServiceThreads
	overrideThreads := 0
	overrideSeen := false
	for _, override := range params.EngineOverrides {
		rawPath, rawValue, hasValue := strings.Cut(override, "=")
		parts := splitEngineOverridePath(rawPath)
		if len(parts) != 3 || parts[0] != "load" || parts[1] != "service" || parts[2] != "threads" {
			continue
		}
		if !hasValue {
			return nil, engineOverrideError(override, "expected path=value")
		}

		value, err := parseEngineOverrideValue(rawValue)
		if err != nil {
			return nil, engineOverrideError(override, "value is not valid YAML")
		}
		threads, ok := value.(int)
		if !ok {
			return nil, engineOverrideError(
				override, "load.service.threads must be an integer")
		}
		if threads < 0 {
			return nil, engineOverrideError(
				override, "load.service.threads must be non-negative")
		}
		if overrideSeen && threads != overrideThreads {
			return nil, fmt.Errorf("conflicting load.service.threads engine overrides: %d and %d", overrideThreads, threads)
		}
		overrideThreads = threads
		overrideSeen = true
	}

	if overrideSeen {
		if effectiveThreads > 0 && effectiveThreads != overrideThreads {
			return nil, fmt.Errorf("conflicting service thread settings: --service-threads=%d and load.service.threads=%d", effectiveThreads, overrideThreads)
		}
		effectiveThreads = overrideThreads
	}
	if effectiveThreads == 0 {
		return nil, nil
	}
	return []string{fmt.Sprintf("%s%d", loadServiceThreadsStartupArg, effectiveThreads)}, nil
}
