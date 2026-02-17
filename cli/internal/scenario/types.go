package scenario

// Params holds all parameters needed to generate a Spt scenario.
type Params struct {
	WorkloadType string
	Endpoint     string
	Endpoints    []string
	AccessKey    string
	SecretKey    string
	Bucket       string
	Prefix       string
	Threads      int
	ObjectSize   string
	ObjectCount  int
	Duration     string
	AuthVersion  int
	Cleanup      bool // Automatically delete created objects after test
	KeepScenario bool // Keep the scenario file after test completes
	// Multi-endpoint controls
	SliceEndpoints bool // Partition endpoint list across nodes in distributed runs

	// Read workload
	SeedCount int // Number of seed objects for read benchmark (default: 2500)

	// RDMA acceleration
	UseRdma            bool   // Use s3-rdma driver instead of s3
	RdmaLocalIP        string // Local RDMA interface IP (required for RDMA)
	RdmaThresholdBytes int64  // Min object size for RDMA (default: 1048576)
	RdmaFallback       bool   // Fall back to HTTP if RDMA init fails (default: false)
	RdmaDevice         string // RDMA device name (default: "auto")
	RdmaLogLevel       string // Native RDMA log level (default: "WARN")
	RdmaTimeoutMs      int64  // RDMA operation timeout (default: 30000)

	// TUI layout
	MinimalTUI bool // Start TUI with graphs and messages panels collapsed
}

// ScenarioParams is a temporary alias for Params kept for backward
// compatibility while the codebase migrates away from the stuttered name.
// TODO: remove alias after dependent code is updated.
//
//nolint:revive // compatibility alias retained during migration away from stuttered name
type ScenarioParams = Params
