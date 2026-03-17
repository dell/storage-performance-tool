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
	// Engine tuning
	ServiceThreads int // VT carrier thread parallelism (0 = JVM default)

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

	// S3 Tables workload
	Tables TablesParams
}

// TablesParams holds parameters specific to the S3 Tables workload.
type TablesParams struct {
	TestVector          string // tps | compaction | catalog
	TableBucket         string // table bucket name
	Namespace           string // namespace within the bucket
	TableName           string // target table name
	ConcurrentWriters   int    // concurrent Iceberg commit threads
	CommitFreqMs        int    // target ms between commits per writer
	TargetFileSizeBytes int64  // target Parquet file size in bytes
	IngestFileSizeBytes int64  // small file size for compaction seed in bytes
	TotalIngestBytes    int64  // total data volume for compaction seed in bytes
	NamespaceCount      int    // namespaces to seed for catalog test
	TablesPerNs         int    // tables per namespace for catalog test
	ReadConcurrency     int    // concurrent catalog readers
	CompactionTimeoutMs int64  // max wait for compaction in milliseconds
	NoProvision         bool   // skip provision phase (reuse existing resources)
}

// ScenarioParams is a temporary alias for Params kept for backward
// compatibility while the codebase migrates away from the stuttered name.
// TODO: remove alias after dependent code is updated.
//
//nolint:revive // compatibility alias retained during migration away from stuttered name
type ScenarioParams = Params
