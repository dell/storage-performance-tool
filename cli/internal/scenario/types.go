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
	PartSize     string // Multipart upload part size (e.g. "64MB"); empty = single PUT
	MpuObjects   int    // Max concurrent multipart objects in flight (0 = unlimited)
	MpuParts     int    // Max concurrent parts in flight per multipart object (0 = unlimited)
	ObjectCount  int
	Duration     string
	AuthVersion  int
	Cleanup      bool // Automatically delete created objects after test
	KeepScenario bool // Keep the scenario file after test completes
	// Engine tuning
	ServiceThreads  int      // VT carrier thread parallelism (0 = JVM default)
	EngineOverrides []string // Advanced engine defaults overrides as YAML path=value entries
	PrefixShards    int      // Optional count of fixed-width object-key prefix directories (0 = disabled)

	// Multi-endpoint controls
	SliceEndpoints bool // Partition endpoint list across nodes in distributed runs

	// Read workload
	SeedCount             int    // Number of seed objects for read benchmark (default: 2500)
	ItemsFile             string // Path to a local items.csv for read workload (skips seed phase)
	ReadShuffle           bool   // Enable batch-local item shuffling for the read phase
	ReadShuffleBatchSize  int    // Read-phase batch size override used when ReadShuffle is enabled
	ReadPhasePauseSeconds int    // Seconds to settle between read scenario phases

	// External item files bind-mounted into the engine container.
	ItemFileMounts  []FileMount
	ItemStagingDirs []string

	// Write workload
	SaveItems bool // Save items.csv to the step log directory for later retrieval

	// RDMA acceleration
	S3Driver           string // Storage driver selection: "default"/"netty" → s3, "aws" → s3-aws, "rdma" → s3-rdma
	RdmaLocalIP        string // Local RDMA interface IP (required for RDMA)
	RdmaThresholdBytes int64  // Min object size for RDMA (default: 1048576)
	RdmaFallback       bool   // Fall back to HTTP if RDMA init fails (default: false)
	RdmaDevice         string // RDMA device name (default: "auto")
	RdmaLogLevel       string // Native RDMA log level (default: "WARN")
	RdmaTimeoutMs      int64  // RDMA operation timeout (default: 30000)

	// Checksum validation
	Checksum string // Checksum algorithm: crc32, crc32c, sha1, sha256, crc64-nvme (empty = disabled)

	ObjectDataCompressibility float64 // Object data compressibility percentage [0..100]
	ObjectDataDedupable       bool    // True keeps repeating ring-buffer data dedupe-friendly; false enables anti-dedupe stamping

	// Stable timestamp for step IDs.  When set, all Generate*Scenario
	// functions use this value instead of calling time.Now(), ensuring that
	// repeated generation from the same Params produces identical step IDs.
	BaseTimestamp string
	RunID         int64 // Positive run identity shared by every verification step and staged input marker

	// TUI layout
	MinimalTUI                  bool
	AllowEmptySelection         bool
	DeferVerification           bool
	Versions                    string // read-verify discovery policy: current or all
	IntegrityMaxConsoleFailures int    // Start TUI with graphs and messages panels collapsed

	// S3 Tables workload
	Tables TablesParams

	// Mixed workload distribution (integer percentages, must sum to 100)
	GetDistrib    int // Percentage of GET (read) operations (default: 45)
	PutDistrib    int // Percentage of PUT (create) operations (default: 15)
	DeleteDistrib int // Percentage of DELETE operations (default: 10)
	StatDistrib   int // Percentage of STAT (HEAD) operations (default: 30)

	// External item files for mixed workload (optional)
	ReadItemsFile   string // Pre-created items for the READ+STAT pool (skips seed phase)
	DeleteItemsFile string // Pre-created items to pre-populate DELETE queue (relaxes delete<=put constraint)
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

// IsIntegrityWorkload reports whether params require the persisted-data integrity engine contract.
func IsIntegrityWorkload(params Params) bool {
	return params.WorkloadType == WorkloadTypeWriteVerify || params.WorkloadType == WorkloadTypeReadVerify
}

// resolveStorageDriverType maps the user-facing S3Driver value to the engine
// storage-driver-type string. An empty value (or "default"/"netty") resolves
// to the standard Netty-based "s3" driver.
func resolveStorageDriverType(s3Driver string) string {
	switch s3Driver {
	case S3DriverAws:
		return storageDriverTypeS3Aws
	case S3DriverRdma:
		return storageDriverTypeS3Rdma
	case S3DriverDefault, S3DriverNetty, "":
		return storageDriverTypeS3
	default:
		return storageDriverTypeS3
	}
}
