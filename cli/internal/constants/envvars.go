package constants

// Environment variable keys used by spt
const (
	EnvSptImage       = "SPT_IMAGE"
	EnvSptGitHubToken = "SPT_GITHUB_TOKEN" // #nosec G101 -- env var name only
	EnvHosts          = "HOSTS"
	EnvS3Endpoint     = "S3_ENDPOINT"
	EnvS3AccessKey    = "S3_ACCESS_KEY" // #nosec G101 -- env var names only
	EnvS3SecretKey    = "S3_SECRET_KEY" // #nosec G101 -- env var names only
	EnvS3Bucket       = "S3_BUCKET"
	EnvS3AuthVersion  = "S3_AUTH_VERSION"
	EnvSkipImagePull  = "SPT_SKIP_IMAGE_PULL"
	EnvSptJavaOpts    = "SPT_JAVA_OPTS"
	EnvRdmaEnabled    = "SPT_RDMA"
	EnvS3Driver       = "SPT_S3_DRIVER"
	EnvServiceThreads = "SPT_SERVICE_THREADS"

	// Multipart upload configuration
	EnvPartSize = "SPT_PART_SIZE"

	// Checksum configuration
	EnvChecksum = "SPT_CHECKSUM"

	// Object data shaping configuration
	EnvObjectDataCompressibility = "SPT_OBJECT_DATA_COMPRESSIBILITY"
	EnvObjectDataDedupable       = "SPT_OBJECT_DATA_DEDUPABLE"

	// RDMA configuration environment variables
	EnvRdmaLocalIP   = "RDMA_LOCAL_IP"
	EnvRdmaThreshold = "RDMA_THRESHOLD_BYTES"
	EnvRdmaFallback  = "RDMA_FALLBACK_ENABLED"
	EnvRdmaDevice    = "RDMA_DEVICE"
	EnvRdmaLogLevel  = "RDMA_LOG_LEVEL"
	EnvRdmaTimeout   = "RDMA_TIMEOUT_MS"
)
