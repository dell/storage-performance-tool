package constants

// Environment variable keys used by spt
const (
	EnvSptImage      = "SPT_IMAGE"
	EnvHosts         = "HOSTS"
	EnvS3Endpoint    = "S3_ENDPOINT"
	EnvS3AccessKey   = "S3_ACCESS_KEY" // #nosec G101 -- env var names only
	EnvS3SecretKey   = "S3_SECRET_KEY" // #nosec G101 -- env var names only
	EnvS3Bucket      = "S3_BUCKET"
	EnvS3AuthVersion = "S3_AUTH_VERSION"
	EnvSkipImagePull = "SPT_SKIP_IMAGE_PULL"
	EnvRdmaEnabled   = "SPT_RDMA"

	// RDMA configuration environment variables
	EnvRdmaLocalIp   = "RDMA_LOCAL_IP"
	EnvRdmaThreshold = "RDMA_THRESHOLD_BYTES"
	EnvRdmaFallback  = "RDMA_FALLBACK_ENABLED"
	EnvRdmaDevice    = "RDMA_DEVICE"
	EnvRdmaLogLevel  = "RDMA_LOG_LEVEL"
	EnvRdmaTimeout   = "RDMA_TIMEOUT_MS"
)
