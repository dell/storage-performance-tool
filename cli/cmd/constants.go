/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
// Package cmd implements the command-line interface for spt.
package cmd

import "github.com/dell/storage-performance-tool/cli/internal/constants"

// Docker constants - reference from internal constants to avoid duplication
const (
	DefaultSptImage = constants.DefaultSptImage
)

// Workload type constants
const (
	WorkloadTypeWrite  = "write"
	WorkloadTypeRead   = "read"
	WorkloadTypeMixed  = "mixed"
	WorkloadTypeDelete = "delete"
	WorkloadTypeList   = "list"
	WorkloadTypeMock   = "mock"
	WorkloadTypeTables = "tables"
)

// ValidWorkloadTypes contains all valid workload types
var ValidWorkloadTypes = []string{
	WorkloadTypeWrite,
	WorkloadTypeRead,
	WorkloadTypeMixed,
	WorkloadTypeDelete,
	WorkloadTypeList,
	WorkloadTypeMock,
	WorkloadTypeTables,
}

// Error message constants
const (
	ErrMissingEndpoint                = "--endpoint flag is required for %s workload"
	ErrMissingAccessKey               = "--access-key flag is required for %s workload"
	ErrMissingSecretKey               = "--secret-key flag is required for %s workload" // #nosec G101: constant phrase contains "secret" but is not a credential
	ErrMissingBucket                  = "--bucket flag is required for %s workload"
	ErrDurationOrCount                = "cannot specify both --object-count and --duration"
	ErrInvalidWorkloadType            = "invalid workload type: %s. Must be one of: write, read, mixed, delete, list, mock"
	ErrWorkloadNotImplemented         = "workload type '%s' is not yet implemented. Currently only 'write', 'read', 'list', and 'mock' are supported"
	ErrInvalidEndpointURL             = "invalid endpoint URL: %w"
	ErrMissingHostname                = "endpoint URL must include a hostname"
	ErrFlagNotSupported               = "%s flag is not supported for %s workload"
	ErrInvalidAuthVersion             = "invalid auth version: %d (supported values: 2 or 4)"
	ErrReadShuffleBatchRequiresToggle = "--shuffle-batch-size requires --shuffle"
	ErrReadShuffleBatchPositive       = "--shuffle-batch-size must be > 0"
	ErrReadShuffleBatchTooLarge       = "--shuffle-batch-size must be <= %d"
)

// Spt parameter constants
const (
	SptStorageDriverType       = "--storage-driver-type"
	SptStorageDriverTypeS3     = "s3"
	SptStorageDriverTypeS3Aws  = "s3-aws"
	SptStorageDriverTypeS3Rdma = "s3-rdma"
	SptStorageDriverTypeMock   = "dummy-mock"
	SptStorageNetNodeAddrs     = "--storage-net-node-addrs"
	SptStorageNetNodePort      = "--storage-net-node-port"
	SptStorageNetSSLEnabled    = "--storage-net-ssl-enabled"
	SptStorageAuthUID          = "--storage-auth-uid"
	SptStorageAuthSecret       = "--storage-auth-secret" // #nosec G101: CLI flag name, not a credential
	SptItemOutputPath          = "--item-output-path"
	SptLoadOpType              = "--load-op-type"
	SptLoadOpTypeCreate        = "create"
	SptStorageDriverLimitConc  = "--storage-driver-limit-concurrency"
	SptItemDataSize            = "--item-data-size"
	SptLoadOpLimitCount        = "--load-op-limit-count"
	SptLoadStepLimitTime       = "--load-step-limit-time"
)

// URL scheme constants
const (
	SchemeHTTP  = "http"
	SchemeHTTPS = "https"
)
