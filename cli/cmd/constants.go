/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
// Package cmd implements the command-line interface for spt.
package cmd

import (
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

// Docker constants - reference from internal constants to avoid duplication
const (
	DefaultSptImage                  = constants.DefaultSptImage
	flagDeferVerification            = "defer-verification"
	flagIntegrityRuntimeIdentityTier = "integrity-runtime-identity-tier"
	flagVersions                     = "versions"
	flagDeleteBatchSize              = "delete-batch-size"
	flagDeleteExisting               = "delete-existing"
	flagAllowEmptyPrefix             = "allow-empty-prefix"
)

// Workload type constants
const (
	WorkloadTypeWrite       = workload.Write
	WorkloadTypeRead        = workload.Read
	WorkloadTypeWriteVerify = workload.WriteVerify
	WorkloadTypeReadVerify  = workload.ReadVerify
	WorkloadTypeMixed       = workload.Mixed
	WorkloadTypeDelete      = workload.Delete
	WorkloadTypeList        = workload.List
	WorkloadTypeMock        = workload.Mock
	WorkloadTypeTables      = workload.Tables
)

// ValidWorkloadTypes contains all accepted workload types in registry order.
var ValidWorkloadTypes = workload.Names()

// Error message constants
const (
	ErrMissingEndpoint                = "--endpoint flag is required for %s workload"
	ErrMissingAccessKey               = "--access-key flag is required for %s workload"
	ErrMissingSecretKey               = "--secret-key flag is required for %s workload" // #nosec G101: constant phrase contains "secret" but is not a credential
	ErrMissingBucket                  = "--bucket flag is required for %s workload"
	ErrDurationOrCount                = "cannot specify both --object-count and --duration"
	ErrInvalidWorkloadType            = "invalid workload type: %s"
	ErrWorkloadNotImplemented         = "workload type '%s' is not yet implemented; run 'spt run --help' to list implemented workloads"
	ErrInvalidEndpointURL             = "invalid endpoint URL: %w"
	ErrMissingHostname                = "endpoint URL must include a hostname"
	ErrFlagNotSupported               = "%s flag is not supported for %s workload"
	ErrInvalidAuthVersion             = "invalid auth version: %d (supported values: 2 or 4)"
	ErrReadShuffleBatchRequiresToggle = "--shuffle-batch-size requires --shuffle"
	ErrReadShuffleBatchPositive       = "--shuffle-batch-size must be > 0"
	ErrReadShuffleBatchTooLarge       = "--shuffle-batch-size must be <= %d"
	ErrReadPhasePausePositive         = "--read-phase-pause-seconds must be > 0"
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
