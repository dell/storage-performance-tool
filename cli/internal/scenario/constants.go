package scenario

import (
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

const (
	workloadTypeWrite       = workload.Write
	workloadTypeRead        = workload.Read
	workloadTypeWriteVerify = workload.WriteVerify
	workloadTypeReadVerify  = workload.ReadVerify
	workloadTypeMixed       = workload.Mixed
	workloadTypeDelete      = workload.Delete
	workloadTypeMock        = workload.Mock
	workloadTypeList        = workload.List
	workloadTypeTables      = workload.Tables

	storageDriverTypeS3       = "s3"
	storageDriverTypeS3Aws    = "s3-aws"
	storageDriverTypeS3Rdma   = "s3-rdma"
	storageDriverTypeS3Tables = "s3-tables"

	// S3DriverDefault selects the standard Netty-based S3 driver.
	S3DriverDefault = "default"
	// S3DriverNetty is an alias for S3DriverDefault.
	S3DriverNetty = "netty"
	// S3DriverAws selects the AWS SDK S3 driver.
	S3DriverAws = "aws"
	// S3DriverRdma selects the RDMA-accelerated S3 driver.
	S3DriverRdma = "rdma"

	// VersionsCurrent discovers only each key's current visible object.
	VersionsCurrent = "current"
	// VersionsAll discovers every data version and excludes delete markers.
	VersionsAll = "all"

	// ChecksumCRC32 selects CRC-32 checksum validation.
	ChecksumCRC32 = "crc32"
	// ChecksumCRC32C selects CRC-32C checksum validation.
	ChecksumCRC32C = "crc32c"
	// ChecksumSHA1 selects SHA-1 checksum validation.
	ChecksumSHA1 = "sha1"
	// ChecksumSHA256 selects SHA-256 checksum validation.
	ChecksumSHA256 = constants.IntegrityAlgorithmSHA256
	// ChecksumCRC64NVME selects CRC64-NVME checksum validation.
	ChecksumCRC64NVME = "crc64-nvme"

	itemTypeData          = "data"
	itemTypePath          = "path"
	itemNamingTypeRandom  = "random"
	integrityModeMetadata = constants.IntegrityModeMetadata
	loadOpTypeNoop        = "noop"
	loadOpTypeList        = "list"

	metricsAveragePeriodFiveSeconds = "5s"
	defaultIntegrityObjectCount     = 1000

	listStepSuffix             = "list"
	listNamingRadix            = 36
	listOpLimitRateUnlimited   = 0
	listDelimiterDefault       = ""
	listFetchMetadataDefault   = false
	listIncludeVersionsDefault = false

	templateKeyConcurrency          = "Concurrency"
	templateKeyItemSize             = "ItemSize"
	templateKeyItemCount            = "ItemCount"
	templateKeyOutputPath           = "OutputPath"
	templateKeyDuration             = "Duration"
	templateKeyPartSize             = "PartSize"
	templateKeyHasPartSize          = "HasPartSize"
	templateKeyTimestamp            = "Timestamp"
	templateKeyStepID               = "StepID"
	templateKeyStepIDCreate         = "StepIDCreate"
	templateKeyStepIDSeed           = "StepIDSeed"
	templateKeyStepIDRead           = "StepIDRead"
	templateKeyStepIDDelete         = "StepIDDelete"
	templateKeySeedCount            = "SeedCount"
	templateKeyBucketPath           = "BucketPath"
	templateKeyBatchSize            = "BatchSize"
	templateKeyOpLimitCount         = "OpLimitCount"
	templateKeyOpLimitRate          = "OpLimitRate"
	templateKeyHasOpLimitCount      = "HasOpLimitCount"
	templateKeyHasDuration          = "HasDuration"
	templateKeyHasPrefix            = "HasPrefix"
	templateKeyPrefix               = "Prefix"
	templateKeyStorageDriverType    = "StorageDriverType"
	templateKeyItemType             = "ItemType"
	templateKeyItemNamingType       = "NamingType"
	templateKeyLoadOpType           = "LoadOpType"
	templateKeyMetricsAveragePeriod = "MetricsAveragePeriod"
	templateKeyNamingRadix          = "NamingRadix"
	templateKeyListDelimiter        = "ListDelimiter"
	templateKeyFetchMetadata        = "FetchMetadata"
	templateKeyIncludeVersions      = "IncludeVersions"
	templateKeyMaxKeys              = "MaxKeys"
	templateKeySaveItems            = "SaveItems"
	templateKeyItemsFile            = "ItemsFile"
	templateKeyReadShuffle          = "ReadShuffle"
	templateKeyReadShuffleBatchSize = "ReadShuffleBatchSize"
	templateKeyReadPhasePause       = "ReadPhasePauseSeconds"

	stepOpCreate  = "create"
	stepOpRead    = "read"
	stepOpSeed    = "seed"
	stepOpDelete  = "delete"
	stepOpCleanup = "cleanup"
	stepOpList    = "list"
	stepOpMixed   = "mixed"
)

const (
	// MinDeleteBatchSize is the smallest valid standalone DELETE request size.
	MinDeleteBatchSize = 1
	// DefaultDeleteObjectSize is the explicit payload size for seeded DELETE inventories.
	DefaultDeleteObjectSize = "1KiB"
	// DefaultDeleteBatchSize is the default standalone DELETE request size.
	DefaultDeleteBatchSize = 100
	// MaxDeleteBatchSize is the S3 DeleteObjects request limit.
	MaxDeleteBatchSize = 1000
	// DefaultMaxFailedObjects is the standalone DELETE object-unit failure budget.
	DefaultMaxFailedObjects int64 = 100000
	// FailureBudgetModeFixed selects a fixed failed-object threshold.
	FailureBudgetModeFixed = constants.DeleteFailurePolicyModeFixed
	// FailureBudgetModePercentage selects a cumulative attempted-object percentage.
	FailureBudgetModePercentage = constants.DeleteFailurePolicyModePercentage
	// SelectionOrderCanonical names the deterministic global DELETE selection order.
	SelectionOrderCanonical = constants.DeleteSelectionOrderCanonical
)

// IsSeededDeleteCleanupStepID reports whether stepID identifies the cleanup phase of a
// seeded DELETE scenario.
func IsSeededDeleteCleanupStepID(stepID string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(stepID)), "-"+stepOpCleanup)
}

// DefaultFailureBudgetGrace delays positive percentage evaluation during the measured phase.
const DefaultFailureBudgetGrace = 30 * time.Second

// DefaultDeleteVerificationTimeout bounds each enabled DELETE verification phase independently.
const DefaultDeleteVerificationTimeout = 30 * time.Second

// DefaultReadPhasePauseSeconds preserves the historical pause between read
// scenario phases while allowing qualification runs to request a longer settle.
const DefaultReadPhasePauseSeconds = 10

// Tables opMode constants
const (
	tablesOpModeProvision      = "provision"
	tablesOpModeTableWrite     = "tableWrite"
	tablesOpModeCatalogSeed    = "catalogSeed"
	tablesOpModeTableCatalog   = "tableCatalog"
	tablesOpModeCompactionPoll = "tableCompactionPoll"
)

// Tables test vector constants
const (
	tablesTestVectorTPS        = "tps"
	tablesTestVectorCompaction = "compaction"
	tablesTestVectorCatalog    = "catalog"
)

// Template keys for tables scenarios
const (
	templateKeyTablesBucket           = "TablesBucket"
	templateKeyTablesNamespace        = "TablesNamespace"
	templateKeyTablesTableName        = "TablesTableName"
	templateKeyTablesConcurrency      = "TablesConcurrency"
	templateKeyTablesCommitFreqMs     = "TablesCommitFreqMs"
	templateKeyTablesTargetFileSizeB  = "TablesTargetFileSizeBytes"
	templateKeyTablesIngestFileSizeB  = "TablesIngestFileSizeBytes"
	templateKeyTablesTotalIngestB     = "TablesTotalIngestBytes"
	templateKeyTablesNamespaceCount   = "TablesNamespaceCount"
	templateKeyTablesTablesPerNs      = "TablesTablesPerNs"
	templateKeyTablesReadConcurrency  = "TablesReadConcurrency"
	templateKeyTablesCompactionToutMs = "TablesCompactionTimeoutMs"
	templateKeyTablesDuration         = "TablesDuration"
	templateKeyTablesStepIDProvision  = "TablesStepIDProvision"
	templateKeyTablesStepIDWrite      = "TablesStepIDWrite"
	templateKeyTablesStepIDCompaction = "TablesStepIDCompaction"
	templateKeyTablesStepIDSeed       = "TablesStepIDSeed"
	templateKeyTablesStepIDCatalog    = "TablesStepIDCatalog"
	templateKeyTablesPort             = "TablesPort"
	templateKeyTablesSSL              = "TablesSSL"
)

// Exported workload identifiers for packages that need to branch on scenario type
// without duplicating literal strings.
const (
	WorkloadTypeList        = workloadTypeList
	WorkloadTypeRead        = workloadTypeRead
	WorkloadTypeWriteVerify = workloadTypeWriteVerify
	WorkloadTypeReadVerify  = workloadTypeReadVerify
	WorkloadTypeTables      = workloadTypeTables
	WorkloadTypeMixed       = workloadTypeMixed
	WorkloadTypeDelete      = workloadTypeDelete
)

// Mixed workload template keys
const (
	templateKeyGetDistrib       = "GetDistrib"
	templateKeyPutDistrib       = "PutDistrib"
	templateKeyDeleteDistrib    = "DeleteDistrib"
	templateKeyStatDistrib      = "StatDistrib"
	templateKeyHasGetDistrib    = "HasGetDistrib"
	templateKeyHasPutDistrib    = "HasPutDistrib"
	templateKeyHasDeleteDistrib = "HasDeleteDistrib"
	templateKeyHasStatDistrib   = "HasStatDistrib"
	templateKeyStepIDMixed      = "StepIDMixed"
	templateKeyStepIDSeedClean  = "StepIDSeedCleanup"
	templateKeyStepIDPutClean   = "StepIDPutCleanup"
	templateKeyReadItemsFile    = "ReadItemsFile"
	templateKeyDeleteItemsFile  = "DeleteItemsFile"
	templateKeyHasDeleteItems   = "HasDeleteItemsFile"
	templateKeyHasReadItems     = "HasReadItemsFile"
)

// Mixed workload defaults
const (
	MixedDefaultGetDistrib    = 45
	MixedDefaultStatDistrib   = 30
	MixedDefaultPutDistrib    = 15
	MixedDefaultDeleteDistrib = 10
	MixedDefaultSeedCount     = 2500
)
