package scenario

const (
	workloadTypeWrite  = "write"
	workloadTypeRead   = "read"
	workloadTypeMock   = "mock"
	workloadTypeList   = "list"
	workloadTypeTables = "tables"

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

	// ChecksumCRC32 selects CRC-32 checksum validation.
	ChecksumCRC32 = "crc32"
	// ChecksumCRC32C selects CRC-32C checksum validation.
	ChecksumCRC32C = "crc32c"
	// ChecksumSHA1 selects SHA-1 checksum validation.
	ChecksumSHA1 = "sha1"
	// ChecksumSHA256 selects SHA-256 checksum validation.
	ChecksumSHA256 = "sha256"

	itemTypeData         = "data"
	itemTypePath         = "path"
	itemNamingTypeRandom = "random"
	loadOpTypeNoop       = "noop"
	loadOpTypeList       = "list"

	metricsAveragePeriodFiveSeconds = "5s"

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

	stepOpCreate = "create"
	stepOpRead   = "read"
	stepOpSeed   = "seed"
	stepOpDelete = "delete"
)

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
	templateKeyTablesEndpoint         = "TablesEndpoint"
	templateKeyTablesAccessKey        = "TablesAccessKey"
	templateKeyTablesSecretKey        = "TablesSecretKey"
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
	WorkloadTypeList   = workloadTypeList
	WorkloadTypeRead   = workloadTypeRead
	WorkloadTypeTables = workloadTypeTables
)
