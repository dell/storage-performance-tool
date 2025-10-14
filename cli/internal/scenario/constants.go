package scenario

const (
	workloadTypeWrite = "write"
	workloadTypeMock  = "mock"
	workloadTypeList  = "list"

	storageDriverTypeS3  = "s3"
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
	templateKeyTimestamp            = "Timestamp"
	templateKeyStepID               = "StepID"
	templateKeyStepIDCreate         = "StepIDCreate"
	templateKeyStepIDDelete         = "StepIDDelete"
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

	stepOpCreate = "create"
	stepOpDelete = "delete"
)

// Exported workload identifiers for packages that need to branch on scenario type
// without duplicating literal strings.
const (
	WorkloadTypeList = workloadTypeList
)
