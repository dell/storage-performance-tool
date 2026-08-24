package scenario

import (
	"fmt"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// GenerateWriteVerifyScenario composes CREATE and, unless deferred, one finite verification READ and optional verified-only cleanup.
func GenerateWriteVerifyScenario(params Params) (string, error) {
	if params.RunID <= 0 {
		return "", fmt.Errorf("write-verify requires a positive run id")
	}
	if params.ObjectCount < 0 {
		return "", fmt.Errorf("write-verify object count must be non-negative")
	}
	if params.DeferVerification && params.Cleanup {
		return "", fmt.Errorf("deferred write-verify does not support cleanup")
	}
	ts := resolveTimestamp(params)
	createID := formatStepID(1, ts, constants.IntegrityStepRoleCreate)
	readID := formatStepID(2, ts, constants.IntegrityStepRoleVerify)
	deleteID := formatStepID(3, ts, stepOpDelete)
	bucketPath := "/" + strings.TrimPrefix(params.Bucket, "/")
	count := params.ObjectCount
	if count <= 0 && strings.TrimSpace(params.Duration) == "" {
		count = defaultIntegrityObjectCount
	}
	duration := ""
	if count <= 0 {
		duration = params.Duration
	}
	namingPrefix := ""
	if strings.TrimSpace(params.Prefix) != "" {
		prefix := strings.TrimLeft(strings.TrimSpace(params.Prefix), "/")
		if prefix == "" {
			return "", fmt.Errorf("write-verify prefix must contain a non-slash character")
		}
		namingPrefix = prefix
	}
	driver := resolveStorageDriverType(params.S3Driver)
	return executeIntegrityScenario("write-verify", writeVerifyScenarioData{
		CreateStep: createID,
		ReadStep:   readID,
		DeleteStep: deleteID,
		CreateStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance: constants.IntegrityProvenanceNone,
			},
		},
		ReadStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance: constants.IntegrityProvenanceEngineStep, ExpectedProducerID: createID,
			},
		},
		DeleteStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance: constants.IntegrityProvenanceEngineStep, ExpectedProducerID: readID,
			},
		},
		BucketPath:       bucketPath,
		ObjectSize:       params.ObjectSize,
		PartSize:         params.PartSize,
		NamingPrefix:     namingPrefix,
		CreateLimitCount: count,
		CreateDuration:   duration,
		Cleanup:          params.Cleanup,
		Deferred:         params.DeferVerification,
	})
}

// GenerateReadVerifyScenario composes LIST discovery (or trusted staged input) and one finite verification READ.
func GenerateReadVerifyScenario(params Params) (string, error) {
	if params.RunID <= 0 {
		return "", fmt.Errorf("read-verify requires a positive run id")
	}
	if params.ObjectCount < 0 {
		return "", fmt.Errorf("read-verify object count must be non-negative")
	}
	ts := resolveTimestamp(params)
	versions := params.Versions
	if versions == "" {
		versions = VersionsCurrent
	}
	if versions != VersionsCurrent && versions != VersionsAll {
		return "", fmt.Errorf("read-verify versions must be %q or %q", VersionsCurrent, VersionsAll)
	}
	if params.ItemsFile != "" && versions != VersionsCurrent {
		return "", fmt.Errorf("read-verify all-version discovery cannot be used with an items file")
	}
	params.Versions = versions
	driver := resolveStorageDriverType(params.S3Driver)
	bucketPath := "/" + strings.TrimPrefix(params.Bucket, "/")
	readNumber := 2
	producerID := formatStepID(1, ts, constants.IntegrityStepRoleList)
	provenance := constants.IntegrityProvenanceEngineStep
	discovery := params.ItemsFile == ""
	if params.ItemsFile != "" {
		readNumber = 1
		producerID = constants.IntegrityCLIStagerProducerID
		provenance = constants.IntegrityProvenanceCLIStager
	}
	readID := formatStepID(readNumber, ts, constants.IntegrityStepRoleVerify)
	selectionMaxCount := params.ObjectCount
	return executeIntegrityScenario("read-verify", readVerifyScenarioData{
		Discovery: discovery,
		ListStep:  formatStepID(1, ts, constants.IntegrityStepRoleList),
		ReadStep:  readID,
		ListStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance: constants.IntegrityProvenanceNone,
				MaxCount:   &selectionMaxCount,
			},
		},
		ReadStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance: provenance, ExpectedProducerID: producerID,
			},
		},
		BucketPath:      bucketPath,
		Prefix:          params.Prefix,
		ItemsFile:       params.ItemsFile,
		IncludeVersions: versions == VersionsAll,
	})
}

// GenerateDeleteScenario renders the public count- and duration-based DELETE workload.
func GenerateDeleteScenario(params Params) (string, error) {
	if params.RunID <= 0 {
		return "", fmt.Errorf("delete requires a positive run id")
	}
	if params.DeleteBatchSize < MinDeleteBatchSize || params.DeleteBatchSize > MaxDeleteBatchSize {
		return "", fmt.Errorf("delete batch size must be between %d and %d",
			MinDeleteBatchSize, MaxDeleteBatchSize)
	}
	if params.Threads <= 0 {
		return "", fmt.Errorf("delete threads must be positive")
	}
	failureBudget, err := resolveDeleteFailureBudget(params)
	if err != nil {
		return "", err
	}
	verification, err := resolveDeleteVerification(params)
	if err != nil {
		return "", err
	}
	duration := strings.TrimSpace(params.Duration)
	if duration != "" && params.ObjectCount != 0 {
		return "", fmt.Errorf("delete object count and duration are mutually exclusive")
	}
	selectionOrder := params.SelectionOrder
	if selectionOrder == "" {
		selectionOrder = SelectionOrderCanonical
	}
	if selectionOrder != SelectionOrderCanonical {
		return "", fmt.Errorf("delete selection order must be %q", SelectionOrderCanonical)
	}
	if params.DeleteExisting && strings.TrimSpace(params.ItemsFile) != "" {
		return "", fmt.Errorf("delete existing-prefix and explicit-manifest sources are mutually exclusive")
	}
	if params.DeleteExisting {
		return generateExistingPrefixDeleteScenario(params, selectionOrder, failureBudget, verification)
	}
	if strings.TrimSpace(params.ItemsFile) == "" {
		return generateSeededDeleteScenario(params, selectionOrder, failureBudget, verification)
	}
	if params.Cleanup {
		return "", fmt.Errorf("delete explicit-manifest mode cannot use cleanup because SPT did not create the selected objects")
	}
	return executeIntegrityScenario("delete-manifest", deleteManifestScenarioData{
		DeleteStep: formatStepID(1, resolveTimestamp(params), stepOpDelete),
		DeleteStorage: integrityStorageTemplateData{
			Driver:      resolveStorageDriverType(params.S3Driver),
			Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance:         constants.IntegrityProvenanceCLIStager,
				ExpectedProducerID: constants.IntegrityCLIStagerProducerID,
			},
		},
		ItemsFile:      params.ItemsFile,
		BatchSize:      params.DeleteBatchSize,
		SelectionOrder: selectionOrder,
		Duration:       duration,
		FailureBudget:  failureBudget,
		Verification:   verification,
	})
}

func generateExistingPrefixDeleteScenario(
	params Params,
	selectionOrder string,
	failureBudget deleteFailureBudgetTemplateData,
	verification deleteVerificationTemplateData,
) (string, error) {
	if params.Cleanup {
		return "", fmt.Errorf("delete existing-prefix mode cannot use cleanup because SPT did not create the selected objects")
	}
	bucket := strings.TrimSpace(params.Bucket)
	if bucket == "" {
		return "", fmt.Errorf("delete existing-prefix mode requires a bucket")
	}
	if params.Prefix == "" && !params.AllowEmptyPrefix {
		return "", fmt.Errorf("delete existing-prefix mode requires a nonempty prefix unless allow-empty-prefix is explicitly enabled")
	}
	if strings.HasPrefix(params.Prefix, "/") {
		return "", fmt.Errorf("delete existing-prefix prefix must not start with '/' because S3 LIST removes that slash")
	}
	versions := params.Versions
	if versions == "" {
		versions = VersionsCurrent
	}
	if versions != VersionsCurrent {
		return "", fmt.Errorf("delete existing-prefix mode supports current-key discovery only")
	}
	if params.ObjectCount < 0 {
		return "", fmt.Errorf("delete object count must be non-negative")
	}

	ts := resolveTimestamp(params)
	listStep := formatStepID(1, ts, stepOpList)
	driver := resolveStorageDriverType(params.S3Driver)
	selectionMaxCount := params.ObjectCount
	return executeIntegrityScenario("delete-existing", deleteExistingScenarioData{
		ListStep:   listStep,
		DeleteStep: formatStepID(2, ts, stepOpDelete),
		ListStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance:      constants.IntegrityProvenanceNone,
				MaxCount:        &selectionMaxCount,
				RequireNonEmpty: true,
			},
		},
		DeleteStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance:         constants.IntegrityProvenanceEngineStep,
				ExpectedProducerID: listStep,
			},
		},
		BucketPath:     "/" + strings.TrimPrefix(bucket, "/"),
		Prefix:         params.Prefix,
		ListBatchSize:  defaultListBatchSize,
		BatchSize:      params.DeleteBatchSize,
		SelectionOrder: selectionOrder,
		Duration:       strings.TrimSpace(params.Duration),
		FailureBudget:  failureBudget,
		Verification:   verification,
	})
}

func generateSeededDeleteScenario(
	params Params,
	selectionOrder string,
	failureBudget deleteFailureBudgetTemplateData,
	verification deleteVerificationTemplateData,
) (string, error) {
	if strings.TrimSpace(params.Bucket) == "" {
		return "", fmt.Errorf("delete seeded mode requires a bucket")
	}
	if params.ObjectCount < 0 {
		return "", fmt.Errorf("delete object count must be non-negative")
	}
	duration := strings.TrimSpace(params.Duration)
	seedCount := params.ObjectCount
	if duration != "" {
		seedCount = params.SeedCount
		if seedCount < 0 {
			return "", fmt.Errorf("delete seed object count must be positive for duration mode")
		}
	}
	if seedCount == 0 {
		seedCount = DefaultDeleteObjectCount
	}
	objectSize := strings.TrimSpace(params.ObjectSize)
	if objectSize == "" {
		objectSize = DefaultDeleteObjectSize
	}
	ts := resolveTimestamp(params)
	seedStep := formatStepID(1, ts, stepOpSeed)
	deleteStep := formatStepID(2, ts, stepOpDelete)
	driver := resolveStorageDriverType(params.S3Driver)
	return executeIntegrityScenario("delete-seeded", deleteSeededScenarioData{
		SeedStep:    seedStep,
		DeleteStep:  deleteStep,
		CleanupStep: formatStepID(3, ts, stepOpCleanup),
		SeedStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance:              constants.IntegrityProvenanceNone,
				RequireExactOutputCount: true,
			},
		},
		DeleteStorage: integrityStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
			Integrity: integrityTemplateData{
				Provenance:         constants.IntegrityProvenanceEngineStep,
				ExpectedProducerID: seedStep,
			},
		},
		CleanupStorage: deleteCleanupStorageTemplateData{
			Driver: driver, Concurrency: params.Threads,
		},
		BucketPath:     "/" + strings.TrimPrefix(strings.TrimSpace(params.Bucket), "/"),
		ObjectSize:     objectSize,
		Namespace:      deleteSeedNamespace(params.Prefix, params.RunID),
		SeedCount:      seedCount,
		BatchSize:      params.DeleteBatchSize,
		SelectionOrder: selectionOrder,
		Duration:       duration,
		FailureBudget:  failureBudget,
		Verification:   verification,
		Cleanup:        params.Cleanup,
	})
}

func resolveDeleteVerification(params Params) (deleteVerificationTemplateData, error) {
	timeout := params.VerificationTimeout
	if timeout == 0 {
		timeout = DefaultDeleteVerificationTimeout
	}
	if timeout <= 0 || timeout%time.Millisecond != 0 {
		return deleteVerificationTemplateData{}, fmt.Errorf("delete verification timeout must be a positive whole number of milliseconds")
	}
	postVerification := params.VerifyDelete
	if params.ValidateDeleteInventory && !params.VerifyDeleteExplicit {
		postVerification = true
	}
	return deleteVerificationTemplateData{
		PreValidation:             params.ValidateDeleteInventory,
		PostVerification:          postVerification,
		VerificationTimeoutMillis: timeout.Milliseconds(),
	}, nil
}

func resolveDeleteFailureBudget(params Params) (deleteFailureBudgetTemplateData, error) {
	mode := params.FailureBudgetMode
	maxFailedObjects := params.MaxFailedObjects
	grace := params.FailureBudgetGrace
	if grace < 0 {
		return deleteFailureBudgetTemplateData{}, fmt.Errorf("delete failure-budget grace must be non-negative")
	}
	if grace%time.Second != 0 {
		return deleteFailureBudgetTemplateData{}, fmt.Errorf("delete failure-budget grace must use whole seconds")
	}
	if mode == "" {
		mode = FailureBudgetModeFixed
		maxFailedObjects = DefaultMaxFailedObjects
		grace = DefaultFailureBudgetGrace
	}
	return deleteFailureBudgetTemplateData{
		Mode:              mode,
		MaxFailedObjects:  maxFailedObjects,
		MaxFailurePercent: params.MaxFailurePercent,
		GraceSeconds:      int64(grace / time.Second),
	}, nil
}

func deleteSeedNamespace(root string, runID int64) string {
	root = strings.Trim(strings.TrimSpace(root), "/")
	namespace := fmt.Sprintf("spt-delete-%d/", runID)
	if root == "" {
		return namespace
	}
	return root + "/" + namespace
}

func quoteJS(value string) string {
	return fmt.Sprintf("\"%s\"", escapeJSONString(value))
}
