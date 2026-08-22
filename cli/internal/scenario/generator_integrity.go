package scenario

import (
	"fmt"
	"strings"

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

// GenerateDeleteScenario renders the internally available finite-count DELETE slices. The
// public workload registry remains gated until the complete DELETE feature is qualified.
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
	selectionOrder := params.SelectionOrder
	if selectionOrder == "" {
		selectionOrder = SelectionOrderCanonical
	}
	if selectionOrder != SelectionOrderCanonical {
		return "", fmt.Errorf("delete selection order must be %q", SelectionOrderCanonical)
	}
	if strings.TrimSpace(params.ItemsFile) == "" {
		return generateSeededDeleteScenario(params, selectionOrder)
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
	})
}

func generateSeededDeleteScenario(params Params, selectionOrder string) (string, error) {
	if strings.TrimSpace(params.Duration) != "" {
		return "", fmt.Errorf("delete seeded finite count mode does not yet support duration")
	}
	if strings.TrimSpace(params.Bucket) == "" {
		return "", fmt.Errorf("delete seeded mode requires a bucket")
	}
	if params.ObjectCount < 0 {
		return "", fmt.Errorf("delete object count must be non-negative")
	}
	seedCount := params.ObjectCount
	if seedCount == 0 {
		seedCount = DefaultDeleteObjectCount
	}
	objectSize := strings.TrimSpace(params.ObjectSize)
	if objectSize == "" {
		objectSize = DefaultDeleteObjectSize
	}
	ts := resolveTimestamp(params)
	seedStep := formatStepID(1, ts, stepOpSeed)
	driver := resolveStorageDriverType(params.S3Driver)
	return executeIntegrityScenario("delete-seeded", deleteSeededScenarioData{
		SeedStep:   seedStep,
		DeleteStep: formatStepID(2, ts, stepOpDelete),
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
		BucketPath:     "/" + strings.TrimPrefix(strings.TrimSpace(params.Bucket), "/"),
		ObjectSize:     objectSize,
		Namespace:      deleteSeedNamespace(params.Prefix, params.RunID),
		SeedCount:      seedCount,
		BatchSize:      params.DeleteBatchSize,
		SelectionOrder: selectionOrder,
	})
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
