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
		BucketPath: bucketPath,
		Prefix:     params.Prefix,
		ItemsFile:  params.ItemsFile,
	})
}

func quoteJS(value string) string {
	return fmt.Sprintf("\"%s\"", escapeJSONString(value))
}
