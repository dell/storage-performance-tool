package scenario

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

const defaultListBatchSize = 1000

type scenarioGenerator func(Params) (string, error)

var scenarioGenerators = map[string]scenarioGenerator{
	workload.Write:       GenerateWriteScenario,
	workload.Read:        GenerateReadScenario,
	workload.WriteVerify: GenerateWriteVerifyScenario,
	workload.ReadVerify:  GenerateReadVerifyScenario,
	workload.Delete:      GenerateDeleteScenario,
	workload.Mixed:       GenerateMixedScenario,
	workload.List:        GenerateListScenario,
	workload.Mock:        GenerateMockScenario,
	workload.Tables:      GenerateTablesScenario,
}

// GenerateScenario creates a JavaScript scenario from parameters. The workload registry is the
// public support gate; every supported workload must have a registered scenario generator.
func GenerateScenario(params Params) (string, error) {
	spec, ok := workload.Lookup(params.WorkloadType)
	if !ok {
		return "", fmt.Errorf("unsupported workload type: %s", params.WorkloadType)
	}
	generator, ok := scenarioGenerators[spec.Name]
	if !ok {
		return "", fmt.Errorf("implemented workload has no scenario generator: %s", spec.Name)
	}
	return generator(params)
}

// GenerateWriteScenario creates a write scenario with optional cleanup
func GenerateWriteScenario(params Params) (string, error) {
	// Build bucket path
	bucketPath := "/" + strings.TrimPrefix(params.Bucket, "/")

	// Prepare template data
	// Use a single run-level timestamp for natural sorting across steps
	ts := resolveTimestamp(params)

	// Determine storage driver type
	driverType := resolveStorageDriverType(params.S3Driver)

	// Format strings as quoted JavaScript literals
	data := map[string]interface{}{
		templateKeyConcurrency:       params.Threads,
		templateKeyItemSize:          fmt.Sprintf(`"%s"`, escapeJSONString(params.ObjectSize)),
		templateKeyItemCount:         params.ObjectCount,
		templateKeyOutputPath:        fmt.Sprintf(`"%s"`, escapeJSONString(bucketPath)),
		templateKeyDuration:          fmt.Sprintf(`"%s"`, escapeJSONString(params.Duration)),
		templateKeyPartSize:          fmt.Sprintf(`"%s"`, escapeJSONString(params.PartSize)),
		templateKeyHasPartSize:       params.PartSize != "",
		templateKeyTimestamp:         time.Now().Unix(),
		templateKeyStorageDriverType: fmt.Sprintf(`"%s"`, driverType),
		templateKeySaveItems:         params.SaveItems,
		// Step IDs using shared timestamp and ordered numbers
		templateKeyStepID:       formatStepID(1, ts, stepOpCreate),
		templateKeyStepIDCreate: formatStepID(1, ts, stepOpCreate),
		templateKeyStepIDDelete: formatStepID(2, ts, stepOpDelete),
	}

	// Choose appropriate template based on parameters
	var tmplStr string

	if params.Cleanup {
		// Write with cleanup (delete after create)
		if params.ObjectCount > 0 {
			tmplStr = writeWithCleanupTemplate
		} else if params.Duration != "" {
			tmplStr = writeWithCleanupDurationTemplate
		} else {
			// Default to 1000 objects for cleanup scenarios
			data[templateKeyItemCount] = 1000
			tmplStr = writeWithCleanupTemplate
		}
	} else {
		// Write only (no cleanup)
		if params.ObjectCount > 0 {
			tmplStr = writeOnlyTemplate
		} else if params.Duration != "" {
			tmplStr = writeOnlyDurationTemplate
		} else {
			// Default template with 1000 objects
			tmplStr = writeOnlyDefaultTemplate
		}
	}

	// Parse and execute template
	tmpl, err := template.New("scenario").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

// GenerateReadScenario creates a read benchmark scenario with seed, read, and optional cleanup phases.
func GenerateReadScenario(params Params) (string, error) {
	bucketPath := "/" + strings.TrimPrefix(params.Bucket, "/")

	ts := resolveTimestamp(params)

	driverType := resolveStorageDriverType(params.S3Driver)

	seedCount := params.SeedCount
	if seedCount <= 0 {
		seedCount = constants.DefaultSeedObjectCount
	}

	readShuffleBatchSize := 0
	if params.ReadShuffle {
		readShuffleBatchSize = params.ReadShuffleBatchSize
		switch {
		case readShuffleBatchSize <= 0:
			readShuffleBatchSize = constants.ReadShuffleDefaultBatchSize
		case readShuffleBatchSize > constants.ReadShuffleMaxBatchSize:
			readShuffleBatchSize = constants.ReadShuffleMaxBatchSize
		}
	}
	readPhasePauseSeconds := params.ReadPhasePauseSeconds
	if readPhasePauseSeconds <= 0 {
		readPhasePauseSeconds = DefaultReadPhasePauseSeconds
	}

	data := map[string]interface{}{
		templateKeyConcurrency:          params.Threads,
		templateKeyItemSize:             fmt.Sprintf(`"%s"`, escapeJSONString(params.ObjectSize)),
		templateKeyItemCount:            params.ObjectCount,
		templateKeyOutputPath:           fmt.Sprintf(`"%s"`, escapeJSONString(bucketPath)),
		templateKeyDuration:             fmt.Sprintf(`"%s"`, escapeJSONString(params.Duration)),
		templateKeyPartSize:             fmt.Sprintf(`"%s"`, escapeJSONString(params.PartSize)),
		templateKeyHasPartSize:          params.PartSize != "",
		templateKeyTimestamp:            time.Now().Unix(),
		templateKeyStorageDriverType:    fmt.Sprintf(`"%s"`, driverType),
		templateKeySeedCount:            seedCount,
		templateKeyReadShuffle:          params.ReadShuffle,
		templateKeyReadShuffleBatchSize: readShuffleBatchSize,
		templateKeyReadPhasePause:       readPhasePauseSeconds,
		// Step IDs: seed=1, read=2, delete=3 (read-from-file: read=1, delete=2)
		templateKeyStepIDSeed:   formatStepID(1, ts, stepOpSeed),
		templateKeyStepIDRead:   formatStepID(2, ts, stepOpRead),
		templateKeyStepIDDelete: formatStepID(3, ts, stepOpDelete),
	}

	var tmplStr string

	// When ItemsFile is set, use read-from-file templates (skip seed phase)
	if params.ItemsFile != "" {
		data[templateKeyItemsFile] = fmt.Sprintf(`"%s"`, escapeJSONString(params.ItemsFile))
		// Renumber step IDs: read=1, delete=2 (no seed)
		data[templateKeyStepIDRead] = formatStepID(1, ts, stepOpRead)
		data[templateKeyStepIDDelete] = formatStepID(2, ts, stepOpDelete)

		if params.Cleanup {
			if params.ObjectCount > 0 {
				tmplStr = readFromFileCountCleanupTemplate
			} else if params.Duration != "" {
				tmplStr = readFromFileDurationCleanupTemplate
			} else {
				// Default: duration-based with cleanup
				data[templateKeyDuration] = `"60s"`
				tmplStr = readFromFileDurationCleanupTemplate
			}
		} else {
			if params.ObjectCount > 0 {
				tmplStr = readFromFileCountTemplate
			} else if params.Duration != "" {
				tmplStr = readFromFileDurationTemplate
			} else {
				// Default: duration-based without cleanup
				data[templateKeyDuration] = `"60s"`
				tmplStr = readFromFileDurationTemplate
			}
		}
	} else {
		// Standard read templates with seed phase
		if params.Cleanup {
			if params.ObjectCount > 0 {
				tmplStr = readWithCleanupCountTemplate
			} else if params.Duration != "" {
				tmplStr = readWithCleanupDurationTemplate
			} else {
				tmplStr = readDefaultTemplate
			}
		} else {
			if params.ObjectCount > 0 {
				tmplStr = readNoCleanupCountTemplate
			} else if params.Duration != "" {
				tmplStr = readNoCleanupDurationTemplate
			} else {
				// Default: cleanup=true with defaults
				tmplStr = readDefaultTemplate
			}
		}
	}

	tmpl, err := template.New("readScenario").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse read template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute read template: %w", err)
	}

	return buf.String(), nil
}

// GenerateListScenario creates a scenario for benchmarking S3 ListObject operations.
func GenerateListScenario(params Params) (string, error) {
	concurrency := params.Threads
	if concurrency <= 0 {
		concurrency = 1
	}

	bucketPath := "/" + strings.TrimPrefix(params.Bucket, "/")
	baseTS := resolveTimestamp(params)

	driverType := resolveStorageDriverType(params.S3Driver)

	opLimit := 0
	if params.ObjectCount > 0 {
		opLimit = params.ObjectCount
	}
	hasOpLimitCount := opLimit > 0

	trimmedDuration := strings.TrimSpace(params.Duration)
	hasDuration := trimmedDuration != ""
	durationValue := ""
	if hasDuration {
		durationValue = fmt.Sprintf(`"%s"`, escapeJSONString(trimmedDuration))
	}

	trimmedPrefix := strings.TrimSpace(params.Prefix)
	hasPrefix := trimmedPrefix != ""
	prefixValue := ""
	if hasPrefix {
		prefixValue = fmt.Sprintf(`"%s"`, escapeJSONString(trimmedPrefix))
	}

	data := map[string]interface{}{
		templateKeyConcurrency:          concurrency,
		templateKeyBucketPath:           fmt.Sprintf(`"%s"`, escapeJSONString(bucketPath)),
		templateKeyBatchSize:            defaultListBatchSize,
		templateKeyOpLimitCount:         opLimit,
		templateKeyOpLimitRate:          listOpLimitRateUnlimited,
		templateKeyHasOpLimitCount:      hasOpLimitCount,
		templateKeyStepID:               formatStepID(1, baseTS, listStepSuffix),
		templateKeyHasDuration:          hasDuration,
		templateKeyDuration:             durationValue,
		templateKeyHasPrefix:            hasPrefix,
		templateKeyPrefix:               prefixValue,
		templateKeyStorageDriverType:    fmt.Sprintf(`"%s"`, escapeJSONString(driverType)),
		templateKeyItemType:             fmt.Sprintf(`"%s"`, escapeJSONString(itemTypePath)),
		templateKeyItemNamingType:       fmt.Sprintf(`"%s"`, escapeJSONString(itemNamingTypeRandom)),
		templateKeyLoadOpType:           fmt.Sprintf(`"%s"`, escapeJSONString(loadOpTypeList)),
		templateKeyMetricsAveragePeriod: fmt.Sprintf(`"%s"`, escapeJSONString(metricsAveragePeriodFiveSeconds)),
		templateKeyNamingRadix:          listNamingRadix,
		templateKeyListDelimiter:        fmt.Sprintf(`"%s"`, escapeJSONString(listDelimiterDefault)),
		templateKeyFetchMetadata:        listFetchMetadataDefault,
		templateKeyIncludeVersions:      listIncludeVersionsDefault,
		templateKeyMaxKeys:              defaultListBatchSize,
	}

	tmpl, err := template.New("listScenario").Parse(listWorkloadTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse list template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute list template: %w", err)
	}

	return buf.String(), nil
}

// GenerateMockScenario creates a scenario for mock testing with optional cleanup
func GenerateMockScenario(params Params) (string, error) {
	// shared base timestamp for this scenario
	ts := resolveTimestamp(params)
	if params.Cleanup && params.ObjectCount > 0 {
		// Use PipelineLoad for cleanup - create then delete
		stepCreate := formatStepID(1, ts, "create")
		stepDelete := formatStepID(2, ts, "delete")
		scenario := fmt.Sprintf(`// Mock pipeline configuration for create and delete operations
var concurrency = %d;
var itemSize = "%s";
var itemCount = %d;

var sharedConfig = {
  "storage": {
    "driver": {
      "type": "dummy-mock",
      "limit": {
        "concurrency": concurrency
      }
    }
  },
  "item": {
    "data": {
      "size": itemSize
    }
  }
};

var createConfig = {
  "load": {
    "op": {
      "type": "create",
      "limit": {
        "count": itemCount
      }
    },
    "step": {
      "id": "%s"
    }
  }
};

var deleteConfig = {
  "load": {
    "op": {
      "type": "delete"
    },
    "step": {
      "id": "%s"
    }
  }
};

PipelineLoad
  .config(sharedConfig)
  .append(createConfig)
  .append(deleteConfig)
  .run();`,
			params.Threads,
			escapeJSONString(params.ObjectSize),
			params.ObjectCount,
			stepCreate,
			stepDelete)

		return scenario, nil
	}

	// Build simple mock scenario (no cleanup)
	var scenario string

	if params.ObjectCount > 0 {
		stepID := formatStepID(1, ts, "create")
		scenario = fmt.Sprintf(`// Mock operation configuration
var concurrency = %d;
var itemSize = "%s";
var itemCount = %d;

var config = {
  "storage": {
    "driver": {
      "type": "dummy-mock",
      "limit": {
        "concurrency": concurrency
      }
    }
  },
  "item": {
    "data": {
      "size": itemSize
    }
  },
  "load": {
    "op": {
      "type": "create",
      "limit": {
        "count": itemCount
      }
    },
    "step": {
      "id": "%s"
    }
  }
};

Load
  .config(config)
  .run();`,
			params.Threads,
			escapeJSONString(params.ObjectSize),
			params.ObjectCount,
			stepID)
	} else if params.Duration != "" {
		stepID := formatStepID(1, ts, "create")
		scenario = fmt.Sprintf(`// Mock operation configuration
var concurrency = %d;
var itemSize = "%s";
var duration = "%s";

var config = {
  "storage": {
    "driver": {
      "type": "dummy-mock",
      "limit": {
        "concurrency": concurrency
      }
    }
  },
  "item": {
    "data": {
      "size": itemSize
    }
  },
  "load": {
    "op": {
      "type": "create"
    },
    "step": {
      "limit": {
        "time": duration
      },
      "id": "%s"
    }
  }
};

Load
  .config(config)
  .run();`,
			params.Threads,
			escapeJSONString(params.ObjectSize),
			escapeJSONString(params.Duration),
			stepID)
	} else {
		// Default to 100 objects if no limit specified
		stepID := formatStepID(1, ts, "create")
		scenario = fmt.Sprintf(`// Mock operation configuration (default count)
var concurrency = %d;
var itemSize = "%s";
var itemCount = 100;

var config = {
  "storage": {
    "driver": {
      "type": "dummy-mock",
      "limit": {
        "concurrency": concurrency
      }
    }
  },
  "item": {
    "data": {
      "size": itemSize
    }
  },
  "load": {
    "op": {
      "type": "create",
      "limit": {
        "count": itemCount
      }
    },
    "step": {
      "id": "%s"
    }
  }
};

Load
  .config(config)
  .run();`,
			params.Threads,
			escapeJSONString(params.ObjectSize),
			stepID)
	}

	return scenario, nil
}

// BuildEndpointArgs creates CLI arguments for endpoint configuration and metrics settings
// DEPRECATED: This function is being phased out in favor of using defaults.yaml configuration
// via the GenerateDefaults function. It's kept for backward compatibility with the old flow.
func BuildEndpointArgs(_ Params) ([]string, error) {
	// With the new API-based approach, all configuration goes through defaults.yaml
	// This function now returns empty args as the configuration is handled differently
	// TODO: Remove this function once all callers have been updated to use the new flow
	return []string{}, nil
}

// escapeJSONString safely escapes a string for use in JSON
func escapeJSONString(s string) string {
	// Use Go's built-in JSON marshaling to properly escape the string
	escaped, _ := json.Marshal(s)
	// Remove the surrounding quotes as we'll add them in the template
	return string(escaped[1 : len(escaped)-1])
}
