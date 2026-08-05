package scenario

import (
	"bytes"
	"fmt"
	"text/template"
	"time"
)

// GenerateTablesScenario generates a JS scenario for an S3 Tables workload.
func GenerateTablesScenario(params Params) (string, error) {
	tp := params.Tables

	duration := params.Duration
	if duration == "" {
		duration = "5m"
	}

	ts := resolveTimestamp(params)

	data := map[string]interface{}{
		templateKeyTablesBucket:           tp.TableBucket,
		templateKeyTablesNamespace:        tp.Namespace,
		templateKeyTablesTableName:        tp.TableName,
		templateKeyTablesConcurrency:      tp.ConcurrentWriters,
		templateKeyTablesCommitFreqMs:     tp.CommitFreqMs,
		templateKeyTablesTargetFileSizeB:  tp.TargetFileSizeBytes,
		templateKeyTablesIngestFileSizeB:  tp.IngestFileSizeBytes,
		templateKeyTablesTotalIngestB:     tp.TotalIngestBytes,
		templateKeyTablesNamespaceCount:   tp.NamespaceCount,
		templateKeyTablesTablesPerNs:      tp.TablesPerNs,
		templateKeyTablesReadConcurrency:  tp.ReadConcurrency,
		templateKeyTablesCompactionToutMs: tp.CompactionTimeoutMs,
		templateKeyTablesDuration:         duration,
		"NoProvision":                     tp.NoProvision,
		// opMode values
		"OpModeProvision":      tablesOpModeProvision,
		"OpModeTableWrite":     tablesOpModeTableWrite,
		"OpModeCatalogSeed":    tablesOpModeCatalogSeed,
		"OpModeTableCatalog":   tablesOpModeTableCatalog,
		"OpModeCompactionPoll": tablesOpModeCompactionPoll,
		// Step IDs
		templateKeyTablesStepIDProvision:  formatStepID(1, ts, "provision"),
		templateKeyTablesStepIDWrite:      formatStepID(2, ts, "write"),
		templateKeyTablesStepIDCompaction: formatStepID(3, ts, "compaction"),
		templateKeyTablesStepIDSeed:       formatStepID(2, ts, "seed"),
		templateKeyTablesStepIDCatalog:    formatStepID(3, ts, "catalog"),
		// Derived counts
		"TablesIngestFileCount": ingestFileCount(tp.TotalIngestBytes, tp.IngestFileSizeBytes),
		"TablesSeedCount":       tp.NamespaceCount * tp.TablesPerNs,
		"Timestamp":             time.Now().Unix(),
	}

	var tmplStr string
	switch tp.TestVector {
	case tablesTestVectorTPS, "":
		tmplStr = tablesTPSTemplate
	case tablesTestVectorCompaction:
		tmplStr = tablesCompactionTemplate
	case tablesTestVectorCatalog:
		tmplStr = tablesCatalogTemplate
	default:
		return "", fmt.Errorf("unknown --test-vector %q: must be tps, compaction, or catalog", tp.TestVector)
	}

	// Use a FuncMap so templates can call "not"
	funcMap := template.FuncMap{
		"not": func(v bool) bool { return !v },
	}
	tmpl, err := template.New("tables").Funcs(funcMap).Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse tables template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute tables template: %w", err)
	}

	return buf.String(), nil
}

// ingestFileCount returns the number of small files needed to reach totalBytes.
// Returns 1 if either value is zero to avoid division by zero.
func ingestFileCount(totalBytes, fileSizeBytes int64) int64 {
	if fileSizeBytes <= 0 || totalBytes <= 0 {
		return 1
	}
	count := totalBytes / fileSizeBytes
	if totalBytes%fileSizeBytes != 0 {
		count++
	}
	return count
}
