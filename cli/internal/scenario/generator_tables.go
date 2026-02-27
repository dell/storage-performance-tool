package scenario

import (
	"bytes"
	"fmt"
	"net/url"
	"text/template"
	"time"
)

// GenerateTablesScenario generates a JS scenario for an S3 Tables workload.
func GenerateTablesScenario(params Params) (string, error) {
	tp := params.Tables

	// Parse endpoint for host/port/ssl
	ep := params.Endpoint
	if ep == "" && len(params.Endpoints) > 0 {
		ep = params.Endpoints[0]
	}
	if ep == "" {
		return "", fmt.Errorf("--endpoint is required for tables workload")
	}
	u, err := url.Parse(ep)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint %q: %w", ep, err)
	}
	host := u.Hostname()
	portStr := u.Port()
	if portStr == "" {
		if u.Scheme == "https" {
			portStr = "443"
		} else {
			portStr = "80"
		}
	}
	port := 0
	if _, scanErr := fmt.Sscanf(portStr, "%d", &port); scanErr != nil {
		return "", fmt.Errorf("invalid port in endpoint %q", ep)
	}
	ssl := u.Scheme == "https"

	duration := params.Duration
	if duration == "" {
		duration = "5m"
	}

	ts := baseTimestamp()

	data := map[string]interface{}{
		templateKeyTablesEndpoint:         host,
		templateKeyTablesAccessKey:        params.AccessKey,
		templateKeyTablesSecretKey:        params.SecretKey,
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
		templateKeyTablesPort:             port,
		templateKeyTablesSSL:              ssl,
		"NoProvision":                     tp.NoProvision,
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
