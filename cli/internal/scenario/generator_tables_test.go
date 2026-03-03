package scenario

import (
	"strings"
	"testing"
)

// baseTablesParams returns a minimal valid TablesParams for testing.
func baseTablesParams() Params {
	return Params{
		WorkloadType: workloadTypeTables,
		Endpoint:     "http://172.17.0.2:4566",
		AccessKey:    "testkey",
		SecretKey:    "testsecret",
		Duration:     "30s",
		Tables: TablesParams{
			TestVector:          tablesTestVectorTPS,
			TableBucket:         "spt-tables",
			Namespace:           "default",
			TableName:           "spt-bench",
			ConcurrentWriters:   4,
			CommitFreqMs:        500,
			TargetFileSizeBytes: 1048576,  // 1MB
			IngestFileSizeBytes: 102400,   // 100KB
			TotalIngestBytes:    10485760, // 10MB
			NamespaceCount:      10,
			TablesPerNs:         10,
			ReadConcurrency:     4,
			CompactionTimeoutMs: 14400000, // 4h
			NoProvision:         false,
		},
	}
}

func TestGenerateTablesScenario_TPS(t *testing.T) {
	params := baseTablesParams()
	params.Tables.TestVector = tablesTestVectorTPS

	out, err := GenerateTablesScenario(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	checks := []string{
		`"type": "s3-tables"`,
		`"concurrency": concurrency`,
		`"uid": "testkey"`,
		`"secret": "testsecret"`,
		`"bucket": bucket`,
		`"namespace": namespace`,
		`"tableName": tableName`,
		`"commitFreqMs": 500`,
		`"targetFileSizeBytes": 1048576`,
		`opMode = "provision"`,
		`opMode = "tableWrite"`,
		`"time": "30s"`,
		`"type": "noop"`,
		`"type": "create"`,
		`-provision"`,
		`-write"`,
	}
	for _, c := range checks {
		if !strings.Contains(out, c) {
			t.Errorf("TPS scenario missing %q\nFull output:\n%s", c, out)
		}
	}

	// Compaction step must NOT appear in TPS scenario
	if strings.Contains(out, `opMode = "tableCompactionPoll"`) {
		t.Error("TPS scenario should not contain tableCompactionPoll")
	}
}

func TestGenerateTablesScenario_TPS_NoProvision(t *testing.T) {
	params := baseTablesParams()
	params.Tables.NoProvision = true

	out, err := GenerateTablesScenario(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Contains(out, `opMode = "provision"`) {
		t.Error("NoProvision=true should omit provision phase")
	}
	if !strings.Contains(out, `opMode = "tableWrite"`) {
		t.Error("NoProvision=true should still include write phase")
	}
}

func TestGenerateTablesScenario_Compaction(t *testing.T) {
	params := baseTablesParams()
	params.Tables.TestVector = tablesTestVectorCompaction

	out, err := GenerateTablesScenario(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	checks := []string{
		`opMode = "provision"`,
		`opMode = "tableWrite"`,
		`opMode = "tableCompactionPoll"`,
		`"concurrency": 1`,
		`compactionConfig.storage.s3tables.compactionPollTimeoutMs = 14400000`,
		`"ingestFileSizeBytes": 102400`,
		`"totalIngestBytes": 10485760`,
		`-provision"`,
		`-write"`,
		`-compaction"`,
	}
	for _, c := range checks {
		if !strings.Contains(out, c) {
			t.Errorf("compaction scenario missing %q\nFull output:\n%s", c, out)
		}
	}

	// ingestFileCount: 10MB / 100KB = 102.4 → ceil = 103
	if !strings.Contains(out, `"count": 103`) {
		t.Errorf("compaction seed phase should have count=103 (ceil(10MB/100KB))\nFull output:\n%s", out)
	}
}

func TestGenerateTablesScenario_Catalog(t *testing.T) {
	params := baseTablesParams()
	params.Tables.TestVector = tablesTestVectorCatalog

	out, err := GenerateTablesScenario(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	checks := []string{
		`opMode = "provision"`,
		`opMode = "catalogSeed"`,
		`opMode = "tableCatalog"`,
		`"namespaceCount": 10`,
		`"tablesPerNamespace": 10`,
		`"concurrency": 4`, // read concurrency
		`"type": "create"`,
		`"time": "30s"`,
		`-provision"`,
		`-seed"`,
		`-catalog"`,
	}
	for _, c := range checks {
		if !strings.Contains(out, c) {
			t.Errorf("catalog scenario missing %q\nFull output:\n%s", c, out)
		}
	}

	// seed count: 10 namespaces × 10 tables = 100
	if !strings.Contains(out, `"count": 100`) {
		t.Errorf("catalog seed phase should have count=100 (10×10)\nFull output:\n%s", out)
	}
}

func TestGenerateTablesScenario_HTTPS(t *testing.T) {
	// Net/ssl config is no longer embedded in the JS template — it is passed via
	// CLI defaults config so that the confuse library can handle list values correctly.
	// The generator should succeed with an HTTPS endpoint without error.
	params := baseTablesParams()
	params.Endpoint = "https://s3tables.us-east-1.amazonaws.com"

	out, err := GenerateTablesScenario(params)
	if err != nil {
		t.Fatalf("unexpected error for HTTPS endpoint: %v", err)
	}
	if !strings.Contains(out, `"type": "s3-tables"`) {
		t.Errorf("HTTPS scenario should still produce valid s3-tables config\nFull output:\n%s", out)
	}
	// Net/ssl must NOT be embedded in the JS (would cause InvalidValuePathException)
	if strings.Contains(out, `"addrs"`) {
		t.Errorf("net.addrs must not be embedded in tables JS template\nFull output:\n%s", out)
	}
}

func TestGenerateTablesScenario_InvalidVector(t *testing.T) {
	params := baseTablesParams()
	params.Tables.TestVector = "invalid"

	_, err := GenerateTablesScenario(params)
	if err == nil {
		t.Fatal("expected error for invalid test vector, got nil")
	}
}

// TestTablesTemplates_UseRunNotStartAwait verifies that all tables templates use
// .run() instead of .start()/.await()/.close(), which is required for the engine
// to enforce configured step time limits.
func TestTablesTemplates_UseRunNotStartAwait(t *testing.T) {
	vectors := []string{tablesTestVectorTPS, tablesTestVectorCompaction, tablesTestVectorCatalog}
	for _, v := range vectors {
		params := baseTablesParams()
		params.Tables.TestVector = v
		out, err := GenerateTablesScenario(params)
		if err != nil {
			t.Fatalf("[%s] unexpected error: %v", v, err)
		}
		if strings.Contains(out, ".start()") {
			t.Errorf("[%s] template uses .start(); must use .run() for time-limit enforcement", v)
		}
		if strings.Contains(out, ".await()") {
			t.Errorf("[%s] template uses .await(); must use .run() for time-limit enforcement", v)
		}
		if strings.Contains(out, ".close()") {
			t.Errorf("[%s] template uses .close(); must use .run() for time-limit enforcement", v)
		}
		if !strings.Contains(out, ".run()") {
			t.Errorf("[%s] template missing .run() call", v)
		}
	}
}

func TestGenerateScenario_TablesDispatch(t *testing.T) {
	params := baseTablesParams()

	out, err := GenerateScenario(params)
	if err != nil {
		t.Fatalf("GenerateScenario dispatch to tables failed: %v", err)
	}
	if !strings.Contains(out, `"type": "s3-tables"`) {
		t.Error("GenerateScenario should dispatch to tables generator")
	}
}

func TestIngestFileCount(t *testing.T) {
	cases := []struct {
		total    int64
		fileSize int64
		want     int64
	}{
		{10 * 1024 * 1024, 100 * 1024, 103}, // 10MB / 100KB = 102.4 → 103
		{1024 * 1024, 1024 * 1024, 1},       // exact division
		{0, 100 * 1024, 1},                  // zero total → 1
		{100 * 1024, 0, 1},                  // zero file size → 1
		{100, 10, 10},                       // exact
		{101, 10, 11},                       // ceil
	}
	for _, c := range cases {
		got := ingestFileCount(c.total, c.fileSize)
		if got != c.want {
			t.Errorf("ingestFileCount(%d, %d) = %d, want %d", c.total, c.fileSize, got, c.want)
		}
	}
}
