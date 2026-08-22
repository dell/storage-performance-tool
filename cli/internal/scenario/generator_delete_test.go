package scenario

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestDeleteCLIContractFixtureMatchesPreparedRuntimeInputs(t *testing.T) {
	fixture := func(name string) string {
		t.Helper()
		_, sourceFile, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("resolve generator test source path")
		}
		return filepath.Clean(filepath.Join(
			filepath.Dir(sourceFile), "..", "..", "..", "engine", "extensions", "storage-drivers",
			"implementations", "s3", "src", "test", "resources", "delete-cli-contract", name,
		))
	}
	prepared, err := PrepareExternalItemFiles(Params{
		WorkloadType:    workload.Delete,
		RunID:           777,
		ItemsFile:       fixture("source.csv"),
		Bucket:          "bucket",
		Threads:         1,
		S3Driver:        S3DriverNetty,
		DeleteBatchSize: 2,
		BaseTimestamp:   "20260822.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = CleanupPreparedItemFiles(t.Context(), prepared) }()

	if len(prepared.ItemFileMounts) != 2 ||
		prepared.ItemFileMounts[0].ContainerPath != "/spt-input/items/verify-input.csv" ||
		prepared.ItemFileMounts[1].ContainerPath != "/spt-input/items/verify-input.complete.json" {
		t.Fatalf("prepared mounts = %+v", prepared.ItemFileMounts)
	}
	for i, name := range []string{"verify-input.csv", "verify-input.complete.json"} {
		actual, readErr := os.ReadFile(prepared.ItemFileMounts[i].HostPath)
		if readErr != nil {
			t.Fatal(readErr)
		}
		expected, readErr := os.ReadFile(fixture(name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !bytes.Equal(actual, expected) {
			t.Fatalf("prepared %s differs from cross-project contract fixture:\n%s", name, actual)
		}
	}
	generated, err := GenerateDeleteScenario(prepared)
	if err != nil {
		t.Fatal(err)
	}
	encodedScenario, err := os.ReadFile(fixture("scenario.b64"))
	if err != nil {
		t.Fatal(err)
	}
	expectedScenario, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encodedScenario)))
	if err != nil {
		t.Fatal(err)
	}
	if generated != string(expectedScenario) {
		t.Fatalf("generated scenario differs from engine contract fixture:\n%s", generated)
	}
}

func TestPrepareDeleteManifestAndGenerateTerminalScenario(t *testing.T) {
	source := filepath.Join(t.TempDir(), "delete.csv")
	file, err := os.Create(source)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	for _, record := range [][]string{
		{"bucket", "key", "size", "version_id"},
		{"bucket-a", "z,key", "7", "version-z"},
		{"bucket-a", "alpha", "9", ""},
		{"bucket-a", "z,key", "7", "version-z"},
	} {
		if err = writer.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	prepared, err := PrepareExternalItemFiles(Params{
		WorkloadType:    workload.Delete,
		RunID:           777,
		ItemsFile:       source,
		Bucket:          "bucket-a",
		ObjectCount:     1,
		DeleteBatchSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = CleanupPreparedItemFiles(t.Context(), prepared) }()
	if prepared.ItemsFile != containerItemFilesDir+"/"+integrity.VerifyInputName {
		t.Fatalf("prepared items path = %q", prepared.ItemsFile)
	}
	if prepared.SelectionSourceCount != 3 || prepared.SelectionUniqueCount != 2 ||
		prepared.SelectionSelectedCount != 1 || prepared.SelectionOrder != SelectionOrderCanonical {
		t.Fatalf("unexpected prepared selection metadata: %+v", prepared)
	}
	if prepared.SelectionSHA256 == "" {
		t.Fatal("prepared selection digest is empty")
	}

	generated, err := GenerateDeleteScenario(Params{
		WorkloadType:    workload.Delete,
		RunID:           777,
		ItemsFile:       prepared.ItemsFile,
		Threads:         4,
		S3Driver:        S3DriverNetty,
		DeleteBatchSize: 1,
		SelectionOrder:  SelectionOrderCanonical,
		BaseTimestamp:   "20260822.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildStepPlanFromScenario(generated)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].Op != stepOpDelete {
		t.Fatalf("unexpected delete plan: %+v", plan.Steps)
	}
	configs := parseGeneratedScenarioConfigs(t, generated)
	if len(configs) != 1 {
		t.Fatalf("delete scenario has %d configs, want 1", len(configs))
	}
	config := configs[0]
	if got := generatedConfigValue(t, config, "load", "op", "type"); got != "delete" {
		t.Fatalf("operation type = %#v", got)
	}
	if got := generatedConfigValue(t, config, "load", "op", "delete", "standalone"); got != true {
		t.Fatalf("standalone mode = %#v", got)
	}
	if got := generatedConfigValue(t, config, "load", "op", "delete", "batchSize"); got != float64(1) {
		t.Fatalf("delete batch size = %#v", got)
	}
	if got := generatedConfigValue(t, config, "item", "input", "file"); got != prepared.ItemsFile {
		t.Fatalf("delete input = %#v", got)
	}
	if got := generatedConfigValue(t, config, "storage", "integrity", "input", "provenance"); got != constants.IntegrityProvenanceCLIStager {
		t.Fatalf("delete provenance = %#v", got)
	}
	if got := generatedConfigValue(t, config, "storage", "integrity", "input", "expectedProducerId"); got != constants.IntegrityCLIStagerProducerID {
		t.Fatalf("delete producer = %#v", got)
	}
	if _, ok := configPath(config, "load", "op", "limit", "count"); ok {
		t.Fatal("public object count was mapped to generic request-count limit")
	}
	if !strings.Contains(generated, "selection_order=canonical") ||
		!strings.Contains(generated, "cross-tool") {
		t.Fatalf("generated scenario does not record canonical-order warning:\n%s", generated)
	}
}

func TestPrepareDeleteManifestRejectsMultiBucketBatching(t *testing.T) {
	source := filepath.Join(t.TempDir(), "delete.csv")
	if err := os.WriteFile(source, []byte(
		"bucket,key,size,version_id\n"+
			"bucket-a,key-a,1,\n"+
			"bucket-b,key-b,1,version-b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := PrepareExternalItemFiles(Params{
		WorkloadType:    workload.Delete,
		RunID:           778,
		ItemsFile:       source,
		DeleteBatchSize: 2,
	})
	if err == nil || !strings.Contains(err.Error(), "multi-bucket") || !strings.Contains(err.Error(), "--delete-batch-size=1") {
		t.Fatalf("PrepareExternalItemFiles() error = %v", err)
	}
}

func TestGenerateDeleteScenarioRejectsIncompleteManifestContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		params Params
		detail string
	}{
		{name: "run id", params: Params{ItemsFile: "/in.csv", DeleteBatchSize: 1}, detail: "positive run id"},
		{name: "items file", params: Params{RunID: 1, DeleteBatchSize: 1}, detail: "canonical items file"},
		{name: "batch low", params: Params{RunID: 1, ItemsFile: "/in.csv", DeleteBatchSize: 0}, detail: "between 1 and 1000"},
		{name: "batch high", params: Params{RunID: 1, ItemsFile: "/in.csv", DeleteBatchSize: 1001}, detail: "between 1 and 1000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := GenerateDeleteScenario(test.params)
			if err == nil || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("GenerateDeleteScenario() error = %v, want %q", err, test.detail)
			}
		})
	}
}

func TestDeleteGeneratorIsInternalWhilePublicRegistryRemainsGated(t *testing.T) {
	spec, ok := workload.Lookup(workload.Delete)
	if !ok || spec.Implemented {
		t.Fatalf("delete registry gate = %+v, found=%t; want implemented=false", spec, ok)
	}
	generated, err := GenerateScenario(Params{
		WorkloadType:    workload.Delete,
		RunID:           1,
		ItemsFile:       "/spt-input/items/verify-input.csv",
		Threads:         1,
		DeleteBatchSize: 1,
	})
	if err != nil || !strings.Contains(generated, "DeleteLoad.config") {
		t.Fatalf("internal delete generation = %q, %v", generated, err)
	}
}

func configPath(config map[string]any, path ...string) (any, bool) {
	var value any = config
	for _, key := range path {
		object, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok = object[key]
		if !ok {
			return nil, false
		}
	}
	return value, true
}
