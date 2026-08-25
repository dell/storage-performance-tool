package scenario

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
	"gopkg.in/yaml.v3"
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
	if strings.Contains(generated, "seedMillis") || strings.Contains(generated, "discoveryMillis") {
		t.Fatalf("explicit-manifest DELETE reported an inapplicable named setup phase:\n%s", generated)
	}
	if !strings.Contains(generated, "var setupStartedNanos = com.dell.spt.base.load.step.DurationTime.monotonicEpochNanos()") ||
		!strings.Contains(generated, `"workflowStartedEpochNanos": setupStartedNanos`) {
		t.Fatalf("explicit-manifest DELETE omitted the full workflow boundary:\n%s", generated)
	}
	if !strings.Contains(generated, "selection_order=canonical") ||
		!strings.Contains(generated, "cross-tool") {
		t.Fatalf("generated scenario does not record canonical-order warning:\n%s", generated)
	}
}

func TestGenerateExplicitManifestDeleteRejectsCleanup(t *testing.T) {
	_, err := GenerateDeleteScenario(Params{
		WorkloadType:    workload.Delete,
		RunID:           778,
		ItemsFile:       "/spt-input/items/verify-input.csv",
		Threads:         1,
		DeleteBatchSize: 1,
		Cleanup:         true,
	})
	if err == nil || !strings.Contains(err.Error(), "did not create") {
		t.Fatalf("explicit-manifest cleanup error = %v", err)
	}
}

func TestGeneratedDeleteScenariosFreezeSelectionMetadataBeforeDispatch(t *testing.T) {
	tests := []struct {
		name   string
		params Params
	}{
		{
			name: "prepared manifest",
			params: Params{WorkloadType: workload.Delete, RunID: 901, ItemsFile: "/spt-input/items/verify-input.csv",
				Threads: 1, S3Driver: S3DriverNetty, DeleteBatchSize: 1, BaseTimestamp: "20260822.120000.000"},
		},
		{
			name: "seeded inventory",
			params: Params{WorkloadType: workload.Delete, RunID: 902, Bucket: "bucket", ObjectCount: 2,
				Threads: 1, S3Driver: S3DriverNetty, DeleteBatchSize: 1, BaseTimestamp: "20260822.120000.000"},
		},
		{
			name: "discovered inventory",
			params: Params{WorkloadType: workload.Delete, RunID: 903, Bucket: "bucket", Prefix: "safe/", ObjectCount: 2,
				DeleteExisting: true, Threads: 1, S3Driver: S3DriverNetty, DeleteBatchSize: 1, BaseTimestamp: "20260822.120000.000"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generated, err := GenerateDeleteScenario(test.params)
			if err != nil {
				t.Fatal(err)
			}
			freeze := strings.Index(generated, "StandaloneDeleteSelection.fromManifest")
			dispatch := strings.Index(generated, "DeleteLoad.config")
			if freeze < 0 || dispatch < 0 || freeze > dispatch {
				t.Fatalf("selection metadata was not frozen before DELETE dispatch:\n%s", generated)
			}
			for _, field := range []string{
				`"selectionOrder": "canonical"`,
				`"selected": deleteSelection.selected()`,
				`"selectedCurrentKey": deleteSelection.selectedCurrentKey()`,
				`"selectedExactVersion": deleteSelection.selectedExactVersion()`,
				`"selectedBuckets": deleteSelection.selectedBuckets()`,
			} {
				if !strings.Contains(generated, field) {
					t.Fatalf("generated DELETE omitted frozen selection field %q:\n%s", field, generated)
				}
			}
		})
	}
}

func TestDeleteScenarioTemplatesRenderSelectionOrderFromSharedData(t *testing.T) {
	const selectionOrder = "selection-order-sentinel"
	tests := []struct {
		name         string
		templateName string
		data         any
	}{
		{
			name:         "manifest",
			templateName: "delete-manifest",
			data:         deleteManifestScenarioData{SelectionOrder: selectionOrder},
		},
		{
			name:         "seeded",
			templateName: "delete-seeded",
			data:         deleteSeededScenarioData{SelectionOrder: selectionOrder},
		},
		{
			name:         "existing prefix",
			templateName: "delete-existing",
			data:         deleteExistingScenarioData{SelectionOrder: selectionOrder},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generated, err := executeIntegrityScenario(test.templateName, test.data)
			if err != nil {
				t.Fatal(err)
			}
			want := `"selectionOrder": "` + selectionOrder + `"`
			if !strings.Contains(generated, want) {
				t.Fatalf("generated DELETE did not use the supplied selection order %q:\n%s", want, generated)
			}
		})
	}
}

func TestGenerateSeededDeleteScenarioFreezesUniqueInventoryBeforeTimedDelete(t *testing.T) {
	generated, err := GenerateDeleteScenario(Params{
		WorkloadType:    workload.Delete,
		RunID:           880,
		Bucket:          "bucket-a",
		Prefix:          "/team/root/",
		ObjectCount:     3,
		Threads:         2,
		S3Driver:        S3DriverAws,
		DeleteBatchSize: 2,
		BaseTimestamp:   "20260822.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(generated, "ListLoad.config") || strings.Contains(generated, `"type": "list"`) {
		t.Fatalf("seeded DELETE must not discover existing objects:\n%s", generated)
	}
	if strings.Contains(generated, "currentTimeMillis") ||
		!strings.Contains(generated, "var setupStartedNanos = com.dell.spt.base.load.step.DurationTime.monotonicEpochNanos()") ||
		!strings.Contains(generated, "var seedStartedNanos = java.lang.System.nanoTime()") ||
		!strings.Contains(generated, `"seedMillis": seedDurationMillis`) ||
		!strings.Contains(generated, `"workflowStartedEpochNanos": setupStartedNanos`) {
		t.Fatalf("seeded DELETE omitted measured seed phase:\n%s", generated)
	}

	plan, err := BuildStepPlanFromScenario(generated)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 2 || plan.Steps[0].Op != stepOpSeed || plan.Steps[1].Op != stepOpDelete {
		t.Fatalf("seeded DELETE plan = %+v, want seed then delete", plan.Steps)
	}

	configs := parseGeneratedScenarioConfigs(t, generated)
	if len(configs) != 2 {
		t.Fatalf("seeded DELETE configs = %d, want CREATE and DELETE", len(configs))
	}
	seed, deleteConfig := configs[0], configs[1]
	if got := generatedConfigValue(t, seed, "load", "op", "type"); got != "create" {
		t.Fatalf("seed operation = %#v, want create", got)
	}
	if got := generatedConfigValue(t, seed, "load", "op", "limit", "count"); got != float64(3) {
		t.Fatalf("seed count = %#v, want 3", got)
	}
	if got := generatedConfigValue(t, seed, "item", "data", "size"); got != "1KiB" {
		t.Fatalf("seed object size = %#v, want 1KiB", got)
	}
	if got := generatedConfigValue(t, seed, "item", "naming", "prefix"); got != "team/root/spt-delete-880/" {
		t.Fatalf("seed namespace = %#v, want safe unique child of supplied root", got)
	}
	if got := generatedConfigValue(t, seed, "item", "output", "file"); got != "writtenFile" {
		t.Fatalf("seed manifest output = %#v, want writtenFile", got)
	}
	if got := generatedConfigValue(t, seed, "storage", "integrity", "input", "provenance"); got != constants.IntegrityProvenanceNone {
		t.Fatalf("seed provenance = %#v, want none", got)
	}
	if got := generatedConfigValue(t, seed, "storage", "integrity", "output", "requireExactCount"); got != true {
		t.Fatalf("seed exact-output policy = %#v, want true", got)
	}
	if got := generatedConfigValue(t, deleteConfig, "item", "input", "file"); got != "writtenFile" {
		t.Fatalf("timed DELETE input = %#v, want frozen writtenFile", got)
	}
	if got := generatedConfigValue(t, deleteConfig, "storage", "integrity", "input", "provenance"); got != constants.IntegrityProvenanceEngineStep {
		t.Fatalf("timed DELETE provenance = %#v, want engine_step", got)
	}
	if got := generatedConfigValue(t, deleteConfig, "storage", "integrity", "input", "expectedProducerId"); got != plan.Steps[0].ID {
		t.Fatalf("timed DELETE producer = %#v, want %q", got, plan.Steps[0].ID)
	}
	if got := generatedConfigValue(t, deleteConfig, "load", "op", "delete", "standalone"); got != true {
		t.Fatalf("timed DELETE standalone = %#v, want true", got)
	}
	if _, ok := configPath(deleteConfig, "load", "op", "limit", "count"); ok {
		t.Fatal("global object count was incorrectly mapped to timed request-count limit")
	}
}

func TestGenerateSeededDeleteScenarioUsesExplicitFiniteDefaults(t *testing.T) {
	generated, err := GenerateDeleteScenario(Params{
		WorkloadType:    workload.Delete,
		RunID:           881,
		Bucket:          "bucket-a",
		Threads:         1,
		DeleteBatchSize: DefaultDeleteBatchSize,
		BaseTimestamp:   "20260822.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	configs := parseGeneratedScenarioConfigs(t, generated)
	if got := generatedConfigValue(t, configs[0], "load", "op", "limit", "count"); got != float64(2500) {
		t.Fatalf("default seed count = %#v, want 2500", got)
	}
	if got := generatedConfigValue(t, configs[0], "item", "data", "size"); got != "1KiB" {
		t.Fatalf("default seed size = %#v, want 1KiB", got)
	}
	if got := generatedConfigValue(t, configs[0], "item", "naming", "prefix"); got != "spt-delete-881/" {
		t.Fatalf("default seed namespace = %#v", got)
	}
}

func TestGenerateExistingPrefixDeleteScenarioFreezesCurrentKeysBeforeTimedDelete(t *testing.T) {
	generated, err := GenerateDeleteScenario(Params{
		WorkloadType:    workload.Delete,
		RunID:           882,
		Bucket:          "existing-bucket",
		Prefix:          "guarded/root/",
		ObjectCount:     3,
		Threads:         2,
		S3Driver:        S3DriverNetty,
		DeleteBatchSize: 2,
		DeleteExisting:  true,
		SelectionOrder:  SelectionOrderCanonical,
		BaseTimestamp:   "20260822.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(generated, "currentTimeMillis") ||
		!strings.Contains(generated, "var setupStartedNanos = com.dell.spt.base.load.step.DurationTime.monotonicEpochNanos()") ||
		!strings.Contains(generated, "var discoveryStartedNanos = java.lang.System.nanoTime()") ||
		!strings.Contains(generated, `"discoveryMillis": discoveryDurationMillis`) ||
		!strings.Contains(generated, `"workflowStartedEpochNanos": setupStartedNanos`) {
		t.Fatalf("existing-prefix DELETE omitted measured discovery phase:\n%s", generated)
	}
	plan, err := BuildStepPlanFromScenario(generated)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 2 || plan.Steps[0].Op != stepOpList || plan.Steps[1].Op != stepOpDelete {
		t.Fatalf("existing-prefix DELETE plan = %+v, want discovery then delete", plan.Steps)
	}

	configs := parseGeneratedScenarioConfigs(t, generated)
	if len(configs) != 2 {
		t.Fatalf("existing-prefix DELETE configs = %d, want LIST and DELETE", len(configs))
	}
	discovery, deleteConfig := configs[0], configs[1]
	if got := generatedConfigValue(t, discovery, "item", "input", "path"); got != "/existing-bucket" {
		t.Fatalf("discovery bucket = %#v", got)
	}
	if got := generatedConfigValue(t, discovery, "item", "naming", "prefix"); got != "guarded/root/" {
		t.Fatalf("discovery prefix = %#v", got)
	}
	if got := generatedConfigValue(t, discovery, "load", "batch", "size"); got != float64(defaultListBatchSize) {
		t.Fatalf("discovery batch size = %#v, want shared default %d", got, defaultListBatchSize)
	}
	if got := generatedConfigValue(t, discovery, "load", "op", "list", "max_keys"); got != float64(defaultListBatchSize) {
		t.Fatalf("discovery max keys = %#v, want shared default %d", got, defaultListBatchSize)
	}
	if got := generatedConfigValue(t, discovery, "load", "op", "list", "include_versions"); got != false {
		t.Fatalf("existing-prefix version mode = %#v, want current-key listing", got)
	}
	if got := generatedConfigValue(t, discovery, "storage", "integrity", "selection", "maxCount"); got != float64(3) {
		t.Fatalf("global discovered object cap = %#v, want 3", got)
	}
	if got := generatedConfigValue(t, discovery, "storage", "integrity", "selection", "requireNonEmpty"); got != true {
		t.Fatalf("empty-discovery guard = %#v, want true", got)
	}
	if got := generatedConfigValue(t, deleteConfig, "item", "input", "file"); got != "verifyInputFile" {
		t.Fatalf("timed DELETE input = %#v, want frozen discovery manifest", got)
	}
	if got := generatedConfigValue(t, deleteConfig, "storage", "integrity", "input", "provenance"); got != constants.IntegrityProvenanceEngineStep {
		t.Fatalf("timed DELETE provenance = %#v", got)
	}
	if got := generatedConfigValue(t, deleteConfig, "storage", "integrity", "input", "expectedProducerId"); got != plan.Steps[0].ID {
		t.Fatalf("timed DELETE producer = %#v, want %q", got, plan.Steps[0].ID)
	}
	if _, ok := configPath(deleteConfig, "load", "op", "limit", "count"); ok {
		t.Fatal("global object count was mapped to timed DELETE request-count limit")
	}
	if !strings.Contains(generated, "Discovery is setup and excluded from DELETE request timing") {
		t.Fatalf("scenario omitted untimed discovery contract:\n%s", generated)
	}
}

func TestGenerateExistingPrefixDeleteAlwaysRequiresNonemptyDiscoveryIndependentlyFromCap(t *testing.T) {
	for _, test := range []struct {
		name        string
		objectCount int
		wantMax     float64
	}{
		{name: "uncapped zero", objectCount: 0, wantMax: 0},
		{name: "capped", objectCount: 3, wantMax: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			generated, err := GenerateDeleteScenario(Params{
				WorkloadType:    workload.Delete,
				RunID:           882,
				Bucket:          "existing-bucket",
				Prefix:          "guarded/root/",
				ObjectCount:     test.objectCount,
				Threads:         1,
				DeleteBatchSize: 1,
				DeleteExisting:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			discovery := parseGeneratedScenarioConfigs(t, generated)[0]
			if got := generatedConfigValue(
				t, discovery, "storage", "integrity", "selection", "requireNonEmpty",
			); got != true {
				t.Fatalf("empty-discovery guard = %#v, want true", got)
			}
			if got := generatedConfigValue(
				t, discovery, "storage", "integrity", "selection", "maxCount",
			); got != test.wantMax {
				t.Fatalf("selection cap = %#v, want %#v", got, test.wantMax)
			}
		})
	}
}

func TestIntegrityTemplateRequiresNonemptySelectionWithoutMaxCount(t *testing.T) {
	var rendered bytes.Buffer
	if err := integrityScenarioTemplates.ExecuteTemplate(
		&rendered, "integrity", integrityTemplateData{RequireNonEmpty: true},
	); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte("{"+rendered.String()+"}"), &config); err != nil {
		t.Fatalf("parse rendered integrity config: %v\n%s", err, rendered.String())
	}
	if got := generatedConfigValue(
		t, config, "integrity", "selection", "requireNonEmpty",
	); got != true {
		t.Fatalf("empty-discovery guard without maxCount = %#v, want true", got)
	}
	if _, ok := configPath(config, "integrity", "selection", "maxCount"); ok {
		t.Fatalf("nil selection cap unexpectedly rendered maxCount:\n%s", rendered.String())
	}
}

func TestGenerateWholeBucketDeleteExplicitlyOverridesInheritedPrefix(t *testing.T) {
	params := Params{
		WorkloadType:     workload.Delete,
		RunID:            883,
		Endpoint:         "http://127.0.0.1:9000",
		Bucket:           "existing-bucket",
		AllowEmptyPrefix: true,
		DeleteExisting:   true,
		Threads:          1,
		DeleteBatchSize:  1,
		SelectionOrder:   SelectionOrderCanonical,
		EngineOverrides:  []string{"item.naming.prefix=unexpected/narrowing/"},
		BaseTimestamp:    "20260822.120000.000",
	}
	defaultsYAML, err := GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var defaults map[string]any
	if err := yaml.Unmarshal(defaultsYAML, &defaults); err != nil {
		t.Fatal(err)
	}
	defaultPrefix, ok := configPath(defaults, "item", "naming", "prefix")
	if !ok || defaultPrefix != "unexpected/narrowing/" {
		t.Fatalf("test precondition: inherited prefix = %#v, present=%t\n%s", defaultPrefix, ok, defaultsYAML)
	}

	generated, err := GenerateDeleteScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	discovery := parseGeneratedScenarioConfigs(t, generated)[0]
	scenarioPrefix, ok := configPath(discovery, "item", "naming", "prefix")
	if !ok {
		t.Fatalf("whole-bucket scenario omitted the exact empty prefix and would inherit %#v:\n%s", defaultPrefix, generated)
	}
	// Load.config overlays the posted defaults. Rendering the empty leaf makes the effective
	// discovery scope the whole exact bucket even when an advanced override supplied a prefix.
	effectivePrefix := defaultPrefix
	if ok {
		effectivePrefix = scenarioPrefix
	}
	if effectivePrefix != "" {
		t.Fatalf("whole-bucket effective prefix = %#v, want explicit empty string", effectivePrefix)
	}
}

func TestGenerateExistingPrefixDeletePinsExhaustiveDiscoveryLimits(t *testing.T) {
	params := Params{
		WorkloadType:    workload.Delete,
		RunID:           884,
		Endpoint:        "http://127.0.0.1:9000",
		Bucket:          "existing-bucket",
		Prefix:          "guarded/",
		DeleteExisting:  true,
		Threads:         1,
		DeleteBatchSize: 1,
		SelectionOrder:  SelectionOrderCanonical,
		EngineOverrides: []string{
			"load.op.limit.count=1",
			"load.step.limit.time=1s",
			"load.step.limit.size=1",
			"item.input.file=/engine-visible/narrow.csv",
		},
		BaseTimestamp: "20260822.120000.000",
	}
	defaultsYAML, err := GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var defaults map[string]any
	if err := yaml.Unmarshal(defaultsYAML, &defaults); err != nil {
		t.Fatal(err)
	}
	for _, path := range [][]string{
		{"load", "op", "limit", "count"},
		{"load", "step", "limit", "time"},
		{"load", "step", "limit", "size"},
	} {
		if value, ok := configPath(defaults, path...); !ok || value == 0 {
			t.Fatalf("test precondition: hostile inherited limit %v = %#v, present=%t\n%s",
				path, value, ok, defaultsYAML)
		}
	}
	if value, ok := configPath(defaults, "item", "input", "file"); !ok || value != "/engine-visible/narrow.csv" {
		t.Fatalf("test precondition: hostile inherited item input file = %#v, present=%t\n%s",
			value, ok, defaultsYAML)
	}

	generated, err := GenerateDeleteScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	discovery := parseGeneratedScenarioConfigs(t, generated)[0]
	for _, path := range [][]string{
		{"load", "op", "limit", "count"},
		{"load", "step", "limit", "time"},
		{"load", "step", "limit", "size"},
	} {
		value, ok := configPath(discovery, path...)
		if !ok || value != float64(0) {
			t.Fatalf("discovery limit %v = %#v, present=%t; want explicit zero override:\n%s",
				path, value, ok, generated)
		}
	}
	if value, ok := configPath(discovery, "item", "input", "file"); !ok || value != "" {
		t.Fatalf("discovery item input file = %#v, present=%t; want explicit empty override:\n%s",
			value, ok, generated)
	}
}

func TestGenerateExistingPrefixDeleteScenarioRequiresGuardedCurrentKeyScope(t *testing.T) {
	for _, test := range []struct {
		name   string
		params Params
		detail string
	}{
		{
			name: "empty prefix", detail: "allow-empty-prefix",
			params: Params{RunID: 1, Bucket: "b", DeleteExisting: true, Threads: 1, DeleteBatchSize: 1},
		},
		{
			name: "slash prefix aliases whole bucket", detail: "must not start with '/'",
			params: Params{RunID: 1, Bucket: "b", Prefix: "/", DeleteExisting: true, Threads: 1, DeleteBatchSize: 1},
		},
		{
			name: "leading slash changes exact prefix", detail: "must not start with '/'",
			params: Params{RunID: 1, Bucket: "b", Prefix: "/p/", DeleteExisting: true, AllowEmptyPrefix: true, Threads: 1, DeleteBatchSize: 1},
		},
		{
			name: "all versions", detail: "current-key",
			params: Params{RunID: 1, Bucket: "b", Prefix: "p/", DeleteExisting: true, Versions: VersionsAll, Threads: 1, DeleteBatchSize: 1},
		},
		{
			name: "manifest source conflict", detail: "mutually exclusive",
			params: Params{RunID: 1, Bucket: "b", Prefix: "p/", DeleteExisting: true, ItemsFile: "items.csv", Threads: 1, DeleteBatchSize: 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := GenerateDeleteScenario(test.params)
			if err == nil || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("GenerateDeleteScenario() error = %v, want %q", err, test.detail)
			}
		})
	}
}

func TestGenerateExistingPrefixWholeBucketPreservesExplicitAllSelection(t *testing.T) {
	generated, err := GenerateDeleteScenario(Params{
		WorkloadType:     workload.Delete,
		RunID:            883,
		Bucket:           "whole-bucket",
		DeleteExisting:   true,
		AllowEmptyPrefix: true,
		Threads:          1,
		DeleteBatchSize:  1,
		SelectionOrder:   SelectionOrderCanonical,
		BaseTimestamp:    "20260822.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	discovery := parseGeneratedScenarioConfigs(t, generated)[0]
	if got := generatedConfigValue(t, discovery, "item", "naming", "prefix"); got != "" {
		t.Fatalf("whole-bucket discovery prefix = %#v, want explicit empty string", got)
	}
	if got := generatedConfigValue(t, discovery, "storage", "integrity", "selection", "maxCount"); got != float64(0) {
		t.Fatalf("whole-bucket object cap = %#v, want 0 (all)", got)
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
		{name: "seed bucket", params: Params{RunID: 1, Threads: 1, DeleteBatchSize: 1}, detail: "requires a bucket"},
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

func TestGenerateSeededDeleteScenarioRejectsNonFiniteOrInvalidInputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		params Params
		detail string
	}{
		{name: "bucket", params: Params{RunID: 1, Threads: 1, DeleteBatchSize: 1}, detail: "requires a bucket"},
		{name: "negative count", params: Params{RunID: 1, Bucket: "b", ObjectCount: -1, Threads: 1, DeleteBatchSize: 1}, detail: "non-negative"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := GenerateDeleteScenario(test.params)
			if err == nil || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("GenerateDeleteScenario() error = %v, want %q", err, test.detail)
			}
		})
	}
}

func TestDeleteGeneratorIsPublicAndProducesATimedPhase(t *testing.T) {
	spec, ok := workload.Lookup(workload.Delete)
	if !ok || !spec.Implemented {
		t.Fatalf("delete registry gate = %+v, found=%t; want implemented=true", spec, ok)
	}
	generated, err := GenerateScenario(Params{
		WorkloadType:    workload.Delete,
		RunID:           1,
		ItemsFile:       "/spt-input/items/verify-input.csv",
		Threads:         1,
		DeleteBatchSize: 1,
	})
	if err != nil || !strings.Contains(generated, "DeleteLoad.config") ||
		!strings.Contains(generated, `"standalone": true`) {
		t.Fatalf("public delete generation = %q, %v", generated, err)
	}
}

func TestGenerateDeleteDurationScenariosUseFiniteLiveInventoriesWithoutRecycle(t *testing.T) {
	tests := []struct {
		name       string
		params     Params
		wantSetup  string
		wantSource string
	}{
		{
			name: "seeded",
			params: Params{
				WorkloadType: workload.Delete, RunID: 21, Bucket: "owned", Duration: "45s",
				SeedCount: 4321, Threads: 3, DeleteBatchSize: 100,
			},
			wantSetup:  `"limit": {"count": 4321}`,
			wantSource: "CreateLoad.config",
		},
		{
			name: "manifest",
			params: Params{
				WorkloadType: workload.Delete, RunID: 22, ItemsFile: "/input/items.csv",
				Duration: "45s", Threads: 3, DeleteBatchSize: 1,
			},
			wantSource: "Standalone DELETE explicit-manifest",
		},
		{
			name: "existing prefix",
			params: Params{
				WorkloadType: workload.Delete, RunID: 23, Bucket: "existing", Prefix: "safe/",
				DeleteExisting: true, Duration: "45s", Threads: 3, DeleteBatchSize: 100,
			},
			wantSource: "Load.config",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generated, err := GenerateDeleteScenario(test.params)
			if err != nil {
				t.Fatalf("GenerateDeleteScenario() error = %v", err)
			}
			for _, want := range []string{
				test.wantSource,
				`"delete": {"standalone": true, "batchSize":`,
				`"duration": true`,
				`"limit": {"time": "45s"}`,
				`"recycle": {"mode": false}`,
				`"retry": false`,
				`"wait": {"finish": true}`,
			} {
				if !strings.Contains(generated, want) {
					t.Fatalf("duration DELETE scenario omitted %q:\n%s", want, generated)
				}
			}
			if test.wantSetup != "" && !strings.Contains(generated, test.wantSetup) {
				t.Fatalf("duration DELETE setup omitted %q:\n%s", test.wantSetup, generated)
			}
			deleteStep := generated[strings.LastIndex(generated, "DeleteLoad.config"):]
			if strings.Contains(deleteStep, `"limit": {"count"`) {
				t.Fatalf("duration DELETE incorrectly mapped identities to request count:\n%s", deleteStep)
			}
			if strings.Contains(deleteStep, "preValidationMillis") {
				t.Fatalf("manifest selection scan must not populate Ticket 14 pre-validation:\n%s", deleteStep)
			}
		})
	}
}

func TestGenerateSeededDeleteDurationDefaultsToFinite2500IdentityInventory(t *testing.T) {
	generated, err := GenerateDeleteScenario(Params{
		WorkloadType: workload.Delete, RunID: 24, Bucket: "owned", Duration: "30s",
		Threads: 1, DeleteBatchSize: DefaultDeleteBatchSize,
	})
	if err != nil {
		t.Fatalf("GenerateDeleteScenario() error = %v", err)
	}
	if !strings.Contains(generated, `"limit": {"count": 2500}`) {
		t.Fatalf("seeded duration default inventory missing:\n%s", generated)
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
