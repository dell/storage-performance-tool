package scenario

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
)

func TestGeneratedIntegrityPlansResolveSharedStepRoles(t *testing.T) {
	tests := []struct {
		name     string
		generate func() (string, error)
		want     func(StepPlan) integrity.StepRoles
	}{
		{
			name: "write verify",
			generate: func() (string, error) {
				return GenerateWriteVerifyScenario(Params{
					WorkloadType: WorkloadTypeWriteVerify, RunID: 501,
					Bucket: "bucket", ObjectSize: "1KiB", ObjectCount: 1, Threads: 1,
					BaseTimestamp: "20260801.120000.000",
				})
			},
			want: func(plan StepPlan) integrity.StepRoles {
				return integrity.StepRoles{Create: plan.Steps[0].ID, Read: plan.Steps[1].ID}
			},
		},
		{
			name: "read verify discovery",
			generate: func() (string, error) {
				return GenerateReadVerifyScenario(Params{
					WorkloadType: WorkloadTypeReadVerify, RunID: 502,
					Bucket: "bucket", ObjectCount: 1, Threads: 1,
					BaseTimestamp: "20260801.120000.000",
				})
			},
			want: func(plan StepPlan) integrity.StepRoles {
				return integrity.StepRoles{List: plan.Steps[0].ID, Read: plan.Steps[1].ID}
			},
		},
		{
			name: "read verify staged",
			generate: func() (string, error) {
				return GenerateReadVerifyScenario(Params{
					WorkloadType: WorkloadTypeReadVerify, RunID: 503,
					Bucket: "bucket", ObjectCount: 1, Threads: 1, ItemsFile: "/spt-input/items.csv",
					BaseTimestamp: "20260801.120000.000",
				})
			},
			want: func(plan StepPlan) integrity.StepRoles {
				return integrity.StepRoles{Read: plan.Steps[0].ID}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenarioText, err := test.generate()
			if err != nil {
				t.Fatal(err)
			}
			plan, err := BuildStepPlanFromScenario(scenarioText)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(plan.Steps))
			for _, step := range plan.Steps {
				ids = append(ids, step.ID)
			}
			if got, want := integrity.ResolveStepRoles(ids, nil), test.want(plan); got != want {
				t.Fatalf("ResolveStepRoles(generated plan) = %+v, want %+v", got, want)
			}
		})
	}
}

func TestGenerateWriteVerifyScenarioContract(t *testing.T) {
	got, err := GenerateWriteVerifyScenario(Params{
		WorkloadType: WorkloadTypeWriteVerify,
		RunID:        42, Bucket: "bucket", ObjectSize: "1MiB", ObjectCount: 3,
		Threads: 2, Cleanup: true, S3Driver: S3DriverAws,
		BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "written.csv") || !strings.Contains(got, "verified.csv") {
		t.Fatalf("scenario is missing its manifest artifact paths:\n%s", got)
	}
	plan, err := BuildStepPlanFromScenario(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %+v", plan.Steps)
	}
	for i, want := range []string{stepOpCreate, "verify", stepOpDelete} {
		if plan.Steps[i].Op != want {
			t.Fatalf("step %d operation = %q, want %q", i, plan.Steps[i].Op, want)
		}
	}
	configs := parseGeneratedScenarioConfigs(t, got)
	if len(configs) != 3 {
		t.Fatalf("expected CREATE, READ, and DELETE configs, got %d", len(configs))
	}
	for i, phase := range []string{"CREATE", "READ", "DELETE"} {
		if gotDriver := generatedConfigValue(t, configs[i], "storage", "driver", "type"); gotDriver != "s3-aws" {
			t.Fatalf("%s driver = %#v, want s3-aws", phase, gotDriver)
		}
		if gotConcurrency := generatedConfigValue(t, configs[i], "storage", "driver", "limit", "concurrency"); gotConcurrency != float64(2) {
			t.Fatalf("%s concurrency = %#v, want 2", phase, gotConcurrency)
		}
	}
	if gotCount := generatedConfigValue(t, configs[0], "load", "op", "limit", "count"); gotCount != float64(3) {
		t.Fatalf("CREATE count = %#v, want 3", gotCount)
	}
	if gotProvenance := generatedConfigValue(t, configs[1], "storage", "integrity", "input", "provenance"); gotProvenance != constants.IntegrityProvenanceEngineStep {
		t.Fatalf("READ provenance = %#v, want engine_step", gotProvenance)
	}
	if gotProducer := generatedConfigValue(t, configs[1], "storage", "integrity", "input", "expectedProducerId"); gotProducer != plan.Steps[0].ID {
		t.Fatalf("READ producer = %#v, want %q", gotProducer, plan.Steps[0].ID)
	}
	if gotProducer := generatedConfigValue(t, configs[2], "storage", "integrity", "input", "expectedProducerId"); gotProducer != plan.Steps[1].ID {
		t.Fatalf("DELETE producer = %#v, want %q", gotProducer, plan.Steps[1].ID)
	}
	if gotInput := generatedConfigValue(t, configs[2], "item", "input", "file"); gotInput != "verifiedFile" {
		t.Fatalf("DELETE input = %#v, want verifiedFile", gotInput)
	}
	for i, config := range configs {
		if persisted := generatedConfigValue(t, config, "output", "metrics", "summary", "persist"); persisted != true {
			t.Fatalf("config %d summary persistence = %#v, want true", i, persisted)
		}
	}
}

func TestGenerateDeferredWriteVerifyScenarioContainsOnlyCreate(t *testing.T) {
	got, err := GenerateWriteVerifyScenario(Params{
		WorkloadType: WorkloadTypeWriteVerify,
		RunID:        46, Bucket: "bucket", ObjectSize: "1MiB", ObjectCount: 3,
		Threads: 2, DeferVerification: true,
		BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "ReadLoad.config") || strings.Contains(got, "DeleteLoad.config") || strings.Contains(got, "verified.csv") {
		t.Fatalf("deferred scenario contains a verification or cleanup phase:\n%s", got)
	}
	plan, err := BuildStepPlanFromScenario(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].Op != stepOpCreate {
		t.Fatalf("deferred scenario steps = %+v, want one CREATE", plan.Steps)
	}
	configs := parseGeneratedScenarioConfigs(t, got)
	if len(configs) != 1 {
		t.Fatalf("deferred scenario configs = %d, want one CREATE", len(configs))
	}
	if output := generatedConfigValue(t, configs[0], "item", "output", "file"); output != "writtenFile" {
		t.Fatalf("deferred CREATE output = %#v, want writtenFile", output)
	}
}

func TestGenerateDeferredWriteVerifyRejectsCleanup(t *testing.T) {
	_, err := GenerateWriteVerifyScenario(Params{
		WorkloadType: WorkloadTypeWriteVerify,
		RunID:        47, Bucket: "bucket", ObjectSize: "1MiB", ObjectCount: 1,
		Threads: 1, DeferVerification: true, Cleanup: true,
		BaseTimestamp: "20260730.120000.000",
	})
	if err == nil || !strings.Contains(err.Error(), "does not support cleanup") {
		t.Fatalf("deferred cleanup error = %v", err)
	}
}

func TestGenerateWriteVerifyPrefixIsAppliedOnlyAtCreateAndCleanupUsesVerifiedManifest(t *testing.T) {
	got, err := GenerateWriteVerifyScenario(Params{
		WorkloadType: WorkloadTypeWriteVerify, RunID: 43, Bucket: "bucket",
		Prefix: "/qa/team~run/", ObjectSize: "1MiB", ObjectCount: 2, Threads: 1, Cleanup: true,
		BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(got, "qa/team~run/") != 1 {
		t.Fatalf("prefix should only generate keys during CREATE: %s", got)
	}
	configs := parseGeneratedScenarioConfigs(t, got)
	if prefix := generatedConfigValue(t, configs[0], "item", "naming", "prefix"); prefix != "qa/team~run/" {
		t.Fatalf("CREATE naming prefix = %#v, want qa/team~run/", prefix)
	}
	if input := generatedConfigValue(t, configs[2], "item", "input", "file"); input != "verifiedFile" {
		t.Fatalf("cleanup input = %#v, want verifiedFile", input)
	}
	if _, err = GenerateWriteVerifyScenario(Params{
		WorkloadType: WorkloadTypeWriteVerify, RunID: 44, Bucket: "bucket",
		Prefix: "///", ObjectSize: "1MiB", ObjectCount: 1, Threads: 1,
	}); err == nil {
		t.Fatal("slash-only write-verify prefix should fail")
	}
}

func TestGenerateDurationWriteVerifyTransitionsToOneFiniteManifestRead(t *testing.T) {
	got, err := GenerateWriteVerifyScenario(Params{
		WorkloadType: WorkloadTypeWriteVerify, RunID: 45, Bucket: "bucket",
		ObjectSize: "1MiB", Duration: "30s", Threads: 2,
		BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	configs := parseGeneratedScenarioConfigs(t, got)
	if len(configs) != 2 {
		t.Fatalf("duration write-verify configs = %d, want CREATE and READ", len(configs))
	}
	if count := generatedConfigValue(t, configs[0], "load", "op", "limit", "count"); count != float64(0) {
		t.Fatalf("duration CREATE count = %#v, want 0", count)
	}
	if duration := generatedConfigValue(t, configs[0], "load", "step", "limit", "time"); duration != "30s" {
		t.Fatalf("duration CREATE time = %#v, want 30s", duration)
	}
	if input := generatedConfigValue(t, configs[1], "item", "input", "file"); input != "writtenFile" {
		t.Fatalf("verification READ input = %#v, want writtenFile", input)
	}
	load := configs[1]["load"].(map[string]any)
	op := load["op"].(map[string]any)
	if recycle, ok := op["recycle"]; ok {
		t.Fatalf("verification READ must be finite, recycle = %#v", recycle)
	}
}

func TestIntegrityScenarioGeneratorsRejectNegativeObjectCounts(t *testing.T) {
	for _, generate := range []struct {
		name string
		fn   func(Params) (string, error)
	}{
		{name: "write-verify", fn: GenerateWriteVerifyScenario},
		{name: "read-verify", fn: GenerateReadVerifyScenario},
	} {
		t.Run(generate.name, func(t *testing.T) {
			_, err := generate.fn(Params{
				WorkloadType: generate.name, RunID: 46, Bucket: "bucket",
				ObjectCount: -1, Threads: 1,
			})
			if err == nil || !strings.Contains(err.Error(), "non-negative") {
				t.Fatalf("negative count error = %v", err)
			}
		})
	}
}

func TestGenerateReadVerifyScenarioDiscoveryAndStagedInput(t *testing.T) {
	discovery, err := GenerateReadVerifyScenario(Params{
		WorkloadType: WorkloadTypeReadVerify, RunID: 9, Bucket: "b", Prefix: "p/",
		ObjectCount: 7, Threads: 4, BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	discoveryPlan, err := BuildStepPlanFromScenario(discovery)
	if err != nil {
		t.Fatal(err)
	}
	if len(discoveryPlan.Steps) != 2 || discoveryPlan.Steps[0].Op != listStepSuffix || discoveryPlan.Steps[1].Op != "verify" {
		t.Fatalf("unexpected discovery plan: %+v", discoveryPlan.Steps)
	}
	discoveryConfigs := parseGeneratedScenarioConfigs(t, discovery)
	if len(discoveryConfigs) != 2 {
		t.Fatalf("expected LIST and READ configs, got %d", len(discoveryConfigs))
	}
	if maxCount := generatedConfigValue(t, discoveryConfigs[0], "storage", "integrity", "selection", "maxCount"); maxCount != float64(7) {
		t.Fatalf("LIST maxCount = %#v, want 7", maxCount)
	}
	if fetchMetadata := generatedConfigValue(t, discoveryConfigs[0], "load", "op", "list", "fetch_metadata"); fetchMetadata != true {
		t.Fatalf("LIST fetch_metadata = %#v, want true", fetchMetadata)
	}
	if prefix := generatedConfigValue(t, discoveryConfigs[0], "item", "naming", "prefix"); prefix != "p/" {
		t.Fatalf("LIST prefix = %#v, want p/", prefix)
	}
	if producer := generatedConfigValue(t, discoveryConfigs[1], "storage", "integrity", "input", "expectedProducerId"); producer != discoveryPlan.Steps[0].ID {
		t.Fatalf("discovery READ producer = %#v, want %q", producer, discoveryPlan.Steps[0].ID)
	}

	staged, err := GenerateReadVerifyScenario(Params{
		WorkloadType: WorkloadTypeReadVerify, RunID: 10, Bucket: "b", Threads: 1,
		ItemsFile: "/spt-input/items/verify-input.csv", BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	stagedPlan, err := BuildStepPlanFromScenario(staged)
	if err != nil {
		t.Fatal(err)
	}
	if len(stagedPlan.Steps) != 1 || stagedPlan.Steps[0].Number != 1 || stagedPlan.Steps[0].Op != "verify" {
		t.Fatalf("unexpected staged-input plan: %+v", stagedPlan.Steps)
	}
	stagedConfigs := parseGeneratedScenarioConfigs(t, staged)
	if len(stagedConfigs) != 1 {
		t.Fatalf("staged-input scenario should contain one READ config, got %d", len(stagedConfigs))
	}
	if provenance := generatedConfigValue(t, stagedConfigs[0], "storage", "integrity", "input", "provenance"); provenance != constants.IntegrityProvenanceCLIStager {
		t.Fatalf("staged READ provenance = %#v, want cli_stager", provenance)
	}
	if producer := generatedConfigValue(t, stagedConfigs[0], "storage", "integrity", "input", "expectedProducerId"); producer != constants.IntegrityCLIStagerProducerID {
		t.Fatalf("staged READ producer = %#v, want %q", producer, constants.IntegrityCLIStagerProducerID)
	}
	if input := generatedConfigValue(t, stagedConfigs[0], "item", "input", "file"); input != "/spt-input/items/verify-input.csv" {
		t.Fatalf("staged READ input = %#v", input)
	}
}

func TestGenerateDefaultsIncludesVerificationRunID(t *testing.T) {
	got, err := GenerateDefaults(Params{
		WorkloadType: WorkloadTypeReadVerify, RunID: 123,
		Endpoint: "http://localhost:9000", AccessKey: "a", SecretKey: "s", Threads: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "run:\n    id: 123") && !strings.Contains(string(got), "run:\n  id: 123") {
		t.Fatalf("defaults missing run id:\n%s", got)
	}
}

func parseGeneratedScenarioConfigs(t *testing.T, scenarioText string) []map[string]any {
	t.Helper()
	const configStart = ".config("
	configs := make([]map[string]any, 0, 3)
	searchFrom := 0
	for {
		relativeStart := strings.Index(scenarioText[searchFrom:], configStart)
		if relativeStart < 0 {
			break
		}
		objectStart := searchFrom + relativeStart + len(configStart)
		if objectStart >= len(scenarioText) || scenarioText[objectStart] != '{' {
			t.Fatalf("generated config does not start with an object at offset %d", objectStart)
		}
		depth := 0
		inString := false
		escaped := false
		objectEnd := -1
		for i := objectStart; i < len(scenarioText); i++ {
			ch := scenarioText[i]
			if inString {
				if escaped {
					escaped = false
					continue
				}
				if ch == '\\' {
					escaped = true
				} else if ch == '"' {
					inString = false
				}
				continue
			}
			switch ch {
			case '"':
				inString = true
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					objectEnd = i + 1
				}
			}
			if objectEnd >= 0 {
				break
			}
		}
		if objectEnd < 0 {
			t.Fatalf("unterminated generated config object at offset %d", objectStart)
		}
		rawConfig := scenarioText[objectStart:objectEnd]
		for _, variable := range []string{"writtenFile", "verifiedFile", "verifyInputFile"} {
			rawConfig = strings.ReplaceAll(rawConfig, variable, quoteJS(variable))
		}
		var config map[string]any
		if err := json.Unmarshal([]byte(rawConfig), &config); err != nil {
			t.Fatalf("generated config is not structurally valid JSON: %v\n%s", err, rawConfig)
		}
		configs = append(configs, config)
		searchFrom = objectEnd
	}
	if len(configs) == 0 {
		t.Fatal("generated scenario contains no config objects")
	}
	return configs
}

func generatedConfigValue(t *testing.T, config map[string]any, path ...string) any {
	t.Helper()
	var value any = config
	for _, key := range path {
		object, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("generated config path %s reaches non-object value %#v", strings.Join(path, "."), value)
		}
		value, ok = object[key]
		if !ok {
			t.Fatalf("generated config path %s is missing key %q", strings.Join(path, "."), key)
		}
	}
	return value
}
