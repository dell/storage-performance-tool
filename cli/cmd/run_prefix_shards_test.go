package cmd

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
)

func prefixShardTestCommand(t *testing.T, value string, changed bool, hosts string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.Flags().Int(flagPrefixShards, prefixShardsAuto, "")
	cmd.Flags().String("test-hosts", "127.0.0.1", "")
	if err := cmd.Flags().Set(flagPrefixShards, value); err != nil {
		t.Fatal(err)
	}
	cmd.Flags().Lookup(flagPrefixShards).Changed = changed
	if err := cmd.Flags().Set("test-hosts", hosts); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func TestResolvePrefixShardsDefaultsToAggregateConcurrency(t *testing.T) {
	params := scenario.Params{WorkloadType: WorkloadTypeWrite, Threads: 16}
	cmd := prefixShardTestCommand(t, "-1", false, "root@wrk1,root@wrk2,root@wrk3")

	got, err := resolvePrefixShardCount(cmd, params)
	if err != nil {
		t.Fatalf("resolvePrefixShardCount() error = %v", err)
	}
	if got != 48 {
		t.Fatalf("resolvePrefixShardCount() = %d, want 48", got)
	}
}

func TestResolvePrefixShardsPreservesExplicitValueAndOptOut(t *testing.T) {
	params := scenario.Params{WorkloadType: WorkloadTypeWrite, Threads: 16}
	for _, tt := range []struct {
		value string
		want  int
	}{{"0", 0}, {"7", 7}} {
		cmd := prefixShardTestCommand(t, tt.value, true, "root@wrk1,root@wrk2,root@wrk3")
		got, err := resolvePrefixShardCount(cmd, params)
		if err != nil {
			t.Fatalf("resolvePrefixShardCount(%s) error = %v", tt.value, err)
		}
		if got != tt.want {
			t.Fatalf("resolvePrefixShardCount(%s) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

func TestResolvePrefixShardsLeavesNonGeneratingWorkloadsFlat(t *testing.T) {
	cmd := prefixShardTestCommand(t, "-1", false, "root@wrk1,root@wrk2,root@wrk3")
	for _, params := range []scenario.Params{
		{WorkloadType: WorkloadTypeList, Threads: 16},
		{WorkloadType: WorkloadTypeDelete, Threads: 16},
		{WorkloadType: WorkloadTypeRead, Threads: 16, ItemsFile: "items.csv"},
	} {
		got, err := resolvePrefixShardCount(cmd, params)
		if err != nil {
			t.Fatalf("resolvePrefixShardCount(%s) error = %v", params.WorkloadType, err)
		}
		if got != 0 {
			t.Fatalf("resolvePrefixShardCount(%s) = %d, want 0", params.WorkloadType, got)
		}
	}
}

func TestResolvePrefixShardsRejectsInvalidAutoHosts(t *testing.T) {
	params := scenario.Params{WorkloadType: WorkloadTypeWrite, Threads: 16}
	cmd := prefixShardTestCommand(t, "-1", false, "root@")
	if _, err := resolvePrefixShardCount(cmd, params); err == nil {
		t.Fatal("resolvePrefixShardCount() error = nil, want invalid hosts error")
	}
}

func TestApplyPrefixShardsAddsBoundedEngineOverride(t *testing.T) {
	params := scenario.Params{WorkloadType: WorkloadTypeWrite}
	if err := applyPrefixShards(&params, 48); err != nil {
		t.Fatalf("applyPrefixShards() error = %v", err)
	}
	if params.PrefixShards != 48 {
		t.Fatalf("PrefixShards = %d, want 48", params.PrefixShards)
	}
	if len(params.EngineOverrides) != 1 || params.EngineOverrides[0] != "item.naming.shards=48" {
		t.Fatalf("EngineOverrides = %v, want item.naming.shards=48", params.EngineOverrides)
	}
	if output := formatScenarioParams(params); !strings.Contains(output, "Prefix Shards: 48") {
		t.Fatalf("summary omitted prefix shard count: %q", output)
	}
}

func TestApplyPrefixShardsRejectsInapplicableAndConflictingInputs(t *testing.T) {
	tests := []struct {
		name   string
		params scenario.Params
		count  int
		want   string
	}{
		{name: "negative", params: scenario.Params{WorkloadType: WorkloadTypeWrite}, count: -1, want: "non-negative"},
		{name: "list", params: scenario.Params{WorkloadType: WorkloadTypeList}, count: 16, want: "not supported"},
		{name: "read from file", params: scenario.Params{WorkloadType: WorkloadTypeRead, ItemsFile: "items.csv"}, count: 16, want: "--items-file"},
		{
			name: "advanced override", count: 16, want: "conflicts",
			params: scenario.Params{
				WorkloadType:    WorkloadTypeWrite,
				EngineOverrides: []string{"item-naming-shards=8"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyPrefixShards(&tt.params, tt.count)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("applyPrefixShards() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestApplyPrefixShardsAllowsSeededReadAndMixedCreate(t *testing.T) {
	for _, workload := range []string{WorkloadTypeRead, WorkloadTypeMixed} {
		params := scenario.Params{WorkloadType: workload}
		if err := applyPrefixShards(&params, 16); err != nil {
			t.Fatalf("applyPrefixShards(%s) error = %v", workload, err)
		}
	}
}

func TestPrefixShardsPreserveGeneratedItemHandoffThroughReadAndDelete(t *testing.T) {
	tests := []struct {
		name   string
		params scenario.Params
		phases []string
	}{
		{
			name: "seeded read cleanup",
			params: scenario.Params{
				WorkloadType: WorkloadTypeRead, Bucket: "bucket", Threads: 4,
				ObjectSize: "10K", Duration: "30s", SeedCount: 100, Cleanup: true,
				Endpoint: "http://storage.example", AccessKey: "test-access", SecretKey: "test-secret",
			},
			phases: []string{"PreconditionLoad", "ReadLoad", "DeleteLoad"},
		},
		{
			name: "write cleanup",
			params: scenario.Params{
				WorkloadType: WorkloadTypeWrite, Bucket: "bucket", Threads: 4,
				ObjectSize: "10K", ObjectCount: 100, Cleanup: true,
				Endpoint: "http://storage.example", AccessKey: "test-access", SecretKey: "test-secret",
			},
			phases: []string{"CreateLoad", "DeleteLoad"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := applyPrefixShards(&tt.params, 48); err != nil {
				t.Fatalf("applyPrefixShards() error = %v", err)
			}
			defaults, err := scenario.GenerateDefaults(tt.params)
			if err != nil {
				t.Fatalf("GenerateDefaults() error = %v", err)
			}
			if !strings.Contains(string(defaults), "shards: 48") {
				t.Fatalf("generated defaults omitted prefix shards:\n%s", defaults)
			}
			scenarioJS, err := scenario.GenerateScenario(tt.params)
			if err != nil {
				t.Fatalf("GenerateScenario() error = %v", err)
			}
			for _, phase := range tt.phases {
				if !strings.Contains(scenarioJS, phase) {
					t.Fatalf("scenario omitted %s:\n%s", phase, scenarioJS)
				}
			}
			if !strings.Contains(scenarioJS, "itemsFile") {
				t.Fatalf("scenario omitted generated-item handoff:\n%s", scenarioJS)
			}
		})
	}
}
