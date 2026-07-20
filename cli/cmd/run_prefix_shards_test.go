package cmd

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

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
