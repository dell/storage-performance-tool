/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/engineformat"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// validateEngineFormatFlags refuses an --object-size or --duration the engine cannot parse, before
// anything starts. Each value is checked only where the generated scenario sends it to the engine,
// so a value a route ignores today stays unchecked. The checks are never narrower than the engine.
// tablesTestVector is the --test-vector flag; it matters only for the tables workload.
func validateEngineFormatFlags(params scenario.Params, tablesTestVector string) error {
	if scenarioSendsObjectSize(params) {
		if err := engineformat.CheckSize(params.ObjectSize); err != nil {
			return fmt.Errorf("--object-size must be a size such as 4KiB, 1MB or 1KB-1MB, got %q", params.ObjectSize)
		}
	}
	if scenarioSendsDuration(params, tablesTestVector) {
		if err := engineformat.CheckDuration(params.Duration); err != nil {
			return fmt.Errorf("--duration must be a time such as 30s, 5m or 1h, got %q", params.Duration)
		}
	}
	return nil
}

// scenarioSendsObjectSize reports whether the scenario for params carries --object-size as the
// engine's item.data.size. List, tables and read-verify never do, nor does a DELETE that selects
// existing objects from a manifest or a prefix.
func scenarioSendsObjectSize(params scenario.Params) bool {
	switch params.WorkloadType {
	case WorkloadTypeList, WorkloadTypeTables, WorkloadTypeReadVerify:
		return false
	case WorkloadTypeDelete:
		return isSeededDelete(params)
	default:
		return true
	}
}

// scenarioSendsDuration reports whether the scenario for params carries --duration as the
// engine's load.step.limit.time. Every workload that takes the flag sends it, except the tables
// compaction vector. read-verify refuses the flag before this runs.
func scenarioSendsDuration(params scenario.Params, tablesTestVector string) bool {
	if params.WorkloadType == WorkloadTypeTables {
		return scenario.TablesVectorSendsDuration(tablesTestVector)
	}
	return true
}

// isSeededDelete reports whether a DELETE run writes its own objects first: it has no manifest
// (--items-file) and does not select an existing prefix (--delete-existing).
func isSeededDelete(params scenario.Params) bool {
	return params.WorkloadType == WorkloadTypeDelete && strings.TrimSpace(params.ItemsFile) == "" &&
		!params.DeleteExisting
}
