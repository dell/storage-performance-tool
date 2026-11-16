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
// anything starts. Each value is checked only where the generated scenario sends it to the engine:
// list, tables, read-verify and manifest or existing-prefix DELETE never send --object-size, so a
// value there is ignored today and stays unchecked. The checks are never narrower than the engine.
func validateEngineFormatFlags(params scenario.Params) error {
	if scenarioSendsObjectSize(params) {
		if err := engineformat.CheckSize(params.ObjectSize); err != nil {
			return fmt.Errorf("--object-size must be a size such as 4KiB, 1MB or 1KB-1MB, got %q", params.ObjectSize)
		}
	}
	if err := engineformat.CheckDuration(params.Duration); err != nil {
		return fmt.Errorf("--duration must be a time such as 30s, 5m or 1h, got %q", params.Duration)
	}
	return nil
}

// scenarioSendsObjectSize reports whether the scenario for params carries --object-size as the
// engine's item.data.size.
func scenarioSendsObjectSize(params scenario.Params) bool {
	switch params.WorkloadType {
	case WorkloadTypeList, WorkloadTypeTables, WorkloadTypeReadVerify:
		return false
	case WorkloadTypeDelete:
		// Only seeded DELETE writes objects; a manifest or an existing prefix selects them.
		return strings.TrimSpace(params.ItemsFile) == "" && !params.DeleteExisting
	default:
		return true
	}
}
