package summary

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

const lifecycleUnresolvedColumn = "unresolved"

// loadUnresolvedOperations returns the step's unresolved count from its merged lifecycle file. The
// count only adds a summary warning, so a file that is absent or unreadable counts as 0 and never
// affects the step's status or errors. The per-node copies are never read: the merged file already
// has one row per worker.
func loadUnresolvedOperations(runDir string, step *results.StepManifest) int64 {
	entry := findArtifactEntry(step, constants.ResultsArtifactSuffixOperationLifecycle)
	if entry == nil || entry.Status != fileStatusOK {
		return 0
	}
	root, err := os.OpenRoot(runDir)
	if err != nil {
		return 0
	}
	defer func() { _ = root.Close() }()
	// Only a regular file: opening a named pipe here would hold up the summary.
	info, err := root.Stat(entry.Name)
	if err != nil || !info.Mode().IsRegular() {
		return 0
	}
	file, err := root.Open(entry.Name)
	if err != nil {
		return 0
	}
	defer func() { _ = file.Close() }()
	count, err := parseUnresolvedOperations(file)
	if err != nil {
		return 0
	}
	return count
}

// parseUnresolvedOperations sums the unresolved column. Any bad row rejects the whole file, because
// a partial sum would print a wrong count.
func parseUnresolvedOperations(r io.Reader) (int64, error) {
	reader := csv.NewReader(r)
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("lifecycle header: %w", err)
	}
	column := -1
	for i, name := range header {
		if name != lifecycleUnresolvedColumn {
			continue
		}
		if column >= 0 {
			return 0, errors.New("duplicate lifecycle unresolved column")
		}
		column = i
	}
	if column < 0 {
		return 0, errors.New("missing lifecycle unresolved column")
	}
	total := int64(0)
	for {
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return 0, fmt.Errorf("lifecycle row: %w", readErr)
		}
		value, parseErr := rangeUnsigned(row[column])
		if parseErr != nil {
			return 0, fmt.Errorf("lifecycle unresolved: %w", parseErr)
		}
		if total > math.MaxInt64-value {
			return 0, errors.New("lifecycle unresolved total overflows")
		}
		total += value
	}
}

// unresolvedOperationsWarning is empty for a step with none, and for DELETE and partial READ steps,
// whose own report sections print their unresolved counts.
func unresolvedOperationsWarning(label string, step *StepData) string {
	count := step.UnresolvedOperations
	if count <= 0 || step.Delete != nil || step.RangeRead != nil {
		return ""
	}
	noun := "operations"
	if count == 1 {
		noun = "operation"
	}
	return fmt.Sprintf(
		"%s: %s %s unresolved at the stop, in neither Success nor Failed; Data Moved can include their bytes (%s)",
		label, formatInt(count), noun, constants.ResultsArtifactSuffixOperationLifecycle,
	)
}
