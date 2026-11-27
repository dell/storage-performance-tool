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

// lifecycleCountColumns are the per-worker counts the seed shortfall warning reads, in the order
// of LifecycleCounts' fields.
var lifecycleCountColumns = [...]string{"selected", "accepted", "failed", "unattempted"}

// LifecycleCounts is one worker's row of operation.lifecycle.csv, or the sum of the rows.
type LifecycleCounts struct {
	Selected    int64
	Accepted    int64
	Failed      int64
	Unattempted int64
	Unresolved  int64
}

// OperationLifecycle is a step's merged operation.lifecycle.csv: one row per worker and their sum.
type OperationLifecycle struct {
	Rows  []LifecycleCounts
	Total LifecycleCounts
	// CountsKnown is false when a selected, accepted, failed or unattempted value is absent or
	// unreadable; Unresolved is still exact then, and only the seed shortfall warning is lost.
	CountsKnown bool
}

// loadOperationLifecycle reads the step's merged lifecycle file. The counts only add summary
// warnings, so a file that is absent or unreadable gives nil and never affects the step's status or
// errors. The per-node copies are never read: the merged file already has one row per worker.
func loadOperationLifecycle(runDir string, step *results.StepManifest) *OperationLifecycle {
	entry := findArtifactEntry(step, constants.ResultsArtifactSuffixOperationLifecycle)
	if entry == nil || entry.Status != fileStatusOK {
		return nil
	}
	root, err := os.OpenRoot(runDir)
	if err != nil {
		return nil
	}
	defer func() { _ = root.Close() }()
	// Only a regular file: opening a named pipe here would hold up the summary.
	info, err := root.Stat(entry.Name)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	file, err := root.Open(entry.Name)
	if err != nil {
		return nil
	}
	defer func() { _ = file.Close() }()
	lifecycle, err := parseOperationLifecycle(file)
	if err != nil {
		return nil
	}
	return lifecycle
}

// parseOperationLifecycle reads every worker row. Any bad unresolved value rejects the whole file,
// because a partial sum would print a wrong count. A bad, absent or repeated selected, accepted,
// failed or unattempted column only clears CountsKnown, so the unresolved warning stays as it was.
func parseOperationLifecycle(r io.Reader) (*OperationLifecycle, error) {
	reader := csv.NewReader(r)
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("lifecycle header: %w", err)
	}
	column := -1
	counts := [len(lifecycleCountColumns)]int{}
	for i := range counts {
		counts[i] = -1
	}
	countsKnown := true
	for i, name := range header {
		if name == lifecycleUnresolvedColumn {
			if column >= 0 {
				return nil, errors.New("duplicate lifecycle unresolved column")
			}
			column = i
			continue
		}
		for c, countName := range lifecycleCountColumns {
			if name != countName {
				continue
			}
			if counts[c] >= 0 {
				countsKnown = false
			}
			counts[c] = i
		}
	}
	if column < 0 {
		return nil, errors.New("missing lifecycle unresolved column")
	}
	for _, c := range counts {
		if c < 0 {
			countsKnown = false
		}
	}
	lifecycle := &OperationLifecycle{CountsKnown: countsKnown}
	for {
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			if !lifecycle.CountsKnown {
				lifecycle.Rows = nil
				lifecycle.Total = LifecycleCounts{Unresolved: lifecycle.Total.Unresolved}
			}
			return lifecycle, nil
		}
		if readErr != nil {
			return nil, fmt.Errorf("lifecycle row: %w", readErr)
		}
		value, parseErr := rangeUnsigned(row[column])
		if parseErr != nil {
			return nil, fmt.Errorf("lifecycle unresolved: %w", parseErr)
		}
		next, ok := addCount(lifecycle.Total.Unresolved, value)
		if !ok {
			return nil, errors.New("lifecycle unresolved total overflows")
		}
		lifecycle.Total.Unresolved = next
		worker := LifecycleCounts{Unresolved: value}
		if lifecycle.CountsKnown {
			lifecycle.CountsKnown = lifecycle.addCounts(row, counts, &worker)
		}
		lifecycle.Rows = append(lifecycle.Rows, worker)
	}
}

// addCounts reads one row's counts into worker and adds them to the total; false on a bad value.
func (l *OperationLifecycle) addCounts(row []string, columns [len(lifecycleCountColumns)]int, worker *LifecycleCounts) bool {
	fields := [...]*int64{&worker.Selected, &worker.Accepted, &worker.Failed, &worker.Unattempted}
	totals := [...]*int64{&l.Total.Selected, &l.Total.Accepted, &l.Total.Failed, &l.Total.Unattempted}
	for i, column := range columns {
		value, err := rangeUnsigned(row[column])
		if err != nil {
			return false
		}
		sum, ok := addCount(*totals[i], value)
		if !ok {
			return false
		}
		*fields[i] = value
		*totals[i] = sum
	}
	return true
}

func addCount(total, value int64) (int64, bool) {
	if total > math.MaxInt64-value {
		return 0, false
	}
	return total + value, true
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
