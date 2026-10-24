package summary

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
)

// SteadyTotals contains parsed records from <step>.metrics.steady.total.csv.
type SteadyTotals struct {
	StepID string
	Rows   []SteadyTotalsRow
}

// SteadyTotalsRow is one metrics context's steady-window row: the total-row statistics
// computed over the window, followed by the window status.
type SteadyTotalsRow struct {
	MetricsTotalsRow
	Status string
}

// SteadyRates holds a phase's steady-window rates when every window of the phase is valid,
// and otherwise the short reason the summary shows after "n/a".
type SteadyRates struct {
	Valid          bool
	ThroughputOps  float64
	BandwidthMiBps float64
	Reason         string
}

const (
	columnSteadyStart     = "SteadyStart[s]"
	columnSteadyTurnovers = "SteadyTurnovers"
	columnSteadyTrend     = "SteadyTrend[%]"
	columnSteadyCV        = "SteadyCV"
	columnSteadyStatus    = "SteadyStatus"
)

// Steady-Window Status values written by the engine.
const (
	steadyStatusValid      = "valid"
	steadyStatusShort      = "short"
	steadyStatusNotReached = "not-reached"
	steadyStatusIncomplete = "incomplete"
)

// Reasons shown after "n/a" when a phase has no valid steady rates.
const (
	steadyReasonShort      = "short"
	steadyReasonNotReached = "not reached"
	steadyReasonIncomplete = "incomplete"
	steadyReasonMissing    = "missing"
	steadyReasonUnreadable = "unreadable"
)

var steadyReasonEngineTooOld = fmt.Sprintf("engine < %d.%d",
	constants.ResultsSteadyMetricsMinEngineMajor, constants.ResultsSteadyMetricsMinEngineMinor)

var steadyColumns = []string{
	columnSteadyStart,
	columnSteadyTurnovers,
	columnSteadyTrend,
	columnSteadyCV,
	columnSteadyStatus,
}

func parseSteadyTotals(stepID, path string) (*SteadyTotals, error) {
	rows, err := parseTotalsCSV(path, "steady metrics", steadyColumns, parseSteadyTotalsRow)
	if err != nil {
		return nil, err
	}
	return &SteadyTotals{StepID: stepID, Rows: rows}, nil
}

func parseSteadyTotalsRow(record []string, index map[string]int) (SteadyTotalsRow, error) {
	totals, err := parseMetricsTotalsRow(record, index)
	if err != nil {
		return SteadyTotalsRow{}, err
	}
	status := strings.TrimSpace(valueAt(record, index, columnSteadyStatus))
	switch status {
	case steadyStatusValid, steadyStatusShort, steadyStatusNotReached, steadyStatusIncomplete:
	default:
		return SteadyTotalsRow{}, fmt.Errorf("unknown %s %q", columnSteadyStatus, status)
	}
	return SteadyTotalsRow{MetricsTotalsRow: totals, Status: status}, nil
}

// deriveSteadyRates covers the total rows a phase reports: every row of a mixed phase,
// otherwise the row its Avg columns use. Rates are summed only when every window is valid.
// stepData must carry at least one total row.
func deriveSteadyRates(stepData *StepData, mixed bool, absentReason string) SteadyRates {
	if stepData.SteadyUnreadable {
		return SteadyRates{Reason: steadyReasonUnreadable}
	}
	if stepData.Steady == nil {
		return SteadyRates{Reason: absentReason}
	}
	totals := stepData.Metrics.Rows
	counterparts, ok := pairSteadyRows(totals, stepData.Steady.Rows)
	if !ok {
		return SteadyRates{Reason: steadyReasonUnreadable}
	}
	var used []int
	if mixed {
		for i, row := range totals {
			if normalizeReportOperation(row.Operation) != "" {
				used = append(used, i)
			}
		}
	} else {
		used = []int{selectMetricsRowIndex(stepData.Metrics, operationFromStep(stepData.StepID, stepData.Metrics))}
	}

	rates := SteadyRates{Valid: true}
	worst := ""
	for _, i := range used {
		row := counterparts[i]
		if row.Status != steadyStatusValid {
			if steadyStatusRank(row.Status) > steadyStatusRank(worst) {
				worst = row.Status
			}
			continue
		}
		rates.ThroughputOps += row.ThroughputAvgOps
		rates.BandwidthMiBps += row.BandwidthAvgMiBps
	}
	if worst != "" {
		return SteadyRates{Reason: steadyStatusReason(worst)}
	}
	return rates
}

// pairSteadyRows matches each total row with the steady row of the same metrics context: the
// k-th row of an operation type in one file pairs with the k-th row of that type in the other.
// Both files carry one row per context, so any difference in rows makes the pair unusable.
func pairSteadyRows(totals []MetricsTotalsRow, steady []SteadyTotalsRow) ([]SteadyTotalsRow, bool) {
	if len(totals) != len(steady) {
		return nil, false
	}
	steadyByOperation := make(map[string][]SteadyTotalsRow, len(steady))
	for _, row := range steady {
		op := normalizeReportOperation(row.Operation)
		steadyByOperation[op] = append(steadyByOperation[op], row)
	}
	seen := make(map[string]int, len(steadyByOperation))
	counterparts := make([]SteadyTotalsRow, len(totals))
	for i, row := range totals {
		op := normalizeReportOperation(row.Operation)
		candidates := steadyByOperation[op]
		if seen[op] >= len(candidates) {
			return nil, false
		}
		counterparts[i] = candidates[seen[op]]
		seen[op]++
	}
	return counterparts, true
}

// steadyStatusRank orders the statuses that are not valid as the engine's aggregate does.
func steadyStatusRank(status string) int {
	switch status {
	case steadyStatusShort:
		return 1
	case steadyStatusNotReached:
		return 2
	case steadyStatusIncomplete:
		return 3
	default:
		return 0
	}
}

func steadyStatusReason(status string) string {
	switch status {
	case steadyStatusShort:
		return steadyReasonShort
	case steadyStatusNotReached:
		return steadyReasonNotReached
	case steadyStatusIncomplete:
		return steadyReasonIncomplete
	default:
		return steadyReasonUnreadable
	}
}

// steadyAbsentReason explains a missing steady file: an Entry Node release that predates the
// artifact is expected not to write it, and every other absence is reported as missing.
func steadyAbsentReason(identity *engineinfo.Manifest) string {
	if engineIdentityPredatesSteadyMetrics(identity) {
		return steadyReasonEngineTooOld
	}
	return steadyReasonMissing
}

// engineIdentityPredatesSteadyMetrics reports whether the participant that writes step totals
// (the entry or standalone engine) is a release before the steady artifact. An engine without
// the build-information endpoint predates 5.15. Development builds and unknown versions do not
// establish a release, so they never count as predating it.
func engineIdentityPredatesSteadyMetrics(identity *engineinfo.Manifest) bool {
	if identity == nil {
		return false
	}
	for _, participant := range identity.Participants {
		if participant.Role != engineinfo.RoleEntry && participant.Role != engineinfo.RoleStandalone {
			continue
		}
		if participant.CollectionStatus == engineinfo.StatusLegacyEndpointUnavailable {
			return true
		}
		for _, build := range identity.Builds {
			if build.BuildID == participant.BuildID && !build.Development {
				return versionBefore(build.Version,
					constants.ResultsSteadyMetricsMinEngineMajor, constants.ResultsSteadyMetricsMinEngineMinor)
			}
		}
		return false
	}
	return false
}

// versionBefore reports whether a semantic version's major.minor precedes major.minor; an
// unparseable version is not before anything.
func versionBefore(version string, major, minor int) bool {
	majorText, rest, ok := strings.Cut(version, ".")
	if !ok {
		return false
	}
	minorText, _, ok := strings.Cut(rest, ".")
	if !ok {
		return false
	}
	gotMajor, majorErr := strconv.Atoi(majorText)
	gotMinor, minorErr := strconv.Atoi(minorText)
	if majorErr != nil || minorErr != nil {
		return false
	}
	return gotMajor < major || (gotMajor == major && gotMinor < minor)
}
