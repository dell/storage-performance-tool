package summary

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

const canonicalBooleanFalse = "false"

// RangeReadEvidence retains distinct worker/context policies and checked step totals.
type RangeReadEvidence struct {
	Complete bool
	Rows     []RangeReadRow
	Totals   map[string]int64
}

// RangeReadRow is one versioned terminal context record.
type RangeReadRow struct {
	RunID, WorkerID string
	ContextIndex    int64
	Mode            string
	Size, Alignment int64
	FixedOffset     *int64
	Terminal        bool
	Counts          map[string]int64
}

var rangeCountColumns = strings.Fields("selected accepted failed unattempted unresolved terminal_results generator_buffered driver_queued in_flight attempted requests_sent successful_bytes local_selection_errors http_failures response_validation_failures transport_failures http_attempt_failures response_validation_attempt_failures transport_attempt_failures failed_received_bytes unresolved_received_bytes")

func parseRangeRead(r io.Reader, stepID string) (*RangeReadEvidence, error) {
	reader := csv.NewReader(r)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("range header: %w", err)
	}
	columns := map[string]int{}
	for i, k := range header {
		if _, ok := columns[k]; ok {
			return nil, fmt.Errorf("duplicate range column %s", k)
		}
		columns[k] = i
	}
	required := append(strings.Fields("schema_version engine_run_id step_id worker_id context_index terminal mode size fixed_offset_present fixed_offset alignment overflow"), rangeCountColumns...)
	for _, k := range required {
		if _, ok := columns[k]; !ok {
			return nil, fmt.Errorf("missing range column %s", k)
		}
	}
	result := &RangeReadEvidence{Complete: true, Totals: map[string]int64{}}
	seen := map[string]bool{}
	runID := ""
	for {
		values, e := reader.Read()
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return nil, fmt.Errorf("range row: %w", e)
		}
		get := func(k string) string { return values[columns[k]] }
		if get("schema_version") != "1" || get("step_id") != stepID || get("worker_id") == "" {
			return nil, fmt.Errorf("invalid range schema or step/worker identity")
		}
		if _, e = rangeUnsigned(get("engine_run_id")); e != nil {
			return nil, e
		}
		if runID != "" && runID != get("engine_run_id") {
			return nil, fmt.Errorf("mixed engine run identities")
		}
		runID = get("engine_run_id")
		row := RangeReadRow{RunID: runID, WorkerID: get("worker_id"), Mode: get("mode"), Counts: map[string]int64{}}
		if row.ContextIndex, e = rangeUnsigned(get("context_index")); e != nil {
			return nil, e
		}
		identity := row.WorkerID + "/" + strconv.FormatInt(row.ContextIndex, 10)
		if seen[identity] {
			return nil, fmt.Errorf("duplicate range worker/context")
		}
		seen[identity] = true
		if row.Size, e = rangeUnsigned(get("size")); e != nil || row.Size == 0 {
			return nil, fmt.Errorf("invalid range size")
		}
		if row.Alignment, e = rangeUnsigned(get("alignment")); e != nil || row.Alignment == 0 {
			return nil, fmt.Errorf("invalid range alignment")
		}
		switch get("fixed_offset_present") {
		case canonicalBooleanTrue:
			offset, parseErr := rangeUnsigned(get("fixed_offset"))
			if parseErr != nil {
				return nil, parseErr
			}
			if row.Mode != "fixed" || offset%row.Alignment != 0 || offset > math.MaxInt64-(row.Size-1) {
				return nil, fmt.Errorf("invalid fixed range policy")
			}
			row.FixedOffset = &offset
		case canonicalBooleanFalse:
			if row.Mode != "random" || get("fixed_offset") != "" {
				return nil, fmt.Errorf("invalid random range policy")
			}
		default:
			return nil, fmt.Errorf("invalid offset presence")
		}
		if get("terminal") != canonicalBooleanTrue && get("terminal") != canonicalBooleanFalse {
			return nil, fmt.Errorf("invalid terminal flag")
		}
		row.Terminal = get("terminal") == canonicalBooleanTrue
		if get("overflow") != canonicalBooleanFalse {
			return nil, fmt.Errorf("range counters overflowed or invalid overflow flag")
		}
		for _, k := range rangeCountColumns {
			v, parseErr := rangeUnsigned(get(k))
			if parseErr != nil {
				return nil, fmt.Errorf("%s: %w", k, parseErr)
			}
			row.Counts[k] = v
		}
		if e = validateRangeCounts(row); e != nil {
			return nil, e
		}
		for k, v := range row.Counts {
			if result.Totals[k] > math.MaxInt64-v {
				return nil, fmt.Errorf("range aggregate overflow: %s", k)
			}
			result.Totals[k] += v
		}
		result.Complete = result.Complete && row.Terminal
		result.Rows = append(result.Rows, row)
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("range artifact has no context rows")
	}
	return result, nil
}

func rangeUnsigned(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty unsigned byte/count value")
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid unsigned byte/count value")
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

func validateRangeCounts(r RangeReadRow) error {
	c := r.Counts
	equalSum := func(target string, parts ...string) bool {
		sum := int64(0)
		for _, p := range parts {
			if sum > math.MaxInt64-c[p] {
				return false
			}
			sum += c[p]
		}
		return c[target] == sum
	}
	if c["accepted"] > math.MaxInt64/r.Size || c["successful_bytes"] != c["accepted"]*r.Size {
		return fmt.Errorf("range successful bytes do not reconcile")
	}
	if r.Terminal && (!equalSum("selected", "accepted", "failed", "unattempted", "unresolved") || !equalSum("terminal_results", "accepted", "failed") || !equalSum("attempted", "accepted", "failed", "unresolved") || !equalSum("failed", "local_selection_errors", "http_failures", "response_validation_failures", "transport_failures") || c["generator_buffered"] != 0 || c["driver_queued"] != 0 || c["in_flight"] != 0) {
		return fmt.Errorf("terminal range counters do not reconcile")
	}
	return nil
}

func loadRangeRead(runDir string, step *results.StepManifest) (*RangeReadEvidence, error) {
	name := step.StepID + "." + constants.ResultsArtifactSuffixRangeRead
	for _, entry := range step.Files {
		if entry.Name != name || entry.Status != fileStatusOK {
			continue
		}
		root, err := os.OpenRoot(runDir)
		if err != nil {
			return nil, err
		}
		defer func() { _ = root.Close() }()
		f, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		return parseRangeRead(f, step.StepID)
	}
	return nil, nil
}
