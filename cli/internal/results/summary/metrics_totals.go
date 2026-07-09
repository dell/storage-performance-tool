package summary

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

// MetricsTotals contains parsed records from <step>.metrics.total.csv.
type MetricsTotals struct {
	StepID string
	Rows   []MetricsTotalsRow
}

// MetricsTotalsRow captures total statistics produced by Spt for a single operation type.
type MetricsTotalsRow struct {
	Operation           string
	Concurrency         float64
	ConcurrencyMean     float64
	NodeCount           int64
	SuccessCount        int64
	FailureCount        int64
	SizeBytes           int64
	StepDurationSeconds float64
	DurationSumSeconds  float64
	ThroughputAvgOps    float64
	ThroughputLastOps   float64
	BandwidthAvgMiBps   float64
	BandwidthLastMiBps  float64
	DurationAvgMicros   float64
	DurationP50Micros   float64
	DurationP90Micros   float64
	DurationP99Micros   float64
	DurationP999Micros  float64
	LatencyAvgMicros    float64
	LatencyP50Micros    float64
	LatencyP90Micros    float64
	LatencyP99Micros    float64
	LatencyP999Micros   float64
	TTFBAvgMicros       float64
	TTFBP50Micros       float64
	TTFBP90Micros       float64
	TTFBP99Micros       float64
	TTFBP999Micros      float64
	SampleTimestamp     string
}

const (
	columnDateTime        = "DateTimeISO8601"
	columnOpType          = "OpType"
	columnConcurrency     = "Concurrency"
	columnConcurrencyMean = "ConcurrencyMean"
	columnNodeCount       = "NodeCount"
	columnCountSucc       = "CountSucc"
	columnCountFail       = "CountFail"
	columnSize            = "Size"
	columnStepDuration    = "StepDuration[s]"
	columnDurationSum     = "DurationSum[s]"
	columnTPAvg           = "TPAvg[op/s]"
	columnTPLast          = "TPLast[op/s]"
	columnBWAvg           = "BWAvg[MiB/s]"
	columnBWAvgLegacy     = "BWAvg[MB/s]"
	columnBWLast          = "BWLast[MiB/s]"
	columnBWLastLegacy    = "BWLast[MB/s]"
	columnDurationAvg     = "DurationAvg[us]"
	columnDurationP50     = "DurationQ_0.5[us]"
	columnDurationP90     = "DurationQ_0.9[us]"
	columnDurationP99     = "DurationQ_0.99[us]"
	columnDurationP999    = "DurationQ_0.999[us]"
	columnLatencyAvg      = "LatencyAvg[us]"
	columnLatencyP50      = "LatencyQ_0.5[us]"
	columnLatencyP90      = "LatencyQ_0.9[us]"
	columnLatencyP99      = "LatencyQ_0.99[us]"
	columnLatencyP999     = "LatencyQ_0.999[us]"
	columnTTFBAvg         = "TtfbAvg[us]"
	columnTTFBP50         = "TtfbQ_0.5[us]"
	columnTTFBP90         = "TtfbQ_0.9[us]"
	columnTTFBP99         = "TtfbQ_0.99[us]"
	columnTTFBP999        = "TtfbQ_0.999[us]"
)

var requiredColumns = []string{
	columnOpType,
	columnCountSucc,
	columnCountFail,
	columnSize,
	columnTPAvg,
	columnTPLast,
	columnDurationAvg,
	columnLatencyAvg,
}

var requiredColumnGroups = [][]string{
	{columnBWAvg, columnBWAvgLegacy},
	{columnBWLast, columnBWLastLegacy},
}

func parseMetricsTotals(stepID, path string) (*MetricsTotals, error) {
	file, err := os.Open(path) // #nosec G304 -- CSV path validated when manifest is loaded
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("metrics totals empty")
		}
		return nil, fmt.Errorf("read metrics header: %w", err)
	}

	index := mapColumnIndexes(header)
	missing := missingColumns(requiredColumns, index)
	missing = append(missing, missingColumnGroups(requiredColumnGroups, index)...)
	if len(missing) > 0 {
		return nil, fmt.Errorf("metrics totals missing columns: %s", strings.Join(missing, ", "))
	}

	rows := make([]MetricsTotalsRow, 0, 4)
	recordIndex := 1
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read metrics row %d: %w", recordIndex, err)
		}
		if len(record) == 0 {
			recordIndex++
			continue
		}
		row, parseErr := parseMetricsTotalsRow(record, index)
		if parseErr != nil {
			return nil, fmt.Errorf("parse metrics row %d: %w", recordIndex, parseErr)
		}
		rows = append(rows, row)
		recordIndex++
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("metrics totals contains no data rows")
	}
	return &MetricsTotals{StepID: stepID, Rows: rows}, nil
}

func parseMetricsTotalsRow(record []string, index map[string]int) (MetricsTotalsRow, error) {
	var row MetricsTotalsRow

	row.Operation = strings.TrimSpace(valueAt(record, index, columnOpType))
	row.SampleTimestamp = strings.TrimSpace(valueAt(record, index, columnDateTime))
	var err error

	if row.Concurrency, err = parseFloat(record, index, columnConcurrency); err != nil {
		return row, err
	}
	if row.ConcurrencyMean, err = parseFloat(record, index, columnConcurrencyMean); err != nil {
		return row, err
	}
	if row.NodeCount, err = parseInt(record, index, columnNodeCount); err != nil {
		return row, err
	}
	if row.SuccessCount, err = parseInt(record, index, columnCountSucc); err != nil {
		return row, err
	}
	if row.FailureCount, err = parseInt(record, index, columnCountFail); err != nil {
		return row, err
	}
	if row.SizeBytes, err = parseInt(record, index, columnSize); err != nil {
		return row, err
	}
	if row.StepDurationSeconds, err = parseFloat(record, index, columnStepDuration); err != nil {
		return row, err
	}
	if row.DurationSumSeconds, err = parseFloat(record, index, columnDurationSum); err != nil {
		return row, err
	}
	if row.ThroughputAvgOps, err = parseFloat(record, index, columnTPAvg); err != nil {
		return row, err
	}
	if row.ThroughputLastOps, err = parseFloat(record, index, columnTPLast); err != nil {
		return row, err
	}
	if row.BandwidthAvgMiBps, err = parseFloatAny(record, index, columnBWAvg, columnBWAvgLegacy); err != nil {
		return row, err
	}
	if row.BandwidthLastMiBps, err = parseFloatAny(record, index, columnBWLast, columnBWLastLegacy); err != nil {
		return row, err
	}
	if row.DurationAvgMicros, err = parseFloat(record, index, columnDurationAvg); err != nil {
		return row, err
	}
	if row.DurationP50Micros, err = parseFloat(record, index, columnDurationP50); err != nil {
		return row, err
	}
	row.DurationP90Micros, _ = parseFloat(record, index, columnDurationP90)
	row.DurationP99Micros, _ = parseFloat(record, index, columnDurationP99)
	row.DurationP999Micros, _ = parseFloat(record, index, columnDurationP999)
	if row.LatencyAvgMicros, err = parseFloat(record, index, columnLatencyAvg); err != nil {
		return row, err
	}
	if row.LatencyP50Micros, err = parseFloat(record, index, columnLatencyP50); err != nil {
		return row, err
	}
	row.LatencyP90Micros, _ = parseFloat(record, index, columnLatencyP90)
	row.LatencyP99Micros, _ = parseFloat(record, index, columnLatencyP99)
	row.LatencyP999Micros, _ = parseFloat(record, index, columnLatencyP999)
	row.TTFBAvgMicros, _ = parseFloat(record, index, columnTTFBAvg)
	row.TTFBP50Micros, _ = parseFloat(record, index, columnTTFBP50)
	row.TTFBP90Micros, _ = parseFloat(record, index, columnTTFBP90)
	row.TTFBP99Micros, _ = parseFloat(record, index, columnTTFBP99)
	row.TTFBP999Micros, _ = parseFloat(record, index, columnTTFBP999)

	return row, nil
}

func mapColumnIndexes(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, col := range header {
		index[strings.TrimSpace(col)] = i
	}
	return index
}

func missingColumns(required []string, index map[string]int) []string {
	var missing []string
	for _, col := range required {
		if _, ok := index[col]; !ok {
			missing = append(missing, col)
		}
	}
	return missing
}

func missingColumnGroups(required [][]string, index map[string]int) []string {
	var missing []string
	for _, aliases := range required {
		found := false
		for _, col := range aliases {
			if _, ok := index[col]; ok {
				found = true
				break
			}
		}
		if !found && len(aliases) > 0 {
			missing = append(missing, strings.Join(aliases, " or "))
		}
	}
	return missing
}

func valueAt(record []string, index map[string]int, column string) string {
	pos, ok := index[column]
	if !ok || pos >= len(record) {
		return ""
	}
	return record[pos]
}

func parseFloat(record []string, index map[string]int, column string) (float64, error) {
	val := strings.TrimSpace(valueAt(record, index, column))
	if val == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSuffix(val, "%"), 64)
	if err != nil {
		return 0, fmt.Errorf("parse float %s: %w", column, err)
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("parse float %s: invalid value %q", column, val)
	}
	return parsed, nil
}

func parseFloatAny(record []string, index map[string]int, columns ...string) (float64, error) {
	for _, column := range columns {
		if _, ok := index[column]; ok {
			return parseFloat(record, index, column)
		}
	}
	return 0, fmt.Errorf("missing column %s", strings.Join(columns, " or "))
}

func parseInt(record []string, index map[string]int, column string) (int64, error) {
	val := strings.TrimSpace(valueAt(record, index, column))
	if val == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse int %s: %w", column, err)
	}
	return parsed, nil
}
