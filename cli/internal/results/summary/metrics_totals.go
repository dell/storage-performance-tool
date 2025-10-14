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
	BandwidthAvgMBps    float64
	BandwidthLastMBps   float64
	DurationAvgMicros   float64
	DurationP50Micros   float64
	LatencyAvgMicros    float64
	LatencyP50Micros    float64
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
	columnBWAvg           = "BWAvg[MB/s]"
	columnBWLast          = "BWLast[MB/s]"
	columnDurationAvg     = "DurationAvg[us]"
	columnDurationP50     = "DurationQ_0.5[us]"
	columnLatencyAvg      = "LatencyAvg[us]"
	columnLatencyP50      = "LatencyQ_0.5[us]"
)

var requiredColumns = []string{
	columnOpType,
	columnCountSucc,
	columnCountFail,
	columnSize,
	columnTPAvg,
	columnTPLast,
	columnBWAvg,
	columnBWLast,
	columnDurationAvg,
	columnLatencyAvg,
}

func parseMetricsTotals(stepID, path string) (*MetricsTotals, error) {
	file, err := os.Open(path)
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
	if row.BandwidthAvgMBps, err = parseFloat(record, index, columnBWAvg); err != nil {
		return row, err
	}
	if row.BandwidthLastMBps, err = parseFloat(record, index, columnBWLast); err != nil {
		return row, err
	}
	if row.DurationAvgMicros, err = parseFloat(record, index, columnDurationAvg); err != nil {
		return row, err
	}
	if row.DurationP50Micros, err = parseFloat(record, index, columnDurationP50); err != nil {
		return row, err
	}
	if row.LatencyAvgMicros, err = parseFloat(record, index, columnLatencyAvg); err != nil {
		return row, err
	}
	if row.LatencyP50Micros, err = parseFloat(record, index, columnLatencyP50); err != nil {
		return row, err
	}

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
