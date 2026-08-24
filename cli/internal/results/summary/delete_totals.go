package summary

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
)

const deleteTotalsSchemaVersion = "1"

var deleteTotalsColumns = []string{
	"schema_version", "request_unit", "object_unit", "batch_unit", "mode",
	"configured_batch_size", "selection_order", "requests_attempted",
	"requests_full_success", "requests_partial", "requests_failed",
	"requests_unresolved", "objects_selected", "objects_attempted",
	"objects_accepted", "objects_failed", "objects_unattempted",
	"objects_unresolved", "batch_actual_requests", "batch_actual_objects",
	"batch_full_count", "batch_partial_count", "terminal_reconciled",
}

func parseDeleteTotalsV1(path string) (*deletemetrics.Metrics, error) {
	file, err := os.Open(path) // #nosec G304 -- path comes from the fetched results manifest
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read DELETE totals v1 header: %w", err)
	}
	if !equalStringSlices(header, deleteTotalsColumns) {
		return nil, fmt.Errorf("DELETE totals v1 has a noncanonical header")
	}
	indexes := mapColumnIndexes(header)
	record, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read DELETE totals v1 row: %w", err)
	}
	if extra, readErr := reader.Read(); readErr != io.EOF || extra != nil {
		if readErr != nil {
			return nil, fmt.Errorf("read DELETE totals v1 trailing row: %w", readErr)
		}
		return nil, fmt.Errorf("DELETE totals v1 must contain exactly one aggregate row")
	}
	value := func(name string) string { return valueAt(record, indexes, name) }
	if value("schema_version") != deleteTotalsSchemaVersion ||
		value("request_unit") != deletemetrics.RequestUnit ||
		value("object_unit") != deletemetrics.ObjectUnit ||
		value("batch_unit") != deletemetrics.RequestUnit {
		return nil, fmt.Errorf("DELETE totals v1 schema or units are incompatible")
	}
	parseI64 := func(name string) (int64, error) {
		parsed, parseErr := strconv.ParseInt(value(name), 10, 64)
		if parseErr != nil || parsed < 0 || strconv.FormatInt(parsed, 10) != value(name) {
			return 0, fmt.Errorf("DELETE totals v1 %s must be a nonnegative integer", name)
		}
		return parsed, nil
	}
	parseInt := func(name string) (int, error) {
		parsed, parseErr := strconv.Atoi(value(name))
		if parseErr != nil || parsed <= 0 || parsed > deletemetrics.MaxConfiguredBatchSize ||
			strconv.Itoa(parsed) != value(name) {
			return 0, fmt.Errorf("DELETE totals v1 %s is invalid", name)
		}
		return parsed, nil
	}
	read := func(names ...string) ([]int64, error) {
		values := make([]int64, len(names))
		for i, name := range names {
			values[i], err = parseI64(name)
			if err != nil {
				return nil, err
			}
		}
		return values, nil
	}
	requestValues, err := read("requests_attempted", "requests_full_success", "requests_partial", "requests_failed", "requests_unresolved")
	if err != nil {
		return nil, err
	}
	objectValues, err := read("objects_selected", "objects_attempted", "objects_accepted", "objects_failed", "objects_unattempted", "objects_unresolved")
	if err != nil {
		return nil, err
	}
	batchValues, err := read("batch_actual_requests", "batch_actual_objects", "batch_full_count", "batch_partial_count")
	if err != nil {
		return nil, err
	}
	batchSize, err := parseInt("configured_batch_size")
	if err != nil {
		return nil, err
	}
	mode := value("mode")
	if (mode != constants.DeleteIdentityModeSingle && mode != constants.DeleteIdentityModeBatch) ||
		(mode == constants.DeleteIdentityModeSingle) != (batchSize == 1) ||
		value("selection_order") != constants.DeleteSelectionOrderCanonical {
		return nil, fmt.Errorf("DELETE totals v1 result identity is incompatible")
	}
	reconciled := value("terminal_reconciled") == "true"
	if !reconciled {
		return nil, fmt.Errorf("DELETE totals v1 terminal evidence is incomplete")
	}
	requestTerminals, err := sumDeleteTotals(requestValues[1:]...)
	if err != nil {
		return nil, err
	}
	objectAttempts, err := sumDeleteTotals(objectValues[2], objectValues[3], objectValues[5])
	if err != nil {
		return nil, err
	}
	objectSelection, err := sumDeleteTotals(objectValues[1], objectValues[4])
	if err != nil {
		return nil, err
	}
	batchRequests, err := sumDeleteTotals(batchValues[2], batchValues[3])
	if err != nil {
		return nil, err
	}
	if requestValues[0] != requestTerminals ||
		objectValues[1] != objectAttempts ||
		objectValues[0] != objectSelection ||
		batchValues[0] != requestValues[0] || batchValues[1] != objectValues[1] ||
		batchValues[0] != batchRequests {
		return nil, fmt.Errorf("DELETE totals v1 counters do not reconcile")
	}
	if err := validateDeleteBatchShape(
		int64(batchSize), batchValues[1], batchValues[2], batchValues[3],
	); err != nil {
		return nil, err
	}
	mean := 0.0
	fullPercent := 0.0
	if batchValues[0] > 0 {
		mean = float64(batchValues[1]) / float64(batchValues[0])
		fullPercent = float64(batchValues[2]) * 100 / float64(batchValues[0])
	}
	if math.IsNaN(mean) || math.IsInf(mean, 0) || math.IsNaN(fullPercent) || math.IsInf(fullPercent, 0) {
		return nil, fmt.Errorf("DELETE totals v1 batch values are invalid")
	}
	return &deletemetrics.Metrics{
		Units:              deletemetrics.Units{Requests: deletemetrics.RequestUnit, Objects: deletemetrics.ObjectUnit, Batches: deletemetrics.RequestUnit},
		Requests:           deletemetrics.Requests{Attempted: requestValues[0], FullSuccess: requestValues[1], Partial: requestValues[2], Failed: requestValues[3], Unresolved: requestValues[4]},
		Objects:            deletemetrics.Objects{Selected: objectValues[0], Attempted: objectValues[1], Accepted: objectValues[2], Failed: objectValues[3], Unattempted: objectValues[4], Unresolved: objectValues[5]},
		Batches:            deletemetrics.Batches{ConfiguredSize: batchSize, ActualRequestCount: batchValues[0], ActualObjectCount: batchValues[1], MeanObjectsPerRequest: mean, FullBatchCount: batchValues[2], PartialBatchCount: batchValues[3], FullBatchPercent: fullPercent},
		Identity:           deletemetrics.Identity{Mode: mode, ConfiguredBatchSize: batchSize, SelectionOrder: value("selection_order")},
		Completion:         deletemetrics.Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
		TerminalReconciled: true,
	}, nil
}

func validateDeleteBatchShape(configuredSize, actualObjects, fullBatches, partialBatches int64) error {
	fullObjects, err := checkedDeleteMultiply(fullBatches, configuredSize)
	if err != nil {
		return fmt.Errorf("DELETE totals v1 batch counter overflow: %w", err)
	}
	if configuredSize == 1 {
		if partialBatches != 0 || actualObjects != fullObjects {
			return fmt.Errorf("DELETE totals v1 batch shape does not reconcile")
		}
		return nil
	}
	minimumPartialObjects := partialBatches
	maximumPartialObjects, err := checkedDeleteMultiply(partialBatches, configuredSize-1)
	if err != nil {
		return fmt.Errorf("DELETE totals v1 batch counter overflow: %w", err)
	}
	minimumObjects, err := checkedDeleteAdd(fullObjects, minimumPartialObjects)
	if err != nil {
		return fmt.Errorf("DELETE totals v1 batch counter overflow: %w", err)
	}
	maximumObjects, err := checkedDeleteAdd(fullObjects, maximumPartialObjects)
	if err != nil {
		return fmt.Errorf("DELETE totals v1 batch counter overflow: %w", err)
	}
	if actualObjects < minimumObjects || actualObjects > maximumObjects {
		return fmt.Errorf("DELETE totals v1 batch shape does not reconcile")
	}
	return nil
}

func sumDeleteTotals(values ...int64) (int64, error) {
	var sum int64
	for _, value := range values {
		var err error
		sum, err = checkedDeleteAdd(sum, value)
		if err != nil {
			return 0, fmt.Errorf("DELETE totals v1 counter overflow: %w", err)
		}
	}
	return sum, nil
}

func preferDeleteTotalsV1(
	existing, durable *deletemetrics.Metrics,
	evidence *DeleteArtifactEvidence,
) (*deletemetrics.Metrics, error) {
	if durable == nil {
		return existing, nil
	}
	if existing == nil {
		return durable, nil
	}
	if evidence == nil ||
		existing.FailurePolicy.OperationalFailedObjects != evidence.OperationalFailures ||
		existing.FailurePolicy.ExcludedFailedObjects != evidence.ProtocolFailures {
		return nil, fmt.Errorf("DELETE raw and schema-v4 failure classifications conflict")
	}
	if existing.Versions != evidence.Versions {
		return nil, fmt.Errorf("DELETE raw and schema-v4 version dimensions conflict")
	}
	if !equalDeleteBuckets(existing.Buckets, evidence.Buckets) {
		return nil, fmt.Errorf("DELETE raw and schema-v4 bucket dimensions conflict")
	}
	existingRequests := existing.Requests
	existingRequests.PerSecond = 0
	durableRequests := durable.Requests
	durableRequests.PerSecond = 0
	existingObjects := existing.Objects
	existingObjects.PerSecond = 0
	durableObjects := durable.Objects
	durableObjects.PerSecond = 0
	if existing.Units != durable.Units || existingRequests != durableRequests ||
		existingObjects != durableObjects || existing.Batches != durable.Batches ||
		existing.Identity != durable.Identity ||
		existing.Completion.TerminalReconciled != durable.Completion.TerminalReconciled ||
		existing.TerminalReconciled != durable.TerminalReconciled {
		return nil, fmt.Errorf("DELETE totals v1 conflicts with schema-v4 terminal evidence")
	}
	merged := *existing
	merged.Units = durable.Units
	merged.Requests = durable.Requests
	merged.Requests.PerSecond = existing.Requests.PerSecond
	merged.Objects = durable.Objects
	merged.Objects.PerSecond = existing.Objects.PerSecond
	merged.Batches = durable.Batches
	merged.Identity = durable.Identity
	merged.Completion = durable.Completion
	merged.TerminalReconciled = durable.TerminalReconciled
	return &merged, nil
}

func equalDeleteBuckets(left, right []deletemetrics.Bucket) bool {
	if len(left) != len(right) {
		return false
	}
	leftByName := make(map[string]deletemetrics.Bucket, len(left))
	for _, bucket := range left {
		leftByName[bucket.Bucket] = bucket
	}
	if len(leftByName) != len(left) {
		return false
	}
	for _, bucket := range right {
		if leftByName[bucket.Bucket] != bucket {
			return false
		}
	}
	return true
}
