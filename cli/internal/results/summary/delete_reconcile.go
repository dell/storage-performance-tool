package summary

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"unicode/utf8"

	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
)

type deleteEvidenceDimensions struct {
	versions deletemetrics.Versions
	buckets  []deletemetrics.Bucket
}

type deleteCSVStream struct {
	file     *os.File
	reader   *csv.Reader
	path     string
	expected int
	line     int64
}

func openDeleteCSVStream(path string, expectedHeader []string) (*deleteCSVStream, error) {
	file, err := os.Open(path) // #nosec G304 -- path is a fetched result artifact
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil || !equalStringSlices(header, expectedHeader) {
		_ = file.Close()
		return nil, fmt.Errorf("DELETE artifact %s has a noncanonical header", filepath.Base(path))
	}
	return &deleteCSVStream{
		file: file, reader: reader, path: path, expected: len(expectedHeader), line: 1,
	}, nil
}

func (stream *deleteCSVStream) next(ctx context.Context) ([]string, bool, error) {
	if err := checkDeleteContext(ctx); err != nil {
		return nil, false, err
	}
	row, err := stream.reader.Read()
	if errors.Is(err, io.EOF) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("parse DELETE artifact %s: %w", filepath.Base(stream.path), err)
	}
	stream.line++
	if len(row) != stream.expected {
		return nil, false, fmt.Errorf(
			"DELETE artifact %s row %d has %d fields", filepath.Base(stream.path), stream.line, len(row),
		)
	}
	for _, field := range row {
		if !utf8.ValidString(field) {
			return nil, false, fmt.Errorf(
				"DELETE artifact %s row %d is not valid UTF-8", filepath.Base(stream.path), stream.line,
			)
		}
	}
	return row, true, nil
}

func walkDeleteCSV(
	ctx context.Context,
	path string,
	expectedHeader []string,
	visit func(row []string, line int64) error,
) (rows int64, returnedErr error) {
	stream, err := openDeleteCSVStream(path, expectedHeader)
	if err != nil {
		return 0, err
	}
	defer func() { returnedErr = errors.Join(returnedErr, stream.file.Close()) }()
	for {
		row, ok, err := stream.next(ctx)
		if err != nil {
			return rows, err
		}
		if !ok {
			return rows, nil
		}
		rows, err = checkedDeleteAdd(rows, 1)
		if err != nil {
			return rows, err
		}
		if err := visit(row, stream.line); err != nil {
			return rows, err
		}
	}
}

func validateCanonicalManifest(ctx context.Context, path, name string) (int64, error) {
	var prior *deleteManifestIdentity
	return walkDeleteCSV(ctx, path, canonicalManifestColumns, func(row []string, line int64) error {
		identity, err := parseDeleteManifestIdentity(row)
		if err != nil {
			return fmt.Errorf("DELETE %s row %d: %w", name, line, err)
		}
		if prior != nil && compareDeleteManifest(*prior, identity) >= 0 {
			return fmt.Errorf("DELETE %s is not strictly canonical", name)
		}
		prior = &identity
		return nil
	})
}

func parseDeleteManifestIdentity(row []string) (deleteManifestIdentity, error) {
	if len(row) != len(canonicalManifestColumns) || row[0] == "" || row[1] == "" {
		return deleteManifestIdentity{}, fmt.Errorf("invalid canonical object identity")
	}
	size, err := canonicalNonnegativeCSVInteger(row[2])
	if err != nil {
		return deleteManifestIdentity{}, fmt.Errorf("invalid canonical object size")
	}
	return deleteManifestIdentity{bucket: row[0], key: row[1], size: size, version: row[3]}, nil
}

func compareDeleteManifest(left, right deleteManifestIdentity) int {
	if compared := compareString(left.bucket, right.bucket); compared != 0 {
		return compared
	}
	if compared := compareString(left.key, right.key); compared != 0 {
		return compared
	}
	if compared := compareString(left.version, right.version); compared != 0 {
		return compared
	}
	if left.size < right.size {
		return -1
	}
	if left.size > right.size {
		return 1
	}
	return 0
}

func compareDeleteObjectManifest(left, right []string) int {
	leftIdentity, _ := parseDeleteManifestIdentity(left[4:8])
	rightIdentity, _ := parseDeleteManifestIdentity(right[4:8])
	if compared := compareDeleteManifest(leftIdentity, rightIdentity); compared != 0 {
		return compared
	}
	return compareString(left[2], right[2])
}

func compareDeleteObjectRequest(left, right []string) int {
	if compared := compareString(left[1], right[1]); compared != 0 {
		return compared
	}
	return compareString(left[2], right[2])
}

func reconcileDeleteSelection(ctx context.Context, selectionPath, objectsPath string) (returnedErr error) {
	selection, err := openDeleteCSVStream(selectionPath, canonicalManifestColumns)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, selection.file.Close()) }()
	objects, err := openDeleteCSVStream(objectsPath, deleteObjectColumns)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, objects.file.Close()) }()
	var priorObject *deleteManifestIdentity
	for {
		selectionRow, hasSelection, err := selection.next(ctx)
		if err != nil {
			return err
		}
		objectRow, hasObject, err := objects.next(ctx)
		if err != nil {
			return err
		}
		if !hasSelection || !hasObject {
			if hasSelection != hasObject {
				return fmt.Errorf("DELETE target reconciliation does not exactly cover the frozen selection")
			}
			return nil
		}
		selectedIdentity, err := parseDeleteManifestIdentity(selectionRow)
		if err != nil {
			return err
		}
		objectIdentity, err := parseDeleteManifestIdentity(objectRow[4:8])
		if err != nil {
			return err
		}
		if priorObject != nil && compareDeleteManifest(*priorObject, objectIdentity) >= 0 {
			return fmt.Errorf("DELETE target reconciliation repeats a frozen object identity")
		}
		priorObject = &objectIdentity
		if compareDeleteManifest(selectedIdentity, objectIdentity) != 0 {
			return fmt.Errorf("DELETE target reconciliation does not exactly cover the frozen selection")
		}
	}
}

func deriveDeleteEvidenceDimensions(
	ctx context.Context,
	objectsPath string,
) (deleteEvidenceDimensions, error) {
	var dimensions deleteEvidenceDimensions
	var currentBucket deletemetrics.Bucket
	var retainedBuckets int
	overflow := deletemetrics.Bucket{Bucket: deletemetrics.OverflowBucket}
	overflowPresent := false
	mergeOverflow := func(bucket deletemetrics.Bucket) error {
		overflowPresent = true
		var err error
		overflow.Selected, err = checkedDeleteAdd(overflow.Selected, bucket.Selected)
		if err == nil {
			overflow.Attempted, err = checkedDeleteAdd(overflow.Attempted, bucket.Attempted)
		}
		if err == nil {
			overflow.Accepted, err = checkedDeleteAdd(overflow.Accepted, bucket.Accepted)
		}
		if err == nil {
			overflow.Failed, err = checkedDeleteAdd(overflow.Failed, bucket.Failed)
		}
		return err
	}
	flushBucket := func() error {
		if currentBucket.Bucket == "" {
			return nil
		}
		if retainedBuckets < deletemetrics.MaxBucketMetrics {
			retainedBuckets++
			if currentBucket.Bucket != deletemetrics.OverflowBucket {
				dimensions.buckets = append(dimensions.buckets, currentBucket)
				return nil
			}
		}
		return mergeOverflow(currentBucket)
	}
	_, err := walkDeleteCSV(ctx, objectsPath, deleteObjectColumns, func(row []string, _ int64) error {
		if currentBucket.Bucket != "" && currentBucket.Bucket != row[4] {
			if compared := compareString(currentBucket.Bucket, row[4]); compared >= 0 {
				return fmt.Errorf("DELETE target bucket dimensions are not canonical")
			}
			if err := flushBucket(); err != nil {
				return err
			}
			currentBucket = deletemetrics.Bucket{}
		}
		if currentBucket.Bucket == "" {
			currentBucket.Bucket = row[4]
		}
		var addErr error
		currentBucket.Selected, addErr = checkedDeleteAdd(currentBucket.Selected, 1)
		if addErr != nil {
			return addErr
		}
		if row[7] == "" {
			dimensions.versions.CurrentKey, addErr = checkedDeleteAdd(dimensions.versions.CurrentKey, 1)
		} else {
			dimensions.versions.ExactVersion, addErr = checkedDeleteAdd(dimensions.versions.ExactVersion, 1)
		}
		if addErr != nil {
			return addErr
		}
		if row[8] != deleteOutcomeUnattempted {
			currentBucket.Attempted, addErr = checkedDeleteAdd(currentBucket.Attempted, 1)
		}
		if addErr != nil {
			return addErr
		}
		switch row[8] {
		case deleteOutcomeAccepted:
			currentBucket.Accepted, addErr = checkedDeleteAdd(currentBucket.Accepted, 1)
		case deleteOutcomeFailed:
			currentBucket.Failed, addErr = checkedDeleteAdd(currentBucket.Failed, 1)
		}
		return addErr
	})
	if err != nil {
		return dimensions, err
	}
	if err := flushBucket(); err != nil {
		return dimensions, err
	}
	if overflowPresent {
		dimensions.buckets = append(dimensions.buckets, overflow)
	}
	return dimensions, nil
}

func reconcileDeleteRequestLinks(ctx context.Context, requestsPath, objectsPath string) (returnedErr error) {
	requests, err := openDeleteCSVStream(requestsPath, deleteRequestColumns)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, requests.file.Close()) }()
	objects, err := openDeleteCSVStream(objectsPath, deleteObjectColumns)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, objects.file.Close()) }()
	objectRow, hasObject, err := objects.next(ctx)
	if err != nil {
		return err
	}
	for hasObject && objectRow[1] == "" {
		if objectRow[8] != deleteOutcomeUnattempted {
			return fmt.Errorf("DELETE attempted target has no request link")
		}
		objectRow, hasObject, err = objects.next(ctx)
		if err != nil {
			return err
		}
	}
	for {
		requestRow, hasRequest, err := requests.next(ctx)
		if err != nil {
			return err
		}
		if !hasRequest {
			break
		}
		requestID := requestRow[1]
		if hasObject && compareString(objectRow[1], requestID) < 0 {
			return fmt.Errorf("DELETE target reconciliation has a missing request link")
		}
		var linked int64
		var linkedOutcomes [4]int64
		var linkedFailureClassifications [2]int64
		for hasObject && objectRow[1] == requestID {
			if objectRow[8] == deleteOutcomeUnattempted {
				return fmt.Errorf("DELETE unattempted target claims an API request")
			}
			linked, err = checkedDeleteAdd(linked, 1)
			if err != nil {
				return err
			}
			outcome := deleteObjectOutcome(objectRow[8])
			linkedOutcomes[outcome], err = checkedDeleteAdd(linkedOutcomes[outcome], 1)
			if err != nil {
				return err
			}
			switch objectRow[9] {
			case "operational":
				linkedFailureClassifications[0], err = checkedDeleteAdd(linkedFailureClassifications[0], 1)
			case "protocol":
				linkedFailureClassifications[1], err = checkedDeleteAdd(linkedFailureClassifications[1], 1)
			}
			if err != nil {
				return err
			}
			objectRow, hasObject, err = objects.next(ctx)
			if err != nil {
				return err
			}
		}
		expected, _ := canonicalNonnegativeCSVInteger(requestRow[3])
		if linked != expected {
			return fmt.Errorf("DELETE request %q target count does not reconcile", requestID)
		}
		if err := validateDeleteRequestOutcomeComposition(
			requestRow[4], linkedOutcomes, linkedFailureClassifications, linked,
		); err != nil {
			return fmt.Errorf("DELETE request %q: %w", requestID, err)
		}
	}
	if hasObject {
		return fmt.Errorf("DELETE target reconciliation has a missing request link")
	}
	return nil
}

func validateDeleteRequestOutcomeComposition(
	requestOutcome string,
	targets [4]int64,
	failureClassifications [2]int64,
	linked int64,
) error {
	accepted, failed := targets[0], targets[1]
	unattempted, unresolved := targets[2], targets[3]
	operational, protocol := failureClassifications[0], failureClassifications[1]
	compatible := false
	switch requestOutcome {
	case deleteRequestFullSuccess:
		compatible = accepted == linked && operational == 0 && protocol == 0
	case deleteRequestPartial:
		attempted, err := checkedDeleteAdd(accepted, failed)
		compatible = err == nil && accepted > 0 && failed > 0 && attempted == linked &&
			operational == failed && protocol == 0
	case deleteOutcomeFailed:
		compatible = failed == linked &&
			((operational == linked && protocol == 0) || (protocol == linked && operational == 0))
	case deleteOutcomeUnresolved:
		compatible = unresolved == linked && operational == 0 && protocol == 0
	}
	if !compatible || unattempted != 0 {
		return fmt.Errorf("request outcome contradicts target reconciliation")
	}
	return nil
}

func reconcileDeleteResidual(ctx context.Context, residualPath, objectsPath string) (returnedErr error) {
	residual, err := openDeleteCSVStream(residualPath, canonicalManifestColumns)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, residual.file.Close()) }()
	objects, err := openDeleteCSVStream(objectsPath, deleteObjectColumns)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, objects.file.Close()) }()
	residualRow, hasResidual, err := residual.next(ctx)
	if err != nil {
		return err
	}
	for {
		objectRow, hasObject, err := objects.next(ctx)
		if err != nil {
			return err
		}
		if !hasObject {
			break
		}
		if objectRow[8] == deleteOutcomeAccepted {
			continue
		}
		if !hasResidual {
			return fmt.Errorf("DELETE residual does not match failed, unattempted, and unresolved targets")
		}
		objectIdentity, err := parseDeleteManifestIdentity(objectRow[4:8])
		if err != nil {
			return err
		}
		residualIdentity, err := parseDeleteManifestIdentity(residualRow)
		if err != nil {
			return err
		}
		if compareDeleteManifest(objectIdentity, residualIdentity) != 0 {
			return fmt.Errorf("DELETE residual does not match failed, unattempted, and unresolved targets")
		}
		residualRow, hasResidual, err = residual.next(ctx)
		if err != nil {
			return err
		}
	}
	if hasResidual {
		return fmt.Errorf("DELETE residual does not match failed, unattempted, and unresolved targets")
	}
	return nil
}

func deleteRequestOutcome(value string) int {
	switch value {
	case deleteRequestFullSuccess:
		return 0
	case deleteRequestPartial:
		return 1
	case deleteOutcomeFailed:
		return 2
	case deleteOutcomeUnresolved:
		return 3
	default:
		return -1
	}
}

func deleteObjectOutcome(value string) int {
	switch value {
	case deleteOutcomeAccepted:
		return 0
	case deleteOutcomeFailed:
		return 1
	case deleteOutcomeUnattempted:
		return 2
	case deleteOutcomeUnresolved:
		return 3
	default:
		return -1
	}
}

func canonicalNonnegativeCSVInteger(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("value %q is not a canonical nonnegative integer", value)
	}
	return parsed, nil
}

func checkedDeleteAdd(left, right int64) (int64, error) {
	if right < 0 || left > math.MaxInt64-right {
		return 0, fmt.Errorf("DELETE artifact counter overflow")
	}
	return left + right, nil
}

func checkedDeleteMultiply(left, right int64) (int64, error) {
	if left < 0 || right < 0 || (left != 0 && right > math.MaxInt64/left) {
		return 0, fmt.Errorf("DELETE artifact counter overflow")
	}
	return left * right, nil
}
