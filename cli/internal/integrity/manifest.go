package integrity

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	manifestSortChunkRecords = 64 * 1024
	manifestMergeFanIn       = 64
)

func (r ManifestRecord) compare(other ManifestRecord) int {
	if value := strings.Compare(r.bucket, other.bucket); value != 0 {
		return value
	}
	if value := strings.Compare(r.key, other.key); value != 0 {
		return value
	}
	return strings.Compare(r.version, other.version)
}

func (r ManifestRecord) fields() []string {
	return []string{r.bucket, r.key, strconv.FormatInt(r.size, 10), r.version}
}

func parseManifestRecord(fields []string, recordNumber int) (ManifestRecord, error) {
	if len(fields) != len(canonicalHeader) {
		return ManifestRecord{}, fmt.Errorf("canonical record %d has %d fields, want %d", recordNumber, len(fields), len(canonicalHeader))
	}
	if fields[0] == "" || fields[1] == "" {
		return ManifestRecord{}, fmt.Errorf("canonical record %d has an empty bucket or key", recordNumber)
	}
	for index, field := range fields {
		if !utf8.ValidString(field) {
			return ManifestRecord{}, fmt.Errorf("canonical record %d field %d is not valid UTF-8", recordNumber, index+1)
		}
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || size < 0 {
		return ManifestRecord{}, fmt.Errorf("canonical record %d has invalid nonnegative size %q", recordNumber, fields[2])
	}
	return ManifestRecord{bucket: fields[0], key: fields[1], size: size, version: fields[3]}, nil
}

// ValidateCompletion verifies the two-file commit record and the complete canonical manifest.
func ValidateCompletion(manifestPath, completionPath string, runID int64, producerKind, producerID, artifact string) (Completion, error) {
	return validateCompletion(
		manifestPath, completionPath, runID, producerKind, producerID, artifact, true,
	)
}

func validateCompletionForPromotion(
	manifestPath, completionPath string,
	runID int64,
	producerKind, producerID, artifact string,
) (Completion, error) {
	return validateCompletion(
		manifestPath, completionPath, runID, producerKind, producerID, artifact, false,
	)
}

func validateCompletion(
	manifestPath, completionPath string,
	runID int64,
	producerKind, producerID, artifact string,
	requireCanonicalManifestName bool,
) (Completion, error) {
	var marker Completion
	marker, err := readCompletionRecord(completionPath)
	if err != nil {
		return marker, err
	}
	err = validateCompletionRecord(
		manifestPath, marker, runID, producerKind, producerID, artifact,
		requireCanonicalManifestName,
	)
	return marker, err
}

func readCompletionRecord(completionPath string) (Completion, error) {
	var marker Completion
	markerFile, err := os.Open(completionPath) // #nosec G304 -- result/staging path selected by the caller
	if err != nil {
		return marker, fmt.Errorf("open completion record: %w", err)
	}
	defer func() { _ = markerFile.Close() }()
	decoder := json.NewDecoder(markerFile)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&marker); err != nil {
		return marker, fmt.Errorf("decode completion record: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return marker, fmt.Errorf("decode completion record: multiple JSON values are not allowed")
		}
		return marker, fmt.Errorf("decode completion record trailing data: %w", err)
	}
	if err = markerFile.Close(); err != nil {
		return marker, fmt.Errorf("close completion record: %w", err)
	}
	return marker, nil
}

func validateCompletionRecord(
	manifestPath string,
	marker Completion,
	runID int64,
	producerKind, producerID, artifact string,
	requireCanonicalManifestName bool,
) error {
	if (marker.Version != 1 && marker.Version != 2) || marker.Status != "complete" {
		return fmt.Errorf("completion record has unsupported version/status %d/%q", marker.Version, marker.Status)
	}
	if runID <= 0 || marker.RunID != runID {
		return fmt.Errorf("completion run_id %d does not match expected %d", marker.RunID, runID)
	}
	if marker.ProducerKind != producerKind || marker.ProducerID != producerID {
		return fmt.Errorf("completion producer %q/%q does not match expected %q/%q", marker.ProducerKind, marker.ProducerID, producerKind, producerID)
	}
	if marker.Artifact != artifact {
		return fmt.Errorf("completion artifact %q does not match %q", marker.Artifact, artifact)
	}
	if requireCanonicalManifestName && filepath.Base(manifestPath) != artifact {
		return fmt.Errorf("committed manifest filename %q does not match artifact %q", filepath.Base(manifestPath), artifact)
	}
	if marker.SourceRecordCount < 0 || marker.UniqueRecordCount < 0 || marker.SelectedRecordCount < 0 ||
		marker.SourceRecordCount < marker.UniqueRecordCount || marker.UniqueRecordCount < marker.SelectedRecordCount {
		return fmt.Errorf("completion record counts are inconsistent")
	}
	if marker.ExcludedDeleteMarkerCount < 0 || (marker.Version == 1 && marker.ExcludedDeleteMarkerCount != 0) {
		return fmt.Errorf("completion record excluded delete marker count is inconsistent")
	}

	evidence, err := validateCanonicalManifestEvidence(manifestPath)
	if err != nil {
		return err
	}
	if marker.ManifestBytes != evidence.bytes || marker.ManifestSHA256 != evidence.sha256 ||
		len(marker.ManifestSHA256) != sha256.Size*2 || marker.ManifestSHA256 != strings.ToLower(marker.ManifestSHA256) {
		return fmt.Errorf("completion length/digest does not match committed manifest")
	}
	if evidence.count != marker.SelectedRecordCount {
		return fmt.Errorf("manifest has %d records but completion selected_record_count is %d", evidence.count, marker.SelectedRecordCount)
	}
	return nil
}

type canonicalManifestEvidence struct {
	count  int
	bytes  int64
	sha256 string
}

type byteCounter struct{ count int64 }

func (counter *byteCounter) Write(data []byte) (int, error) {
	counter.count += int64(len(data))
	return len(data), nil
}

func validateCanonicalManifestEvidence(path string) (canonicalManifestEvidence, error) {
	var evidence canonicalManifestEvidence
	file, err := os.Open(path) // #nosec G304 -- result/staging path selected by the caller
	if err != nil {
		return evidence, err
	}
	hasher := sha256.New()
	counter := &byteCounter{}
	canonicalHasher := sha256.New()
	canonicalCounter := &byteCounter{}
	canonicalWriter := newCanonicalCSVWriter(io.MultiWriter(canonicalHasher, canonicalCounter))
	reader := newCanonicalCSVReader(io.TeeReader(file, io.MultiWriter(hasher, counter)))
	header, err := reader.Read()
	if err != nil || !equalFields(header, canonicalHeader) {
		_ = file.Close()
		return evidence, fmt.Errorf("manifest %s does not have the exact canonical header", filepath.Base(path))
	}
	if err := canonicalWriter.Write(header); err != nil {
		_ = file.Close()
		return evidence, fmt.Errorf("canonicalize manifest header: %w", err)
	}
	count := 0
	var prior *ManifestRecord
	for {
		fields, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			if closeErr := file.Close(); closeErr != nil {
				return evidence, fmt.Errorf("close committed manifest: %w", closeErr)
			}
			canonicalWriter.Flush()
			if writeErr := canonicalWriter.Error(); writeErr != nil {
				return evidence, fmt.Errorf("canonicalize committed manifest: %w", writeErr)
			}
			if counter.count != canonicalCounter.count ||
				!bytes.Equal(hasher.Sum(nil), canonicalHasher.Sum(nil)) {
				return evidence, fmt.Errorf("manifest %s is not in canonical physical CSV form", filepath.Base(path))
			}
			evidence.count = count
			evidence.bytes = counter.count
			evidence.sha256 = hex.EncodeToString(hasher.Sum(nil))
			return evidence, nil
		}
		if readErr != nil {
			_ = file.Close()
			return evidence, fmt.Errorf("parse manifest record %d: %w", count+2, readErr)
		}
		count++
		record, parseErr := parseManifestRecord(fields, count+1)
		if parseErr != nil {
			_ = file.Close()
			return evidence, parseErr
		}
		if fields[2] != strconv.FormatInt(record.size, 10) {
			_ = file.Close()
			return evidence, fmt.Errorf("canonical record %d has noncanonical size %q", count+1, fields[2])
		}
		if writeErr := canonicalWriter.Write(fields); writeErr != nil {
			_ = file.Close()
			return evidence, fmt.Errorf("canonicalize manifest record %d: %w", count+1, writeErr)
		}
		if prior != nil {
			switch compared := prior.compare(record); {
			case compared == 0:
				_ = file.Close()
				return evidence, fmt.Errorf("manifest %s has duplicate identity at record %d", filepath.Base(path), count+1)
			case compared > 0:
				_ = file.Close()
				return evidence, fmt.Errorf("manifest %s is not strictly ordered at record %d", filepath.Base(path), count+1)
			}
		}
		prior = &record
	}
}

// sortManifestBounded external-sorts and de-duplicates a canonical manifest without retaining the
// complete selection in memory. Conflicting sizes for one identity are rejected.
func sortManifestBounded(path, tempDir string) (sortedPath string, uniqueCount int, err error) {
	return sortManifestBoundedContext(context.Background(), path, tempDir)
}

func sortManifestBoundedContext(ctx context.Context, path, tempDir string) (sortedPath string, uniqueCount int, err error) {
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	file, err := os.Open(path) // #nosec G304 -- result path selected by the caller
	if err != nil {
		return "", 0, err
	}
	reader := newCanonicalCSVReader(file)
	header, err := reader.Read()
	if err != nil || !equalFields(header, canonicalHeader) {
		_ = file.Close()
		return "", 0, fmt.Errorf("manifest %s does not have the exact canonical header", filepath.Base(path))
	}

	var chunks []string
	cleanup := func() {
		for _, chunk := range chunks {
			_ = os.Remove(chunk)
		}
	}
	defer func() {
		_ = file.Close()
		cleanup()
	}()

	recordNumber := 1
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		batch := make([]ManifestRecord, 0, manifestSortChunkRecords)
		for len(batch) < manifestSortChunkRecords {
			if err := ctx.Err(); err != nil {
				return "", 0, err
			}
			fields, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				return "", 0, fmt.Errorf("parse manifest record %d: %w", recordNumber+1, readErr)
			}
			recordNumber++
			record, parseErr := parseManifestRecord(fields, recordNumber)
			if parseErr != nil {
				return "", 0, parseErr
			}
			batch = append(batch, record)
		}
		if len(batch) == 0 {
			break
		}
		sort.Slice(batch, func(i, j int) bool { return batch[i].compare(batch[j]) < 0 })
		chunk, writeErr := writeSortedChunk(tempDir, batch)
		if writeErr != nil {
			return "", 0, writeErr
		}
		chunks = append(chunks, chunk)
		if len(batch) < manifestSortChunkRecords {
			break
		}
	}
	return mergeSortedChunksContext(ctx, tempDir, chunks)
}

func writeSortedChunk(tempDir string, records []ManifestRecord) (string, error) {
	file, err := os.CreateTemp(tempDir, ".integrity-sort-chunk-*")
	if err != nil {
		return "", err
	}
	path := file.Name()
	writer := newCanonicalCSVWriter(file)
	var prior *ManifestRecord
	for i := range records {
		record := records[i]
		if prior != nil && prior.compare(record) == 0 {
			if prior.size != record.size {
				_ = file.Close()
				_ = os.Remove(path)
				return "", fmt.Errorf("manifest identity %q has conflicting sizes", record.identity())
			}
			continue
		}
		if err = writer.Write(record.fields()); err != nil {
			break
		}
		recordCopy := record
		prior = &recordCopy
	}
	writer.Flush()
	if err == nil {
		err = writer.Error()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

type mergeSource struct {
	file   *os.File
	reader csvRecordReader
}

type mergeItem struct {
	record ManifestRecord
	source int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].record.compare(h[j].record) < 0 }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(value any)    { *h = append(*h, value.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func mergeSortedChunksContext(ctx context.Context, tempDir string, chunks []string) (string, int, error) {
	working := append([]string(nil), chunks...)
	var intermediates []string
	cleanup := func() {
		for _, path := range intermediates {
			_ = os.Remove(path)
		}
	}
	for len(working) > manifestMergeFanIn {
		next := make([]string, 0, (len(working)+manifestMergeFanIn-1)/manifestMergeFanIn)
		for start := 0; start < len(working); start += manifestMergeFanIn {
			end := start + manifestMergeFanIn
			if end > len(working) {
				end = len(working)
			}
			merged, _, mergeErr := mergeSortedChunkGroupContext(ctx, tempDir, working[start:end], false)
			if mergeErr != nil {
				cleanup()
				return "", 0, mergeErr
			}
			intermediates = append(intermediates, merged)
			next = append(next, merged)
		}
		working = next
	}
	result, count, err := mergeSortedChunkGroupContext(ctx, tempDir, working, true)
	cleanup()
	return result, count, err
}

func mergeSortedChunkGroupContext(ctx context.Context, tempDir string, chunks []string, includeHeader bool) (string, int, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	out, err := os.CreateTemp(tempDir, ".integrity-sorted-*")
	if err != nil {
		return "", 0, err
	}
	outPath := out.Name()
	success := false
	defer func() {
		if !success {
			_ = os.Remove(outPath)
		}
	}()
	writer := newCanonicalCSVWriter(out)
	if includeHeader {
		if err = writer.Write(canonicalHeader); err != nil {
			_ = out.Close()
			return "", 0, err
		}
	}

	sources := make([]mergeSource, 0, len(chunks))
	defer func() {
		for i := range sources {
			_ = sources[i].file.Close()
		}
	}()
	queue := &mergeHeap{}
	heap.Init(queue)
	for _, chunk := range chunks {
		file, openErr := os.Open(chunk) // #nosec G304 -- private temporary path
		if err := ctx.Err(); err != nil {
			_ = out.Close()
			return "", 0, err
		}
		if openErr != nil {
			_ = out.Close()
			return "", 0, openErr
		}
		source := mergeSource{file: file, reader: newCanonicalCSVReader(file)}
		sources = append(sources, source)
		if record, readErr := readMergeRecord(source.reader); readErr == nil {
			heap.Push(queue, mergeItem{record: record, source: len(sources) - 1})
		} else if !errors.Is(readErr, io.EOF) {
			_ = out.Close()
			return "", 0, readErr
		}
	}

	count := 0
	var prior *ManifestRecord
	for queue.Len() > 0 {
		if err := ctx.Err(); err != nil {
			_ = out.Close()
			return "", 0, err
		}
		item := heap.Pop(queue).(mergeItem)
		if prior != nil && prior.compare(item.record) == 0 {
			if prior.size != item.record.size {
				_ = out.Close()
				return "", 0, fmt.Errorf("manifest identity %q has conflicting sizes", item.record.identity())
			}
		} else {
			if err = writer.Write(item.record.fields()); err != nil {
				_ = out.Close()
				return "", 0, err
			}
			recordCopy := item.record
			prior = &recordCopy
			count++
		}
		next, readErr := readMergeRecord(sources[item.source].reader)
		if readErr == nil {
			heap.Push(queue, mergeItem{record: next, source: item.source})
		} else if !errors.Is(readErr, io.EOF) {
			_ = out.Close()
			return "", 0, readErr
		}
	}
	writer.Flush()
	if err = writer.Error(); err == nil {
		err = out.Close()
	} else {
		_ = out.Close()
	}
	if err != nil {
		return "", 0, err
	}
	success = true
	return outPath, count, nil
}

func readMergeRecord(reader csvRecordReader) (ManifestRecord, error) {
	fields, err := reader.Read()
	if err != nil {
		return ManifestRecord{}, err
	}
	return parseManifestRecord(fields, 0)
}
