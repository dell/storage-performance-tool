// Package integrity implements CLI-owned verification manifest staging and finalization.
package integrity

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// VerifyInputName and the constants in this block define CLI-staged integrity artifacts.
const (
	VerifyInputName           = constants.ResultsArtifactSuffixVerifyInput
	VerifyInputCompletionName = constants.ResultsArtifactSuffixVerifyInputCompletion
	CLIStagerProducerID       = constants.IntegrityCLIStagerProducerID
)

var canonicalHeader = []string{"bucket", "key", "size", "version_id"}

// StagedInputManifest describes one private, immutable CLI-staged manifest and its commit evidence.
type StagedInputManifest struct {
	StagingDir      string
	ManifestPath    string
	CompletionPath  string
	Completion      Completion
	MultipleBuckets bool
	// Format is the format the source was read in. For a saved items file, ConvertedRows is the
	// number of rows converted, Buckets the first few distinct buckets in file order, BucketCount
	// the number of distinct buckets and MoreBuckets true when there were more than it counts.
	Format        ItemsFileFormat
	ConvertedRows int
	Buckets       []string
	BucketCount   int
	MoreBuckets   bool
}

type inputManifestStagePolicy struct {
	maxSelectedRecords int
	expectedBucket     string
	requireNonEmpty    bool
	// acceptSavedItems reads a source without the canonical header as a saved engine items file
	// (DELETE only).
	acceptSavedItems bool
}

// ManifestRecord is one canonical object identity and expected size.
type ManifestRecord struct {
	bucket  string
	key     string
	size    int64
	version string
}

func (r ManifestRecord) identity() string { return r.bucket + "\x00" + r.key + "\x00" + r.version }

// Completion is the versioned commit record for one canonical manifest.
type Completion struct {
	Version                   int    `json:"version"`
	Status                    string `json:"status"`
	RunID                     int64  `json:"run_id"`
	ProducerKind              string `json:"producer_kind"`
	ProducerID                string `json:"producer_id"`
	Artifact                  string `json:"artifact"`
	SourceRecordCount         int    `json:"source_record_count"`
	UniqueRecordCount         int    `json:"unique_record_count"`
	SelectedRecordCount       int    `json:"selected_record_count"`
	ExcludedDeleteMarkerCount int    `json:"excluded_delete_marker_count,omitempty"`
	ManifestBytes             int64  `json:"manifest_bytes"`
	ManifestSHA256            string `json:"manifest_sha256"`
}

// StageInputManifest validates a complete canonical user manifest, makes a deterministic immutable
// private copy, and commits the matching CLI-stager completion record.
func StageInputManifest(source string, runID int64) (stagingDir, manifestPath, completionPath string, err error) {
	return stageInputManifestWithOperations(source, runID, durableOSOperations)
}

// StageDeleteInputManifest stages a DELETE selection after applying its safety assertion and
// global object-count cap. The bucket assertion is checked against every source row before the
// sorted selection is capped.
func StageDeleteInputManifest(
	source string,
	runID int64,
	maxSelectedRecords int,
	expectedBucket string,
) (StagedInputManifest, error) {
	if maxSelectedRecords < 0 {
		return StagedInputManifest{}, fmt.Errorf("DELETE object count must be non-negative")
	}
	var staged StagedInputManifest
	dir, manifest, completion, err := stageInputManifestWithPolicyAndOperations(
		source,
		runID,
		inputManifestStagePolicy{
			maxSelectedRecords: maxSelectedRecords,
			expectedBucket:     expectedBucket,
			requireNonEmpty:    true,
			acceptSavedItems:   true,
		},
		&staged,
		durableOSOperations,
	)
	if err != nil {
		return StagedInputManifest{}, err
	}
	staged.StagingDir = dir
	staged.ManifestPath = manifest
	staged.CompletionPath = completion
	return staged, nil
}

func stageInputManifestWithOperations(
	source string,
	runID int64,
	operations durablePublicationOperations,
) (stagingDir, manifestPath, completionPath string, err error) {
	return stageInputManifestWithPolicyAndOperations(
		source, runID, inputManifestStagePolicy{}, nil, operations)
}

func stageInputManifestWithPolicyAndOperations(
	source string,
	runID int64,
	policy inputManifestStagePolicy,
	staged *StagedInputManifest,
	operations durablePublicationOperations,
) (stagingDir, manifestPath, completionPath string, err error) {
	if runID <= 0 {
		return "", "", "", fmt.Errorf("verification run id must be positive")
	}
	stagingDir, err = os.MkdirTemp("", "spt-integrity-input-")
	if err != nil {
		return "", "", "", fmt.Errorf("create private input staging: %w", err)
	}
	stagingRoot, err := os.OpenRoot(stagingDir)
	if err != nil {
		return "", "", "", fmt.Errorf("open private input staging: %w", err)
	}
	defer func() { _ = stagingRoot.Close() }()
	createdStagingDir := stagingDir
	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(createdStagingDir)
		}
	}()

	in, err := os.Open(source) // #nosec G304 -- explicit user-selected local input file
	if err != nil {
		return "", "", "", fmt.Errorf("open input manifest: %w", err)
	}
	defer func() { _ = in.Close() }()
	reader := newStagingCSVReader(in)
	engineCheck := stagingEngineCheck{deleteRoute: policy.acceptSavedItems}
	if info, statErr := in.Stat(); statErr == nil {
		engineCheck.regularFile = info.Mode().IsRegular()
	}
	header, err := reader.Read()
	savedItems := false
	// savedInput is where the saved items reader reads the file from its start.
	var savedInput io.Reader
	switch {
	case err == nil && !equalFields(header, canonicalHeader) && policy.acceptSavedItems:
		if savedErr := savedRowUnderBareCRHeader(reader.Raw()); savedErr != nil {
			return "", "", "", savedErr
		}
		savedItems = true
	case err != nil && policy.acceptSavedItems && len(reader.Raw()) > 0:
		// The canonical reader refused the first line, such as one with a quote in the name or
		// one over its limit. Unless the engine would read the canonical header there, the line is
		// read as a saved row, so a saved file gets the line's own reason, not the header's.
		firstBytes := bytes.Clone(reader.Raw())
		if savedErr := engineCheck.refusalUnderHeader(reader, 1); savedErr != nil {
			return "", "", "", savedErr
		}
		if !engineCheck.regularFile {
			// A stream may not go on yet: judge the first line from the bytes already read, and
			// read on only when it parses as a saved row, so a refusal never waits for a writer.
			known := append(bytes.Clone(firstBytes), reader.Buffered()...)
			if savedErr := savedFirstLineRefusal(known, reader.Done()); savedErr != nil {
				return "", "", "", savedErr
			}
		}
		if engineCheck.regularFile {
			// The check above may have read on past the first record: read the file again.
			if _, seekErr := in.Seek(0, io.SeekStart); seekErr != nil {
				return "", "", "", fmt.Errorf("read items file again: %w", seekErr)
			}
			savedInput = in
		} else {
			// The check read only buffered bytes, so the rest follows the first record's bytes.
			savedInput = io.MultiReader(bytes.NewReader(firstBytes), reader.Rest())
		}
		savedItems = true
	case err != nil || !equalFields(header, canonicalHeader):
		if savedErr := engineCheck.refusal(reader, 1); savedErr != nil {
			return "", "", "", savedErr
		}
		if err != nil {
			return "", "", "", fmt.Errorf("read canonical header: %w", err)
		}
		return "", "", "", fmt.Errorf("items file must use exact header %q", canonicalHeader)
	}

	rawPath := filepath.Join(stagingDir, ".verify-input.raw.csv")
	raw, err := stagingRoot.OpenFile(".verify-input.raw.csv", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", "", "", fmt.Errorf("create staged input: %w", err)
	}
	writer := newCanonicalCSVWriter(raw)
	writeErr := writer.Write(canonicalHeader)
	sourceCount := 0
	firstBucket := ""
	multipleBuckets := false
	var buckets savedItemsBuckets
	if savedItems && writeErr == nil {
		if savedInput == nil {
			// Read the file again from its start: the first record's bytes, then the rest of the
			// same open input, so a named pipe is read once.
			savedInput = io.MultiReader(bytes.NewReader(bytes.Clone(reader.Raw())), reader.Rest())
		}
		sourceCount, writeErr = readSavedItems(
			savedInput,
			func(record ManifestRecord, line int) error {
				// The row's bucket is not printed: a refusal of a saved file names no object.
				if policy.expectedBucket != "" && record.bucket != policy.expectedBucket {
					return fmt.Errorf("items file line %d has a bucket (the first segment of its "+
						"object name) that does not match --bucket %q", line, policy.expectedBucket)
				}
				if firstBucket == "" {
					firstBucket = record.bucket
				} else if record.bucket != firstBucket {
					multipleBuckets = true
				}
				buckets.add(record.bucket)
				if err := writer.Write(record.fields()); err != nil {
					return fmt.Errorf("stage canonical input: %w", err)
				}
				return nil
			})
		if writeErr != nil {
			// Returned directly: the message is for the user, without the staging prefix below.
			_ = raw.Close()
			return "", "", "", writeErr
		}
	}
	for writeErr == nil && !savedItems {
		fields, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			writeErr = fmt.Errorf("parse canonical record %d: %w", sourceCount+2, readErr)
			if savedErr := engineCheck.refusal(reader, sourceCount+2); savedErr != nil {
				_ = raw.Close()
				return "", "", "", savedErr
			}
			break
		}
		sourceCount++
		record, parseErr := parseManifestRecord(fields, sourceCount+1)
		if parseErr != nil {
			writeErr = parseErr
			if savedErr := engineCheck.refusal(reader, sourceCount+1); savedErr != nil {
				_ = raw.Close()
				return "", "", "", savedErr
			}
			break
		}
		// Returned directly: the message is for the user, without the staging prefix below.
		if IsSavedItemFields(record.key, record.version) {
			_ = raw.Close()
			return "", "", "", engineCheck.savedRowError(engineCheck.engineRecord(sourceCount + 1))
		}
		if savedErr := engineCheck.accepted(reader, sourceCount+1); savedErr != nil {
			_ = raw.Close()
			return "", "", "", savedErr
		}
		if policy.expectedBucket != "" && record.bucket != policy.expectedBucket {
			writeErr = fmt.Errorf(
				"canonical record %d bucket %q does not match --bucket %q",
				sourceCount+1, record.bucket, policy.expectedBucket)
			break
		}
		if firstBucket == "" {
			firstBucket = record.bucket
		} else if record.bucket != firstBucket {
			multipleBuckets = true
		}
		writeErr = writer.Write(record.fields())
	}
	writer.Flush()
	if writeErr == nil {
		writeErr = writer.Error()
	}
	if closeErr := raw.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return "", "", "", fmt.Errorf("stage canonical input: %w", writeErr)
	}

	sortedPath, uniqueCount, err := sortManifestBounded(rawPath, stagingDir)
	if err != nil {
		var conflict *conflictingSizesError
		if savedItems && errors.As(err, &conflict) {
			return "", "", "", savedItemsConflictError(rawPath, conflict.record)
		}
		return "", "", "", fmt.Errorf("canonicalize staged input: %w", err)
	}
	_ = os.Remove(rawPath)
	if policy.requireNonEmpty && uniqueCount == 0 {
		_ = os.Remove(sortedPath)
		return "", "", "", fmt.Errorf("DELETE manifest must contain at least one object identity")
	}
	selectedCount := uniqueCount
	if policy.maxSelectedRecords > 0 && policy.maxSelectedRecords < selectedCount {
		selectedPath, selectionErr := selectCanonicalManifestPrefix(
			sortedPath, stagingDir, policy.maxSelectedRecords)
		if selectionErr != nil {
			_ = os.Remove(sortedPath)
			return "", "", "", fmt.Errorf("cap canonical staged input: %w", selectionErr)
		}
		_ = os.Remove(sortedPath)
		sortedPath = selectedPath
		selectedCount = policy.maxSelectedRecords
	}
	manifestPath = filepath.Join(stagingDir, VerifyInputName)
	if err = durableRenameWithOperations(sortedPath, manifestPath, operations); err != nil {
		return "", "", "", fmt.Errorf("commit staged manifest: %w", err)
	}

	manifest, err := os.Open(manifestPath) // #nosec G304 -- private path created above
	if err != nil {
		return "", "", "", err
	}
	hasher := sha256.New()
	manifestBytes, hashErr := io.Copy(hasher, manifest)
	if closeErr := manifest.Close(); hashErr == nil {
		hashErr = closeErr
	}
	if hashErr != nil {
		return "", "", "", fmt.Errorf("hash staged manifest: %w", hashErr)
	}
	marker := Completion{
		Version: constants.IntegrityCompletionVersionLegacy, Status: "complete", RunID: runID,
		ProducerKind: constants.IntegrityProvenanceCLIStager, ProducerID: CLIStagerProducerID,
		Artifact: VerifyInputName, SourceRecordCount: sourceCount,
		UniqueRecordCount: uniqueCount, SelectedRecordCount: selectedCount,
		ManifestBytes: manifestBytes, ManifestSHA256: hex.EncodeToString(hasher.Sum(nil)),
	}
	markerBytes, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return "", "", "", err
	}
	completionPath = filepath.Join(stagingDir, VerifyInputCompletionName)
	if err = writeFileDurableAtomicWithOperations(
		completionPath,
		append(markerBytes, '\n'),
		"."+VerifyInputCompletionName+".staging-*",
		operations,
	); err != nil {
		return "", "", "", fmt.Errorf("write staged completion: %w", err)
	}
	if staged != nil {
		staged.Completion = marker
		staged.MultipleBuckets = multipleBuckets
		staged.Format = ItemsFileFormatCanonical
		if savedItems {
			staged.Format = ItemsFileFormatSavedItems
			staged.ConvertedRows = sourceCount
			staged.Buckets = buckets.names
			staged.BucketCount = len(buckets.seen)
			staged.MoreBuckets = buckets.more
		}
	}
	success = true
	return stagingDir, manifestPath, completionPath, nil
}

// stagingRescanTailBytes bounds what a refusal reads from a regular file past the bytes the
// canonical reader read: enough for the rest of the record that holds the byte the canonical reader
// refused and for the records after it. The bound counts from the byte after the canonical reader
// stopped, which can be inside the refused record. A record whose line end is the bound's last byte
// is judged; one whose end is not seen within the bound, such as a last record without a line end
// that reaches it, is not.
const stagingRescanTailBytes = maxCanonicalCSVRecordBytes

// stagingEngineCheck judges the stager's input by the engine's CSV rules where they part from the
// canonical reader's, without opening the source again: it reads the bytes of the record the
// canonical reader just read (Raw) and, after a refusal, the input that follows them. The two
// readers end records at the same LF or CRLF outside quotes; the engine also ends one at a bare
// CR, which the canonical reader keeps in a field. extra counts the records the engine reads beyond
// the canonical reader's so far, so that a refusal gives the engine's record number. regularFile
// says the source is a regular file, whose reads end; on any other source a refusal reads only
// what is already buffered, so it never waits for a writer.
type stagingEngineCheck struct {
	extra       int
	regularFile bool
	// deleteRoute is set for DELETE, whose saved-row refusal adds that removing the header works.
	deleteRoute bool
}

// savedRowError is the saved-row refusal for the route: deleteSavedItemRowError on DELETE.
func (check *stagingEngineCheck) savedRowError(record int) error {
	if check.deleteRoute {
		return deleteSavedItemRowError(record)
	}
	return savedItemRowError(record)
}

// engineRecord returns the engine's number for the canonical reader's record number.
func (check *stagingEngineCheck) engineRecord(canonicalRecord int) int {
	return canonicalRecord + check.extra
}

// accepted judges a record the canonical reader read and parsed, and will publish. Only a record
// with a bare CR in an unquoted field reads differently for the engine, as more than one record;
// when one of them is a saved engine items row, it returns the saved-row refusal. Every such
// record is judged, whatever the engine made of earlier ones: the engine reads the stager's copy,
// not the source.
func (check *stagingEngineCheck) accepted(reader *canonicalCSVReader, canonicalRecord int) error {
	if !reader.RecordHasBareCR() {
		return nil
	}
	verdict := scanAsEngineReads(bytes.NewReader(reader.Raw()), engineScan{
		firstRecord: check.engineRecord(canonicalRecord),
		everyRecord: true,
	})
	if verdict.found {
		return check.savedRowError(verdict.record)
	}
	check.extra += verdict.records - 1
	return nil
}

// refusal explains a record the canonical reader refused, when the engine's CSV rules find a saved
// engine items row from that record's start on: under the canonical header, the saved-row refusal
// with the engine's record number; without the header, the header refusal with the saved-row
// explanation. It returns nil when they find none, and the caller keeps its own error. On a
// regular file it reads at most stagingRescanTailBytes more; on a pipe or other stream, only the
// bytes already buffered, so a row not yet buffered keeps the caller's error.
func (check *stagingEngineCheck) refusal(reader *canonicalCSVReader, canonicalRecord int) error {
	verdict := check.scanRefused(reader, canonicalRecord)
	switch {
	case !verdict.found:
		return nil
	case verdict.underHeader:
		return check.savedRowError(verdict.record)
	default:
		return fmt.Errorf("items file must use exact header %q: %w", canonicalHeader, savedItemRowError(verdict.record))
	}
}

// refusalUnderHeader is refusal for a record DELETE will read as a saved row instead: only a saved
// engine items row the engine reads under the canonical header is refused here.
func (check *stagingEngineCheck) refusalUnderHeader(reader *canonicalCSVReader, canonicalRecord int) error {
	if verdict := check.scanRefused(reader, canonicalRecord); verdict.found && verdict.underHeader {
		return check.savedRowError(verdict.record)
	}
	return nil
}

// scanRefused reads a refused record and what follows it by the engine's CSV rules, within the
// bounds refusal describes.
func (check *stagingEngineCheck) scanRefused(reader *canonicalCSVReader, canonicalRecord int) engineVerdict {
	read := reader.Raw()
	if len(read) == 0 {
		return engineVerdict{}
	}
	var rest io.Reader
	var cut func() bool
	if check.regularFile {
		tail := &cappedReader{input: reader.Rest(), left: stagingRescanTailBytes}
		rest, cut = tail, func() bool { return tail.capped }
	} else {
		// Unless the canonical reader met the end, whether the stream goes on is unknown without
		// another read: treat it as cut. So a record is judged here only when its line end is
		// already buffered: a last record without one is not, even after the writer closed.
		rest, cut = bytes.NewReader(reader.Buffered()), func() bool { return !reader.done }
	}
	return scanAsEngineReads(io.MultiReader(bytes.NewReader(read), rest), engineScan{
		firstRecord: check.engineRecord(canonicalRecord),
		fileStart:   canonicalRecord == 1,
		cut:         cut,
	})
}

func selectCanonicalManifestPrefix(path, tempDir string, maxRecords int) (string, error) {
	input, err := os.Open(path) // #nosec G304 -- private staging path created above
	if err != nil {
		return "", err
	}
	defer func() { _ = input.Close() }()
	reader := newCanonicalCSVReader(input)
	header, err := reader.Read()
	if err != nil || !equalFields(header, canonicalHeader) {
		return "", fmt.Errorf("sorted manifest does not have the exact canonical header")
	}
	output, err := os.CreateTemp(tempDir, ".verify-input.selected-*")
	if err != nil {
		return "", err
	}
	outputPath := output.Name()
	success := false
	defer func() {
		if !success {
			_ = output.Close()
			_ = os.Remove(outputPath)
		}
	}()
	writer := newCanonicalCSVWriter(output)
	if err = writer.Write(header); err != nil {
		return "", err
	}
	for recordIndex := 0; recordIndex < maxRecords; recordIndex++ {
		fields, readErr := reader.Read()
		if readErr != nil {
			return "", fmt.Errorf("read selected record %d: %w", recordIndex+1, readErr)
		}
		if err = writer.Write(fields); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		return "", err
	}
	if err = output.Close(); err != nil {
		return "", err
	}
	success = true
	return outputPath, nil
}

func equalFields(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
