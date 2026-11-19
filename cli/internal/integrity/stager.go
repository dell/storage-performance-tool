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
}

type inputManifestStagePolicy struct {
	maxSelectedRecords int
	expectedBucket     string
	requireNonEmpty    bool
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
	var engineCheck stagingEngineCheck
	header, err := reader.Read()
	if err != nil || !equalFields(header, canonicalHeader) {
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
	for writeErr == nil {
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
			return "", "", "", savedItemRowError(engineCheck.engineRecord(sourceCount + 1))
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
	}
	success = true
	return stagingDir, manifestPath, completionPath, nil
}

// stagingRescanTailBytes bounds what a refusal reads from the open input past the bytes the
// canonical reader read: enough for the rest of the record that holds the byte the canonical reader
// refused and for the records after it, up to one more canonical record's worth.
const stagingRescanTailBytes = maxCanonicalCSVRecordBytes

// stagingEngineCheck judges the stager's input by the engine's CSV rules where they part from the
// canonical reader's, without opening the source again: it reads the bytes of the record the
// canonical reader just read (Raw) and, after a refusal, a bounded part of the same open input.
// The two readers end records at the same LF or CRLF outside quotes; the engine also ends one at a
// bare CR, which the canonical reader keeps in a field. extra counts the records the engine has
// read beyond the canonical reader's so far, and refused is set once the engine would take no
// object from the file, after which nothing more is judged.
type stagingEngineCheck struct {
	extra   int
	refused bool
}

// engineRecord returns the engine's number for the canonical reader's record number.
func (check *stagingEngineCheck) engineRecord(canonicalRecord int) int {
	return canonicalRecord + check.extra
}

// accepted judges a record the canonical reader read and parsed. Only a record with a bare CR in an
// unquoted field reads differently for the engine, as more than one record; when one of them is a
// saved engine items row, it returns the saved-row refusal.
func (check *stagingEngineCheck) accepted(reader *canonicalCSVReader, canonicalRecord int) error {
	if check.refused || !reader.RecordHasBareCR() {
		return nil
	}
	verdict := scanAsEngineReads(bytes.NewReader(reader.Raw()), engineScan{
		firstRecord: check.engineRecord(canonicalRecord),
	})
	check.extra += verdict.records - 1
	check.refused = verdict.refused
	if verdict.found {
		return savedItemRowError(verdict.record)
	}
	return nil
}

// refusal explains a record the canonical reader refused, when the engine's CSV rules find a saved
// engine items row from that record's start to at most stagingRescanTailBytes past the refused
// byte: under the canonical header, the saved-row refusal with the engine's record number; without
// the header, the header refusal with the saved-row explanation. It returns nil when they find
// none, and the caller keeps its own error.
func (check *stagingEngineCheck) refusal(reader *canonicalCSVReader, canonicalRecord int) error {
	read := reader.Raw()
	if check.refused || len(read) == 0 {
		return nil
	}
	tail := &cappedReader{input: reader.Rest(), left: stagingRescanTailBytes}
	verdict := scanAsEngineReads(io.MultiReader(bytes.NewReader(read), tail), engineScan{
		firstRecord: check.engineRecord(canonicalRecord),
		fileStart:   canonicalRecord == 1,
		cut:         func() bool { return tail.capped },
	})
	switch {
	case !verdict.found:
		return nil
	case verdict.underHeader:
		return savedItemRowError(verdict.record)
	default:
		return fmt.Errorf("items file must use exact header %q: %w", canonicalHeader, savedItemRowError(verdict.record))
	}
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
