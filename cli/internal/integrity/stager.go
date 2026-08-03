// Package integrity implements CLI-owned verification manifest staging and finalization.
package integrity

import (
	"crypto/sha256"
	"encoding/csv"
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
	Version             int    `json:"version"`
	Status              string `json:"status"`
	RunID               int64  `json:"run_id"`
	ProducerKind        string `json:"producer_kind"`
	ProducerID          string `json:"producer_id"`
	Artifact            string `json:"artifact"`
	SourceRecordCount   int    `json:"source_record_count"`
	UniqueRecordCount   int    `json:"unique_record_count"`
	SelectedRecordCount int    `json:"selected_record_count"`
	ManifestBytes       int64  `json:"manifest_bytes"`
	ManifestSHA256      string `json:"manifest_sha256"`
}

// StageInputManifest validates a complete canonical user manifest, makes a deterministic immutable
// private copy, and commits the matching CLI-stager completion record.
func StageInputManifest(source string, runID int64) (stagingDir, manifestPath, completionPath string, err error) {
	return stageInputManifestWithOperations(source, runID, durableOSOperations)
}

func stageInputManifestWithOperations(
	source string,
	runID int64,
	operations durablePublicationOperations,
) (stagingDir, manifestPath, completionPath string, err error) {
	if runID <= 0 {
		return "", "", "", fmt.Errorf("verification run id must be positive")
	}
	stagingDir, err = os.MkdirTemp("", "spt-integrity-input-")
	if err != nil {
		return "", "", "", fmt.Errorf("create private input staging: %w", err)
	}
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
	reader := csv.NewReader(in)
	reader.FieldsPerRecord = len(canonicalHeader)
	header, err := reader.Read()
	if err != nil {
		return "", "", "", fmt.Errorf("read canonical header: %w", err)
	}
	if !equalFields(header, canonicalHeader) {
		return "", "", "", fmt.Errorf("items file must use exact header %q", canonicalHeader)
	}

	rawPath := filepath.Join(stagingDir, ".verify-input.raw.csv")
	raw, err := os.OpenFile(rawPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", "", "", fmt.Errorf("create staged input: %w", err)
	}
	writer := newCanonicalCSVWriter(raw)
	writeErr := writer.Write(canonicalHeader)
	sourceCount := 0
	for writeErr == nil {
		fields, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			writeErr = fmt.Errorf("parse canonical record %d: %w", sourceCount+2, readErr)
			break
		}
		sourceCount++
		record, parseErr := parseManifestRecord(fields, sourceCount+1)
		if parseErr != nil {
			writeErr = parseErr
			break
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
		Version: 1, Status: "complete", RunID: runID,
		ProducerKind: constants.IntegrityProvenanceCLIStager, ProducerID: CLIStagerProducerID,
		Artifact: VerifyInputName, SourceRecordCount: sourceCount,
		UniqueRecordCount: uniqueCount, SelectedRecordCount: uniqueCount,
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
	success = true
	return stagingDir, manifestPath, completionPath, nil
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
