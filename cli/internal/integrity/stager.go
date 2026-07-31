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
	"sort"
	"strconv"
)

// VerifyInputName and the constants in this block define CLI-staged integrity artifacts.
const (
	VerifyInputName           = "verify-input.csv"
	VerifyInputCompletionName = "verify-input.complete.json"
	CLIStagerProducerID       = "spt-cli-items-stager-v1"
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
	if runID <= 0 {
		return "", "", "", fmt.Errorf("verification run id must be positive")
	}
	in, err := os.Open(source) // #nosec G304 -- explicit user-selected local input file
	if err != nil {
		return "", "", "", fmt.Errorf("open input manifest: %w", err)
	}
	defer func() { _ = in.Close() }()

	reader := csv.NewReader(in)
	reader.FieldsPerRecord = 4
	header, err := reader.Read()
	if err != nil {
		return "", "", "", fmt.Errorf("read canonical header: %w", err)
	}
	if !equalFields(header, canonicalHeader) {
		return "", "", "", fmt.Errorf("items file must use exact header %q", canonicalHeader)
	}

	sourceCount := 0
	unique := make(map[string]ManifestRecord)
	for {
		fields, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", "", "", fmt.Errorf("parse canonical record %d: %w", sourceCount+2, readErr)
		}
		sourceCount++
		if fields[0] == "" || fields[1] == "" {
			return "", "", "", fmt.Errorf("canonical record %d has an empty bucket or key", sourceCount+1)
		}
		size, parseErr := strconv.ParseInt(fields[2], 10, 64)
		if parseErr != nil || size < 0 {
			return "", "", "", fmt.Errorf("canonical record %d has invalid nonnegative size %q", sourceCount+1, fields[2])
		}
		next := ManifestRecord{bucket: fields[0], key: fields[1], size: size, version: fields[3]}
		if prior, exists := unique[next.identity()]; exists && prior.size != next.size {
			return "", "", "", fmt.Errorf("canonical identity at record %d has conflicting sizes", sourceCount+1)
		}
		unique[next.identity()] = next
	}

	selected := make([]ManifestRecord, 0, len(unique))
	for _, next := range unique {
		selected = append(selected, next)
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].bucket != selected[j].bucket {
			return selected[i].bucket < selected[j].bucket
		}
		if selected[i].key != selected[j].key {
			return selected[i].key < selected[j].key
		}
		return selected[i].version < selected[j].version
	})

	stagingDir, err = os.MkdirTemp("", "spt-integrity-input-")
	if err != nil {
		return "", "", "", fmt.Errorf("create private input staging: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(stagingDir)
		}
	}()

	manifestPath = filepath.Join(stagingDir, VerifyInputName)
	manifestTmp := manifestPath + ".staging"
	out, err := os.OpenFile(manifestTmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", "", "", err
	}
	writer := csv.NewWriter(out)
	writer.UseCRLF = true
	writeErr := writer.Write(canonicalHeader)
	for _, next := range selected {
		if writeErr == nil {
			writeErr = writer.Write([]string{next.bucket, next.key, strconv.FormatInt(next.size, 10), next.version})
		}
	}
	writer.Flush()
	if writeErr == nil {
		writeErr = writer.Error()
	}
	if closeErr := out.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return "", "", "", fmt.Errorf("write staged manifest: %w", writeErr)
	}
	if err = os.Rename(manifestTmp, manifestPath); err != nil {
		return "", "", "", fmt.Errorf("commit staged manifest: %w", err)
	}

	bytes, err := os.ReadFile(manifestPath) // #nosec G304 -- private path created above
	if err != nil {
		return "", "", "", err
	}
	digest := sha256.Sum256(bytes)
	marker := Completion{
		Version: 1, Status: "complete", RunID: runID,
		ProducerKind: "cli_stager", ProducerID: CLIStagerProducerID,
		Artifact: VerifyInputName, SourceRecordCount: sourceCount,
		UniqueRecordCount: len(unique), SelectedRecordCount: len(selected),
		ManifestBytes: int64(len(bytes)), ManifestSHA256: hex.EncodeToString(digest[:]),
	}
	markerBytes, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return "", "", "", err
	}
	completionPath = filepath.Join(stagingDir, VerifyInputCompletionName)
	markerTmp := completionPath + ".staging"
	if err = os.WriteFile(markerTmp, append(markerBytes, '\n'), 0o600); err != nil {
		return "", "", "", fmt.Errorf("write staged completion: %w", err)
	}
	if err = os.Rename(markerTmp, completionPath); err != nil {
		return "", "", "", fmt.Errorf("commit staged completion: %w", err)
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
