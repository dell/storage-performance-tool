package summary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

var deleteRequestColumns = []string{
	deleteColumnSchemaVersion, "request_id", "batch_id", "target_count", "outcome",
	"node", "start_us", "duration_us", "latency_us",
}

var deleteObjectColumns = []string{
	deleteColumnSchemaVersion, "request_id", "target_id", "target_index", deleteColumnBucket, deleteColumnKey,
	deleteColumnSize, deleteColumnVersionID, "outcome", "error_classification", "error",
}

var deleteVerificationColumns = []string{
	deleteColumnSchemaVersion, "target_id", "target_index", deleteColumnBucket, deleteColumnKey, deleteColumnSize, deleteColumnVersionID,
	"operational_outcome", "pre_enabled", "pre_presence", "post_enabled", "post_presence",
	"correctness_failure", "inconclusive", "residual",
}

var canonicalManifestColumns = []string{deleteColumnBucket, deleteColumnKey, deleteColumnSize, deleteColumnVersionID}

const (
	deleteColumnSchemaVersion = "schema_version"
	deleteColumnBucket        = "bucket"
	deleteColumnKey           = "key"
	deleteColumnSize          = "size"
	deleteColumnVersionID     = "version_id"
	deleteOutcomeAccepted     = "accepted"
	deleteOutcomeFailed       = "failed"
	deleteOutcomeUnattempted  = "unattempted"
	deleteOutcomeUnresolved   = "unresolved"
	deletePresenceDisabled    = "disabled"
	deletePresenceAbsent      = "absent"
	deletePresencePresent     = "present"
	canonicalBooleanTrue      = "true"
	deleteRequestFullSuccess  = "full_success"
	deleteRequestPartial      = "partial"
)

var deleteNodeSourcePattern = regexp.MustCompile(`^(delete\.metrics\.total|delete\.requests|delete\.objects|delete\.verification)\.node-[0-9]{3}\.csv$`)

// DeleteArtifactEvidence summarizes a hash-bound, fully reconciled DELETE artifact set.
type DeleteArtifactEvidence struct {
	RequestRows         int64
	TargetRows          int64
	ResidualRows        int64
	VerificationRows    int64
	SelectionRows       int64
	SelectionSourceRows int64
	SelectionUniqueRows int64
	SelectionSHA256     string
	SelectionProducer   string
	SelectionProducerID string
	Contributors        []string
	OperationalFailures int64
	ProtocolFailures    int64
	Versions            deletemetrics.Versions
	Buckets             []deletemetrics.Bucket
	Verification        *deletemetrics.Verification
}

type deleteCompletionV1 struct {
	Version             int               `json:"version"`
	Status              string            `json:"status"`
	SchemaVersion       string            `json:"schema_version"`
	Mode                string            `json:"mode"`
	ConfiguredBatchSize int               `json:"configured_batch_size"`
	SelectionOrder      string            `json:"selection_order"`
	Contributors        []string          `json:"contributors"`
	RequestRows         int64             `json:"request_rows"`
	TargetRows          int64             `json:"target_rows"`
	ResidualRows        int64             `json:"residual_rows"`
	VerificationRows    *int64            `json:"verification_rows,omitempty"`
	SHA256              map[string]string `json:"sha256"`
}

func loadDeleteEvidence(
	ctx context.Context,
	runDir string,
	sm *results.StepManifest,
	deleteArtifactsVersion int,
	standaloneDeleteExpected bool,
) (*DeleteArtifactEvidence, *deletemetrics.Metrics, error) {
	triggerSuffixes := []string{
		constants.ResultsArtifactSuffixDeleteMetricsTotal,
		constants.ResultsArtifactSuffixDeleteRequests,
		constants.ResultsArtifactSuffixDeleteObjects,
		constants.ResultsArtifactSuffixDeleteCompletion,
	}
	found := standaloneDeleteExpected
	for _, suffix := range triggerSuffixes {
		if entry := findArtifactEntry(sm, suffix); entry != nil && entry.Status != fileStatusMissing {
			found = true
			break
		}
	}
	if !found {
		prefix := sm.StepID + "."
		for _, entry := range sm.Files {
			name := strings.TrimPrefix(entry.Name, prefix)
			if deleteNodeSourcePattern.MatchString(name) {
				found = true
				break
			}
		}
	}
	if !found {
		return nil, nil, nil
	}

	required := []string{
		constants.ResultsArtifactSuffixDeleteMetricsTotal,
		constants.ResultsArtifactSuffixDeleteRequests,
		constants.ResultsArtifactSuffixDeleteObjects,
		constants.ResultsArtifactSuffixItems,
		constants.ResultsArtifactSuffixVerifyInput,
		constants.ResultsArtifactSuffixVerifyInputCompletion,
	}
	requireVerification := deleteArtifactsVersion >= constants.ResultsDeleteArtifactsVersion ||
		findArtifactEntry(sm, constants.ResultsArtifactSuffixDeleteVerification) != nil
	if requireVerification {
		required = append(required, constants.ResultsArtifactSuffixDeleteVerification)
	}
	required = append(required, constants.ResultsArtifactSuffixDeleteCompletion)
	paths := make(map[string]string, len(required))
	for _, suffix := range required {
		entry := findArtifactEntry(sm, suffix)
		if entry == nil || entry.Status != fileStatusOK {
			return nil, nil, fmt.Errorf("DELETE artifact publication is incomplete: missing %s", suffix)
		}
		paths[suffix] = filepath.Join(runDir, entry.Name)
	}

	completion, err := readDeleteCompletion(paths[constants.ResultsArtifactSuffixDeleteCompletion])
	if err != nil {
		return nil, nil, err
	}
	expectedCompletionVersion := 1
	if requireVerification {
		expectedCompletionVersion = 2
	}
	if completion.Version != expectedCompletionVersion || completion.Status != "complete" ||
		completion.SchemaVersion != deleteTotalsSchemaVersion ||
		completion.RequestRows < 0 || completion.TargetRows < 0 || completion.ResidualRows < 0 ||
		len(completion.Contributors) == 0 {
		return nil, nil, fmt.Errorf("DELETE completion record is incompatible or incomplete")
	}
	if requireVerification && (completion.VerificationRows == nil || *completion.VerificationRows < 0) {
		return nil, nil, fmt.Errorf("DELETE completion record is missing verification row evidence")
	}
	if !requireVerification && completion.VerificationRows != nil {
		return nil, nil, fmt.Errorf("DELETE v1 completion unexpectedly contains verification row evidence")
	}
	expectedHashes := required[:len(required)-1]
	if len(completion.SHA256) != len(expectedHashes) {
		return nil, nil, fmt.Errorf("DELETE completion record has an incomplete artifact hash set")
	}
	for _, suffix := range expectedHashes {
		expected, ok := completion.SHA256[suffix]
		if !ok || len(expected) != sha256.Size*2 || expected != strings.ToLower(expected) {
			return nil, nil, fmt.Errorf("DELETE completion record has an invalid %s hash", suffix)
		}
		actual, hashErr := fileSHA256(paths[suffix])
		if hashErr != nil {
			return nil, nil, fmt.Errorf("hash DELETE artifact %s: %w", suffix, hashErr)
		}
		if actual != expected {
			return nil, nil, fmt.Errorf("DELETE artifact %s does not match completion hash", suffix)
		}
	}

	totals, err := parseDeleteTotalsV1(paths[constants.ResultsArtifactSuffixDeleteMetricsTotal])
	if err != nil {
		return nil, nil, err
	}
	if totals.Identity.Mode != completion.Mode ||
		totals.Identity.ConfiguredBatchSize != completion.ConfiguredBatchSize ||
		totals.Identity.SelectionOrder != completion.SelectionOrder {
		return nil, nil, fmt.Errorf("DELETE completion identity does not match totals v1")
	}

	selectionMarker, err := integrity.ValidateFetchedCompletion(
		paths[constants.ResultsArtifactSuffixVerifyInput],
		paths[constants.ResultsArtifactSuffixVerifyInputCompletion],
		constants.ResultsArtifactSuffixVerifyInput,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("validate frozen DELETE selection provenance: %w", err)
	}
	counts, err := validateDeleteEvidenceRows(ctx, paths, completion.Contributors, totals)
	if err != nil {
		return nil, nil, err
	}
	if counts.requests != completion.RequestRows || counts.objects != completion.TargetRows ||
		counts.residual != completion.ResidualRows || counts.selection != totals.Objects.Selected ||
		counts.requests != totals.Requests.Attempted || counts.objects != totals.Objects.Selected {
		return nil, nil, fmt.Errorf("DELETE artifact row counts do not match terminal totals and completion")
	}
	if requireVerification && counts.verification != *completion.VerificationRows {
		return nil, nil, fmt.Errorf("DELETE verification row count does not match completion")
	}
	return &DeleteArtifactEvidence{
		RequestRows: completion.RequestRows, TargetRows: completion.TargetRows,
		ResidualRows: completion.ResidualRows, SelectionRows: counts.selection,
		VerificationRows:    counts.verification,
		SelectionSourceRows: int64(selectionMarker.SourceRecordCount),
		SelectionUniqueRows: int64(selectionMarker.UniqueRecordCount),
		SelectionSHA256:     selectionMarker.ManifestSHA256,
		SelectionProducer:   selectionMarker.ProducerKind,
		SelectionProducerID: selectionMarker.ProducerID,
		Contributors:        append([]string(nil), completion.Contributors...),
		OperationalFailures: counts.operationalFailures,
		ProtocolFailures:    counts.protocolFailures,
		Versions:            counts.versions,
		Buckets:             append([]deletemetrics.Bucket(nil), counts.buckets...),
		Verification:        counts.verificationSummary,
	}, totals, nil
}

type deleteEvidenceCounts struct {
	requests            int64
	objects             int64
	residual            int64
	verification        int64
	selection           int64
	operationalFailures int64
	protocolFailures    int64
	versions            deletemetrics.Versions
	buckets             []deletemetrics.Bucket
	verificationSummary *deletemetrics.Verification
}

type deleteManifestIdentity struct {
	bucket  string
	key     string
	size    int64
	version string
}

func validateDeleteEvidenceRows(
	ctx context.Context,
	paths map[string]string,
	contributors []string,
	totals *deletemetrics.Metrics,
) (counts deleteEvidenceCounts, returnedErr error) {
	contributorSet := stringSet(contributors)
	if len(contributorSet) != len(contributors) || contains(contributorSet, "") {
		return counts, fmt.Errorf("DELETE completion contains invalid or duplicate contributor identity")
	}

	var requestOutcomes [4]int64
	var requestTargets int64
	var priorRequestID string
	requestPath := paths[constants.ResultsArtifactSuffixDeleteRequests]
	rows, err := walkDeleteCSV(ctx, requestPath, deleteRequestColumns, func(row []string, line int64) error {
		if row[0] != deleteTotalsSchemaVersion || row[1] == "" || row[2] == "" ||
			!contains(contributorSet, row[5]) {
			return fmt.Errorf("DELETE request trace row %d is invalid", line)
		}
		outcome := deleteRequestOutcome(row[4])
		if outcome < 0 {
			return fmt.Errorf("DELETE request trace row %d is invalid", line)
		}
		if priorRequestID != "" && compareString(priorRequestID, row[1]) >= 0 {
			return fmt.Errorf("DELETE request trace is not strictly canonical by request identity")
		}
		priorRequestID = row[1]
		count, parseErr := canonicalNonnegativeCSVInteger(row[3])
		if parseErr != nil || count == 0 || count > int64(totals.Identity.ConfiguredBatchSize) {
			return fmt.Errorf("DELETE request trace row %d has an invalid target count", line)
		}
		for _, field := range row[6:9] {
			if _, parseErr := canonicalNonnegativeCSVInteger(field); parseErr != nil {
				return fmt.Errorf("DELETE request trace row %d has an invalid timing", line)
			}
		}
		requestTargets, parseErr = checkedDeleteAdd(requestTargets, count)
		if parseErr != nil {
			return parseErr
		}
		requestOutcomes[outcome], parseErr = checkedDeleteAdd(requestOutcomes[outcome], 1)
		return parseErr
	})
	if err != nil {
		return counts, err
	}
	counts.requests = rows
	if requestOutcomes != [4]int64{
		totals.Requests.FullSuccess, totals.Requests.Partial,
		totals.Requests.Failed, totals.Requests.Unresolved,
	} {
		return counts, fmt.Errorf("DELETE request outcomes do not match terminal totals")
	}

	var objectOutcomes [4]int64
	var priorTargetID string
	objectPath := paths[constants.ResultsArtifactSuffixDeleteObjects]
	rows, err = walkDeleteCSV(ctx, objectPath, deleteObjectColumns, func(row []string, line int64) error {
		outcome := deleteObjectOutcome(row[8])
		classificationCompatible := (outcome == 1 && oneOf(row[9], "operational", "protocol")) ||
			(outcome != 1 && row[9] == "none")
		if row[0] != deleteTotalsSchemaVersion || row[2] == "" || outcome < 0 || !classificationCompatible {
			return fmt.Errorf("DELETE target reconciliation row %d is invalid", line)
		}
		if priorTargetID != "" && compareString(priorTargetID, row[2]) >= 0 {
			return fmt.Errorf("DELETE target reconciliation is not strictly canonical by target identity")
		}
		priorTargetID = row[2]
		targetIndex, parseErr := strconv.ParseInt(row[3], 10, 64)
		if parseErr != nil || targetIndex < -1 || strconv.FormatInt(targetIndex, 10) != row[3] {
			return fmt.Errorf("DELETE target reconciliation row %d has an invalid target index", line)
		}
		if _, identityErr := parseDeleteManifestIdentity(row[4:8]); identityErr != nil {
			return fmt.Errorf("DELETE target reconciliation row %d: %w", line, identityErr)
		}
		objectOutcomes[outcome], parseErr = checkedDeleteAdd(objectOutcomes[outcome], 1)
		if parseErr != nil {
			return parseErr
		}
		switch row[9] {
		case "operational":
			counts.operationalFailures, parseErr = checkedDeleteAdd(counts.operationalFailures, 1)
		case "protocol":
			counts.protocolFailures, parseErr = checkedDeleteAdd(counts.protocolFailures, 1)
		}
		return parseErr
	})
	if err != nil {
		return counts, err
	}
	counts.objects = rows
	if objectOutcomes != [4]int64{
		totals.Objects.Accepted, totals.Objects.Failed,
		totals.Objects.Unattempted, totals.Objects.Unresolved,
	} {
		return counts, fmt.Errorf("DELETE target outcomes do not match terminal totals")
	}

	selectionPath := paths[constants.ResultsArtifactSuffixVerifyInput]
	counts.selection, err = validateCanonicalManifest(ctx, selectionPath, "selection")
	if err != nil {
		return counts, err
	}
	residualPath := paths[constants.ResultsArtifactSuffixItems]
	counts.residual, err = validateCanonicalManifest(ctx, residualPath, "residual")
	if err != nil {
		return counts, err
	}
	if counts.requests != totals.Requests.Attempted || counts.objects != totals.Objects.Selected ||
		counts.selection != totals.Objects.Selected || requestTargets != totals.Objects.Attempted ||
		counts.residual > totals.Objects.Selected {
		return counts, fmt.Errorf("DELETE artifact row counts do not reconcile to terminal totals")
	}

	tempDir, err := os.MkdirTemp("", "spt-delete-validation-")
	if err != nil {
		return counts, err
	}
	defer func() { returnedErr = errors.Join(returnedErr, os.RemoveAll(tempDir)) }()
	objectsByManifest := filepath.Join(tempDir, "objects-by-manifest.csv")
	sortedObjects, err := sortDeleteCSV(
		ctx, objectPath, deleteObjectColumns, objectsByManifest, tempDir, "manifest",
		compareDeleteObjectManifest,
	)
	if err != nil {
		return counts, err
	}
	if sortedObjects != counts.objects {
		return counts, fmt.Errorf("DELETE manifest sort row count changed during validation")
	}
	objectsByRequest := filepath.Join(tempDir, "objects-by-request.csv")
	sortedObjects, err = sortDeleteCSV(
		ctx, objectPath, deleteObjectColumns, objectsByRequest, tempDir, "request",
		compareDeleteObjectRequest,
	)
	if err != nil {
		return counts, err
	}
	if sortedObjects != counts.objects {
		return counts, fmt.Errorf("DELETE request-link sort row count changed during validation")
	}
	if err := reconcileDeleteSelection(ctx, selectionPath, objectsByManifest); err != nil {
		return counts, err
	}
	dimensions, err := deriveDeleteEvidenceDimensions(ctx, objectsByManifest)
	if err != nil {
		return counts, err
	}
	counts.versions = dimensions.versions
	counts.buckets = dimensions.buckets
	if err := reconcileDeleteRequestLinks(ctx, requestPath, objectsByRequest); err != nil {
		return counts, err
	}
	if err := reconcileDeleteResidual(ctx, residualPath, objectsByManifest); err != nil {
		return counts, err
	}
	if verificationPath := paths[constants.ResultsArtifactSuffixDeleteVerification]; verificationPath != "" {
		verification, verificationRows, verifyErr := validateDeleteVerificationEvidence(
			ctx, verificationPath, objectPath, residualPath,
		)
		if verifyErr != nil {
			return counts, verifyErr
		}
		counts.verification = verificationRows
		counts.verificationSummary = verification
	}
	return counts, nil
}

func readDeleteCompletion(path string) (deleteCompletionV1, error) {
	var completion deleteCompletionV1
	file, err := os.Open(path) // #nosec G304 -- path is a fetched result artifact
	if err != nil {
		return completion, err
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&completion); err != nil {
		_ = file.Close()
		return completion, fmt.Errorf("decode DELETE completion: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		_ = file.Close()
		return completion, fmt.Errorf("DELETE completion contains trailing JSON")
	}
	if err := file.Close(); err != nil {
		return completion, err
	}
	return completion, nil
}

func findArtifactEntry(sm *results.StepManifest, suffix string) *results.FileStatus {
	expectedName := sm.StepID + "." + suffix
	for index := range sm.Files {
		entry := &sm.Files[index]
		if entry.Name == expectedName || (entry.Name == suffix && sm.StepID == "") {
			return entry
		}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path) // #nosec G304 -- path is a fetched result artifact
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	_, copyErr := io.Copy(hasher, file)
	closeErr := file.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func contains(set map[string]struct{}, value string) bool {
	_, ok := set[value]
	return ok
}

func oneOf(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
