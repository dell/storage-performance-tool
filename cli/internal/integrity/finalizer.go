package integrity

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

// WrittenName and the constants in this block are canonical integrity result artifact names.
const (
	WrittenName              = constants.ResultsArtifactSuffixWritten
	WrittenCompletionName    = constants.ResultsArtifactSuffixWrittenCompletion
	VerifiedName             = constants.ResultsArtifactSuffixVerified
	VerifiedCompletionName   = constants.ResultsArtifactSuffixVerifiedCompletion
	VerifyRemainingName      = constants.ResultsArtifactSuffixVerifyRemaining
	IntegrityFailuresName    = constants.ResultsArtifactSuffixIntegrityFailures
	IntegrityPerformanceName = constants.ResultsArtifactSuffixIntegrityPerformance
	MultipartLifecycleName   = constants.ResultsArtifactSuffixMultipartLifecycle
)

var failureHeader = []string{
	"timestamp", "node", "step", "driver", "key", "requested_version_id", "returned_version_id",
	"request_id", "reason", "algorithm", "expected_digest", "actual_digest", "expected_size",
	"actual_size", "first_mismatch_offset", "attempt",
}

var performanceHeader = []string{
	"node", "step", "driver", "phase", "algorithm", "objects", "bytes", "hash_worker_seconds",
	"mean_worker_hash_mib_per_second", "time_to_first_request_seconds", "additional_payload_passes",
}

// FinalizeOptions binds the generated step roles and any CLI-staged input to one result run.
type FinalizeOptions struct {
	Context             context.Context
	ResultsRoot         string
	BaseURL             string
	Workload            string
	RunID               int64
	StepIDs             []string
	PlannedStepIDs      []string
	Plan                integrityplan.Plan
	RuntimeRoles        StepRoles
	StagedManifest      string
	StagedCompletion    string
	AllowEmptySelection bool
	MaxConsoleFailures  int
	Multipart           bool
	StepLifecycles      map[string]string
}

// FinalizeOutcome is the integrity-specific machine outcome used by console reporting and exit policy.
type FinalizeOutcome struct {
	SelectionCount             int64                               `json:"selection_count"`
	VerificationAttemptedCount int64                               `json:"verification_attempted_count"`
	VerifiedCount              int64                               `json:"verified_count"`
	CorruptCount               int64                               `json:"corrupt_count"`
	EmptySelection             bool                                `json:"empty_selection"`
	EmptyAllowed               bool                                `json:"empty_allowed"`
	RemainingCount             int64                               `json:"remaining_count"`
	DigestPerformance          results.IntegrityPerformanceSummary `json:"digest_performance"`
	Complete                   bool                                `json:"complete"`
	FailureSamples             []FailureSample                     `json:"-"`
}

// FailureSample is a bounded, sanitized console projection of one canonical failure row.
type FailureSample struct {
	Key              string
	RequestedVersion string
	ReturnedVersion  string
	RequestID        string
	Reason           string
	ExpectedDigest   string
	ActualDigest     string
	ExpectedSize     string
	ActualSize       string
}

// FinalizeResults promotes role-specific evidence, validates commit records, derives the exact
// remaining set, combines performance rows, and atomically updates index.json.
func FinalizeResults(options FinalizeOptions) (outcome FinalizeOutcome, finalErr error) {
	if options.Plan.Valid() {
		if options.Workload != "" && options.Workload != options.Plan.Workload {
			return outcome, fmt.Errorf("finalizer workload %q does not match typed plan %q",
				options.Workload, options.Plan.Workload)
		}
		if options.RunID != 0 && options.RunID != options.Plan.RunID {
			return outcome, fmt.Errorf("finalizer run id %d does not match typed plan %d",
				options.RunID, options.Plan.RunID)
		}
		options.Workload = options.Plan.Workload
		options.RunID = options.Plan.RunID
		options.AllowEmptySelection = options.Plan.AllowEmpty
		options.Multipart = options.Plan.Multipart
	}
	if options.Workload != workload.WriteVerify && options.Workload != workload.ReadVerify {
		return outcome, fmt.Errorf("integrity finalizer does not support workload %q", options.Workload)
	}
	if options.RunID <= 0 || options.ResultsRoot == "" {
		return outcome, fmt.Errorf("integrity finalization requires a positive run id and results root")
	}
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return outcome, err
	}
	reconciled := false
	manifest, err := readResultsManifest(options.ResultsRoot)
	if err != nil {
		return outcome, err
	}
	defer func() {
		for _, name := range []string{
			WrittenName, WrittenCompletionName, VerifyInputName, VerifyInputCompletionName,
			VerifiedName, VerifiedCompletionName, VerifyRemainingName, IntegrityFailuresName,
			IntegrityPerformanceName, MultipartLifecycleName,
		} {
			if _, statErr := os.Stat(filepath.Join(options.ResultsRoot, name)); statErr != nil {
				continue
			}
			if addErr := addRunFile(manifest, options.ResultsRoot, name); addErr != nil && finalErr == nil {
				finalErr = addErr
			}
		}
		outcome.Complete = finalErr == nil && reconciled
		manifest.Integrity = integritySummary(outcome, finalErr)
		if writeErr := writeResultsManifest(options.ResultsRoot, manifest); writeErr != nil && finalErr == nil {
			finalErr = writeErr
		}
	}()
	runtimeRoles := options.RuntimeRoles
	if !options.Plan.Valid() && runtimeRoles == (StepRoles{}) {
		runtimeRoles = ResolveStepRoles(options.StepIDs, manifest)
	}
	plannedRoles := PlannedStepRoles(options.Plan)
	if !options.Plan.Valid() {
		plannedRoles = ResolveStepRoles(options.PlannedStepIDs, nil)
	}
	createStep := firstStepRole(runtimeRoles.Create, plannedRoles.Create)
	listStep := firstStepRole(runtimeRoles.List, plannedRoles.List)
	readStep := firstStepRole(runtimeRoles.Read, plannedRoles.Read)
	if readStep == "" {
		return outcome, fmt.Errorf("verification READ step could not be identified")
	}
	readNotStarted := lifecycleForStepRole(options.StepLifecycles, runtimeRoles.Read, plannedRoles.Read) ==
		string(results.StepLifecycleNotStarted)
	readCounts := operationCounts{}
	if !readNotStarted {
		var metricsErr error
		readCounts, metricsErr = readOperationMetrics(options.ResultsRoot, readStep, "READ")
		if metricsErr != nil {
			return outcome, metricsErr
		}
		outcome.VerificationAttemptedCount = readCounts.success + readCounts.failure
		outcome.CorruptCount = readCounts.corrupt
		if options.BaseURL != "" {
			if err := validateJSONCorruptCount(options.BaseURL, readStep, readCounts.corrupt); err != nil {
				return outcome, err
			}
		}
	}

	inputName := WrittenName
	inputCompletionName := WrittenCompletionName
	inputProducerKind := constants.IntegrityProvenanceEngineStep
	inputProducerID := createStep
	if options.Workload == workload.ReadVerify {
		inputName = VerifyInputName
		inputCompletionName = VerifyInputCompletionName
		inputProducerID = listStep
	}

	inputPath := filepath.Join(options.ResultsRoot, inputName)
	inputCompletionPath := filepath.Join(options.ResultsRoot, inputCompletionName)
	var inputSourcePath, inputSourceCompletionPath, inputPromotionSource string
	if options.Workload == workload.ReadVerify && options.StagedManifest != "" {
		inputProducerKind = constants.IntegrityProvenanceCLIStager
		inputProducerID = CLIStagerProducerID
		inputSourcePath = options.StagedManifest
		inputSourceCompletionPath = options.StagedCompletion
		inputPromotionSource = "staged verification input"
	} else {
		producerStep := createStep
		if options.Workload == workload.ReadVerify {
			producerStep = listStep
		}
		if producerStep == "" {
			if readNotStarted {
				// There is no applicable producer evidence to require. Preserve the
				// structured engine cause without manufacturing missing-artifact noise.
				return outcome, nil
			}
			return outcome, fmt.Errorf("verification input producer step could not be identified")
		}
		if readNotStarted {
			producerLifecycle := lifecycleForStepRole(
				options.StepLifecycles,
				firstStepRole(runtimeRoles.Create, runtimeRoles.List),
				firstStepRole(plannedRoles.Create, plannedRoles.List),
			)
			if producerLifecycle == string(results.StepLifecycleNotStarted) ||
				producerLifecycle == string(results.StepLifecyclePlanned) {
				return outcome, nil
			}
		}
		inputSourcePath = filepath.Join(options.ResultsRoot, producerStep+"."+inputName)
		inputSourceCompletionPath = filepath.Join(options.ResultsRoot, producerStep+"."+inputCompletionName)
		inputPromotionSource = fmt.Sprintf("%s from step %s", inputName, producerStep)
	}
	if err = promoteCompletionPair(
		inputSourcePath, inputSourceCompletionPath,
		inputPath, inputCompletionPath,
		options.RunID, inputProducerKind, inputProducerID, inputName,
	); err != nil {
		return outcome, fmt.Errorf("promote %s: %w", inputPromotionSource, err)
	}
	inputCompletion, err := ValidateCompletion(inputPath, inputCompletionPath, options.RunID, inputProducerKind, inputProducerID, inputName)
	if err != nil {
		return outcome, fmt.Errorf("validate %s: %w", inputName, err)
	}
	if options.Workload == workload.WriteVerify &&
		(inputCompletion.SourceRecordCount != inputCompletion.SelectedRecordCount ||
			inputCompletion.UniqueRecordCount != inputCompletion.SelectedRecordCount) {
		return outcome, fmt.Errorf("%s completion counts must match for CREATE output", inputName)
	}
	outcome.SelectionCount = int64(inputCompletion.SelectedRecordCount)
	if options.Workload == workload.WriteVerify {
		createCounts, countsErr := readOperationMetrics(options.ResultsRoot, createStep, "CREATE")
		if countsErr != nil {
			return outcome, countsErr
		}
		if outcome.SelectionCount != createCounts.success {
			return outcome, fmt.Errorf("written manifest count %d does not equal successful CREATE count %d",
				outcome.SelectionCount, createCounts.success)
		}
	}
	outcome.EmptySelection = outcome.SelectionCount == 0
	outcome.EmptyAllowed = outcome.EmptySelection && options.Workload == workload.ReadVerify && options.AllowEmptySelection
	if readNotStarted {
		// The producer pair is canonical evidence even though the engine
		// correctly prevented the dependent READ from opening its input. The
		// deferred summary remains incomplete while reporting the exact empty
		// selection and zero attempted verification operations.
		return outcome, nil
	}

	verifiedPath := filepath.Join(options.ResultsRoot, VerifiedName)
	verifiedCompletionPath := filepath.Join(options.ResultsRoot, VerifiedCompletionName)
	if err = promoteCompletionPair(
		filepath.Join(options.ResultsRoot, readStep+"."+VerifiedName),
		filepath.Join(options.ResultsRoot, readStep+"."+VerifiedCompletionName),
		verifiedPath, verifiedCompletionPath,
		options.RunID, constants.IntegrityProvenanceEngineStep, readStep, VerifiedName,
	); err != nil {
		return outcome, fmt.Errorf("promote %s from step %s: %w", VerifiedName, readStep, err)
	}
	verifiedCompletion, err := ValidateCompletion(verifiedPath, verifiedCompletionPath, options.RunID, constants.IntegrityProvenanceEngineStep, readStep, VerifiedName)
	if err != nil {
		return outcome, fmt.Errorf("validate %s: %w", VerifiedName, err)
	}
	if verifiedCompletion.SourceRecordCount != verifiedCompletion.SelectedRecordCount ||
		verifiedCompletion.UniqueRecordCount != verifiedCompletion.SelectedRecordCount {
		return outcome, fmt.Errorf("%s completion counts must match for READ output", VerifiedName)
	}
	outcome.VerifiedCount = int64(verifiedCompletion.SelectedRecordCount)
	if outcome.VerifiedCount != readCounts.success {
		return outcome, fmt.Errorf("verified manifest count %d does not equal successful READ count %d",
			outcome.VerifiedCount, readCounts.success)
	}
	if err = promoteStepFile(options.ResultsRoot, readStep, IntegrityFailuresName); err != nil {
		return outcome, err
	}
	failureCount, samples, failureErr := readFailureArtifact(
		filepath.Join(options.ResultsRoot, IntegrityFailuresName), options.MaxConsoleFailures)
	if failureErr != nil {
		return outcome, failureErr
	}
	outcome.FailureSamples = samples
	if failureCount != outcome.CorruptCount {
		return outcome, fmt.Errorf("integrity failure rows %d do not equal CountCorrupt %d", failureCount, outcome.CorruptCount)
	}

	remaining, err := subtractSortedManifests(
		ctx, inputPath, verifiedPath, filepath.Join(options.ResultsRoot, VerifyRemainingName))
	if err != nil {
		return outcome, err
	}
	outcome.RemainingCount = int64(remaining)

	if outcome.VerificationAttemptedCount != outcome.SelectionCount ||
		outcome.VerifiedCount > outcome.VerificationAttemptedCount {
		return outcome, fmt.Errorf("verification operation counts do not reconcile: selected=%d attempted=%d verified=%d",
			outcome.SelectionCount, outcome.VerificationAttemptedCount, outcome.VerifiedCount)
	}

	if err = ctx.Err(); err != nil {
		return outcome, err
	}
	performanceSteps := []string{readStep}
	if options.Workload == workload.WriteVerify {
		performanceSteps = []string{createStep, readStep}
	}
	outcome.DigestPerformance, err = combinePerformance(options.ResultsRoot, performanceSteps)
	if err != nil {
		return outcome, err
	}
	if options.Multipart {
		if err = promoteStepFile(options.ResultsRoot, createStep, MultipartLifecycleName); err != nil {
			return outcome, err
		}
	}

	reconciled = true
	return outcome, nil
}

func integritySummary(outcome FinalizeOutcome, finalErr error) *results.IntegritySummary {
	summary := &results.IntegritySummary{
		Complete:                   outcome.Complete,
		SelectionCount:             outcome.SelectionCount,
		VerificationAttemptedCount: outcome.VerificationAttemptedCount,
		VerifiedCount:              outcome.VerifiedCount,
		CorruptCount:               outcome.CorruptCount,
		RemainingCount:             outcome.RemainingCount,
		EmptySelection:             outcome.EmptySelection,
		EmptyAllowed:               outcome.EmptyAllowed,
		DigestPerformance:          outcome.DigestPerformance,
	}
	if finalErr != nil {
		summary.FinalizationError = finalErr.Error()
	}
	return summary
}

// StepRoles names the producer and verification steps selected from ordered evidence.
type StepRoles struct {
	Create string
	List   string
	Read   string
}

// PlannedStepRoles projects the immutable typed plan into finalizer roles.
func PlannedStepRoles(plan integrityplan.Plan) StepRoles {
	roles := StepRoles{}
	if !plan.Valid() {
		return roles
	}
	if plan.Producer != nil {
		switch plan.Producer.Role {
		case integrityplan.StepRoleCreate:
			roles.Create = plan.Producer.ID
		case integrityplan.StepRoleList:
			roles.List = plan.Producer.ID
		}
	}
	if plan.Verifier.Role == integrityplan.StepRoleVerify {
		roles.Read = plan.Verifier.ID
	}
	return roles
}

// ObservedStepRoles returns only typed plan roles whose exact IDs occur in
// runtime evidence. Timestamp-reassigned runtime IDs bind through the stable
// typed step ordinal; semantic roles are never inferred from an ID suffix.
func ObservedStepRoles(plan integrityplan.Plan, observed []string) StepRoles {
	if !plan.Valid() {
		return StepRoles{}
	}
	byNumber := make(map[int]string, len(observed))
	seen := make(map[string]struct{}, len(observed))
	for _, runtimeID := range observed {
		seen[runtimeID] = struct{}{}
		if number, ok := integrityplan.RuntimeStepNumber(runtimeID); ok {
			existing, exists := byNumber[number]
			switch {
			case !exists:
				byNumber[number] = runtimeID
			case existing != runtimeID:
				// Multiple runtime identities for one ordinal are not enough
				// evidence to bind either identity to a semantic role.
				byNumber[number] = ""
			}
		}
	}
	resolve := func(step *integrityplan.PlannedStep) string {
		if step == nil {
			return ""
		}
		if _, exact := seen[step.ID]; exact {
			return step.ID
		}
		return byNumber[step.Number]
	}
	roles := StepRoles{}
	if plan.Producer != nil {
		switch plan.Producer.Role {
		case integrityplan.StepRoleCreate:
			roles.Create = resolve(plan.Producer)
		case integrityplan.StepRoleList:
			roles.List = resolve(plan.Producer)
		}
	}
	roles.Read = resolve(&plan.Verifier)
	return roles
}

// ResolveStepRoles preserves suffix-based compatibility for legacy callers
// that do not carry a typed plan. New verification runs use exact planned IDs.
func ResolveStepRoles(configured []string, manifest *results.Manifest) StepRoles {
	ids := append([]string(nil), configured...)
	if manifest != nil {
		for _, step := range manifest.Steps {
			if !contains(ids, step.StepID) {
				ids = append(ids, step.StepID)
			}
		}
	}
	roles := StepRoles{}
	for _, id := range ids {
		switch {
		case roles.Create == "" && strings.HasSuffix(id, "-"+constants.IntegrityStepRoleCreate):
			roles.Create = id
		case roles.List == "" && strings.HasSuffix(id, "-"+constants.IntegrityStepRoleList):
			roles.List = id
		case roles.Read == "" && strings.HasSuffix(id, "-"+constants.IntegrityStepRoleVerify):
			roles.Read = id
		}
	}
	return roles
}

func firstStepRole(candidates ...string) string {
	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

func lifecycleForStepRole(lifecycles map[string]string, runtimeStepID, plannedStepID string) string {
	for _, stepID := range []string{runtimeStepID, plannedStepID} {
		if stepID == "" {
			continue
		}
		if lifecycle, ok := lifecycles[stepID]; ok {
			return lifecycle
		}
	}
	return ""
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func promoteStepFile(root, step, name string) error {
	if step == "" {
		return fmt.Errorf("cannot promote %s without a producing step", name)
	}
	source := filepath.Join(root, step+"."+name)
	if err := copyAtomicOrMatch(source, filepath.Join(root, name)); err != nil {
		return fmt.Errorf("promote %s from step %s: %w", name, step, err)
	}
	return nil
}

type completionPairState uint8

const (
	completionPairAbsent completionPairState = iota
	completionPairManifestOnly
	completionPairMarkerOnly
	completionPairPresent
)

// completionPromotionMaxStateTransitions bounds retries when another publisher changes the
// manifest/marker pair between observations.
const completionPromotionMaxStateTransitions = 6

func promoteCompletionPair(
	sourceManifestPath, sourceCompletionPath string,
	destinationManifestPath, destinationCompletionPath string,
	runID int64,
	producerKind, producerID, artifact string,
) error {
	sourceRecord, err := validateCompletionForPromotion(
		sourceManifestPath, sourceCompletionPath,
		runID, producerKind, producerID, artifact,
	)
	if err != nil {
		return fmt.Errorf("validate source completion pair: %w", err)
	}

	for attempt := 0; attempt < completionPromotionMaxStateTransitions; attempt++ {
		state, stateErr := inspectCompletionPairState(
			destinationManifestPath, destinationCompletionPath,
		)
		if stateErr != nil {
			return stateErr
		}
		switch state {
		case completionPairAbsent:
			if err = copyAtomic(sourceManifestPath, destinationManifestPath); err != nil {
				if errors.Is(err, fs.ErrExist) {
					continue
				}
				return fmt.Errorf("publish canonical manifest: %w", err)
			}
		case completionPairManifestOnly:
			if err = validateCompletionRecord(
				destinationManifestPath, sourceRecord,
				runID, producerKind, producerID, artifact, true,
			); err != nil {
				return fmt.Errorf("existing canonical manifest conflicts with source completion: %w", err)
			}
			if err = durableOSOperations.syncFile(destinationManifestPath); err != nil {
				return fmt.Errorf("synchronize recovered canonical manifest: %w", err)
			}
			if err = copyAtomic(sourceCompletionPath, destinationCompletionPath); err != nil {
				if errors.Is(err, fs.ErrExist) {
					continue
				}
				return fmt.Errorf("publish canonical completion record: %w", err)
			}
		case completionPairMarkerOnly:
			return fmt.Errorf(
				"canonical completion record %s exists without manifest %s",
				filepath.Base(destinationCompletionPath),
				filepath.Base(destinationManifestPath),
			)
		case completionPairPresent:
			existingRecord, validateErr := ValidateCompletion(
				destinationManifestPath, destinationCompletionPath,
				runID, producerKind, producerID, artifact,
			)
			if validateErr != nil {
				return fmt.Errorf("existing canonical completion pair conflicts with source: %w", validateErr)
			}
			if existingRecord != sourceRecord {
				return fmt.Errorf("existing canonical completion record does not match source record")
			}
			if err = synchronizeExistingCompletionPair(
				destinationManifestPath, destinationCompletionPath,
			); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("unknown completion pair state %d", state)
		}
	}
	return fmt.Errorf("canonical completion pair changed repeatedly during publication")
}

func inspectCompletionPairState(manifestPath, completionPath string) (completionPairState, error) {
	manifestExists, err := artifactExists(manifestPath)
	if err != nil {
		return completionPairAbsent, fmt.Errorf("inspect canonical manifest: %w", err)
	}
	completionExists, err := artifactExists(completionPath)
	if err != nil {
		return completionPairAbsent, fmt.Errorf("inspect canonical completion record: %w", err)
	}
	switch {
	case manifestExists && completionExists:
		return completionPairPresent, nil
	case manifestExists:
		return completionPairManifestOnly, nil
	case completionExists:
		return completionPairMarkerOnly, nil
	default:
		return completionPairAbsent, nil
	}
}

func artifactExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func synchronizeExistingCompletionPair(manifestPath, completionPath string) error {
	if err := durableOSOperations.syncFile(manifestPath); err != nil {
		return fmt.Errorf("synchronize existing canonical manifest: %w", err)
	}
	if err := durableOSOperations.syncFile(completionPath); err != nil {
		return fmt.Errorf("synchronize existing canonical completion record: %w", err)
	}
	directory, err := durablePublicationDirectory(manifestPath, completionPath)
	if err != nil {
		return err
	}
	if err = durableOSOperations.syncDirectory(directory); err != nil {
		return fmt.Errorf("synchronize existing canonical completion pair directory: %w", err)
	}
	return nil
}

func copyAtomic(source, destination string) error {
	return copyAtomicWithPublisher(source, destination, closeAndPublishTempFileNoReplace)
}

func copyAtomicOrMatch(source, destination string) error {
	return copyAtomicWithPublisher(source, destination, closeAndPublishTempFileNoReplaceOrMatch)
}

func copyAtomicWithPublisher(
	source, destination string,
	publish func(*os.File, string, string) error,
) error {
	in, err := os.Open(source) // #nosec G304 -- internally resolved result/staging path
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	tmp, err := os.CreateTemp(filepath.Dir(destination), ".integrity-promote-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err = io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	err = publish(tmp, tmpPath, destination)
	if err != nil {
		_ = os.Remove(tmpPath)
	}
	return err
}

func subtractSortedManifests(ctx context.Context, inputPath, verifiedPath, destination string) (int, error) {
	input, err := openSortedManifest(inputPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = input.file.Close() }()
	verified, err := openSortedManifest(verifiedPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = verified.file.Close() }()

	tmp, err := os.CreateTemp(filepath.Dir(destination), ".verify-remaining-*")
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	writer := csv.NewWriter(tmp)
	if err = writer.Write(canonicalHeader); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return 0, err
	}
	left, leftErr := input.next()
	right, rightErr := verified.next()
	remaining := 0
	for leftErr == nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
			break
		}
		if rightErr != nil && !errors.Is(rightErr, io.EOF) {
			err = fmt.Errorf("parse verified manifest: %w", rightErr)
			break
		}
		if errors.Is(rightErr, io.EOF) || left.compare(right) < 0 {
			if err = writer.Write(left.fields()); err != nil {
				break
			}
			remaining++
			left, leftErr = input.next()
			continue
		}
		if left.compare(right) > 0 {
			err = fmt.Errorf("verified identity %q is not in the selected input", right.identity())
			break
		}
		if left.size != right.size {
			err = fmt.Errorf("verified identity %q has a different size than selected input", right.identity())
			break
		}
		left, leftErr = input.next()
		right, rightErr = verified.next()
	}
	if err == nil && !errors.Is(leftErr, io.EOF) {
		err = leftErr
	}
	if err == nil && rightErr == nil {
		err = fmt.Errorf("verified identity %q is not in the selected input", right.identity())
	}
	if err == nil && !errors.Is(rightErr, io.EOF) {
		err = rightErr
	}
	writer.Flush()
	if err == nil {
		err = writer.Error()
	}
	if err == nil {
		err = closeAndPublishTempFileNoReplaceOrMatch(tmp, tmpPath, destination)
	} else {
		_ = tmp.Close()
	}
	if err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("derive %s: %w", VerifyRemainingName, err)
	}
	return remaining, nil
}

type sortedManifest struct {
	file   *os.File
	reader *csv.Reader
}

func openSortedManifest(path string) (*sortedManifest, error) {
	file, err := os.Open(path) // #nosec G304 -- private sorted path
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = len(canonicalHeader)
	header, err := reader.Read()
	if err != nil || !equalFields(header, canonicalHeader) {
		_ = file.Close()
		return nil, fmt.Errorf("sorted manifest has invalid header")
	}
	return &sortedManifest{file: file, reader: reader}, nil
}

func (manifest *sortedManifest) next() (ManifestRecord, error) {
	fields, err := manifest.reader.Read()
	if err != nil {
		return ManifestRecord{}, err
	}
	return parseManifestRecord(fields, 0)
}

func readFailureArtifact(filePath string, maxSamples int) (int64, []FailureSample, error) {
	file, err := os.Open(filePath) // #nosec G304 -- internally promoted results path
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = file.Close() }()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = len(failureHeader)
	header, err := reader.Read()
	if err != nil || !equalFields(header, failureHeader) {
		return 0, nil, fmt.Errorf("%s has an invalid header", IntegrityFailuresName)
	}
	allowedReasons := map[string]struct{}{
		"metadata_missing": {}, "metadata_invalid": {}, "algorithm_unsupported": {},
		"size_mismatch": {}, "digest_mismatch": {},
	}
	var count int64
	var samples []FailureSample
	for {
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, nil, fmt.Errorf("parse %s row %d: %w", IntegrityFailuresName, count+2, readErr)
		}
		count++
		if _, ok := allowedReasons[row[8]]; !ok {
			return 0, nil, fmt.Errorf("%s row %d has unsupported reason %q", IntegrityFailuresName, count+1, row[8])
		}
		if maxSamples > 0 && len(samples) < maxSamples {
			samples = append(samples, FailureSample{
				Key: row[4], RequestedVersion: row[5], ReturnedVersion: row[6], RequestID: row[7],
				Reason: row[8], ExpectedDigest: row[10], ActualDigest: row[11], ExpectedSize: row[12], ActualSize: row[13],
			})
		}
	}
	return count, samples, nil
}

type operationCounts struct {
	success int64
	failure int64
	corrupt int64
}

func readOperationMetrics(root, step, opType string) (operationCounts, error) {
	var counts operationCounts
	metricsPath := filepath.Join(root, step+"."+constants.ResultsArtifactSuffixMetricsTotal)
	file, err := os.Open(metricsPath) // #nosec G304 -- internally resolved results path
	if err != nil {
		return counts, fmt.Errorf("open %s metrics: %w", opType, err)
	}
	defer func() { _ = file.Close() }()
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return counts, fmt.Errorf("parse %s metrics: %w", opType, err)
	}
	if len(records) < 2 {
		return counts, fmt.Errorf("%s metrics contain no data rows", opType)
	}
	columns := make(map[string]int, len(records[0]))
	for i, name := range records[0] {
		columns[name] = i
	}
	for _, required := range []string{"OpType", "CountSucc", "CountFail", "CountCorrupt"} {
		if _, ok := columns[required]; !ok {
			return counts, fmt.Errorf("%s metrics missing required column %s", opType, required)
		}
	}
	matched := false
	for rowIndex, row := range records[1:] {
		if len(row) != len(records[0]) || !strings.EqualFold(strings.TrimSpace(row[columns["OpType"]]), opType) {
			continue
		}
		matched = true
		values := []*int64{&counts.success, &counts.failure, &counts.corrupt}
		for i, column := range []string{"CountSucc", "CountFail", "CountCorrupt"} {
			value, parseErr := strconv.ParseInt(row[columns[column]], 10, 64)
			if parseErr != nil {
				return counts, fmt.Errorf("parse %s row %d: %w", column, rowIndex+2, parseErr)
			}
			if value < 0 {
				return counts, fmt.Errorf("%s row %d has negative value %d", column, rowIndex+2, value)
			}
			*values[i] += value
		}
	}
	if !matched {
		return counts, fmt.Errorf("%s metrics contain no %s row", opType, opType)
	}
	if counts.corrupt > counts.failure {
		return counts, fmt.Errorf("%s corrupt count %d exceeds failure count %d", opType, counts.corrupt, counts.failure)
	}
	return counts, nil
}

// ObserveJSONCorruptCount reads the required corruption count from an engine metrics endpoint.
func ObserveJSONCorruptCount(baseURL, readStep string) (int64, error) {
	return ObserveJSONCorruptCountContext(context.Background(), baseURL, readStep)
}

// ObserveJSONCorruptCountContext reads the required corruption count with caller cancellation.
func ObserveJSONCorruptCountContext(ctx context.Context, baseURL, readStep string) (int64, error) {
	for _, endpoint := range []string{"/metrics/fleet/json", "/metrics/json"} {
		url := strings.TrimSuffix(baseURL, "/") + endpoint
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return 0, fmt.Errorf("create required corruption metrics request for %s: %w", endpoint, err)
		}
		response, err := (&http.Client{Timeout: constants.IntegrityMetricsHTTPTimeout}).Do(request)
		if err != nil {
			return 0, fmt.Errorf("fetch required corruption metrics from %s: %w", endpoint, err)
		}
		if response.StatusCode == http.StatusNotFound {
			_ = response.Body.Close()
			continue
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			return 0, fmt.Errorf("required corruption metrics endpoint %s returned HTTP %d", endpoint, response.StatusCode)
		}
		var rows []struct {
			StepID     string `json:"step_id"`
			OpType     string `json:"op_type"`
			Operations struct {
				CorruptCount *int64 `json:"corrupt_count"`
			} `json:"operations"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&rows)
		_ = response.Body.Close()
		if decodeErr != nil {
			return 0, fmt.Errorf("decode required corruption metrics from %s: %w", endpoint, decodeErr)
		}
		var observed int64
		matched := false
		for _, row := range rows {
			if row.StepID != readStep || !strings.EqualFold(row.OpType, "READ") {
				continue
			}
			matched = true
			if row.Operations.CorruptCount == nil {
				return 0, fmt.Errorf("verification JSON metrics missing operations.corrupt_count for step %s", readStep)
			}
			observed += *row.Operations.CorruptCount
		}
		if !matched {
			// A present fleet endpoint without this step may be a single-host empty view; try node metrics.
			if endpoint == "/metrics/fleet/json" {
				continue
			}
			return 0, fmt.Errorf("verification JSON metrics contain no READ row for step %s", readStep)
		}
		if observed < 0 {
			return 0, fmt.Errorf("verification JSON metrics contain a negative corruption count")
		}
		return observed, nil
	}
	return 0, fmt.Errorf("verification JSON metrics are unavailable")
}

func validateJSONCorruptCount(baseURL, readStep string, expected int64) error {
	observed, err := ObserveJSONCorruptCount(baseURL, readStep)
	if err != nil {
		return err
	}
	if observed != expected {
		return fmt.Errorf("JSON corruption count %d does not match CountCorrupt %d", observed, expected)
	}
	return nil
}

func combinePerformance(root string, steps []string) (results.IntegrityPerformanceSummary, error) {
	var summary results.IntegrityPerformanceSummary
	destination := filepath.Join(root, IntegrityPerformanceName)
	tmp, err := os.CreateTemp(root, ".integrity-performance-*")
	if err != nil {
		return summary, err
	}
	tmpPath := tmp.Name()
	writer := csv.NewWriter(tmp)
	if err = writer.Write(performanceHeader); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return summary, err
	}
	seen := make(map[string]struct{})
	phaseOrder := []string{"write_prehash", "read_verify"}
	phaseTotals := make(map[string]*results.IntegrityPhaseSummary, len(phaseOrder))
	for _, phase := range phaseOrder {
		phaseTotals[phase] = &results.IntegrityPhaseSummary{Phase: phase}
	}
	for stepIndex, step := range steps {
		path := filepath.Join(root, step+"."+IntegrityPerformanceName)
		file, openErr := os.Open(path) // #nosec G304 -- internally resolved results path
		if openErr != nil {
			err = fmt.Errorf("open performance artifact for step %s: %w", step, openErr)
			break
		}
		reader := csv.NewReader(file)
		reader.FieldsPerRecord = len(performanceHeader)
		header, readErr := reader.Read()
		if readErr != nil || !equalFields(header, performanceHeader) {
			_ = file.Close()
			err = fmt.Errorf("performance artifact for step %s has invalid header", step)
			break
		}
		for {
			row, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				err = fmt.Errorf("parse performance artifact for step %s: %w", step, readErr)
				break
			}
			phase := row[3]
			if (stepIndex == 0 && len(steps) == 2 && phase != "write_prehash") ||
				((len(steps) == 1 || stepIndex == len(steps)-1) && phase != "read_verify") {
				err = fmt.Errorf("performance artifact step %s has unexpected phase %q", step, phase)
				break
			}
			identity := strings.Join(row[:4], "\x00")
			if _, exists := seen[identity]; exists {
				err = fmt.Errorf("duplicate performance row identity for step %s", step)
				break
			}
			seen[identity] = struct{}{}
			parsed, parseErr := parsePerformanceRow(step, row)
			if parseErr != nil {
				err = parseErr
				break
			}
			addPerformanceTotals(&summary, phaseTotals[phase], parsed)
			if err = writer.Write(row); err != nil {
				break
			}
		}
		_ = file.Close()
		if err != nil {
			break
		}
	}
	writer.Flush()
	if err == nil {
		err = writer.Error()
	}
	if err == nil {
		err = closeAndPublishTempFileNoReplaceOrMatch(tmp, tmpPath, destination)
	} else {
		_ = tmp.Close()
	}
	if err != nil {
		_ = os.Remove(tmpPath)
		return summary, fmt.Errorf("combine %s: %w", IntegrityPerformanceName, err)
	}
	if summary.HashWorkerSeconds > 0 {
		summary.MeanWorkerHashMiBPerSecond = float64(summary.Bytes) /
			float64(constants.BytesPerMiB) / summary.HashWorkerSeconds
	}
	for _, phase := range phaseOrder {
		phaseSummary := phaseTotals[phase]
		if phaseSummary.HashWorkerSeconds > 0 {
			phaseSummary.MeanWorkerHashMiBPerSecond = float64(phaseSummary.Bytes) /
				float64(constants.BytesPerMiB) / phaseSummary.HashWorkerSeconds
		}
		if phaseSummary.Objects > 0 || phaseSummary.Bytes > 0 || phaseSummary.HashWorkerSeconds > 0 {
			summary.Phases = append(summary.Phases, *phaseSummary)
		}
	}
	return summary, nil
}

type performanceRow struct {
	phase                   string
	objects                 int64
	bytes                   int64
	workerSeconds           float64
	initialWriteDelay       *float64
	additionalPayloadPasses int64
}

func parsePerformanceRow(step string, row []string) (performanceRow, error) {
	var parsed performanceRow
	if row[0] == "" || row[1] != step || row[2] == "" {
		return parsed, fmt.Errorf("performance artifact for step %s has invalid row identity", step)
	}
	if row[4] != constants.IntegrityAlgorithmSHA256 {
		return parsed, fmt.Errorf("performance artifact for step %s has unsupported algorithm %q", step, row[4])
	}
	parsed.phase = row[3]
	var err error
	if parsed.objects, err = parseNonnegativePerformanceInt(row[5], "objects", step); err != nil {
		return parsed, err
	}
	if parsed.bytes, err = parseNonnegativePerformanceInt(row[6], "bytes", step); err != nil {
		return parsed, err
	}
	if parsed.workerSeconds, err = parseNonnegativePerformanceFloat(row[7], "hash_worker_seconds", step); err != nil {
		return parsed, err
	}
	if parsed.additionalPayloadPasses, err = parseNonnegativePerformanceInt(row[10], "additional_payload_passes", step); err != nil {
		return parsed, err
	}
	if parsed.bytes > 0 && parsed.workerSeconds == 0 {
		return parsed, fmt.Errorf("performance artifact for step %s reports digest bytes with zero worker time", step)
	}
	if parsed.workerSeconds == 0 {
		if row[8] != "" {
			return parsed, fmt.Errorf("performance artifact for step %s must leave mean worker rate blank without digest work", step)
		}
	} else {
		reportedRate, rateErr := parseNonnegativePerformanceFloat(row[8], "mean_worker_hash_mib_per_second", step)
		if rateErr != nil {
			return parsed, rateErr
		}
		expectedRate := float64(parsed.bytes) / float64(constants.BytesPerMiB) / parsed.workerSeconds
		tolerance := math.Max(1e-9, math.Abs(expectedRate)*1e-6)
		if math.Abs(reportedRate-expectedRate) > tolerance {
			return parsed, fmt.Errorf("performance artifact for step %s has inconsistent mean worker rate", step)
		}
	}
	if parsed.phase == "read_verify" {
		if row[9] != "" {
			return parsed, fmt.Errorf("performance artifact for step %s must leave read initial request delay blank", step)
		}
	} else if row[9] != "" {
		value, delayErr := parseNonnegativePerformanceFloat(row[9], "time_to_first_request_seconds", step)
		if delayErr != nil {
			return parsed, delayErr
		}
		parsed.initialWriteDelay = &value
	} else if parsed.objects > 0 {
		return parsed, fmt.Errorf("performance artifact for step %s is missing initial write delay", step)
	}
	return parsed, nil
}

func parseNonnegativePerformanceInt(value, field, step string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("performance artifact for step %s has invalid %s %q", step, field, value)
	}
	return parsed, nil
}

func parseNonnegativePerformanceFloat(value, field, step string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("performance artifact for step %s has invalid %s %q", step, field, value)
	}
	return parsed, nil
}

func addPerformanceTotals(
	summary *results.IntegrityPerformanceSummary,
	phase *results.IntegrityPhaseSummary,
	row performanceRow,
) {
	summary.Objects += row.objects
	summary.Bytes += row.bytes
	summary.HashWorkerSeconds += row.workerSeconds
	summary.AdditionalPayloadPasses += row.additionalPayloadPasses
	phase.Objects += row.objects
	phase.Bytes += row.bytes
	phase.HashWorkerSeconds += row.workerSeconds
	phase.AdditionalPayloadPasses += row.additionalPayloadPasses
	if row.initialWriteDelay != nil &&
		(summary.InitialWriteDelaySecondsMaxNode == nil || *row.initialWriteDelay > *summary.InitialWriteDelaySecondsMaxNode) {
		value := *row.initialWriteDelay
		summary.InitialWriteDelaySecondsMaxNode = &value
	}
}

func readResultsManifest(root string) (*results.Manifest, error) {
	path := filepath.Join(root, constants.ResultsManifestFileName)
	data, err := os.ReadFile(path) // #nosec G304 -- internally resolved results path
	if err != nil {
		return nil, fmt.Errorf("read results manifest: %w", err)
	}
	manifest := &results.Manifest{}
	if err = json.Unmarshal(data, manifest); err != nil {
		return nil, fmt.Errorf("decode results manifest: %w", err)
	}
	return manifest, nil
}

func addRunFile(manifest *results.Manifest, root, name string) error {
	info, err := os.Stat(filepath.Join(root, name))
	if err != nil {
		return fmt.Errorf("stat run artifact %s: %w", name, err)
	}
	contentType := "application/octet-stream"
	if strings.HasSuffix(name, ".csv") {
		contentType = "text/csv"
	} else if strings.HasSuffix(name, ".json") {
		contentType = "application/json"
	}
	status := results.FileStatus{Name: name, Size: info.Size(), Status: "ok", Modified: info.ModTime().UTC().Format(time.RFC3339), ContentType: contentType}
	for i := range manifest.RunFiles {
		if manifest.RunFiles[i].Name == name {
			manifest.RunFiles[i] = status
			return nil
		}
	}
	manifest.RunFiles = append(manifest.RunFiles, status)
	return nil
}

func writeResultsManifest(root string, manifest *results.Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(root, ".index.json.integrity-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err = tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	err = closeAndPublishTempFile(
		tmp,
		tmpPath,
		filepath.Join(root, constants.ResultsManifestFileName),
	)
	if err != nil {
		_ = os.Remove(tmpPath)
	}
	return err
}
