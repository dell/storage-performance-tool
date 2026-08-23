// Package deletemetrics defines the additive schema-v4 DELETE metrics contract.
package deletemetrics

import (
	"fmt"
	"math"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

const (
	// SchemaVersion is the first metrics schema carrying the DELETE detail contract.
	SchemaVersion = 4
	// RequestUnit names the logical DELETE request and batch counter unit.
	RequestUnit = "logical_api_requests"
	// ObjectUnit names the DELETE target counter unit.
	ObjectUnit = "object_identities"
	// NotApplicable marks byte-oriented metrics that do not apply to DELETE.
	NotApplicable = "not_applicable"
	// OutcomeAccepted is the canonical term for storage-API acceptance.
	OutcomeAccepted = "accepted"
	// OutcomeRunning marks a live DELETE failure-budget decision.
	OutcomeRunning = "running"
	// OutcomeCompletedCleanly marks a terminal run with no operational object failures.
	OutcomeCompletedCleanly = "completed_cleanly"
	// OutcomeCompletedWithinFailureBudget marks a successful run with tolerated failures.
	OutcomeCompletedWithinFailureBudget = "completed_within_failure_budget"
	// OutcomeFailed marks a terminal run that did not satisfy the failure-budget contract.
	OutcomeFailed = "failed"
	// LatencyDefinition names the accepted first-byte request timing interval.
	LatencyDefinition = "first_request_byte_sent_to_first_response_byte_received"
	// DurationDefinition names the accepted full request duration interval.
	DurationDefinition = "request_formulation_to_last_response_byte_received"
	// VerificationNotice is required when removal verification is disabled.
	VerificationNotice = "Verification disabled; results describe logical DELETE API outcomes, not confirmed object removal."
	// MaxBucketMetrics bounds named per-bucket metrics before the overflow bucket.
	MaxBucketMetrics = 100
	// OverflowBucket combines selected bucket identities beyond MaxBucketMetrics.
	OverflowBucket = "__other__"
	// MaxConfiguredBatchSize is the S3 multi-object DELETE target limit.
	MaxConfiguredBatchSize = 1000
)

// Metrics is the additive schema-v4 DELETE metrics payload.
type Metrics struct {
	Units              Units         `json:"units"`
	Requests           Requests      `json:"requests"`
	Objects            Objects       `json:"objects"`
	Batches            Batches       `json:"batches"`
	Completion         Completion    `json:"completion"`
	Versions           Versions      `json:"versions"`
	Buckets            []Bucket      `json:"buckets"`
	Phases             Phases        `json:"phases"`
	Identity           Identity      `json:"identity"`
	FailurePolicy      FailurePolicy `json:"failure_policy"`
	Timing             Timing        `json:"timing"`
	Performance        Performance   `json:"performance"`
	OutcomeTerminology string        `json:"outcome_terminology"`
	Verification       Verification  `json:"verification"`
	TerminalReconciled bool          `json:"terminal_reconciled"`
}

// Completion reports logical request and object completion.
type Completion struct {
	RequestPercent     float64 `json:"request_percent"`
	ObjectPercent      float64 `json:"object_percent"`
	TerminalReconciled bool    `json:"terminal_reconciled"`
}

// Units names the counting unit for each DELETE dimension.
type Units struct {
	Requests string `json:"requests"`
	Objects  string `json:"objects"`
	Batches  string `json:"batches"`
}

// Requests contains logical DELETE API request counters and rate.
type Requests struct {
	Attempted   int64   `json:"attempted"`
	FullSuccess int64   `json:"full_success"`
	Partial     int64   `json:"partial"`
	Failed      int64   `json:"failed"`
	Unresolved  int64   `json:"unresolved"`
	PerSecond   float64 `json:"per_second"`
}

// Objects contains selected object-identity counters and rate.
type Objects struct {
	Selected    int64   `json:"selected"`
	Attempted   int64   `json:"attempted"`
	Accepted    int64   `json:"accepted"`
	Failed      int64   `json:"failed"`
	Unattempted int64   `json:"unattempted"`
	Unresolved  int64   `json:"unresolved"`
	PerSecond   float64 `json:"per_second"`
}

// Batches describes configured and observed batch composition.
type Batches struct {
	ConfiguredSize        int     `json:"configured_size"`
	ActualRequestCount    int64   `json:"actual_request_count"`
	ActualObjectCount     int64   `json:"actual_object_count"`
	MeanObjectsPerRequest float64 `json:"mean_objects_per_request"`
	FullBatchCount        int64   `json:"full_batch_count"`
	PartialBatchCount     int64   `json:"partial_batch_count"`
	FullBatchPercent      float64 `json:"full_batch_percent"`
}

// Versions separates current-key from exact-version identities.
type Versions struct {
	CurrentKey   int64 `json:"current_key"`
	ExactVersion int64 `json:"exact_version"`
}

// Bucket contains bounded per-bucket object counters.
type Bucket struct {
	Bucket    string `json:"bucket"`
	Selected  int64  `json:"selected"`
	Attempted int64  `json:"attempted"`
	Accepted  int64  `json:"accepted"`
	Failed    int64  `json:"failed"`
}

// Phases contains applicable lifecycle durations in seconds.
type Phases struct {
	SeedSeconds             *float64 `json:"seed"`
	DiscoverySeconds        *float64 `json:"discovery"`
	PreValidationSeconds    *float64 `json:"pre_validation"`
	ScheduledDeleteSeconds  *float64 `json:"scheduled_delete_seconds"`
	DrainSeconds            *float64 `json:"drain_seconds"`
	PostVerificationSeconds *float64 `json:"post_verification"`
	CleanupSeconds          *float64 `json:"cleanup"`
	TotalWallSeconds        *float64 `json:"total_wall_seconds"`
}

// Identity records the DELETE selection and batching identity.
type Identity struct {
	Mode                string `json:"mode"`
	ConfiguredBatchSize int    `json:"configured_batch_size"`
	SelectionOrder      string `json:"selection_order"`
}

// FailurePolicy records configured failure bounds and their observation.
type FailurePolicy struct {
	Mode                     string  `json:"mode"`
	Outcome                  string  `json:"outcome"`
	MaxFailedObjects         int64   `json:"max_failed_objects"`
	MaxFailurePercent        float64 `json:"max_failure_percent"`
	GraceSeconds             int64   `json:"grace_seconds"`
	OperationalFailedObjects int64   `json:"operational_failed_objects"`
	ExcludedFailedObjects    int64   `json:"excluded_failed_objects"`
	ObservedFailurePercent   float64 `json:"observed_failure_percent"`
}

// Timing defines and summarizes request-level timing samples.
type Timing struct {
	LatencyDefinition  string      `json:"latency_definition"`
	DurationDefinition string      `json:"duration_definition"`
	Latency            *TimingStat `json:"latency,omitempty"`
	Duration           *TimingStat `json:"duration,omitempty"`
	ObjectLatency      *TimingStat `json:"object_latency"`
}

// TimingStat contains bounded request-level distribution statistics.
type TimingStat struct {
	Count         int64   `json:"count"`
	MeanUs        float64 `json:"mean_us"`
	MinUs         int64   `json:"min_us"`
	P50Us         int64   `json:"p50_us"`
	P90Us         int64   `json:"p90_us"`
	P99Us         int64   `json:"p99_us"`
	P999Us        int64   `json:"p999_us"`
	MaxUs         int64   `json:"max_us"`
	OverflowCount int64   `json:"overflow_count"`
}

// Performance records metric applicability without fabricating transfer data.
type Performance struct {
	ObjectSize string `json:"object_size"`
	DataMoved  string `json:"data_moved"`
	Bandwidth  string `json:"bandwidth"`
	TTFB       string `json:"ttfb"`
}

// Verification states whether logical outcomes were removal-verified.
type Verification struct {
	Enabled          bool   `json:"enabled"`
	RemovalConfirmed bool   `json:"removal_confirmed"`
	Notice           string `json:"notice"`
}

// ValidateTerminal rejects incomplete or internally inconsistent terminal schema-v4 DELETE detail.
func ValidateTerminal(metrics *Metrics) error {
	return validateTerminal(metrics, false)
}

// ValidateTerminalContributor validates a complete node contribution while allowing the
// controller-owned failure-budget outcome to remain live until the fleet verdict is published.
func ValidateTerminalContributor(metrics *Metrics) error {
	return validateTerminal(metrics, true)
}

func validateTerminal(metrics *Metrics, allowRunningOutcome bool) error {
	if metrics == nil {
		return fmt.Errorf("DELETE detail is missing")
	}
	if metrics.Units.Requests != RequestUnit || metrics.Units.Objects != ObjectUnit ||
		metrics.Units.Batches != RequestUnit {
		return fmt.Errorf("DELETE units are incomplete or incompatible")
	}
	if (metrics.Identity.Mode != constants.DeleteIdentityModeSingle &&
		metrics.Identity.Mode != constants.DeleteIdentityModeBatch) ||
		metrics.Identity.ConfiguredBatchSize <= 0 ||
		metrics.Identity.ConfiguredBatchSize > MaxConfiguredBatchSize ||
		metrics.Identity.ConfiguredBatchSize != metrics.Batches.ConfiguredSize ||
		(metrics.Identity.Mode == constants.DeleteIdentityModeSingle) !=
			(metrics.Identity.ConfiguredBatchSize == 1) ||
		metrics.Identity.SelectionOrder != constants.DeleteSelectionOrderCanonical {
		return fmt.Errorf("DELETE result identity is incomplete or incompatible")
	}
	requestTerminal, ok := sumNonNegative(
		metrics.Requests.FullSuccess,
		metrics.Requests.Partial,
		metrics.Requests.Failed,
		metrics.Requests.Unresolved,
	)
	if !ok || metrics.Requests.Attempted != requestTerminal {
		return fmt.Errorf("DELETE request counters do not reconcile")
	}
	objectAttempted, ok := sumNonNegative(
		metrics.Objects.Accepted,
		metrics.Objects.Failed,
		metrics.Objects.Unresolved,
	)
	if !ok || metrics.Objects.Attempted != objectAttempted {
		return fmt.Errorf("DELETE attempted object counters do not reconcile")
	}
	objectSelected, ok := sumNonNegative(objectAttempted, metrics.Objects.Unattempted)
	if !ok || metrics.Objects.Selected != objectSelected {
		return fmt.Errorf("DELETE selected object counters do not reconcile")
	}
	if metrics.Batches.ActualRequestCount != metrics.Requests.Attempted ||
		metrics.Batches.ActualObjectCount != metrics.Objects.Attempted {
		return fmt.Errorf("DELETE batch counters do not match observed requests and objects")
	}
	batchRequests, ok := sumNonNegative(
		metrics.Batches.FullBatchCount,
		metrics.Batches.PartialBatchCount,
	)
	if !ok || batchRequests != metrics.Batches.ActualRequestCount {
		return fmt.Errorf("DELETE full and partial batch counters do not reconcile")
	}
	configuredSize := int64(metrics.Batches.ConfiguredSize)
	fullBatchObjects, fullObjectsOK := multiplyNonNegative(
		metrics.Batches.FullBatchCount, configuredSize)
	partialBatchMaxObjects, partialObjectsOK := multiplyNonNegative(
		metrics.Batches.PartialBatchCount, configuredSize-1)
	minimumBatchObjects, minimumOK := sumNonNegative(
		fullBatchObjects, metrics.Batches.PartialBatchCount)
	maximumBatchObjects, maximumOK := sumNonNegative(
		fullBatchObjects, partialBatchMaxObjects)
	if !fullObjectsOK || !partialObjectsOK || !minimumOK || !maximumOK ||
		metrics.Batches.ActualObjectCount < minimumBatchObjects ||
		metrics.Batches.ActualObjectCount > maximumBatchObjects {
		return fmt.Errorf("DELETE full and partial batch composition is impossible")
	}
	versionSelected, ok := sumNonNegative(metrics.Versions.CurrentKey, metrics.Versions.ExactVersion)
	if !ok || versionSelected != metrics.Objects.Selected {
		return fmt.Errorf("DELETE version counters do not match selected objects")
	}
	if !metrics.TerminalReconciled || !metrics.Completion.TerminalReconciled ||
		metrics.Completion.RequestPercent != 100 || metrics.Completion.ObjectPercent != 100 {
		return fmt.Errorf("DELETE terminal completion is not fully reconciled")
	}
	if !finiteNonNegative(metrics.Requests.PerSecond) || !finiteNonNegative(metrics.Objects.PerSecond) ||
		!finiteNonNegative(metrics.Batches.MeanObjectsPerRequest) ||
		!finiteNonNegative(metrics.Batches.FullBatchPercent) {
		return fmt.Errorf("DELETE derived rates are invalid")
	}
	expectedBatchMean := 0.0
	expectedFullBatchPercent := 0.0
	if metrics.Batches.ActualRequestCount > 0 {
		expectedBatchMean = float64(metrics.Batches.ActualObjectCount) /
			float64(metrics.Batches.ActualRequestCount)
		expectedFullBatchPercent = float64(metrics.Batches.FullBatchCount) * 100 /
			float64(metrics.Batches.ActualRequestCount)
	}
	if !equalDerived(metrics.Batches.MeanObjectsPerRequest, expectedBatchMean) ||
		!equalDerived(metrics.Batches.FullBatchPercent, expectedFullBatchPercent) {
		return fmt.Errorf("DELETE derived batch values do not reconcile")
	}
	policy := metrics.FailurePolicy
	policyFailures, ok := sumNonNegative(
		policy.OperationalFailedObjects, policy.ExcludedFailedObjects)
	if (policy.Mode != constants.DeleteFailurePolicyModeFixed &&
		policy.Mode != constants.DeleteFailurePolicyModePercentage) || !ok ||
		policyFailures != metrics.Objects.Failed || policy.MaxFailedObjects < 0 ||
		!finiteNonNegative(policy.MaxFailurePercent) || policy.MaxFailurePercent > 100 ||
		policy.GraceSeconds < 0 || !finiteNonNegative(policy.ObservedFailurePercent) ||
		policy.ObservedFailurePercent > 100 {
		return fmt.Errorf("DELETE failure policy is incomplete or inconsistent")
	}
	if (!allowRunningOutcome || policy.Outcome != OutcomeRunning) &&
		policy.Outcome != OutcomeCompletedCleanly &&
		policy.Outcome != OutcomeCompletedWithinFailureBudget &&
		policy.Outcome != OutcomeFailed {
		return fmt.Errorf("DELETE terminal failure-budget outcome is unavailable or invalid")
	}
	failureOutcomes, ok := sumNonNegative(metrics.Objects.Accepted, policy.OperationalFailedObjects)
	if !ok {
		return fmt.Errorf("DELETE failure policy outcomes overflow")
	}
	expectedFailurePercent := 0.0
	if failureOutcomes > 0 {
		expectedFailurePercent = float64(policy.OperationalFailedObjects) * 100 /
			float64(failureOutcomes)
	}
	if !equalDerived(policy.ObservedFailurePercent, expectedFailurePercent) {
		return fmt.Errorf("DELETE observed failure percentage does not reconcile")
	}
	if err := validateSuccessfulFailureBudgetOutcome(metrics, expectedFailurePercent); err != nil {
		return err
	}
	if len(metrics.Buckets) > MaxBucketMetrics+1 {
		return fmt.Errorf("DELETE bucket metrics exceed the bounded cardinality")
	}
	if len(metrics.Buckets) > MaxBucketMetrics {
		hasOverflow := false
		for _, bucket := range metrics.Buckets {
			hasOverflow = hasOverflow || bucket.Bucket == OverflowBucket
		}
		if !hasOverflow {
			return fmt.Errorf("DELETE bucket metrics exceed the bounded named cardinality")
		}
	}
	seenBuckets := make(map[string]struct{}, len(metrics.Buckets))
	var bucketSelected, bucketAttempted, bucketAccepted, bucketFailed int64
	for _, bucket := range metrics.Buckets {
		if bucket.Bucket == "" {
			return fmt.Errorf("DELETE bucket identity is empty")
		}
		if _, duplicate := seenBuckets[bucket.Bucket]; duplicate {
			return fmt.Errorf("DELETE bucket identity %q is duplicated", bucket.Bucket)
		}
		seenBuckets[bucket.Bucket] = struct{}{}
		bucketTerminal, lifecycleOK := sumNonNegative(bucket.Accepted, bucket.Failed)
		if !lifecycleOK || bucket.Attempted < 0 || bucket.Selected < 0 ||
			bucket.Attempted > bucket.Selected || bucketTerminal > bucket.Attempted {
			return fmt.Errorf("DELETE bucket %q lifecycle counters are impossible", bucket.Bucket)
		}
		var valid bool
		bucketSelected, valid = addNonNegative(bucketSelected, bucket.Selected)
		if !valid {
			return fmt.Errorf("DELETE bucket selected counters are invalid")
		}
		bucketAttempted, valid = addNonNegative(bucketAttempted, bucket.Attempted)
		if !valid {
			return fmt.Errorf("DELETE bucket attempted counters are invalid")
		}
		bucketAccepted, valid = addNonNegative(bucketAccepted, bucket.Accepted)
		if !valid {
			return fmt.Errorf("DELETE bucket accepted counters are invalid")
		}
		bucketFailed, valid = addNonNegative(bucketFailed, bucket.Failed)
		if !valid {
			return fmt.Errorf("DELETE bucket failed counters are invalid")
		}
	}
	if bucketSelected != metrics.Objects.Selected || bucketAttempted != metrics.Objects.Attempted ||
		bucketAccepted != metrics.Objects.Accepted || bucketFailed != metrics.Objects.Failed {
		return fmt.Errorf("DELETE bucket counters do not match global object counters")
	}
	if metrics.Timing.LatencyDefinition != LatencyDefinition ||
		metrics.Timing.DurationDefinition != DurationDefinition ||
		metrics.Timing.Latency == nil || metrics.Timing.Duration == nil {
		return fmt.Errorf("DELETE timing definitions or populations are incomplete")
	}
	if err := validateTimingStat("latency", metrics.Timing.Latency); err != nil {
		return err
	}
	if err := validateTimingStat("duration", metrics.Timing.Duration); err != nil {
		return err
	}
	timedTerminal, ok := sumNonNegative(
		metrics.Requests.FullSuccess,
		metrics.Requests.Partial,
		metrics.Requests.Failed,
	)
	responseBacked, responseBackedOK := sumNonNegative(
		metrics.Requests.FullSuccess,
		metrics.Requests.Partial,
	)
	if !ok || !responseBackedOK ||
		metrics.Timing.Latency.Count < responseBacked ||
		metrics.Timing.Duration.Count < responseBacked ||
		metrics.Timing.Latency.Count > metrics.Timing.Duration.Count ||
		metrics.Timing.Duration.Count > timedTerminal {
		return fmt.Errorf("DELETE timing sample counts do not match the available terminal population")
	}
	if metrics.Timing.ObjectLatency != nil {
		return fmt.Errorf("DELETE object latency must remain unavailable")
	}
	if metrics.Performance.ObjectSize != NotApplicable || metrics.Performance.DataMoved != NotApplicable ||
		metrics.Performance.Bandwidth != NotApplicable || metrics.Performance.TTFB != NotApplicable {
		return fmt.Errorf("DELETE transfer performance applicability is invalid")
	}
	if metrics.OutcomeTerminology != OutcomeAccepted {
		return fmt.Errorf("DELETE outcome terminology is invalid")
	}
	if !metrics.Verification.Enabled &&
		(metrics.Verification.RemovalConfirmed || metrics.Verification.Notice != VerificationNotice) {
		return fmt.Errorf("DELETE verification state is inconsistent")
	}
	for _, phase := range []*float64{
		metrics.Phases.SeedSeconds,
		metrics.Phases.DiscoverySeconds,
		metrics.Phases.PreValidationSeconds,
		metrics.Phases.ScheduledDeleteSeconds,
		metrics.Phases.DrainSeconds,
		metrics.Phases.PostVerificationSeconds,
		metrics.Phases.CleanupSeconds,
		metrics.Phases.TotalWallSeconds,
	} {
		if phase != nil && !finiteNonNegative(*phase) {
			return fmt.Errorf("DELETE phase duration is invalid")
		}
	}
	if metrics.Phases.ScheduledDeleteSeconds == nil || metrics.Phases.DrainSeconds == nil ||
		metrics.Phases.TotalWallSeconds == nil {
		return fmt.Errorf("DELETE required terminal phase duration is unavailable")
	}
	if metrics.Phases.PreValidationSeconds != nil {
		return fmt.Errorf("DELETE pre-validation duration must remain unavailable")
	}
	if *metrics.Phases.TotalWallSeconds < *metrics.Phases.ScheduledDeleteSeconds ||
		*metrics.Phases.TotalWallSeconds < *metrics.Phases.DrainSeconds {
		return fmt.Errorf("DELETE total wall duration does not contain terminal phases")
	}
	return nil
}

func validateSuccessfulFailureBudgetOutcome(metrics *Metrics, observedFailurePercent float64) error {
	policy := metrics.FailurePolicy
	if policy.Outcome != OutcomeCompletedCleanly &&
		policy.Outcome != OutcomeCompletedWithinFailureBudget {
		return nil
	}
	if err := validateSuccessfulLifecycle(metrics); err != nil {
		return err
	}
	if failureBudgetExceeded(policy, observedFailurePercent) {
		return fmt.Errorf("DELETE successful outcome exceeds the configured failure budget")
	}
	if policy.Outcome == OutcomeCompletedCleanly && policy.OperationalFailedObjects != 0 {
		return fmt.Errorf("DELETE clean outcome contains operational failures")
	}
	if policy.Outcome == OutcomeCompletedWithinFailureBudget && policy.OperationalFailedObjects == 0 {
		return fmt.Errorf("DELETE within-budget outcome has no operational failures")
	}
	return nil
}

func validateSuccessfulLifecycle(metrics *Metrics) error {
	policy := metrics.FailurePolicy
	if policy.ExcludedFailedObjects > 0 ||
		metrics.Requests.Unresolved > 0 || metrics.Objects.Unresolved > 0 {
		return fmt.Errorf("DELETE successful outcome contains excluded or unresolved failures")
	}
	if metrics.Requests.FullSuccess == 0 || metrics.Objects.Accepted == 0 {
		return fmt.Errorf("DELETE successful outcome requires a fully successful request and accepted object")
	}
	responseBackedRequests := metrics.Requests.FullSuccess + metrics.Requests.Partial
	failureClassifiedRequests := metrics.Requests.Partial + metrics.Requests.Failed
	if (responseBackedRequests > 0) != (metrics.Objects.Accepted > 0) ||
		(failureClassifiedRequests > 0) != (metrics.Objects.Failed > 0) {
		return fmt.Errorf("DELETE successful request and object classifications are contradictory")
	}
	return nil
}

func failureBudgetExceeded(policy FailurePolicy, observedFailurePercent float64) bool {
	if policy.Mode == constants.DeleteFailurePolicyModePercentage {
		return observedFailurePercent > policy.MaxFailurePercent
	}
	return policy.OperationalFailedObjects > policy.MaxFailedObjects
}

func multiplyNonNegative(value, factor int64) (int64, bool) {
	if value < 0 || factor < 0 || value > 0 && factor > math.MaxInt64/value {
		return 0, false
	}
	return value * factor, true
}

func validateTimingStat(name string, stat *TimingStat) error {
	if stat.Count < 0 || stat.OverflowCount < 0 || stat.OverflowCount > stat.Count ||
		!finiteNonNegative(stat.MeanUs) || stat.MinUs < 0 || stat.P50Us < 0 ||
		stat.P90Us < 0 || stat.P99Us < 0 || stat.P999Us < 0 || stat.MaxUs < 0 {
		return fmt.Errorf("DELETE %s timing distribution is invalid", name)
	}
	if stat.Count == 0 {
		if stat.MeanUs != 0 || stat.MinUs != 0 || stat.P50Us != 0 || stat.P90Us != 0 ||
			stat.P99Us != 0 || stat.P999Us != 0 || stat.MaxUs != 0 {
			return fmt.Errorf("DELETE %s timing distribution has values without samples", name)
		}
		return nil
	}
	if stat.MinUs > stat.P50Us || stat.P50Us > stat.P90Us || stat.P90Us > stat.P99Us ||
		stat.P99Us > stat.P999Us || stat.P999Us > stat.MaxUs ||
		stat.MeanUs < float64(stat.MinUs) || stat.MeanUs > float64(stat.MaxUs) {
		return fmt.Errorf("DELETE %s timing distribution is not ordered", name)
	}
	return nil
}

func finiteNonNegative(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func equalDerived(actual, expected float64) bool {
	if !finiteNonNegative(actual) || !finiteNonNegative(expected) {
		return false
	}
	scale := math.Max(1, math.Max(math.Abs(actual), math.Abs(expected)))
	return math.Abs(actual-expected) <= 1e-9*scale
}

func sumNonNegative(values ...int64) (int64, bool) {
	var total int64
	for _, value := range values {
		var ok bool
		total, ok = addNonNegative(total, value)
		if !ok {
			return 0, false
		}
	}
	return total, true
}

func addNonNegative(left, right int64) (int64, bool) {
	const maxInt64 = int64(^uint64(0) >> 1)
	if left < 0 || right < 0 || right > maxInt64-left {
		return 0, false
	}
	return left + right, true
}
