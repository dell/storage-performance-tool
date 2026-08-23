package deletemetrics

import "testing"

func TestValidateTerminalRejectsControllerImpossibleSuccessOutcomes(t *testing.T) {
	tests := map[string]func(*Metrics){
		"excluded failure": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 1, 1, 0, 1, 0)
			metrics.FailurePolicy.ExcludedFailedObjects = 1
			metrics.FailurePolicy.Outcome = OutcomeCompletedCleanly
		},
		"unresolved outcome": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 1, 1, 0, 0, 1)
			metrics.FailurePolicy.Outcome = OutcomeCompletedCleanly
		},
		"zero successful request and accepted object": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 0, 0, 1, 0, 0)
			metrics.FailurePolicy.MaxFailedObjects = 1
			metrics.FailurePolicy.OperationalFailedObjects = 1
			metrics.FailurePolicy.ObservedFailurePercent = 100
			metrics.FailurePolicy.Outcome = OutcomeCompletedWithinFailureBudget
		},
		"clean outcome with operational failure": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 1, 1, 1, 0, 0)
			metrics.FailurePolicy.MaxFailedObjects = 1
			metrics.FailurePolicy.OperationalFailedObjects = 1
			metrics.FailurePolicy.ObservedFailurePercent = 50
			metrics.FailurePolicy.Outcome = OutcomeCompletedCleanly
		},
		"within fixed budget above threshold": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 1, 1, 1, 0, 0)
			metrics.FailurePolicy.MaxFailedObjects = 0
			metrics.FailurePolicy.OperationalFailedObjects = 1
			metrics.FailurePolicy.ObservedFailurePercent = 50
			metrics.FailurePolicy.Outcome = OutcomeCompletedWithinFailureBudget
		},
		"within percentage budget above threshold": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 1, 1, 1, 0, 0)
			metrics.FailurePolicy.Mode = "percentage"
			metrics.FailurePolicy.MaxFailurePercent = 49.9
			metrics.FailurePolicy.OperationalFailedObjects = 1
			metrics.FailurePolicy.ObservedFailurePercent = 50
			metrics.FailurePolicy.Outcome = OutcomeCompletedWithinFailureBudget
		},
		"within budget without operational failure": func(metrics *Metrics) {
			metrics.FailurePolicy.Outcome = OutcomeCompletedWithinFailureBudget
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			metrics := validTerminalMetrics()
			mutate(metrics)
			if err := ValidateTerminal(metrics); err == nil {
				t.Fatal("controller-impossible successful outcome was accepted")
			}

			metrics.FailurePolicy.Outcome = OutcomeFailed
			if err := ValidateTerminal(metrics); err != nil {
				t.Fatalf("valid sticky/external failed outcome was rejected: %v", err)
			}
		})
	}
}

func TestValidateTerminalRejectsContradictorySuccessfulRequestObjectClassifications(t *testing.T) {
	tests := map[string]func(*Metrics){
		"failed request without failed object": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 2, 2, 0, 0, 0)
			metrics.Requests.FullSuccess = 1
			metrics.Requests.Failed = 1
		},
		"failed object with only full-success requests": func(metrics *Metrics) {
			setTerminalLifecycle(metrics, 1, 1, 1, 0, 0)
			metrics.Requests.FullSuccess = 2
			metrics.Requests.Failed = 0
			metrics.FailurePolicy.MaxFailedObjects = 1
			metrics.FailurePolicy.OperationalFailedObjects = 1
			metrics.FailurePolicy.ObservedFailurePercent = 50
			metrics.FailurePolicy.Outcome = OutcomeCompletedWithinFailureBudget
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			metrics := validTerminalMetrics()
			mutate(metrics)
			if err := ValidateTerminal(metrics); err == nil {
				t.Fatal("contradictory successful request/object classifications were accepted")
			}
		})
	}
}

func TestValidateTerminalRequiresTimingForResponseBackedSuccessButAllowsPreResponseFailure(t *testing.T) {
	missingSuccessTiming := validTerminalMetrics()
	missingSuccessTiming.Timing.Latency = validTimingStat(0)
	missingSuccessTiming.Timing.Duration = validTimingStat(0)
	if err := ValidateTerminal(missingSuccessTiming); err == nil {
		t.Fatal("successful response-backed request was accepted without timing evidence")
	}

	preResponseFailure := validTerminalMetrics()
	setTerminalLifecycle(preResponseFailure, 0, 0, 1, 0, 0)
	preResponseFailure.FailurePolicy.MaxFailedObjects = 1
	preResponseFailure.FailurePolicy.OperationalFailedObjects = 1
	preResponseFailure.FailurePolicy.ObservedFailurePercent = 100
	preResponseFailure.FailurePolicy.Outcome = OutcomeFailed
	if err := ValidateTerminal(preResponseFailure); err != nil {
		t.Fatalf("pre-response failure without timing evidence was rejected: %v", err)
	}
}

func validTerminalMetrics() *Metrics {
	scheduled := 1.0
	drain := 0.25
	total := 1.25
	return &Metrics{
		Units:      Units{Requests: RequestUnit, Objects: ObjectUnit, Batches: RequestUnit},
		Requests:   Requests{Attempted: 1, FullSuccess: 1, PerSecond: 1},
		Objects:    Objects{Selected: 1, Attempted: 1, Accepted: 1, PerSecond: 1},
		Batches:    Batches{ConfiguredSize: 1, ActualRequestCount: 1, ActualObjectCount: 1, MeanObjectsPerRequest: 1, FullBatchCount: 1, FullBatchPercent: 100},
		Completion: Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
		Versions:   Versions{CurrentKey: 1},
		Buckets:    []Bucket{{Bucket: "bucket-a", Selected: 1, Attempted: 1, Accepted: 1}},
		Phases: Phases{
			ScheduledDeleteSeconds: &scheduled,
			DrainSeconds:           &drain,
			TotalWallSeconds:       &total,
		},
		Identity: Identity{Mode: "single", ConfiguredBatchSize: 1, SelectionOrder: "canonical"},
		FailurePolicy: FailurePolicy{
			Mode: "fixed", Outcome: OutcomeCompletedCleanly, MaxFailedObjects: 0,
		},
		Timing: Timing{
			LatencyDefinition: LatencyDefinition, DurationDefinition: DurationDefinition,
			Latency: validTimingStat(1), Duration: validTimingStat(1),
		},
		Performance: Performance{
			ObjectSize: NotApplicable, DataMoved: NotApplicable,
			Bandwidth: NotApplicable, TTFB: NotApplicable,
		},
		OutcomeTerminology: OutcomeAccepted,
		Verification: Verification{
			Notice: VerificationNotice,
		},
		TerminalReconciled: true,
	}
}

func setTerminalLifecycle(
	metrics *Metrics,
	fullSuccess, accepted, operationalFailed, excludedFailed, unresolved int64,
) {
	failed := operationalFailed + excludedFailed
	requestsFailed := int64(0)
	if failed > 0 {
		requestsFailed = 1
	}
	requestsUnresolved := int64(0)
	if unresolved > 0 {
		requestsUnresolved = 1
	}
	requests := fullSuccess + requestsFailed + requestsUnresolved
	objects := accepted + failed + unresolved
	metrics.Requests = Requests{
		Attempted: requests, FullSuccess: fullSuccess, Failed: requestsFailed,
		Unresolved: requestsUnresolved, PerSecond: float64(requests),
	}
	metrics.Objects = Objects{
		Selected: objects, Attempted: objects, Accepted: accepted,
		Failed: failed, Unresolved: unresolved, PerSecond: float64(objects),
	}
	metrics.Batches = Batches{
		ConfiguredSize: 1, ActualRequestCount: requests, ActualObjectCount: objects,
		MeanObjectsPerRequest: float64(objects) / float64(requests),
		FullBatchCount:        requests, FullBatchPercent: 100,
	}
	metrics.Versions = Versions{CurrentKey: objects}
	metrics.Buckets = []Bucket{{
		Bucket: "bucket-a", Selected: objects, Attempted: objects,
		Accepted: accepted, Failed: failed,
	}}
	timedRequests := fullSuccess + requestsFailed
	metrics.Timing.Latency = validTimingStat(timedRequests)
	metrics.Timing.Duration = validTimingStat(timedRequests)
}

func validTimingStat(count int64) *TimingStat {
	if count == 0 {
		return &TimingStat{}
	}
	return &TimingStat{
		Count: count, MeanUs: 1, MinUs: 1, P50Us: 1, P90Us: 1,
		P99Us: 1, P999Us: 1, MaxUs: 1,
	}
}
