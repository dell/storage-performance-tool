package server

// JSON keys and values shared by several handlers.
const (
	keyError             = "error"
	keyRunID             = "run_id"
	keyStatus            = "status"
	keyMessage           = "message"
	keyJob               = "job"
	keyOptions           = "options"
	keyPhase             = "phase"
	keyThreads           = "threads"
	keyMax               = "max"
	keyMaxRuntimeSeconds = "max_runtime_seconds"
	keyElapsedSeconds    = "elapsed_seconds"
	keyIsActive          = "is_active"
	keySamples           = "samples"
	keyChartSamples      = "chart_samples"
	keyTimestamp         = "timestamp"
	keyCountFailed       = "count_failed"
	keyCumulativeOps     = "cumulative_ops"
	keyIsAggregate       = "is_aggregate"
	keyMin               = "min"
	keyStartedAt         = "started_at"
	keyUpdatedAt         = "updated_at"
	keyWorkloadType      = "workload_type"
	keySourceFile        = "source_file"
	keyIntervalIOPS      = "interval_iops"

	metricBandwidth  = "bandwidth_mib_s"
	metricP50Latency = "p50_latency_us"

	driverWriteVerify = "write-verify"

	// History chart query kind (the "kind" parameter).
	kindWindow = "window"

	// SPT phase codes as the history and live APIs report them, and the
	// verify phase's display name.
	phaseCreate      = "CREATE"
	phaseVerify      = "VERIFY"
	phaseDelete      = "DELETE"
	phaseVerifyLabel = "Verify"

	// Storage verification states.
	verifyPassed  = "passed"
	verifyFailed  = "failed"
	verifyPending = "pending"
	verifyRunning = "running"

	msgEndpointNotFound = "Endpoint not found."
	msgSavedRunNotFound = "Saved run not found."

	// Keys and labels used by the mock driver, per-machine live metrics, the
	// results catalog, run comparison and run log views.
	keyRuns            = "runs"
	keyRun             = "run"
	keyTruncated       = "truncated"
	keyObjectCount     = "object_count"
	keyResultsDir      = "results_dir"
	keyReturnCode      = "returncode"
	keyOpType          = "op_type"
	keyStepID          = "step_id"
	keySampleTS        = "sample_ts"
	keyStartedEpoch    = "started_epoch"
	keyProgressPercent = "progress_percent"
	keySuccessfulOps   = "successful_ops"
	keyPhaseSummaries  = "phase_summaries"
	keyFailed          = "failed"
	keyHost            = "host"
	phaseRead          = "READ"
	metricIOPS         = "iops"
	metricFailed       = "failed_ops"
	metricCorrupt      = "corrupt_ops"
	metricP99Latency   = "p99_latency_us"
	// sptP99Latency is the name SPT itself uses in its metrics files.
	sptP99Latency = "latency_p99_us"
	// sourceDashboard is the --label of every dashboard run.
	sourceDashboard = "dashboard"

	flagRegressed = "regressed"
	flagWithin    = "within"

	integrityPassed = "passed"
	integrityFailed = "failed"

	kindPhase        = "phase"
	logSourceConsole = "console"

	levelInfo  = "info"
	levelWarn  = "warn"
	levelError = "error"
)
