package com.dell.spt.base.metrics;

import com.dell.spt.base.Constants;
import java.util.List;

public interface MetricsConstants {

	String METRIC_NAME_DUR = "duration";
	String METRIC_NAME_LAT = "latency";
	String METRIC_NAME_TTFB = "ttfb";
	String METRIC_NAME_CONC = "concurrency";
	String METRIC_NAME_SUCC = "success_op";
	String METRIC_NAME_FAIL = "failed_op";
	String METRIC_NAME_CORRUPT = "corrupt_op";
	String METRIC_NAME_BYTE = "byte";
	String METRIC_NAME_TIME = "elapsed_time";
	String METRIC_NAME_TEST_STATE = "test_state";
	String METRIC_NAME_COMPLETION = "completion_percent";
	String METRIC_NAME_STEP_COMPLETION = "step_completion_percent";
	String METRIC_NAME_DELETE_DETAIL_EXPECTED = "delete_detail_expected";
	String METRIC_NAME_CONTRIBUTORS_PRESENT = "contributors_present";

	/** Unit for logical DELETE API request and batch counters. */
	String DELETE_REQUEST_UNIT = "logical_api_requests";

	/** Unit for DELETE target counters. */
	String DELETE_OBJECT_UNIT = "object_identities";

	/** Machine-readable marker for byte-oriented metrics that do not apply to DELETE. */
	String DELETE_NOT_APPLICABLE = "not_applicable";

	/** Canonical term for a DELETE target accepted by the storage API. */
	String DELETE_OUTCOME_ACCEPTED = "accepted";

	/** Identity mode for one target per logical DELETE request. */
	String DELETE_IDENTITY_MODE_SINGLE = "single";

	/** Identity mode for multiple targets per logical DELETE request. */
	String DELETE_IDENTITY_MODE_BATCH = "batch";

	/** Deterministic global DELETE target selection order. */
	String DELETE_SELECTION_ORDER_CANONICAL = "canonical";

	/** Fixed failed-object failure-budget policy. */
	String DELETE_FAILURE_POLICY_MODE_FIXED = "fixed";

	/** Cumulative object-outcome percentage failure-budget policy. */
	String DELETE_FAILURE_POLICY_MODE_PERCENTAGE = "percentage";

	/** Live failure-budget decision before terminal controller evaluation. */
	String DELETE_FAILURE_OUTCOME_RUNNING = "running";

	/** Terminal failure-budget decision with no operational object failures. */
	String DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY = "completed_cleanly";

	/** Terminal success containing operational object failures allowed by policy. */
	String DELETE_FAILURE_OUTCOME_COMPLETED_WITHIN_BUDGET = "completed_within_failure_budget";

	/** Terminal failure-budget decision for an invalid or failed run. */
	String DELETE_FAILURE_OUTCOME_FAILED = "failed";

	/** Request-latency definition tied to driver instrumentation markers. */
	String DELETE_LATENCY_DEFINITION = "first_request_byte_sent_to_first_response_byte_received";

	/** Request-duration definition tied to driver instrumentation markers. */
	String DELETE_DURATION_DEFINITION = "request_formulation_to_last_response_byte_received";

	/** Human-readable request-latency definition. */
	String DELETE_LATENCY_DEFINITION_DISPLAY = "first request byte sent to first response byte received";

	/** Human-readable request-duration definition. */
	String DELETE_DURATION_DEFINITION_DISPLAY = "request formulation to last response byte received";

	/** Source of the request timing boundaries. */
	String DELETE_TIMING_MARKER_SOURCE = "driver transport instrumentation boundaries";

	/** Canonical contributor identifier for the entry JVM's local load-step slice. */
	String FLEET_LOCAL_CONTRIBUTOR_ID = "local";

	/** Required notice when removal verification is disabled. */
	String DELETE_VERIFICATION_NOTICE = "Verification disabled; results describe logical DELETE API outcomes, "
					+ "not confirmed object removal.";
	//
	String METADATA_STEP_ID = "load_step_id";
	String METADATA_OP_TYPE = "load_op_type";
	String METADATA_LIMIT_CONC = "storage_driver_limit_concurrency";
	String METADATA_ITEM_DATA_SIZE = "item_data_size";
	String METADATA_START_TIME = "start_time";
	String METADATA_NODE_LIST = "node_list";
	String METADATA_CONTRIBUTOR_IDS = "contributor_ids";
	String METADATA_COMMENT = "user_comment";
	String METADATA_RUN_ID = "run_id";
	// Optional fields used for progress calculation
	String METADATA_LIMIT_OP_COUNT = "load_op_limit_count"; // long, 0 or missing means unlimited
	String METADATA_LIMIT_TIME_SEC = "load_step_limit_time_sec"; // long, seconds; 0 or missing means unlimited
	String METADATA_LIST_SHARD_METRICS = "list_shard_metrics";
	/** Metadata key for the optional detailed DELETE metrics snapshot. */
	String METADATA_DELETE_METRICS = "delete_metrics";
	/** Metadata key for the controller-owned DELETE failure-budget outcome. */
	String METADATA_DELETE_FAILURE_OUTCOME = "delete_failure_outcome";
	//
	List<String> METRIC_LABELS = List.of(
					METADATA_STEP_ID,
					METADATA_OP_TYPE,
					METADATA_LIMIT_CONC,
					METADATA_ITEM_DATA_SIZE,
					METADATA_START_TIME,
					METADATA_NODE_LIST,
					METADATA_COMMENT,
					METADATA_RUN_ID);

	static String[] metricLabelsArray() {
		return METRIC_LABELS.toArray(new String[0]);
	}

	String METRIC_FORMAT = Constants.APP_NAME + "_%s"; // appName_metricName<_aggregationType>
}
