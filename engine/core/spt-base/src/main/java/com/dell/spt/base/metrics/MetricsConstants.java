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
	//
	String METADATA_STEP_ID = "load_step_id";
	String METADATA_OP_TYPE = "load_op_type";
	String METADATA_LIMIT_CONC = "storage_driver_limit_concurrency";
	String METADATA_ITEM_DATA_SIZE = "item_data_size";
	String METADATA_START_TIME = "start_time";
	String METADATA_NODE_LIST = "node_list";
	String METADATA_COMMENT = "user_comment";
	String METADATA_RUN_ID = "run_id";
	// Optional fields used for progress calculation
	String METADATA_LIMIT_OP_COUNT = "load_op_limit_count"; // long, 0 or missing means unlimited
	String METADATA_LIMIT_TIME_SEC = "load_step_limit_time_sec"; // long, seconds; 0 or missing means unlimited
	String METADATA_LIST_SHARD_METRICS = "list_shard_metrics";
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
