package com.dell.spt.base.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/** Created by kurila on 05.05.17. */
public interface Loggers {

	String BASE = Loggers.class.getPackage().getName() + '.';
	String BASE_METRICS = BASE + "metrics.";
	String BASE_METRICS_THRESHOLD = BASE_METRICS + "threshold.";
	Logger CLI = LogManager.getLogger(BASE + "Cli");
	Logger CONFIG = LogManager.getLogger(BASE + "Config");
	Logger DELETE_COMPLETION = LogManager.getLogger(BASE + "DeleteCompletion");
	Logger DELETE_METRICS_TOTAL = LogManager.getLogger(BASE + "DeleteMetricsTotal");
	Logger DELETE_OBJECTS = LogManager.getLogger(BASE + "DeleteObjects");
	Logger DELETE_REQUESTS = LogManager.getLogger(BASE + "DeleteRequests");
	Logger DELETE_RESIDUAL = LogManager.getLogger(BASE + "DeleteResidual");
	Logger DELETE_VERIFICATION = LogManager.getLogger(BASE + "DeleteVerification");
	Logger DELETE_SELECTION = LogManager.getLogger(BASE + "DeleteSelection");
	Logger DELETE_SELECTION_COMPLETION = LogManager.getLogger(BASE + "DeleteSelectionCompletion");
	Logger ERR = LogManager.getLogger(BASE + "Errors");
	Logger INTEGRITY_FAILURES = LogManager.getLogger(BASE + "IntegrityFailures");
	Logger INTEGRITY_PERFORMANCE = LogManager.getLogger(BASE + "IntegrityPerformance");
	Logger OP_TRACES = LogManager.getLogger(BASE + "OpTraces");
	Logger METRICS_EXT_RESULTS_FILE = LogManager.getLogger(BASE_METRICS + "ExtResultsFile");
	Logger METRICS_FILE = LogManager.getLogger(BASE_METRICS + "File");
	Logger METRICS_FILE_TOTAL = LogManager.getLogger(BASE_METRICS + "FileTotal");
	Logger METRICS_STD_OUT = LogManager.getLogger(BASE_METRICS + "StdOut");
	Logger METRICS_THRESHOLD_EXT_RESULTS_FILE = LogManager.getLogger(BASE_METRICS_THRESHOLD + "ExtResultsFile");
	Logger METRICS_THRESHOLD_FILE_TOTAL = LogManager.getLogger(BASE_METRICS_THRESHOLD + "FileTotal");
	Logger MSG = LogManager.getLogger(BASE + "Messages");
	Logger MULTIPART = LogManager.getLogger(BASE + "Multipart");
	Logger OPERATION_LIFECYCLE = LogManager.getLogger(BASE + "OperationLifecycle");
	Logger MULTIPART_LIFECYCLE = LogManager.getLogger(BASE + "MultipartLifecycle");
	Logger TABLES_METRICS = LogManager.getLogger(BASE + "TablesMetrics");
	Logger SCENARIO = LogManager.getLogger(BASE + "Scenario");
	Logger TEST = LogManager.getLogger(BASE + "Test");

	Map<String, String> DESCRIPTIONS_BY_NAME = Map.ofEntries(
					Map.entry(CLI.getName().substring(BASE.length()), "CLI args"),
					Map.entry(CONFIG.getName().substring(BASE.length()), "Base config"),
					Map.entry(DELETE_COMPLETION.getName().substring(BASE.length()), "DELETE Artifact Completion"),
					Map.entry(DELETE_METRICS_TOTAL.getName().substring(BASE.length()), "DELETE Metrics Total v1"),
					Map.entry(DELETE_OBJECTS.getName().substring(BASE.length()), "DELETE Target Reconciliation v1"),
					Map.entry(DELETE_REQUESTS.getName().substring(BASE.length()), "DELETE Request Trace v1"),
					Map.entry(DELETE_RESIDUAL.getName().substring(BASE.length()), "DELETE Residual Inventory"),
					Map.entry(
									DELETE_VERIFICATION.getName().substring(BASE.length()),
									"DELETE Verification Evidence v1"),
					Map.entry(DELETE_SELECTION.getName().substring(BASE.length()), "DELETE Frozen Selection"),
					Map.entry(
									DELETE_SELECTION_COMPLETION.getName().substring(BASE.length()),
									"DELETE Selection Provenance"),
					Map.entry(ERR.getName().substring(BASE.length()), "Errors"),
					Map.entry(INTEGRITY_FAILURES.getName().substring(BASE.length()), "Integrity Failures"),
					Map.entry(INTEGRITY_PERFORMANCE.getName().substring(BASE.length()), "Integrity Performance"),
					Map.entry(OP_TRACES.getName().substring(BASE.length()), "Operation Traces"),
					Map.entry(METRICS_EXT_RESULTS_FILE.getName().substring(BASE.length()), "Ext Results XML"),
					Map.entry(METRICS_FILE.getName().substring(BASE.length()), "Metrics"),
					Map.entry(METRICS_FILE_TOTAL.getName().substring(BASE.length()), "Metrics Total"),
					Map.entry(
									METRICS_THRESHOLD_EXT_RESULTS_FILE.getName().substring(BASE.length()),
									"Threshold Ext Results XML"),
					Map.entry(
									METRICS_THRESHOLD_FILE_TOTAL.getName().substring(BASE.length()),
									"Threshold Metrics Total"),
					Map.entry(MSG.getName().substring(BASE.length()), "Messages"),
					Map.entry(MULTIPART.getName().substring(BASE.length()), "Multipart Upload Phases"),
					Map.entry(OPERATION_LIFECYCLE.getName().substring(BASE.length()), "Terminal Operation Lifecycle v1"),
					Map.entry(MULTIPART_LIFECYCLE.getName().substring(BASE.length()), "Multipart Upload Lifecycle"),
					Map.entry(SCENARIO.getName().substring(BASE.length()), "Scenario"),
					Map.entry(TABLES_METRICS.getName().substring(BASE.length()), "S3 Tables Metrics"));
}
