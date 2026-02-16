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
	Logger ERR = LogManager.getLogger(BASE + "Errors");
	Logger OP_TRACES = LogManager.getLogger(BASE + "OpTraces");
	Logger METRICS_FILE = LogManager.getLogger(BASE_METRICS + "File");
	Logger METRICS_FILE_TOTAL = LogManager.getLogger(BASE_METRICS + "FileTotal");
	Logger METRICS_STD_OUT = LogManager.getLogger(BASE_METRICS + "StdOut");
	Logger METRICS_THRESHOLD_FILE_TOTAL = LogManager.getLogger(BASE_METRICS_THRESHOLD + "FileTotal");
	Logger MSG = LogManager.getLogger(BASE + "Messages");
	Logger MULTIPART = LogManager.getLogger(BASE + "Multipart");
	Logger ITEMS = LogManager.getLogger(BASE + "Items");
	Logger SCENARIO = LogManager.getLogger(BASE + "Scenario");
	Logger TEST = LogManager.getLogger(BASE + "Test");

	Map<String, String> DESCRIPTIONS_BY_NAME = Map.ofEntries(
					Map.entry(CLI.getName().substring(BASE.length()), "CLI args"),
					Map.entry(CONFIG.getName().substring(BASE.length()), "Base config"),
					Map.entry(ERR.getName().substring(BASE.length()), "Errors"),
					Map.entry(OP_TRACES.getName().substring(BASE.length()), "Operation Traces"),
					Map.entry(METRICS_FILE.getName().substring(BASE.length()), "Metrics"),
					Map.entry(METRICS_FILE_TOTAL.getName().substring(BASE.length()), "Metrics Total"),
					Map.entry(
									METRICS_THRESHOLD_FILE_TOTAL.getName().substring(BASE.length()),
									"Threshold Metrics Total"),
					Map.entry(ITEMS.getName().substring(BASE.length()), "Item Inventory"),
					Map.entry(MSG.getName().substring(BASE.length()), "Messages"),
					Map.entry(SCENARIO.getName().substring(BASE.length()), "Scenario"));
}
