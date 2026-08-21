package com.dell.spt.base;

import java.io.File;
import java.util.Locale;

/** Created on 11.07.16. */
public interface Constants {

	String APP_NAME = "spt";
	String DIR_CONFIG = "config";
	String DIR_EXAMPLE = "example";
	String DIR_EXT = "ext";
	String DIR_EXAMPLE_SCENARIO = DIR_EXAMPLE + File.separator + "scenario";
	String PATH_DEFAULTS = DIR_CONFIG + "/" + "defaults.yaml";
	String KEY_HOME_DIR = "home_dir";
	String KEY_STEP_ID = "step_id";
	String KEY_CLASS_NAME = "class_name";
	long LIFECYCLE_POLL_INTERVAL_MILLIS = 1L;
	long TASK_STOP_WAIT_SECONDS = 1L;
	//
	int MIB = 0x10_00_00;
	double K = 1e3;
	double M = 1e6;
	String UNIT_MIB_PER_SECOND = "MiB/s";
	String UNIT_MIB_PER_SECOND_XML = "MiBps";
	String COLUMN_BW_AVG_MIB_PER_SECOND = "BWAvg[" + UNIT_MIB_PER_SECOND + "]";
	String COLUMN_BW_LAST_MIB_PER_SECOND = "BWLast[" + UNIT_MIB_PER_SECOND + "]";
	Locale LOCALE_DEFAULT = Locale.ROOT;
}
