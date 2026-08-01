package com.dell.spt.base.integrity;

import org.apache.commons.csv.CSVFormat;

/** Shared byte-level format for canonical integrity CSV artifacts. */
public final class IntegrityCsvFormat {

	public static final CSVFormat RFC4180_LF = CSVFormat.RFC4180.builder()
					.setRecordSeparator("\n")
					.get();

	private IntegrityCsvFormat() {}
}
