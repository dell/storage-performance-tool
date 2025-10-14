package com.dell.spt.storage.driver.coop.netty.http;

/**
Created by kurila on 11.11.16.
*/
public interface DellConstants {

	String PREFIX_KEY_X_Dell = "x-dell-";
	String KEY_X_Dell_MULTIPART_COPY = PREFIX_KEY_X_Dell + "multipart-copy";
	String KEY_X_Dell_DATE = PREFIX_KEY_X_Dell + "date";
	String KEY_X_Dell_FILESYSTEM_ACCESS_ENABLED = PREFIX_KEY_X_Dell + "filesystem-access-enabled";
	String KEY_X_Dell_NAMESPACE = PREFIX_KEY_X_Dell + "namespace";
	String KEY_X_Dell_SIGNATURE = PREFIX_KEY_X_Dell + "signature";
	String KEY_X_Dell_SUBTENANT_ID = PREFIX_KEY_X_Dell + "subtenant-id";
	String KEY_X_Dell_UID = PREFIX_KEY_X_Dell + "uid";
}
