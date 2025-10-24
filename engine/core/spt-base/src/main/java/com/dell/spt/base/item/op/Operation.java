package com.dell.spt.base.item.op;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.storage.Credential;

/** Created by kurila on 11.07.16. */
public interface Operation<I extends Item> {

	long START_OFFSET_MICROS = currentTimeMillis() * 1000 - nanoTime() / 1000;

	String SLASH = "/";

	int originIndex();

	// PENDING is used to recycle an item which hasn't been successfully finished due to specifics of any API
	// but is not counted as an error. So the item is recycled, but isn't counted either as succ or as error

	// OMIT is used to complete an item for whatever purpose that shouldn't be registered anywhere in the metrics
	// and affect them

	enum Status {
		PENDING, // 0
		ACTIVE, // 1
		INTERRUPTED, // 2
		FAIL_UNKNOWN, // 3
		SUCC, // 4
		FAIL_IO, // 5
		FAIL_TIMEOUT, // 6
		OMIT, // 7
		RESP_FAIL_UNKNOWN, // 8
		RESP_FAIL_CLIENT, // 9
		RESP_FAIL_SVC, // 10
		RESP_FAIL_NOT_FOUND, // 11
		RESP_FAIL_AUTH, // 12
		RESP_FAIL_CORRUPT, // 13
		RESP_FAIL_SPACE, // 14
	}

	OpType type();

	I item();

	String nodeAddr();

	void nodeAddr(final String nodeAddr);

	Status status();

	void status(final Status status);

	String srcPath();

	void srcPath(final String srcPath);

	String dstPath();

	void dstPath(final String dstPath);

	Credential credential();

	void credential(final Credential credential);

	void startRequest() throws IllegalStateException;

	void finishRequest() throws IllegalStateException;

	void startResponse() throws IllegalStateException;

	void finishResponse() throws IllegalStateException;

	long reqTimeStart();

	long reqTimeDone();

	long respTimeStart();

	long respTimeDone();

	long duration();

	long latency();

	default void buildItemPath(final I item, final String itemPath) {
		String itemName = item.name();
		if (itemPath == null || itemPath.isEmpty()) {
			if (!itemName.startsWith("/")) {
				item.name("/" + itemName);
			}
		} else if (!itemName.startsWith(itemPath)) {
			if (itemPath.endsWith("/")) {
				item.name(itemPath + itemName);
			} else {
				item.name(itemPath + "/" + itemName);
			}
		}
	}

	Operation<I> result();

	void reset();
}
