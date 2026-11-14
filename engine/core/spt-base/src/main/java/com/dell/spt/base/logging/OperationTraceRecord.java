package com.dell.spt.base.logging;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;

/** Created by andrey on 24.07.17. */
public final class OperationTraceRecord<I extends Item, O extends Operation<I>> {

	private final String storageNode;
	private final String itemPath;
	/**
	 * The path the request for a manifest item, whose name is the key alone, went to: the operation's
	 * destination path, or else its source path. That is the output path for a CREATE and the bucket
	 * for any other type. Empty when the operation has neither; null for any other item, whose name the
	 * result already placed under its path.
	 */
	private final String manifestRequestPath;
	private final String opTypeCode;
	private final String statusCode;
	private final long reqTimeStart;
	private final long duration;
	private final long respLatency;
	private final long dataLatency;
	private final long transferSize;

	public OperationTraceRecord(final O opResult) {
		storageNode = opResult.nodeAddr();
		final I item = opResult.item();
		final String itemInfo = item.toString();
		if (itemInfo != null) {
			final int commaPos = itemInfo.indexOf(',', 0);
			if (commaPos > 0) {
				itemPath = itemInfo.substring(0, itemInfo.indexOf(',', 0));
			} else {
				itemPath = itemInfo;
			}
		} else {
			itemPath = null;
		}
		// Captured now, not read in format(): operations and items are reused in recycle mode.
		if (item instanceof IntegrityManifestDataItem) {
			final String path = opResult.dstPath() != null ? opResult.dstPath() : opResult.srcPath();
			manifestRequestPath = path != null ? path : "";
		} else {
			manifestRequestPath = null;
		}
		opTypeCode = opResult.type() != null ? opResult.type().name() : "";
		statusCode = opResult.status() != null ? opResult.status().name() : "";
		reqTimeStart = opResult.reqTimeStart();
		duration = opResult.duration();
		var t = opResult.latency();
		respLatency = t <= duration ? t : -1;
		if (opResult instanceof DataOperation dataIoResult) {
			t = dataIoResult.dataLatency();
			dataLatency = t < duration && t > 0 ? t : -1;
			transferSize = dataIoResult.countBytesDone();
		} else {
			dataLatency = -1;
			transferSize = -1;
		}
	}

	public final void format(final StringBuilder strb) {
		if (storageNode != null) {
			strb.append(storageNode);
		}
		strb.append(',');
		if (itemPath != null) {
			if (manifestRequestPath != null) {
				appendRequestPath(strb, manifestRequestPath, itemPath);
			} else {
				strb.append(itemPath);
			}
		}
		strb.append(',');
		strb.append(opTypeCode);
		strb.append(',');
		strb.append(statusCode);
		strb.append(',');
		if (reqTimeStart > 0) {
			strb.append(reqTimeStart);
		}
		strb.append(',');
		if (duration > 0) {
			strb.append(duration);
		}
		strb.append(',');
		if (respLatency > 0) {
			strb.append(respLatency);
		}
		strb.append(',');
		if (dataLatency > 0) {
			strb.append(dataLatency);
		}
		strb.append(',');
		if (transferSize != -1) {
			strb.append(transferSize);
		}
		strb.append('\n');
	}

	/**
	 * Appends the path an HTTP driver requests for the name under the path, as
	 * {@code HttpStorageDriverBase.dataUriPath} builds it, without building a string: the name alone
	 * with a leading slash when the path is empty or the name already holds it, otherwise the rooted
	 * path and the name with exactly one slash between them.
	 */
	private static void appendRequestPath(final StringBuilder strb, final String path, final String name) {
		if (path.isEmpty() || ItemNamePaths.holdsPath(name, path)) {
			if (!name.startsWith(Operation.SLASH)) {
				strb.append('/');
			}
			strb.append(name);
			return;
		}
		if (!path.startsWith(Operation.SLASH)) {
			strb.append('/');
		}
		// Fields are not quoted: like a plain item's path, the row's path ends at its first comma. The
		// name was already cut at its own, so only the path can hold one.
		final int commaPos = path.indexOf(',');
		if (commaPos >= 0) {
			strb.append(path, 0, commaPos);
			return;
		}
		strb.append(path);
		final boolean nameRooted = name.startsWith(Operation.SLASH);
		if (path.endsWith(Operation.SLASH)) {
			strb.append(name, nameRooted ? 1 : 0, name.length());
		} else {
			if (!nameRooted) {
				strb.append('/');
			}
			strb.append(name);
		}
	}
}
