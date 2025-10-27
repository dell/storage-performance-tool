package com.dell.spt.base.logging;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;

/** Created by andrey on 24.07.17. */
public final class OperationTraceRecord<I extends Item, O extends Operation<I>> {

	private final String storageNode;
	private final String itemPath;
	private final String opTypeCode;
	private final String statusCode;
	private final long reqTimeStart;
	private final long duration;
	private final long respLatency;
	private final long dataLatency;
	private final long transferSize;

	public OperationTraceRecord(final O opResult) {
		storageNode = opResult.nodeAddr();
		final String itemInfo = opResult.item().toString();
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
			strb.append(itemPath);
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
}
