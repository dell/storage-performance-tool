package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.MIB;
import static com.dell.spt.base.logging.LogUtil.RESET;
import static com.dell.spt.base.logging.LogUtil.getFailureRatioAnsiColorCode;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.env.DateUtil;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.text.TextStringBuilder;

/**
* Created by kurila on 18.05.17. Not thread safe, relies on the MetricsManager's (caller) exclusive
* invocation lock
*/
public class MetricsAsciiTableLogMessage extends LogMessageBase {

	private static final String LINE_SEPARATOR = System.lineSeparator();

	public static final String TABLE_HEADER = "------------------------------------------------------------------------------------------------------------------------"
					+ LINE_SEPARATOR
					+ " Step Id  | Timestamp  |  Op  |     Concurrency     |       Count       | Step  |   Last Rate    |  Mean    |   Mean    "
					+ LINE_SEPARATOR
					+ " (last 10 |            | type |---------------------|-------------------| Time  |----------------| Latency  | Duration  "
					+ LINE_SEPARATOR
					+ " symbols) |yyMMddHHmmss|      | Current  |   Mean   |   Success  |Failed|  [s]  | [op/s] |[MB/s] |  [us]    |   [us]    "
					+ LINE_SEPARATOR
					+ "----------|------------|------|----------|----------|------------|------|-------|--------|-------|----------|-----------"
					+ LINE_SEPARATOR;
	public static final String TABLE_BORDER_VERTICAL = "|";
	public static final int TABLE_HEADER_PERIOD = 20;
	private static final AtomicLong ROW_OUTPUT_COUNTER = new AtomicLong();
	private final Set<MetricsContext> metrics;
	private volatile String formattedMsg = null;

	public MetricsAsciiTableLogMessage(final Set<MetricsContext> metrics) {
		this.metrics = metrics;
	}

	@Override
	public final void formatTo(final StringBuilder buffer) {
		if (formattedMsg == null) {
			final var strb = new TextStringBuilder();
			AllMetricsSnapshot snapshot;
			long succCount;
			long failCount;
			OpType opType;
			boolean stdOutColorFlag;
			for (final var metricsCtx : metrics) {
				snapshot = metricsCtx.lastSnapshot();
				if (snapshot != null) {
					succCount = snapshot.successSnapshot().count();
					failCount = snapshot.failsSnapshot().count();
					opType = metricsCtx.opType();
					stdOutColorFlag = metricsCtx.stdOutColorEnabled();
					final long rowIndex = ROW_OUTPUT_COUNTER.getAndIncrement();
					if (rowIndex % TABLE_HEADER_PERIOD == 0) {
						strb.append(TABLE_HEADER);
					}
					strb.appendFixedWidthPadLeft(metricsCtx.loadStepId(), 10, ' ')
									.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadLeft(
													DateUtil.formatMetricsTable(Instant.now()), 12, ' ')
									.append(TABLE_BORDER_VERTICAL);
					if (stdOutColorFlag) {
						switch (opType) {
						case NOOP:
							strb.append(LogUtil.NOOP_COLOR);
							break;
						case CREATE:
							strb.append(LogUtil.CREATE_COLOR);
							break;
						case READ:
							strb.append(LogUtil.READ_COLOR);
							break;
						case UPDATE:
							strb.append(LogUtil.UPDATE_COLOR);
							break;
						case DELETE:
							strb.append(LogUtil.DELETE_COLOR);
							break;
						case LIST:
							strb.append(LogUtil.LIST_COLOR);
							break;
						}
					}
					strb.appendFixedWidthPadRight(metricsCtx.opType().name(), 6, ' ');
					if (stdOutColorFlag) {
						strb.append(RESET);
					}
					strb.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadLeft(snapshot.concurrencySnapshot().last(), 10, ' ')
									.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadRight(
													formatFixedWidth(snapshot.concurrencySnapshot().mean(), 10), 10, ' ')
									.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadLeft(succCount, 12, ' ')
									.append(TABLE_BORDER_VERTICAL);
					if (stdOutColorFlag) {
						strb.append(getFailureRatioAnsiColorCode(succCount, failCount));
					}
					strb.appendFixedWidthPadLeft(failCount, 6, ' ');
					if (stdOutColorFlag) {
						strb.append(RESET);
					}
					strb.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadRight((double) snapshot.elapsedTimeMillis() / 1000, 7, ' ')
									.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadRight(
													formatFixedWidth(snapshot.successSnapshot().last(), 8), 8, ' ')
									.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadRight(
													formatFixedWidth(snapshot.byteSnapshot().last() / MIB, 7), 7, ' ')
									.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadLeft((long) snapshot.latencySnapshot().mean(), 10, ' ')
									.append(TABLE_BORDER_VERTICAL)
									.appendFixedWidthPadLeft((long) snapshot.durationSnapshot().mean(), 11, ' ')
									.appendNewLine();
				}
			}
			formattedMsg = strb.toString();
		}
		buffer.append(formattedMsg);
	}
}
