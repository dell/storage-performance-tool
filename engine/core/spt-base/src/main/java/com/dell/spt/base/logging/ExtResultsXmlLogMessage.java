package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.K;
import static com.dell.spt.base.Constants.MIB;
import static com.dell.spt.base.Constants.UNIT_MIB_PER_SECOND_XML;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.system.SizeInBytes;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;

import java.time.ZoneOffset;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Formats a single {@code <result ... />} XML element containing the final metrics for a load step.
 * The output is appended into {@code result.xml} or {@code result-threshold.xml} via the
 * corresponding Log4j2 file appender whose header/footer supply the enclosing root element.
 *
 * <p>The attribute set is kept compatible with the Mongoose&nbsp;3.6.2
 * {@code ExtResultsXmlLogMessage} so that existing PerfDB consumers can parse the file unchanged.
 */
@AsynchronouslyFormattable
public final class ExtResultsXmlLogMessage extends LogMessageBase {

	private static final DateTimeFormatter FMT_DATE_RESULTS = DateTimeFormatter
					.ofPattern("yyyy-MM-dd HH:mm:ss")
					.withZone(ZoneOffset.UTC);

	// Captured when the message is built: Log4j formats it later on its own thread, and by then
	// the step may have closed the context, which resets its start time to -1.
	private final String loadStepId;
	private final long startTimeMillis;
	private final OpType opType;
	private final int concurrencyLimit;
	private final SizeInBytes itemDataSize;
	private final AllMetricsSnapshot snapshot;

	public ExtResultsXmlLogMessage(
					final MetricsContext<?> metricsCtx,
					final AllMetricsSnapshot snapshot) {
		this.loadStepId = metricsCtx.loadStepId();
		this.startTimeMillis = metricsCtx.startTimeStamp();
		this.opType = metricsCtx.opType();
		this.concurrencyLimit = metricsCtx.concurrencyLimit();
		this.itemDataSize = metricsCtx.itemDataSize();
		this.snapshot = snapshot;
	}

	@Override
	public void formatTo(final StringBuilder strb) {

		final TimingMetricSnapshot durationSnapshot = snapshot.durationSnapshot();
		final TimingMetricSnapshot latencySnapshot = snapshot.latencySnapshot();
		final RateMetricSnapshot successSnapshot = snapshot.successSnapshot();
		final RateMetricSnapshot byteSnapshot = snapshot.byteSnapshot();

		final long elapsedTimeMillis = snapshot.elapsedTimeMillis();
		final long endTimeMillis = startTimeMillis + elapsedTimeMillis;

		final int nodeCount = snapshot instanceof DistributedAllMetricsSnapshot distributed
						? distributed.nodeCount()
						: 1;

		strb.append("<result id=\"")
						.append(escapeXml(loadStepId))
						.append('"');

		strb.append(" StartDate=\"")
						.append(FMT_DATE_RESULTS.format(Instant.ofEpochMilli(startTimeMillis)))
						.append('"');
		strb.append(" StartTimestamp=\"")
						.append(startTimeMillis)
						.append('"');

		strb.append(" EndDate=\"")
						.append(FMT_DATE_RESULTS.format(Instant.ofEpochMilli(endTimeMillis)))
						.append('"');
		strb.append(" EndTimestamp=\"")
						.append(endTimeMillis)
						.append('"');

		strb.append(" operation=\"")
						.append(opType.name())
						.append('"');

		strb.append(" threads=\"")
						.append(concurrencyLimit)
						.append('"');
		strb.append(" RequestThreads=\"")
						.append(concurrencyLimit)
						.append('"');
		strb.append(" clients=\"")
						.append(nodeCount)
						.append('"');

		strb.append(" error=\"")
						.append(snapshot.failsSnapshot().count())
						.append('"');

		strb.append(" runtime=\"")
						.append(elapsedTimeMillis / K)
						.append('"');

		strb.append(" filesize=\"")
						.append(BinarySizeFormat.formatSize(itemDataSize))
						.append('"');

		strb.append(" tps=\"")
						.append(successSnapshot.mean())
						.append("\" tps_unit=\"Fileps\"");

		strb.append(" bw=\"")
						.append(byteSnapshot.mean() / MIB)
						.append("\" bw_unit=\"")
						.append(UNIT_MIB_PER_SECOND_XML)
						.append('"');

		strb.append(" latency=\"")
						.append(latencySnapshot.mean())
						.append("\" latency_unit=\"us\"");
		strb.append(" latency_min=\"")
						.append(latencySnapshot.min())
						.append('"');
		strb.append(" latency_max=\"")
						.append(latencySnapshot.max())
						.append('"');

		strb.append(" duration=\"")
						.append(durationSnapshot.mean())
						.append("\" duration_unit=\"us\"");
		strb.append(" duration_min=\"")
						.append(durationSnapshot.min())
						.append('"');
		strb.append(" duration_max=\"")
						.append(durationSnapshot.max())
						.append('"');

		strb.append(" />");
	}

	private static String escapeXml(final String text) {
		if (text == null) {
			return "";
		}
		final StringBuilder sb = new StringBuilder(text.length());
		for (int i = 0; i < text.length(); i++) {
			final char c = text.charAt(i);
			switch (c) {
			case '&' -> sb.append("&amp;");
			case '<' -> sb.append("&lt;");
			case '>' -> sb.append("&gt;");
			case '"' -> sb.append("&quot;");
			case '\'' -> sb.append("&apos;");
			default -> sb.append(c);
			}
		}
		return sb.toString();
	}
}
