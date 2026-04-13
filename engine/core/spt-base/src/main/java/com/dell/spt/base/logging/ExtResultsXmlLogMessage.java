package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.K;
import static com.dell.spt.base.Constants.MIB;

import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

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

	private static final ThreadLocal<DateFormat> FMT_DATE_RESULTS = ThreadLocal.withInitial(() -> {
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		return df;
	});

	private final MetricsContext<?> metricsCtx;
	private final AllMetricsSnapshot snapshot;

	public ExtResultsXmlLogMessage(
					final MetricsContext<?> metricsCtx,
					final AllMetricsSnapshot snapshot) {
		this.metricsCtx = metricsCtx;
		this.snapshot = snapshot;
	}

	@Override
	public void formatTo(final StringBuilder strb) {

		final TimingMetricSnapshot durationSnapshot = snapshot.durationSnapshot();
		final TimingMetricSnapshot latencySnapshot = snapshot.latencySnapshot();
		final RateMetricSnapshot successSnapshot = snapshot.successSnapshot();
		final RateMetricSnapshot byteSnapshot = snapshot.byteSnapshot();

		final long startTimeMillis = metricsCtx.startTimeStamp();
		final long elapsedTimeMillis = snapshot.elapsedTimeMillis();
		final long endTimeMillis = startTimeMillis + elapsedTimeMillis;

		final int nodeCount = snapshot instanceof DistributedAllMetricsSnapshot distributed
						? distributed.nodeCount()
						: 1;

		strb.append("<result id=\"")
						.append(escapeXml(metricsCtx.loadStepId()))
						.append('"');

		strb.append(" StartDate=\"")
						.append(FMT_DATE_RESULTS.get().format(new Date(startTimeMillis)))
						.append('"');
		strb.append(" StartTimestamp=\"")
						.append(startTimeMillis)
						.append('"');

		strb.append(" EndDate=\"")
						.append(FMT_DATE_RESULTS.get().format(new Date(endTimeMillis)))
						.append('"');
		strb.append(" EndTimestamp=\"")
						.append(endTimeMillis)
						.append('"');

		strb.append(" operation=\"")
						.append(metricsCtx.opType().name())
						.append('"');

		strb.append(" threads=\"")
						.append(metricsCtx.concurrencyLimit())
						.append('"');
		strb.append(" RequestThreads=\"")
						.append(metricsCtx.concurrencyLimit())
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
						.append(metricsCtx.itemDataSize())
						.append('"');

		strb.append(" tps=\"")
						.append(successSnapshot.mean())
						.append("\" tps_unit=\"Fileps\"");

		strb.append(" bw=\"")
						.append(byteSnapshot.mean() / MIB)
						.append("\" bw_unit=\"MBps\"");

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

		strb.append(" />\n");
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
