package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;

import java.lang.reflect.Field;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsAsciiTableLogMessageTest {
	@BeforeEach
	void resetStaticRowCounter() throws Exception {
		// Make header printing deterministic (header shown when counter % 20 == 0).
		Field f = MetricsAsciiTableLogMessage.class.getDeclaredField("ROW_OUTPUT_COUNTER");
		f.setAccessible(true);
		f.setLong(null, 0L);
	}

	/**
	 * Verifies that a single row in the table is correctly formatted, including the header, core fields, and memoized values.
	 */
	@Test
	void verifySingleRowLogMessageFormatting() {
		MetricsContext ctx = mock(MetricsContext.class);
		when(ctx.loadStepId()).thenReturn("stepA");
		when(ctx.opType()).thenReturn(OpType.READ);
		when(ctx.stdOutColorEnabled()).thenReturn(false);

		AllMetricsSnapshot snap = mock(AllMetricsSnapshot.class);

		RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		when(success.count()).thenReturn(10L);
		when(success.last()).thenReturn(123.0); // op/s

		RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		when(fails.count()).thenReturn(2L);
		when(fails.last()).thenReturn(0.5);

		ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);
		when(conc.last()).thenReturn(5L);
		when(conc.mean()).thenReturn(3.2);

		RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		when(bytes.last()).thenReturn(2.0 * com.dell.spt.base.Constants.MIB);

		TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		when(latency.mean()).thenReturn(1234.56);

		TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		when(duration.mean()).thenReturn(9876.54);

		when(snap.successSnapshot()).thenReturn(success);
		when(snap.failsSnapshot()).thenReturn(fails);
		when(snap.concurrencySnapshot()).thenReturn(conc);
		when(snap.byteSnapshot()).thenReturn(bytes);
		when(snap.latencySnapshot()).thenReturn(latency);
		when(snap.durationSnapshot()).thenReturn(duration);
		when(snap.elapsedTimeMillis()).thenReturn(7000L); // 7.0 s

		when(ctx.lastSnapshot()).thenReturn(snap);

		Set<MetricsContext> set = new LinkedHashSet<>();
		set.add(ctx);

		MetricsAsciiTableLogMessage msg = new MetricsAsciiTableLogMessage(set);

		StringBuilder buf1 = new StringBuilder();
		msg.formatTo(buf1);
		String out1 = buf1.toString();

		assertTrue(out1.startsWith(MetricsAsciiTableLogMessage.TABLE_HEADER),
						"Table header must precede the first data row");

		assertTrue(out1.contains("stepA"), "Step id should be present in the row");
		assertTrue(out1.contains("|READ  |") || out1.contains("|READ |"),
						"Op type READ should be present in a 6-char field (tolerate minor spacing)");

		assertTrue(Pattern.compile("\\|\\d{12}\\|").matcher(out1).find(),
						"Timestamp column should contain a 12-digit yyMMddHHmmss value");

		assertTrue(out1.contains("| 7.0") || out1.contains("|7.0"), "Elapsed seconds should be rendered (~7.0)");
		assertTrue(out1.contains("123.0"), "Last success rate (op/s) should be rendered");

		assertTrue(out1.contains("|      1234|") || out1.contains("|     1234|"),
						"Latency mean (us) should be present and right-justified");
		assertTrue(out1.contains("|       9876") || out1.contains("|      9876"),
						"Duration mean (us) should be present and right-justified");

		when(success.count()).thenReturn(999L);
		when(fails.count()).thenReturn(999L);
		StringBuilder buf2 = new StringBuilder();
		msg.formatTo(buf2);
		String out2 = buf2.toString();
		assertEquals(out1, out2, "Second format must reuse memoized message (unchanged output)");
	}

	/**
	 * Verifies that the header is printed every 20 rows.
	 *
	 * @throws Exception if the test fails
	 */
	@Test
	void printsHeaderEvery20Rows() throws Exception {

		Field f = MetricsAsciiTableLogMessage.class.getDeclaredField("ROW_OUTPUT_COUNTER");
		f.setAccessible(true);
		f.setLong(null, 19L);

		MetricsContext ctx1 = mockBasicContext("s1", OpType.NOOP, false);
		MetricsContext ctx2 = mockBasicContext("s2", OpType.NOOP, false);

		Set<MetricsContext> set = new LinkedHashSet<>();
		set.add(ctx1);
		set.add(ctx2);

		MetricsAsciiTableLogMessage msg = new MetricsAsciiTableLogMessage(set);

		StringBuilder buf = new StringBuilder();
		msg.formatTo(buf);
		String out = buf.toString();

		int headerCount = countOccurrences(out, MetricsAsciiTableLogMessage.TABLE_HEADER);
		assertEquals(1, headerCount,
						"Header should be printed once when starting from counter=19 and writing two rows");

		assertTrue(out.contains("s1"), "First row for s1 should be present");
		assertTrue(out.contains("s2"), "Second row for s2 should be present");
	}

	/**
	 * Verifies that ANSI colors are inserted when stdout color output is enabled.
	 *
	 */
	@Test
	void shouldInsertOpTypeAndFailureColorsWithStdOutColorsEnabled() {

		MetricsContext ctx = mock(MetricsContext.class);
		when(ctx.loadStepId()).thenReturn("colored");
		when(ctx.opType()).thenReturn(OpType.CREATE);
		when(ctx.stdOutColorEnabled()).thenReturn(true);

		AllMetricsSnapshot snap = mock(AllMetricsSnapshot.class);

		RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		when(success.count()).thenReturn(100L);
		when(success.last()).thenReturn(50.0);

		RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		when(fails.count()).thenReturn(25L);
		when(fails.last()).thenReturn(5.0);

		ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);
		when(conc.last()).thenReturn(1L);
		when(conc.mean()).thenReturn(1.0);

		RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		when(bytes.last()).thenReturn(0.0);

		TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		when(latency.mean()).thenReturn(1.0);

		TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		when(duration.mean()).thenReturn(2.0);

		when(snap.successSnapshot()).thenReturn(success);
		when(snap.failsSnapshot()).thenReturn(fails);
		when(snap.concurrencySnapshot()).thenReturn(conc);
		when(snap.byteSnapshot()).thenReturn(bytes);
		when(snap.latencySnapshot()).thenReturn(latency);
		when(snap.durationSnapshot()).thenReturn(duration);
		when(snap.elapsedTimeMillis()).thenReturn(1000L);

		when(ctx.lastSnapshot()).thenReturn(snap);

		Set<MetricsContext> set = Set.of(ctx);
		MetricsAsciiTableLogMessage msg = new MetricsAsciiTableLogMessage(set);

		StringBuilder buf = new StringBuilder();
		msg.formatTo(buf);
		String out = buf.toString();

		assertTrue(out.contains(LogUtil.CREATE_COLOR),
						"Op type color for CREATE should be inserted when stdout color is enabled");
		assertTrue(out.contains(LogUtil.RESET),
						"RESET should be inserted to restore terminal color after the op-type cell");

		String failureColor = LogUtil.getFailureRatioAnsiColorCode(100L, 25L);
		assertTrue(out.contains(failureColor), "Failure ratio color should be applied for non-zero fails");
		int resetCount = countOccurrences(out, LogUtil.RESET);
		assertTrue(resetCount >= 2, "There should be at least two RESET codes (op-type + fail count)");
	}

	// Helpers

	private static int countOccurrences(String haystack, String needle) {
		int count = 0, from = 0;
		while (true) {
			int idx = haystack.indexOf(needle, from);
			if (idx < 0)
				break;
			count++;
			from = idx + needle.length();
		}
		return count;
	}

	private static MetricsContext mockBasicContext(String stepId, OpType opType, boolean color) {
		MetricsContext ctx = mock(MetricsContext.class);
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(opType);
		when(ctx.stdOutColorEnabled()).thenReturn(color);

		AllMetricsSnapshot snap = mock(AllMetricsSnapshot.class);

		RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		when(success.count()).thenReturn(1L);
		when(success.last()).thenReturn(1.0);

		RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		when(fails.count()).thenReturn(0L);
		when(fails.last()).thenReturn(0.0);

		ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);
		when(conc.last()).thenReturn(1L);
		when(conc.mean()).thenReturn(1.0);

		RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		when(bytes.last()).thenReturn(0.0);

		TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		when(latency.mean()).thenReturn(1.0);

		TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		when(duration.mean()).thenReturn(1.0);

		when(snap.successSnapshot()).thenReturn(success);
		when(snap.failsSnapshot()).thenReturn(fails);
		when(snap.concurrencySnapshot()).thenReturn(conc);
		when(snap.byteSnapshot()).thenReturn(bytes);
		when(snap.latencySnapshot()).thenReturn(latency);
		when(snap.durationSnapshot()).thenReturn(duration);
		when(snap.elapsedTimeMillis()).thenReturn(1000L);

		when(ctx.lastSnapshot()).thenReturn(snap);
		return ctx;
	}
}
