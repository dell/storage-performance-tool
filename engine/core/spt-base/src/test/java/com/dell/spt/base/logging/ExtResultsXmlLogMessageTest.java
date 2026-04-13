package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;

import org.junit.jupiter.api.Test;

class ExtResultsXmlLogMessageTest {

	@Test
	void verifyXmlElementStructure() {
		MetricsContext ctx = mockContext("step-1", OpType.CREATE, 10, 1_000_000L, 1048576L);
		AllMetricsSnapshot snap = mockSnapshot(5000L, 100L, 50.0, 2L, 1048576.0, 500.0, 100L, 900L, 600.0, 200L, 1000L);

		ExtResultsXmlLogMessage msg = new ExtResultsXmlLogMessage(ctx, snap);
		StringBuilder buf = new StringBuilder();
		msg.formatTo(buf);
		String xml = buf.toString();

		assertTrue(xml.startsWith("<result id=\"step-1\""), "Should start with <result id=...");
		assertTrue(xml.endsWith(" />"), "Should end with self-closing tag");
	}

	@Test
	void verifyAllAttributesPresent() {
		MetricsContext ctx = mockContext("writeTest", OpType.CREATE, 16, 1_700_000_000L, 65536L);
		AllMetricsSnapshot snap = mockSnapshot(
						30000L, 5000L, 250.0, 3L, 10485760.0, 1500.0, 800L, 3000L, 2000.0, 500L, 4000L);

		ExtResultsXmlLogMessage msg = new ExtResultsXmlLogMessage(ctx, snap);
		String xml = format(msg);

		assertContainsAttr(xml, "id", "writeTest");
		assertContainsAttr(xml, "operation", "CREATE");
		assertContainsAttr(xml, "threads", "16");
		assertContainsAttr(xml, "RequestThreads", "16");
		assertContainsAttr(xml, "error", "3");
		assertContainsAttr(xml, "tps_unit", "Fileps");
		assertContainsAttr(xml, "bw_unit", "MBps");
		assertContainsAttr(xml, "latency_unit", "us");
		assertContainsAttr(xml, "duration_unit", "us");

		assertTrue(xml.contains("StartDate=\""), "Should contain StartDate");
		assertTrue(xml.contains("StartTimestamp=\""), "Should contain StartTimestamp");
		assertTrue(xml.contains("EndDate=\""), "Should contain EndDate");
		assertTrue(xml.contains("EndTimestamp=\""), "Should contain EndTimestamp");
		assertTrue(xml.contains("filesize=\""), "Should contain filesize");
		assertTrue(xml.contains("runtime=\""), "Should contain runtime");
		assertTrue(xml.contains("clients=\""), "Should contain clients");
	}

	@Test
	void verifyTimestampCalculation() {
		long startTime = 1_700_000_000_000L; // millis
		long elapsed = 60_000L; // 60 seconds

		MetricsContext ctx = mockContext("ts-test", OpType.READ, 1, startTime, 1024L);
		AllMetricsSnapshot snap = mockSnapshot(elapsed, 100L, 10.0, 0L, 1024.0, 100.0, 50L, 200L, 150.0, 80L, 300L);

		String xml = format(new ExtResultsXmlLogMessage(ctx, snap));

		long expectedEnd = startTime + elapsed;
		assertTrue(xml.contains("StartTimestamp=\"" + startTime + "\""), "StartTimestamp should match");
		assertTrue(xml.contains("EndTimestamp=\"" + expectedEnd + "\""), "EndTimestamp should equal start + elapsed");
	}

	@Test
	void verifyRuntimeInSeconds() {
		MetricsContext ctx = mockContext("rt-test", OpType.READ, 1, 0L, 1024L);
		AllMetricsSnapshot snap = mockSnapshot(45_000L, 100L, 10.0, 0L, 1024.0, 100.0, 50L, 200L, 150.0, 80L, 300L);

		String xml = format(new ExtResultsXmlLogMessage(ctx, snap));

		// 45000 ms / 1000.0 = 45.0
		assertTrue(xml.contains("runtime=\"45.0\""), "Runtime should be elapsed millis / 1000");
	}

	@Test
	void verifyBandwidthInMiB() {
		MetricsContext ctx = mockContext("bw-test", OpType.CREATE, 1, 0L, 1024L);
		// bytesMean = 2 * MIB = 2097152.0
		double twoMiB = 2.0 * 0x10_00_00;
		AllMetricsSnapshot snap = mockSnapshot(1000L, 100L, 10.0, 0L, twoMiB, 100.0, 50L, 200L, 150.0, 80L, 300L);

		String xml = format(new ExtResultsXmlLogMessage(ctx, snap));

		assertTrue(xml.contains("bw=\"2.0\""), "Bandwidth should be bytes/s divided by MIB");
	}

	@Test
	void verifyDistributedNodeCount() {
		MetricsContext ctx = mockContext("dist-test", OpType.READ, 8, 0L, 4096L);

		DistributedAllMetricsSnapshot snap = mock(DistributedAllMetricsSnapshot.class);
		when(snap.nodeCount()).thenReturn(4);
		wireSnapshotMethods(snap, 1000L, 100L, 10.0, 0L, 1024.0, 100.0, 50L, 200L, 150.0, 80L, 300L);

		String xml = format(new ExtResultsXmlLogMessage(ctx, snap));

		assertTrue(xml.contains("clients=\"4\""), "Distributed snapshot should report actual node count");
	}

	@Test
	void verifyNonDistributedNodeCountIsOne() {
		MetricsContext ctx = mockContext("local-test", OpType.READ, 1, 0L, 1024L);
		AllMetricsSnapshot snap = mockSnapshot(1000L, 100L, 10.0, 0L, 1024.0, 100.0, 50L, 200L, 150.0, 80L, 300L);

		String xml = format(new ExtResultsXmlLogMessage(ctx, snap));

		assertTrue(xml.contains("clients=\"1\""), "Non-distributed snapshot should default to 1 client");
	}

	@Test
	void verifyXmlEscaping() {
		MetricsContext ctx = mockContext("step<&\"test>", OpType.NOOP, 1, 0L, 1024L);
		AllMetricsSnapshot snap = mockSnapshot(1000L, 0L, 0.0, 0L, 0.0, 0.0, 0L, 0L, 0.0, 0L, 0L);

		String xml = format(new ExtResultsXmlLogMessage(ctx, snap));

		assertTrue(xml.contains("id=\"step&lt;&amp;&quot;test&gt;\""),
						"Special characters in step ID must be XML-escaped");
		assertFalse(xml.contains("id=\"step<"), "Raw < should not appear in attribute value");
	}

	// --- helpers ---

	private static String format(ExtResultsXmlLogMessage msg) {
		StringBuilder buf = new StringBuilder();
		msg.formatTo(buf);
		return buf.toString();
	}

	private static void assertContainsAttr(String xml, String attr, String value) {
		assertTrue(xml.contains(attr + "=\"" + value + "\""),
						"Expected attribute " + attr + "=\"" + value + "\" in: " + xml);
	}

	@SuppressWarnings("unchecked")
	private static MetricsContext mockContext(
					String stepId, OpType opType, int concurrency, long startTimeMillis, long itemSizeBytes) {
		MetricsContext ctx = mock(MetricsContext.class);
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(opType);
		when(ctx.concurrencyLimit()).thenReturn(concurrency);
		when(ctx.startTimeStamp()).thenReturn(startTimeMillis);
		when(ctx.itemDataSize()).thenReturn(new SizeInBytes(itemSizeBytes));
		return ctx;
	}

	private static AllMetricsSnapshot mockSnapshot(
					long elapsedMillis, long successCount, double successMean,
					long failCount, double bytesMean,
					double latencyMean, long latencyMin, long latencyMax,
					double durationMean, long durationMin, long durationMax) {
		AllMetricsSnapshot snap = mock(AllMetricsSnapshot.class);
		wireSnapshotMethods(snap, elapsedMillis, successCount, successMean,
						failCount, bytesMean, latencyMean, latencyMin, latencyMax,
						durationMean, durationMin, durationMax);
		return snap;
	}

	private static void wireSnapshotMethods(
					AllMetricsSnapshot snap,
					long elapsedMillis, long successCount, double successMean,
					long failCount, double bytesMean,
					double latencyMean, long latencyMin, long latencyMax,
					double durationMean, long durationMin, long durationMax) {
		when(snap.elapsedTimeMillis()).thenReturn(elapsedMillis);

		RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		when(success.count()).thenReturn(successCount);
		when(success.mean()).thenReturn(successMean);
		when(snap.successSnapshot()).thenReturn(success);

		RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		when(fails.count()).thenReturn(failCount);
		when(snap.failsSnapshot()).thenReturn(fails);

		RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		when(bytes.mean()).thenReturn(bytesMean);
		when(snap.byteSnapshot()).thenReturn(bytes);

		TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		when(latency.mean()).thenReturn(latencyMean);
		when(latency.min()).thenReturn(latencyMin);
		when(latency.max()).thenReturn(latencyMax);
		when(snap.latencySnapshot()).thenReturn(latency);

		TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		when(duration.mean()).thenReturn(durationMean);
		when(duration.min()).thenReturn(durationMin);
		when(duration.max()).thenReturn(durationMax);
		when(snap.durationSnapshot()).thenReturn(duration);

		ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);
		when(snap.concurrencySnapshot()).thenReturn(conc);
	}
}
