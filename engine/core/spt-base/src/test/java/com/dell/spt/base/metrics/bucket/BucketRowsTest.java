package com.dell.spt.base.metrics.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.MetricsBucketCsvLogMessage;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadySettings;
import com.dell.spt.base.metrics.steady.SteadyStatus;
import java.util.ArrayList;
import java.util.List;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

class BucketRowsTest {

	private static final String STEP = "step";
	private static final List<String> MEMBERS = List.of("other", "beta");
	private static final SteadySettings SETTINGS = new SteadySettings(20.0, 20.0, 20_000L);

	private static BucketMeters meters(final OpType type, final long... durations) {
		final var meters = new BucketMeters(type, MEMBERS);
		int slot = 0;
		for (final long duration : durations) {
			meters.sets()[slot++ % 2].markSucc(1024, duration, duration / 2, duration / 3);
		}
		return meters;
	}

	private static BucketContextReply context(final BucketMeters meters, final BucketSteadyOutcome steady) {
		return new BucketContextReply(
						0, meters.opType(), 10_000L, 5_000L, MEMBERS, meters.terminalRecords(), steady);
	}

	private static BucketRows.Contribution node(final String name, final BucketContextReply... contexts) {
		return new BucketRows.Contribution(name, new BucketMetricsReply(STEP, name, List.of(contexts)), null);
	}

	private static BucketSteadyOutcome measured(
					final BucketMeters meters, final long windowNanos, final long startDelay, final long endDelay) {
		final List<BucketSteadyRecord> records = new ArrayList<>();
		for (final BucketTerminalRecord record : meters.terminalRecords()) {
			records.add(new BucketSteadyRecord(record, windowNanos, startDelay, endDelay, 1_000));
		}
		return BucketSteadyOutcome.measured(records);
	}

	private static SteadyResult aggregate(final double turnovers) {
		return new SteadyResult(
						SteadyStatus.VALID, "test", 5_000L, 60_000_000_000L, turnovers, 1.5, 0.25, 0, 0, null);
	}

	private static List<BucketRows.Row> wholeStep(final List<BucketRows.Contribution> nodes, final List<BucketRows.Cohort> cohort) {
		final List<BucketRows.Row> rows = new ArrayList<>();
		cohort.add(BucketRows.wholeStep(STEP, 0, OpType.CREATE, nodes, 10.0, rows::add));
		return rows;
	}

	private static List<BucketRows.Row> steady(final List<BucketRows.Contribution> nodes, final SteadyResult aggregate) {
		final List<BucketRows.Row> rows = new ArrayList<>();
		BucketRows.steady(STEP, 0, OpType.CREATE, nodes, aggregate, SETTINGS, rows::add);
		return rows;
	}

	/** Item 12: populations merge, not statistics. */
	@Test
	void timingPopulationsMergeSoQuantilesMatchTheWholePopulation() {
		final var low = meters(OpType.CREATE);
		final var high = meters(OpType.CREATE);
		final Histogram all = new Histogram(1L, 3_600_000_000L, 2);
		for (long value = 100; value <= 10_000; value += 100) {
			low.sets()[0].markSucc(1, value, value / 2, value / 3);
			all.recordValue(value);
		}
		for (long value = 1_000_000; value <= 1_020_000; value += 1_000) {
			high.sets()[0].markSucc(1, value, value / 2, value / 3);
			all.recordValue(value);
		}
		final var rows = wholeStep(
						List.of(node("a", context(low, null)), node("b", context(high, null))), new ArrayList<>());
		final var duration = rows.get(0).duration();
		assertEquals(all.getTotalCount(), duration.count());
		assertEquals(all.getValueAtPercentile(50), duration.percentile(0.5));
		assertEquals(all.getValueAtPercentile(99), duration.percentile(0.99));
		assertEquals(1_020_000, duration.max(), "the exact maximum, not the histogram's rounded one");
	}

	@Test
	void slicesGiveTheSameSumsAsOne() {
		final var combined = meters(OpType.CREATE, 100, 200, 300, 400);
		final var one = wholeStep(List.of(node("a", context(combined, null))), new ArrayList<>());
		final var sliceA = meters(OpType.CREATE, 100, 200);
		final var sliceB = meters(OpType.CREATE, 300, 400);
		final var two = wholeStep(
						List.of(node("a", context(sliceA, null)), node("b", context(sliceB, null))), new ArrayList<>());
		assertEquals(one.size(), two.size());
		for (int i = 0; i < one.size(); i++) {
			assertEquals(one.get(i).succeeded(), two.get(i).succeeded());
			assertEquals(one.get(i).bytes(), two.get(i).bytes());
			assertEquals(one.get(i).duration().count(), two.get(i).duration().count());
		}
		assertEquals(BucketData.COMPLETE, two.get(0).data());
		assertEquals(2, two.get(0).nodesReceived());
	}

	@Test
	void rowsKeepTheirIdentityAndTheCatchAllComesLastEvenEmpty() {
		final var rows = wholeStep(List.of(node("a", context(meters(OpType.CREATE, 1, 2), null))), new ArrayList<>());
		assertEquals(3, rows.size());
		assertEquals(BucketKind.MEMBER, rows.get(0).kind());
		assertEquals("other", rows.get(0).bucket(), "a real bucket named other stays a member");
		assertEquals(0, rows.get(0).memberIndex());
		assertEquals(BucketKind.CATCH_ALL, rows.get(2).kind());
		assertNull(rows.get(2).bucket());
		assertEquals(0, rows.get(2).succeeded());
	}

	@Test
	void aMissingWorkerLeavesALabelledSubtotalAndSaysWhy() {
		final List<BucketRows.Cohort> cohorts = new ArrayList<>();
		final var rows = wholeStep(
						List.of(
										node("a", context(meters(OpType.CREATE, 1, 2), null)),
										new BucketRows.Contribution("b", null, "timeout"),
										new BucketRows.Contribution("c", null, null)),
						cohorts);
		assertEquals(BucketData.INCOMPLETE, rows.get(0).data());
		assertEquals(3, rows.get(0).nodesExpected());
		assertEquals(1, rows.get(0).nodesReceived());
		final var cohort = cohorts.get(0);
		assertEquals(List.of("a", "b", "c"), cohort.expected());
		assertEquals(List.of("a"), cohort.received());
		assertEquals("timeout", cohort.missing().get("b"));
		assertEquals("unsupported call", cohort.missing().get("c"));
	}

	@Test
	void zeroRepliesMeanNoRowsAndAnIncompleteCohort() {
		final List<BucketRows.Cohort> cohorts = new ArrayList<>();
		final var rows = wholeStep(List.of(new BucketRows.Contribution("a", null, "timeout")), cohorts);
		assertTrue(rows.isEmpty());
		assertEquals(BucketData.INCOMPLETE, cohorts.get(0).wholeStepData());
	}

	@Test
	void duplicatesAndMismatchesAreMissingForThatTypeOnly() {
		final var put = context(meters(OpType.CREATE, 1), null);
		final var get = context(meters(OpType.READ, 1), null);
		final List<BucketRows.Contribution> nodes = List.of(
						new BucketRows.Contribution("a", new BucketMetricsReply(STEP, "same", List.of(put)), null),
						new BucketRows.Contribution("dup", new BucketMetricsReply(STEP, "same", List.of(put)), null),
						new BucketRows.Contribution("wrong", new BucketMetricsReply("other-step", "x", List.of(put)), null),
						new BucketRows.Contribution("reads-only", new BucketMetricsReply(STEP, "r", List.of(get)), null));
		final var createCohort = BucketRows.wholeStep(STEP, 0, OpType.CREATE, nodes, 1.0, row -> {});
		assertEquals(List.of("a"), createCohort.received());
		assertEquals("duplicate", createCohort.missing().get("dup"));
		assertEquals("mismatched step", createCohort.missing().get("wrong"));
		assertEquals("mismatched type", createCohort.missing().get("reads-only"));
		final var readCohort = BucketRows.wholeStep(STEP, 0, OpType.READ, nodes, 1.0, row -> {});
		assertEquals(List.of("reads-only"), readCohort.received(), "each type has its own contributors");
	}

	@Test
	void aSteadyRowTakesItsCountsFromItsOwnWindowAndRepeatsTheAggregatesIndicators() {
		final var meters = meters(OpType.CREATE, 100, 200);
		final long window = 30_000_000_000L;
		final var rows = steady(
						List.of(node("a", context(meters, measured(meters, window, 2_000_000, 3_000_000)))),
						aggregate(100.0));
		final var steady = rows.get(0).steady();
		assertEquals("valid", steady.status());
		assertEquals(30.0, rows.get(0).stepSeconds());
		assertEquals(100.0, steady.turnovers());
		assertEquals(1.5, steady.trendPercent());
		assertEquals(0.25, steady.cv());
		assertEquals(2.0, steady.startDelayMillis());
		assertEquals(3.0, steady.endDelayMillis());
		assertEquals(5.002, steady.startSeconds(), 1e-9);
	}

	@Test
	void slicesTakeTheShortestWindowAndTheLargestDelays() {
		final var a = meters(OpType.CREATE, 100);
		final var b = meters(OpType.CREATE, 200);
		final var rows = steady(
						List.of(
										node("a", context(a, measured(a, 30_000_000_000L, 1_000_000, 9_000_000))),
										node("b", context(b, measured(b, 25_000_000_000L, 4_000_000, 2_000_000)))),
						aggregate(100.0));
		assertEquals(25.0, rows.get(0).stepSeconds());
		assertEquals(4.0, rows.get(0).steady().startDelayMillis());
		assertEquals(9.0, rows.get(0).steady().endDelayMillis());
		assertEquals(2, rows.get(0).nodesReceived());
	}

	@Test
	void aDelayedOrEmptyWindowIsShortWhileTheAggregateIsValid() {
		final var meters = meters(OpType.CREATE, 100);
		final var delayed = steady(
						List.of(node("a", context(meters, measured(meters, 10_000_000_000L, 0, 0)))), aggregate(100.0));
		assertEquals("short", delayed.get(0).steady().status(), "below the minimum duration");
		final var empty = steady(
						List.of(node("a", context(meters, measured(meters, 30_000_000_000L, 0, 0)))), aggregate(100.0));
		assertEquals("valid", empty.get(0).steady().status());
		assertEquals("short", empty.get(2).steady().status(), "the empty catch-all has no completions");
		final var lowTurnovers = steady(
						List.of(node("a", context(meters, measured(meters, 30_000_000_000L, 0, 0)))), aggregate(5.0));
		assertEquals("short", lowTurnovers.get(0).steady().status(), "the turnovers are the aggregate's");
	}

	@Test
	void anAggregateThatNeverStartedGivesNotReachedRowsWithNoStatistics() {
		final var meters = meters(OpType.CREATE, 100);
		final var rows = steady(
						List.of(node("a", context(meters, BucketSteadyOutcome.notReached()))),
						new SteadyResult(SteadyStatus.NOT_REACHED, "test", SteadyResult.NONE, 0, 0, null, null, 0, 0, null));
		assertEquals(3, rows.size());
		assertEquals("not-reached", rows.get(0).steady().status());
		assertEquals(BucketData.COMPLETE, rows.get(0).data());
		assertFalse(rows.get(0).steady().hasStatistics());
	}

	@Test
	void anUnmeasuredContributorBlanksTheRowsAndAMissingOneTakesPrecedence() {
		final var meters = meters(OpType.CREATE, 100);
		final var unmeasured = steady(
						List.of(
										node("a", context(meters, measured(meters, 30_000_000_000L, 0, 0))),
										node("b", context(meters(OpType.CREATE, 1), BucketSteadyOutcome.unmeasured("deadline")))),
						aggregate(100.0));
		assertEquals(BucketData.UNMEASURED, unmeasured.get(0).data());
		assertEquals("incomplete", unmeasured.get(0).steady().status());
		assertFalse(unmeasured.get(0).steady().hasStatistics());

		final var missing = steady(
						List.of(
										node("a", context(meters(OpType.CREATE, 1), BucketSteadyOutcome.unmeasured("deadline"))),
										new BucketRows.Contribution("b", null, "timeout")),
						aggregate(100.0));
		assertEquals(BucketData.INCOMPLETE, missing.get(0).data(), "incomplete wins over unmeasured");
		assertEquals("incomplete", missing.get(0).steady().status());
	}

	@Test
	void ratesSumEachWorkersOwnNotTheSumOverTheShortestWindow() {
		final var a = new BucketMeters(OpType.CREATE, MEMBERS);
		final var b = new BucketMeters(OpType.CREATE, MEMBERS);
		for (int i = 0; i < 100; i++) {
			a.sets()[0].markSucc(1, 100, 50, 25);
			b.sets()[0].markSucc(1, 100, 50, 25);
		}
		final var rows = steady(
						List.of(
										node("a", context(a, measured(a, 10_000_000_000L, 0, 0))),
										node("b", context(b, measured(b, 5_000_000_000L, 0, 0)))),
						aggregate(100.0));
		assertEquals(30.0, rows.get(0).succeededPerSecond(), 1e-9, "100/10 + 100/5, not 200/5");
		assertEquals(30.0, rows.get(0).bytesPerSecond(), 1e-9);
		assertEquals(5.0, rows.get(0).stepSeconds());
	}

	@Test
	void wholeStepRatesUseEachWorkersOwnElapsedTime() {
		final var meters = meters(OpType.CREATE, 1, 2, 3, 4);
		final var reply = new BucketContextReply(
						0, OpType.CREATE, 4_000L, 0L, MEMBERS, meters.terminalRecords(), null);
		final var rows = wholeStep(List.of(node("a", reply)), new ArrayList<>());
		assertEquals(0.5, rows.get(0).succeededPerSecond(), 1e-9, "2 over this worker's 4 s");
		assertEquals(10.0, rows.get(0).stepSeconds(), "the row's duration stays the step's");
	}

	@Test
	void sameTypeContextsOfOneStepKeepTheirOwnCounts() {
		final var first = meters(OpType.CREATE, 1);
		final var second = meters(OpType.CREATE, 1, 2, 3, 4, 5, 6, 7);
		final var reply = new BucketMetricsReply(STEP, "a", List.of(
						new BucketContextReply(0, OpType.CREATE, 1L, 0L, MEMBERS, first.terminalRecords(), null),
						new BucketContextReply(1, OpType.CREATE, 1L, 0L, MEMBERS, second.terminalRecords(), null)));
		final List<BucketRows.Contribution> nodes = List.of(new BucketRows.Contribution("a", reply, null));
		final List<BucketRows.Row> rows = new ArrayList<>();
		BucketRows.wholeStep(STEP, 1, OpType.CREATE, nodes, 1.0, rows::add);
		assertEquals(4, rows.get(0).succeeded(), "the second context's, not the first's");
		rows.clear();
		BucketRows.wholeStep(STEP, 0, OpType.CREATE, nodes, 1.0, rows::add);
		assertEquals(1, rows.get(0).succeeded());
	}

	@Test
	void anIncompleteAggregateCannotLabelABucketWindow() {
		final var meters = meters(OpType.CREATE, 100);
		final var incomplete = new SteadyResult(
						SteadyStatus.INCOMPLETE, "test", SteadyResult.NONE, 0, 0, null, null, 0, 0, null);
		final var rows = steady(
						List.of(node("a", context(meters, measured(meters, 30_000_000_000L, 0, 0)))), incomplete);
		assertEquals(BucketData.UNMEASURED, rows.get(0).data());
		assertEquals("incomplete", rows.get(0).steady().status());
		assertFalse(rows.get(0).steady().hasStatistics());
	}

	@Test
	void aSteadyStartBelongsToOneSlice() {
		final var a = meters(OpType.CREATE, 100);
		final var b = meters(OpType.CREATE, 200);
		final var replyA = new BucketContextReply(
						0, OpType.CREATE, 1L, 0L, MEMBERS, a.terminalRecords(), measured(a, 30_000_000_000L, 3_000_000_000L, 0));
		final var replyB = new BucketContextReply(
						0, OpType.CREATE, 1L, 4_000L, MEMBERS, b.terminalRecords(), measured(b, 30_000_000_000L, 100_000_000L, 0));
		final var rows = steady(List.of(node("a", replyA), node("b", replyB)), aggregate(100.0));
		assertEquals(4.1, rows.get(0).steady().startSeconds(), 1e-9, "the latest real start, not 4 s + 3 s");
	}

	@Test
	void aRecordInTheWrongSlotIsAnUnusablePayload() {
		final var meters = meters(OpType.CREATE, 1, 2);
		final var records = new ArrayList<>(meters.terminalRecords());
		java.util.Collections.swap(records, 0, 1);
		final var reply = new BucketContextReply(0, OpType.CREATE, 1L, 0L, MEMBERS, records, null);
		final List<BucketRows.Cohort> cohorts = new ArrayList<>();
		final var rows = wholeStep(List.of(node("a", reply)), cohorts);
		assertTrue(rows.isEmpty());
		assertEquals("unusable payload for this type", cohorts.get(0).missing().get("a"));
		assertEquals(BucketData.INCOMPLETE, cohorts.get(0).wholeStepData());
	}

	@Test
	void aNameWithAQuoteRoundTripsThroughACsvReader() {
		final List<String> members = List.of("\"odd\"", "beta");
		final var meters = new BucketMeters(OpType.CREATE, members);
		final var reply = new BucketContextReply(0, OpType.CREATE, 1L, 0L, members, meters.terminalRecords(), null);
		final var rows = wholeStep(List.of(node("a", reply)), new ArrayList<>());
		final String cell = MetricsBucketCsvLogMessage.row(rows.get(0), QUANTILES).split(CELL_SEPARATOR, -1)[3];
		assertEquals("\"\"\"odd\"\"\"", cell);
	}

	@Test
	void formattingARowLeavesNoDecodedHistogramOnTheHeldRecord() throws Exception {
		final var meters = meters(OpType.CREATE, 100, 200);
		final var rows = wholeStep(List.of(node("a", context(meters, null))), new ArrayList<>());
		final var duration = rows.get(0).duration();
		MetricsBucketCsvLogMessage.row(rows.get(0), QUANTILES);
		final var field = com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl.class.getDeclaredField("decodedHistogram");
		field.setAccessible(true);
		assertNull(field.get(duration));
	}

	private static final List<Double> QUANTILES = List.of(0.25, 0.5, 0.75);
	/** Commas outside quotes: the date cell is quoted and holds one, as in the total file. */
	private static final String CELL_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	@Test
	void theHeadersAreTheAuthoritativeOnes() {
		assertEquals(
						"DateTimeISO8601,BucketKind,MemberIndex,Bucket,OpType,NodesExpected,NodesReceived,BucketData,"
										+ "CountSucc,CountFail,CountCorrupt,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],BWAvg[MiB/s],"
										+ "DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],"
										+ "LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us],"
										+ "TtfbAvg[us],TtfbMin[us],TtfbQ_0.25[us],TtfbQ_0.5[us],TtfbQ_0.75[us],TtfbMax[us]",
						MetricsBucketCsvLogMessage.header(QUANTILES, false));
		assertTrue(MetricsBucketCsvLogMessage.header(QUANTILES, true).endsWith(
						",TtfbMax[us],SteadyStart[s],SteadyTurnovers,SteadyTrend[%],SteadyCV,SteadyStatus,StartDelay[ms],EndDelay[ms],PassDuration[ms]"));
		assertTrue(MetricsBucketCsvLogMessage.header(List.of(0.9), false).contains("LatencyQ_0.9[us]"));
	}

	@Test
	void everyRowHasACellForEveryColumn() {
		final var meters = meters(OpType.CREATE, 100, 200);
		final int wholeColumns = MetricsBucketCsvLogMessage.header(QUANTILES, false).split(",").length;
		final int steadyColumns = MetricsBucketCsvLogMessage.header(QUANTILES, true).split(",").length;
		for (final var row : wholeStep(List.of(node("a", context(meters, null))), new ArrayList<>())) {
			assertEquals(wholeColumns, MetricsBucketCsvLogMessage.row(row, QUANTILES).split(CELL_SEPARATOR, -1).length);
		}
		final List<List<BucketRows.Row>> steadySets = List.of(
						steady(List.of(node("a", context(meters, measured(meters, 30_000_000_000L, 0, 0)))), aggregate(100.0)),
						steady(List.of(node("a", context(meters, BucketSteadyOutcome.unmeasured("x")))), aggregate(100.0)),
						steady(List.of(node("a", context(meters, BucketSteadyOutcome.notReached()))), aggregate(100.0)));
		for (final var rows : steadySets) {
			for (final var row : rows) {
				assertEquals(steadyColumns, MetricsBucketCsvLogMessage.row(row, QUANTILES).split(CELL_SEPARATOR, -1).length);
			}
		}
	}

	@Test
	void aMemberAndTheCatchAllRoundTripAsDistinctIdentities() {
		final var rows = wholeStep(List.of(node("a", context(meters(OpType.CREATE, 1), null))), new ArrayList<>());
		final String[] member = MetricsBucketCsvLogMessage.row(rows.get(0), QUANTILES).split(CELL_SEPARATOR, -1);
		final String[] catchAll = MetricsBucketCsvLogMessage.row(rows.get(2), QUANTILES).split(CELL_SEPARATOR, -1);
		assertEquals("member", member[1]);
		assertEquals("0", member[2]);
		assertEquals("other", member[3]);
		assertEquals("catch-all", catchAll[1]);
		assertEquals("", catchAll[2]);
		assertEquals("", catchAll[3]);
	}

	@Test
	void aRangeSelectsTheSameMembersFromEachWorkerAndOneType() {
		final var meters = meters(OpType.CREATE, 1, 2);
		final var reply = new BucketMetricsReply(
						STEP, "a", List.of(context(meters, measured(meters, 1, 0, 0)), context(meters(OpType.READ, 1), null)));
		final var part = reply.within(new BucketMetricsRange(OpType.CREATE, 1, 3));
		assertEquals(1, part.contexts().size());
		assertEquals(2, part.contexts().get(0).wholeStep().size());
		assertEquals("beta", part.contexts().get(0).wholeStep().get(0).bucket());
		assertEquals(2, part.contexts().get(0).steady().records().size());
	}
}
