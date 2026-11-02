package com.dell.spt.perf;

import com.dell.spt.base.metrics.bucket.BucketMeterSet;
import com.dell.spt.base.metrics.bucket.BucketSlotTable;
import com.dell.spt.base.metrics.bucket.BucketTimingMeter;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

/**
 * Hand-written microbenchmark for ticket 01 of bucket metrics: the aggregate's recording alone,
 * against the aggregate plus per-bucket meters built from {@link TimingMeterImpl} unchanged or the
 * shipped {@link BucketMeterSet}, plus the lookup candidates and the histogram copy check. It runs
 * as a plain program on the test classpath, not as part of the test suite:
 *
 * <pre>
 * java -Xms8g -Xmx8g -XX:+UseZGC -cp &lt;test runtime classpath&gt; com.dell.spt.perf.BucketMetricsBench \
 *     [record|lookup|copy|all] [rounds] [roundMillis] [warmRounds] [threadsFilter]
 * </pre>
 *
 * <p>Arms run interleaved within a case, in a rotating order, so machine drift hits all of them.
 * CPU time is each worker thread's own, so it holds when the machine has fewer CPUs than threads,
 * though the contention it sees then is not the contention of the target machine.
 */
public final class BucketMetricsBench {

	private static final int SAMPLES = 1 << 16;
	private static final int STOP_CHECK_MASK = 1023;
	private static final long BYTES = 10_240;
	private static final String[] ARMS = {"aggregate", "timing", "atomic"
	};
	private static final List<GarbageCollectorMXBean> GC_BEANS = ManagementFactory.getGarbageCollectorMXBeans();
	private static final com.sun.management.ThreadMXBean THREADS = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
	private static long sink;

	private record BenchCase(String name, int buckets, int threads) {}

	private record RoundResult(double cpuNsPerOp, double opsPerSec, double allocBytesPerOp, long gcMillis) {}

	private BucketMetricsBench() {}

	public static void main(final String... args) throws Exception {
		final String mode = args.length > 0 ? args[0] : "all";
		final int rounds = args.length > 1 ? Integer.parseInt(args[1]) : 15;
		final long roundMillis = args.length > 2 ? Long.parseLong(args[2]) : 2000;
		final int warmRounds = args.length > 3 ? Integer.parseInt(args[3]) : 5;
		final int threadsFilter = args.length > 4 ? Integer.parseInt(args[4]) : 0;
		printEnvironment(rounds, roundMillis, warmRounds);
		if ("copy".equals(mode) || "all".equals(mode)) {
			copyCheck();
		}
		if ("lookup".equals(mode) || "all".equals(mode)) {
			lookup(rounds, roundMillis, warmRounds);
		}
		if ("record".equals(mode) || "all".equals(mode)) {
			final List<BenchCase> cases = List.of(
							new BenchCase("uncontended", 2, 1),
							new BenchCase("contended", 2, 96),
							new BenchCase("contended", 4, 96),
							new BenchCase("contended", 16, 96),
							new BenchCase("random", 1000, 8),
							new BenchCase("random", 1000, 32),
							new BenchCase("random", 1000, 96));
			System.out.println("kind,case,N,threads,arm,cpu_ns_per_op,cpu_ns_iqr_low,cpu_ns_iqr_high,"
							+ "added_ns_median,added_ns_iqr_low,added_ns_iqr_high,share_of_aggregate_pct,"
							+ "ops_per_sec,alloc_bytes_per_op,gc_pause_ms_per_round,heap_used_mb");
			for (final BenchCase benchCase : cases) {
				if (threadsFilter == 0 || benchCase.threads <= threadsFilter) {
					record(benchCase, rounds, roundMillis, warmRounds);
				}
			}
		}
		System.out.println("# checksum " + sink);
	}

	private static void printEnvironment(final int rounds, final long roundMillis, final int warmRounds) {
		System.out.println("# " + System.getProperty("java.vm.name") + " " + System.getProperty("java.version")
						+ ", " + System.getProperty("os.name") + " " + System.getProperty("os.arch")
						+ ", CPUs=" + Runtime.getRuntime().availableProcessors()
						+ ", maxHeapMb=" + Runtime.getRuntime().maxMemory() / (1 << 20));
		System.out.println("# jvm " + ManagementFactory.getRuntimeMXBean().getInputArguments());
		System.out.println("# rounds=" + rounds + " roundMillis=" + roundMillis + " warmRounds=" + warmRounds
						+ " bucketRange=[1," + TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS + "]"
						+ " digits=" + TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
		final var concurrent = new org.HdrHistogram.ConcurrentHistogram(1L,
						TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
		final var atomic = new AtomicHistogram(1L,
						TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
		System.out.println("# footprint_bytes concurrent=" + concurrent.getEstimatedFootprintInBytes()
						+ " atomic=" + atomic.getEstimatedFootprintInBytes());
	}

	// --- recording ---

	private static final class TimingSet {
		final LongAdder succ = new LongAdder();
		final LongAdder fail = new LongAdder();
		final LongAdder corrupt = new LongAdder();
		final LongAdder bytes = new LongAdder();
		final TimingMeterImpl duration = new TimingMeterImpl("dur");
		final TimingMeterImpl latency = new TimingMeterImpl("lat");
		final TimingMeterImpl ttfb = new TimingMeterImpl("ttfb");

		void markSucc(final long byteCount, final long dur, final long lat, final long first) {
			succ.increment();
			bytes.add(byteCount);
			if (dur > 0) {
				duration.update(dur);
				if (lat > 0) {
					latency.update(lat);
				}
				if (first > 0 && first <= dur) {
					ttfb.update(first);
				}
			}
		}
	}

	private static final class Workload {
		final MetricsContext<?> aggregate;
		final BucketSlotTable table;
		final String[] paths;
		final TimingSet[] timingSets;
		final BucketMeterSet[] atomicSets;
		final long[] durations = new long[SAMPLES];
		final long[] latencies = new long[SAMPLES];
		final long[] ttfbs = new long[SAMPLES];
		volatile boolean stop;

		Workload(final int buckets) throws Exception {
			aggregate = newAggregate();
			final List<String> members = new ArrayList<>(buckets);
			paths = new String[buckets];
			for (int i = 0; i < buckets; i++) {
				final String name = String.format(Locale.ROOT, "spt-bucket-%04d", i);
				members.add(name);
				paths[i] = "/" + name;
			}
			table = new BucketSlotTable(members);
			timingSets = new TimingSet[buckets + 1];
			atomicSets = new BucketMeterSet[buckets + 1];
			for (int i = 0; i <= buckets; i++) {
				timingSets[i] = new TimingSet();
				atomicSets[i] = new BucketMeterSet();
			}
			final Random random = new Random(42);
			for (int i = 0; i < SAMPLES; i++) {
				final long duration = (long) Math.exp(random.nextGaussian() * 0.8 + 7.5) + 1;
				durations[i] = duration;
				latencies[i] = 1 + duration / 2;
				ttfbs[i] = 1 + duration / 3;
			}
		}
	}

	private static MetricsContext<?> newAggregate() throws Exception {
		final MetricsContext<?> ctx = MetricsContextImpl.builder()
						.loadStepId("bucket-metrics-bench")
						.runId(1L)
						.opType(OpType.CREATE)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(BYTES))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
		ctx.start();
		return ctx;
	}

	private static long aggregateLoop(final Workload w, final long seed) {
		long ops = 0;
		final long[] durations = w.durations;
		final long[] latencies = w.latencies;
		final long[] ttfbs = w.ttfbs;
		long state = seed;
		while (true) {
			state ^= state << 13;
			state ^= state >>> 7;
			state ^= state << 17;
			final int sample = (int) state & (SAMPLES - 1);
			w.aggregate.markSucc(BYTES, durations[sample], latencies[sample], ttfbs[sample]);
			if ((++ops & STOP_CHECK_MASK) == 0 && w.stop) {
				return ops;
			}
		}
	}

	private static long timingLoop(final Workload w, final long seed) {
		long ops = 0;
		final long[] durations = w.durations;
		final long[] latencies = w.latencies;
		final long[] ttfbs = w.ttfbs;
		final String[] paths = w.paths;
		final TimingSet[] sets = w.timingSets;
		long state = seed;
		while (true) {
			state ^= state << 13;
			state ^= state >>> 7;
			state ^= state << 17;
			final int sample = (int) state & (SAMPLES - 1);
			final int bucket = (int) ((state >>> 20) % paths.length);
			w.aggregate.markSucc(BYTES, durations[sample], latencies[sample], ttfbs[sample]);
			sets[w.table.slotOfPath(paths[bucket < 0 ? -bucket : bucket])]
							.markSucc(BYTES, durations[sample], latencies[sample], ttfbs[sample]);
			if ((++ops & STOP_CHECK_MASK) == 0 && w.stop) {
				return ops;
			}
		}
	}

	private static long atomicLoop(final Workload w, final long seed) {
		long ops = 0;
		final long[] durations = w.durations;
		final long[] latencies = w.latencies;
		final long[] ttfbs = w.ttfbs;
		final String[] paths = w.paths;
		final BucketMeterSet[] sets = w.atomicSets;
		long state = seed;
		while (true) {
			state ^= state << 13;
			state ^= state >>> 7;
			state ^= state << 17;
			final int sample = (int) state & (SAMPLES - 1);
			final int bucket = (int) ((state >>> 20) % paths.length);
			w.aggregate.markSucc(BYTES, durations[sample], latencies[sample], ttfbs[sample]);
			sets[w.table.slotOfPath(paths[bucket < 0 ? -bucket : bucket])]
							.markSucc(BYTES, durations[sample], latencies[sample], ttfbs[sample]);
			if ((++ops & STOP_CHECK_MASK) == 0 && w.stop) {
				return ops;
			}
		}
	}

	private static RoundResult round(
					final Workload w, final String arm, final int threadCount, final long roundMillis) throws Exception {
		w.stop = false;
		final long[] threadOps = new long[threadCount];
		final long[] threadCpu = new long[threadCount];
		final long[] threadAlloc = new long[threadCount];
		final CyclicBarrier start = new CyclicBarrier(threadCount + 1);
		final Thread[] workers = new Thread[threadCount];
		for (int i = 0; i < threadCount; i++) {
			final int index = i;
			workers[i] = new Thread(() -> {
				try {
					start.await();
					final long id = Thread.currentThread().threadId();
					final long allocBefore = THREADS.getThreadAllocatedBytes(id);
					final long cpuBefore = THREADS.getCurrentThreadCpuTime();
					final long seed = 0x9E3779B97F4A7C15L * (index + 1) + System.nanoTime();
					final long ops = switch (arm) {
					case "aggregate" -> aggregateLoop(w, seed);
					case "timing" -> timingLoop(w, seed);
					default -> atomicLoop(w, seed);
					};
					threadCpu[index] = THREADS.getCurrentThreadCpuTime() - cpuBefore;
					threadAlloc[index] = THREADS.getThreadAllocatedBytes(id) - allocBefore;
					threadOps[index] = ops;
				} catch (final Exception e) {
					throw new IllegalStateException(e);
				}
			}, "bench-" + i);
			workers[i].start();
		}
		final long gcBefore = gcMillis();
		final long startedAt = System.nanoTime();
		start.await();
		Thread.sleep(roundMillis);
		w.stop = true;
		for (final Thread worker : workers) {
			worker.join();
		}
		final double seconds = (System.nanoTime() - startedAt) / 1e9;
		final long ops = Arrays.stream(threadOps).sum();
		final long cpu = Arrays.stream(threadCpu).sum();
		final long alloc = Arrays.stream(threadAlloc).sum();
		return new RoundResult((double) cpu / ops, ops / seconds, (double) alloc / ops, gcMillis() - gcBefore);
	}

	private static long gcMillis() {
		return GC_BEANS.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
	}

	private static void record(
					final BenchCase c, final int rounds, final long roundMillis, final int warmRounds) throws Exception {
		final Workload w = new Workload(c.buckets);
		System.gc();
		final long heapUsedMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1 << 20);
		for (int warm = 0; warm < warmRounds; warm++) {
			for (final String arm : ARMS) {
				round(w, arm, c.threads, roundMillis);
			}
		}
		final Map<String, List<RoundResult>> results = new HashMap<>();
		final Map<String, List<Double>> added = new HashMap<>();
		for (final String arm : ARMS) {
			results.put(arm, new ArrayList<>());
			added.put(arm, new ArrayList<>());
		}
		for (int r = 0; r < rounds; r++) {
			final Map<String, RoundResult> thisRound = new HashMap<>();
			for (int a = 0; a < ARMS.length; a++) {
				final String arm = ARMS[(a + r) % ARMS.length];
				thisRound.put(arm, round(w, arm, c.threads, roundMillis));
			}
			final double base = thisRound.get("aggregate").cpuNsPerOp;
			for (final String arm : ARMS) {
				results.get(arm).add(thisRound.get(arm));
				added.get(arm).add(thisRound.get(arm).cpuNsPerOp - base);
			}
		}
		final double baseCpu = quantile(results.get("aggregate").stream().map(RoundResult::cpuNsPerOp).toList(), 0.5);
		for (final String arm : ARMS) {
			final List<RoundResult> rs = results.get(arm);
			final List<Double> cpu = rs.stream().map(RoundResult::cpuNsPerOp).toList();
			final double addedMedian = quantile(added.get(arm), 0.5);
			System.out.printf(Locale.ROOT,
							"record,%s,%d,%d,%s,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.0f,%.3f,%.1f,%d%n",
							c.name, c.buckets, c.threads, arm,
							quantile(cpu, 0.5), quantile(cpu, 0.25), quantile(cpu, 0.75),
							addedMedian, quantile(added.get(arm), 0.25), quantile(added.get(arm), 0.75),
							100 * addedMedian / baseCpu,
							quantile(rs.stream().map(RoundResult::opsPerSec).toList(), 0.5),
							quantile(rs.stream().map(RoundResult::allocBytesPerOp).toList(), 0.5),
							quantile(rs.stream().map(x -> (double) x.gcMillis).toList(), 0.5),
							heapUsedMb);
		}
		long recorded = 0;
		for (final TimingSet set : w.timingSets) {
			recorded += set.succ.sum() + set.fail.sum() + set.corrupt.sum();
		}
		sink += recorded;
		w.aggregate.close();
	}

	// --- lookup ---

	private static final class FakeOp {
		final String dstPath;
		final String srcPath;
		final int slot;

		FakeOp(final String dstPath, final String srcPath, final int slot) {
			this.dstPath = dstPath;
			this.srcPath = srcPath;
			this.slot = slot;
		}
	}

	private interface LookupArm {
		int slot(FakeOp op);
	}

	private static void lookup(final int rounds, final long roundMillis, final int warmRounds) {
		System.out.println("kind,N,candidate,path_form,ns_per_op,iqr_low,iqr_high");
		for (final int buckets : new int[]{2, 16, 1000
		}) {
			final List<String> members = new ArrayList<>(buckets);
			final String[] memberPaths = new String[buckets];
			final Map<String, Integer> byPath = new HashMap<>();
			for (int i = 0; i < buckets; i++) {
				final String name = String.format(Locale.ROOT, "spt-bucket-%04d", i);
				members.add(name);
				memberPaths[i] = "/" + name;
				byPath.put(memberPaths[i], i);
			}
			final BucketSlotTable table = new BucketSlotTable(members);
			final Random random = new Random(7);
			final FakeOp[] newObjects = new FakeOp[SAMPLES];
			final FakeOp[] existing = new FakeOp[SAMPLES];
			for (int i = 0; i < SAMPLES; i++) {
				final int member = random.nextInt(buckets);
				newObjects[i] = new FakeOp(memberPaths[member], null, member);
				// Reads, deletes and stats: the source path is split from each recorded name.
				final String split = new String(memberPaths[member] + "/s000000" + (i % 8));
				existing[i] = new FakeOp(split, split, -1);
			}
			final Map<String, LookupArm> candidates = new java.util.LinkedHashMap<>();
			candidates.put("field-read-baseline", op -> op.slot);
			candidates.put("first-segment-hash", op -> table.slotOfPath(op.dstPath));
			candidates.put("stamped-slot-else-hash", op -> op.slot >= 0 ? op.slot : table.slotOfPath(op.dstPath));
			candidates.put("path-map-else-hash", op -> {
				final Integer slot = byPath.get(op.dstPath);
				return slot != null ? slot : table.slotOfPath(op.dstPath);
			});
			for (final var candidate : candidates.entrySet()) {
				for (final var form : List.of(Map.entry("member-path", newObjects), Map.entry("split-path", existing))) {
					for (int warm = 0; warm < warmRounds; warm++) {
						lookupRound(form.getValue(), candidate.getValue(), roundMillis);
					}
					final List<Double> ns = new ArrayList<>(rounds);
					for (int r = 0; r < rounds; r++) {
						ns.add(lookupRound(form.getValue(), candidate.getValue(), roundMillis));
					}
					System.out.printf(Locale.ROOT, "lookup,%d,%s,%s,%.2f,%.2f,%.2f%n", buckets, candidate.getKey(),
									form.getKey(), quantile(ns, 0.5), quantile(ns, 0.25), quantile(ns, 0.75));
				}
			}
		}
	}

	private static double lookupRound(final FakeOp[] ops, final LookupArm arm, final long roundMillis) {
		final long deadline = System.nanoTime() + roundMillis * 1_000_000L;
		final long started = System.nanoTime();
		long calls = 0;
		long sum = 0;
		long now;
		do {
			for (final FakeOp op : ops) {
				sum += arm.slot(op);
			}
			calls += ops.length;
			now = System.nanoTime();
		} while (now < deadline);
		sink += sum;
		return (double) (now - started) / calls;
	}

	// --- copy accuracy ---

	/**
	 * Copies an {@link AtomicHistogram} and the current {@code ConcurrentHistogram} while writers run a
	 * known uniform distribution, and again after they stop. A copy must be consistent with itself,
	 * must lose nothing once writers have stopped, and its quantiles must sit within the engine's two
	 * significant digits, plus an allowance for sampling.
	 */
	private static void copyCheck() throws Exception {
		final long low = 1_000;
		final long high = 100_000;
		final double tolerance = 0.015;
		final int writers = Math.max(4, Runtime.getRuntime().availableProcessors());
		final BucketTimingMeter atomic = new BucketTimingMeter("copy");
		final TimingMeterImpl concurrent = new TimingMeterImpl("copy");
		final LongAdder written = new LongAdder();
		final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
		final Thread[] threads = new Thread[writers];
		for (int i = 0; i < writers; i++) {
			final long seed = 31L * (i + 1);
			threads[i] = new Thread(() -> {
				final Random random = new Random(seed);
				while (!stop.get()) {
					final long value = low + (long) (random.nextDouble() * (high - low));
					atomic.update(value);
					concurrent.update(value);
					written.increment();
				}
			});
			threads[i].start();
		}
		// A live copy reads low buckets before high ones, so its skew is the copy's duration over the
		// time recorded. Early rounds, with little recorded and a cold JIT, show that skew; they are
		// reported apart and do not decide the check, because a steady window is far longer than a copy.
		boolean pass = true;
		long atomicCopyNanos = 0;
		long concurrentCopyNanos = 0;
		final int coldRounds = 5;
		final int checkedRounds = 30;
		for (int round = 0; round < coldRounds + checkedRounds; round++) {
			Thread.sleep(round < coldRounds ? 100 : 500);
			final boolean checked = round >= coldRounds;
			final long atomicStart = System.nanoTime();
			final Histogram fromAtomic = atomic.histogramCopy();
			final long concurrentStart = System.nanoTime();
			final Histogram fromConcurrent = concurrent.histogramCopy();
			final long concurrentEnd = System.nanoTime();
			final boolean last = round == coldRounds + checkedRounds - 1;
			final boolean atomicOk = checkCopy((checked ? "" : "cold-") + "atomic-live", fromAtomic, low, high, tolerance, last);
			final boolean concurrentOk = checkCopy(
							(checked ? "" : "cold-") + "concurrent-live", fromConcurrent, low, high, tolerance, last);
			if (checked) {
				pass &= atomicOk && concurrentOk;
				atomicCopyNanos += concurrentStart - atomicStart;
				concurrentCopyNanos += concurrentEnd - concurrentStart;
			}
		}
		System.out.printf(Locale.ROOT, "copy,mean_copy_micros,atomic=%.1f,concurrent=%.1f%n",
						atomicCopyNanos / 1e3 / checkedRounds, concurrentCopyNanos / 1e3 / checkedRounds);
		stop.set(true);
		for (final Thread thread : threads) {
			thread.join();
		}
		final Histogram finalAtomic = atomic.histogramCopy();
		final boolean exact = finalAtomic.getTotalCount() == written.sum()
						&& concurrent.histogramCopy().getTotalCount() == written.sum();
		pass &= exact && checkCopy("atomic-final", finalAtomic, low, high, tolerance, true);
		System.out.println("copy,quiesced_counts_exact=" + exact + ",writers=" + writers + ",written=" + written.sum());
		System.out.println("copy,result=" + (pass ? "PASS" : "FAIL"));
	}

	private static boolean checkCopy(
					final String name, final Histogram copy, final long low, final long high,
					final double tolerance, final boolean print) {
		long buckets = 0;
		for (final var value : copy.recordedValues()) {
			buckets += value.getCountAtValueIteratedTo();
		}
		boolean ok = buckets == copy.getTotalCount() && copy.getTotalCount() > 0;
		final StringBuilder line = new StringBuilder("copy," + name + ",total=" + copy.getTotalCount()
						+ ",self_consistent=" + (buckets == copy.getTotalCount()));
		for (final double percentile : new double[]{50, 90, 99, 99.9
		}) {
			final double expected = low + (high - low) * percentile / 100;
			final double actual = copy.getValueAtPercentile(percentile);
			final double error = Math.abs(actual - expected) / expected;
			ok &= error <= tolerance;
			line.append(String.format(Locale.ROOT, ",p%s_err_pct=%.3f", percentile, 100 * error));
		}
		if (print || !ok) {
			System.out.println(line);
		}
		return ok;
	}

	private static double quantile(final List<Double> values, final double q) {
		final double[] sorted = values.stream().mapToDouble(Double::doubleValue).toArray();
		Arrays.sort(sorted);
		return sorted[(int) Math.round(q * (sorted.length - 1))];
	}
}
