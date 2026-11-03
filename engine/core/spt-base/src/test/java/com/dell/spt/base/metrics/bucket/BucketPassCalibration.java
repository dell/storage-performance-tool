package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ticket 03's calibration of the whole per-set pass and the memory model, on the real fence. It runs
 * as a plain program on the test classpath and is not part of the test suite:
 *
 * <pre>
 * java -Xmx4280m -XX:+UseZGC -cp &lt;test runtime classpath&gt; \
 *     com.dell.spt.base.metrics.bucket.BucketPassCalibration [members] [opTypes] [samplesPerSet] [writers] [phases]
 * </pre>
 */
public final class BucketPassCalibration {

	private static final OpType[] TYPES = {OpType.CREATE, OpType.READ, OpType.UPDATE, OpType.DELETE
	};
	private static final double MIB = 1L << 20;

	private BucketPassCalibration() {}

	public static void main(final String... args) throws Exception {
		final int members = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
		final int types = args.length > 1 ? Integer.parseInt(args[1]) : 4;
		final int samples = args.length > 2 ? Integer.parseInt(args[2]) : 2000;
		final int writers = args.length > 3 ? Integer.parseInt(args[3]) : 8;
		final int phases = args.length > 4 ? Integer.parseInt(args[4]) : 3;
		System.out.println("# " + System.getProperty("java.vm.name") + " " + System.getProperty("java.version")
						+ " CPUs=" + Runtime.getRuntime().availableProcessors()
						+ " maxHeapMiB=" + Runtime.getRuntime().maxMemory() / (1 << 20)
						+ " jvm=" + ManagementFactory.getRuntimeMXBean().getInputArguments());
		System.out.printf(Locale.ROOT, "# members=%d types=%d samplesPerSet=%d writers=%d estimatedSustainedMiB=%.0f%n",
						members, types, samples, writers,
						BucketMetricsBudget.sustainedBytes((long) types * (members + 1)) / MIB);
		System.out.println("phase,start_pass_ms,end_pass_ms,pass_median_us,pass_max_us,base_mib,live_mib,sustained_mib,"
						+ "peak_sampled_mib,retained_mib,encoded_mib,outcome");
		final double baseHeap = usedAfterGc();
		for (int phase = 0; phase < phases; phase++) {
			runPhase(phase, members, types, samples, writers, baseHeap);
		}
	}

	private static void runPhase(
					final int phase,
					final int members,
					final int types,
					final int samples,
					final int writers,
					final double baseHeap) throws Exception {
		final List<String> names = new ArrayList<>(members);
		for (int i = 0; i < members; i++) {
			names.add(String.format(Locale.ROOT, "spt-bucket-%04d", i));
		}
		final var fence = new BucketStepFence("calibration", TimeUnit.MINUTES.toNanos(5), 4, System::nanoTime);
		final List<BucketStepFence.ContextCuts> contexts = new ArrayList<>(types);
		for (int t = 0; t < types; t++) {
			contexts.add(fence.register(new BucketMeters(TYPES[t], names)));
		}
		populate(contexts, samples);
		final double live = usedAfterGc();

		final AtomicBoolean stop = new AtomicBoolean();
		final List<Thread> threads = new ArrayList<>();
		for (int w = 0; w < writers; w++) {
			final long seed = 97L * (w + 1) + phase;
			final Thread thread = new Thread(() -> {
				final Random random = new Random(seed);
				while (!stop.get()) {
					final var context = contexts.get(random.nextInt(contexts.size()));
					final var sets = context.meters().sets();
					final long duration = 100L + random.nextInt(100_000);
					sets[random.nextInt(sets.length)].markSucc(1024, duration, duration / 2, duration / 3);
				}
			}, "calibration-writer-" + w);
			thread.setDaemon(true);
			thread.start();
			threads.add(thread);
		}

		final long startBegin = System.nanoTime();
		for (final var context : contexts) {
			context.onWindowStart(startBegin);
		}
		for (final var context : contexts) {
			while (!context.startPassDone()) {
				Thread.sleep(1);
			}
		}
		final long startMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startBegin);
		final double sustained = usedAfterGc();

		Thread.sleep(2_000);
		final AtomicLong peak = new AtomicLong();
		final AtomicBoolean sampling = new AtomicBoolean(true);
		final Thread sampler = new Thread(() -> {
			while (sampling.get()) {
				final Runtime runtime = Runtime.getRuntime();
				peak.accumulateAndGet(runtime.totalMemory() - runtime.freeMemory(), Math::max);
				try {
					Thread.sleep(5);
				} catch (final InterruptedException e) {
					return;
				}
			}
		}, "calibration-sampler");
		sampler.setDaemon(true);
		sampler.start();

		final long endBegin = System.nanoTime();
		for (final var context : contexts) {
			context.onWindowClose(endBegin, true, null);
		}
		fence.await();
		final long endMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - endBegin);
		sampling.set(false);
		sampler.join();
		stop.set(true);
		for (final Thread thread : threads) {
			thread.join();
		}
		fence.close();

		final List<Long> passes = new ArrayList<>();
		long encoded = 0;
		String outcome = "MEASURED";
		for (final var context : contexts) {
			final var result = context.outcome();
			if (result.kind() != BucketSteadyOutcome.Kind.MEASURED) {
				outcome = result.kind() + ":" + result.reason();
				continue;
			}
			for (final var record : result.records()) {
				passes.add(record.passNanos());
				final var values = record.values();
				encoded += values.duration().compressedHistogram().length
								+ values.latency().compressedHistogram().length
								+ values.timeToFirstByte().compressedHistogram().length;
			}
		}
		passes.sort(Long::compare);
		final long median = passes.isEmpty() ? 0 : passes.get(passes.size() / 2);
		final long max = passes.isEmpty() ? 0 : passes.get(passes.size() - 1);
		final List<BucketSteadyOutcome> retainedOutcomes = new ArrayList<>();
		for (final var context : contexts) {
			retainedOutcomes.add(context.outcome());
		}
		contexts.clear();
		final double retained = usedAfterGc() - baseHeap;
		System.out.printf(Locale.ROOT, "%d,%d,%d,%.1f,%.1f,%.0f,%.0f,%.0f,%.0f,%.1f,%.1f,%s%n",
						phase, startMillis, endMillis, median / 1e3, max / 1e3, baseHeap,
						live - baseHeap, sustained - baseHeap, peak.get() / MIB - baseHeap, retained, encoded / MIB, outcome);
		if (retainedOutcomes.isEmpty()) {
			throw new IllegalStateException();
		}
	}

	private static void populate(final List<BucketStepFence.ContextCuts> contexts, final int samples) {
		final Random random = new Random(1);
		for (final var context : contexts) {
			for (final BucketMeterSet set : context.meters().sets()) {
				for (int i = 0; i < samples; i++) {
					final long duration = 100 + (long) Math.exp(random.nextGaussian() * 1.2 + 8);
					set.markSucc(1024, duration, duration / 2, duration / 3);
				}
			}
		}
	}

	private static double usedAfterGc() throws InterruptedException {
		for (int i = 0; i < 3; i++) {
			System.gc();
			Thread.sleep(150);
		}
		final Runtime runtime = Runtime.getRuntime();
		return (runtime.totalMemory() - runtime.freeMemory()) / MIB;
	}
}
