package com.dell.spt.perf;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.io.IntegrityOperationManifestOutput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.storage.Credential;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Hand-written benchmark of the canonical integrity-manifest writer's per-result cost, time and
 * allocated bytes, for the multi-bucket Class B record. It runs as a plain program on the test
 * classpath against any build:
 *
 * <pre>
 * java -cp &lt;test runtime classpath&gt; com.dell.spt.perf.ManifestWriterBench [rounds] [resultsPerRound]
 * </pre>
 *
 * <p>Case {@code one-bucket} writes results under the configured bucket. Case {@code members}
 * writes results whose destinations are four other buckets, as a target bucket set produces.
 */
public final class ManifestWriterBench {

	private ManifestWriterBench() {}

	public static void main(final String... args) throws Exception {
		final int rounds = args.length > 0 ? Integer.parseInt(args[0]) : 15;
		final int perRound = args.length > 1 ? Integer.parseInt(args[1]) : 200_000;
		final var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
		final Path dir = Files.createTempDirectory("manifest-bench");
		for (final String name : List.of("one-bucket", "members")) {
			final List<String> paths = "members".equals(name)
							? List.of("/bench-0/p", "/bench-1/p", "/bench-2/p", "/bench-3/p")
							: List.of("/bench/p");
			final List<Operation<DataItem>> results = new ArrayList<>(perRound);
			for (int i = 0; i < perRound; i++) {
				final String path = paths.get(i % paths.size());
				final var item = new DataItemImpl(path + "/" + Long.toString(0x3fffffffL * i, 36), i, 1024);
				results.add(new OperationImpl<>(0, OpType.CREATE, item, null, path, Credential.NONE));
			}
			// Warm-up: five rounds.
			for (int r = 0; r < 5; r++) {
				write(dir, name + "-warm-" + r, results, threads, Thread.currentThread().threadId());
			}
			final double[] nanos = new double[rounds];
			final double[] bytes = new double[rounds];
			final long thread = Thread.currentThread().threadId();
			for (int r = 0; r < rounds; r++) {
				final long[] measured = write(dir, name + "-" + r, results, threads, thread);
				nanos[r] = measured[0] / (double) perRound;
				bytes[r] = measured[1] / (double) perRound;
			}
			Arrays.sort(nanos);
			Arrays.sort(bytes);
			System.out.printf(Locale.ROOT, "%-12s median %7.1f ns/result  q1 %7.1f  q3 %7.1f   median %6.1f bytes/result%n",
							name, nanos[rounds / 2], nanos[rounds / 4], nanos[(3 * rounds) / 4], bytes[rounds / 2]);
		}
	}

	/** Returns the nanoseconds and allocated bytes of the put calls alone; opening and the closing sync are excluded. */
	private static long[] write(
					final Path dir,
					final String name,
					final List<Operation<DataItem>> results,
					final com.sun.management.ThreadMXBean threads,
					final long thread) throws Exception {
		final Path manifest = dir.resolve(name + ".csv");
		final long[] measured = new long[2];
		try (final var output = new IntegrityOperationManifestOutput<Operation<DataItem>>(manifest, "/bench", OpType.CREATE)) {
			final long allocatedBefore = threads.getThreadAllocatedBytes(thread);
			final long start = System.nanoTime();
			for (final var result : results) {
				output.put(result);
			}
			measured[0] = System.nanoTime() - start;
			measured[1] = threads.getThreadAllocatedBytes(thread) - allocatedBefore;
		}
		Files.delete(manifest);
		return measured;
	}
}
