package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cold-start characterization of the Netty driver's first-use bucket checks for a large target
 * bucket set, for the multi-bucket qualification. A loopback listener answers every bucket HEAD
 * after a fixed delay, set to the latency measured against the lab target, and every object
 * request at once. One driver instance runs {@code ops} CREATEs spread over {@code members}
 * buckets and over one bucket; the difference is the cost of the first-use checks.
 *
 * <pre>
 * java -cp &lt;s3 test runtime classpath&gt; com.dell.spt.storage.driver.coop.netty.http.s3.FirstUseColdStartBench \
 *     [members] [headDelayMicros] [ops] [concurrency]
 * </pre>
 */
public final class FirstUseColdStartBench {

	private FirstUseColdStartBench() {}

	public static void main(final String... args) throws Exception {
		final int members = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
		final long headDelayMicros = args.length > 1 ? Long.parseLong(args[1]) : 1000;
		final int ops = args.length > 2 ? Integer.parseInt(args[2]) : 4 * members;
		final int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : 8;
		final AtomicLong heads = new AtomicLong();
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.setExecutor(Executors.newFixedThreadPool(64));
		server.createContext("/", exchange -> {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				if ("HEAD".equals(exchange.getRequestMethod())) {
					heads.incrementAndGet();
					try {
						TimeUnit.MICROSECONDS.sleep(headDelayMicros);
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				} else {
					exchange.getResponseHeaders().set("ETag", "\"e\"");
				}
				exchange.sendResponseHeaders(200, -1);
			}
		});
		server.start();
		try {
			for (final int count : new int[]{1, members
			}) {
				heads.set(0);
				final long nanos = run(server.getAddress().getPort(), count, ops, concurrency);
				System.out.printf(Locale.ROOT, "members %4d  ops %6d  bucket HEADs %4d  elapsed %8.1f ms%n",
								count, ops, heads.get(), nanos / 1e6);
			}
		} finally {
			server.stop(0);
			((java.util.concurrent.ExecutorService) server.getExecutor()).shutdownNow();
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static long run(final int port, final int count, final int ops, final int concurrency) throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", concurrency);
		config.val("storage-net-timeoutMilliSec", 30_000);
		config.val("storage-net-node-port", port);
		final var builder = new DataOperationsBuilderImpl(0);
		builder.opType(OpType.CREATE);
		builder.outputPathInput(new ConstantValueInputImpl<>("/bench"));
		builder.credentialInput(new ConstantValueInputImpl<>(EndpointSelectionTestSupport.CREDENTIAL));
		builder.sizeThreshold(Long.MAX_VALUE);
		if (count > 1) {
			builder.targetBucketMembers(TargetBucketSet.memberPaths("/bench", count), 0);
		}
		try (final var driver = (StorageDriver<DataItem, Operation<DataItem>>) new S3StorageDriverExtension().create(
						"cold-start", new SeedDataInput(1, 1024, 1, true), config.configVal("storage"), false, 64)) {
			final var results = new EndpointSelectionTestSupport.Results();
			driver.operationResultOutput((com.github.akurilov.commons.io.Output) results);
			driver.start();
			final long start = System.nanoTime();
			int submitted = 0;
			int completed = 0;
			while (completed < ops) {
				while (submitted < ops && submitted - completed < 2 * concurrency) {
					final var op = (Operation<DataItem>) builder.buildOp(new DataItemImpl("k" + submitted, submitted, 1024));
					if (!driver.put(op)) {
						break;
					}
					submitted++;
				}
				if (results.poll(100) != null) {
					completed++;
				}
			}
			return System.nanoTime() - start;
		}
	}
}
