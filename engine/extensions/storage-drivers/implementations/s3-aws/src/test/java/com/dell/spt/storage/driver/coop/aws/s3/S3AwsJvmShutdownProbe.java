package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The child JVM of {@link S3AwsJvmShutdownTest}: a driver the factory builds has a PUT in flight
 * when JVM shutdown begins, by {@code System.exit} or by a signal, and the loopback target answers
 * it only after shutdown has begun. A shutdown hook then waits for the result, as the engine's
 * stop does, and closes the driver.
 *
 * <p>Arguments: {@code exit} or {@code signal}, then an empty directory for the engine's home.
 * Lines starting with {@link #PREFIX} report what happened.
 */
public final class S3AwsJvmShutdownProbe {

	static final String PREFIX = "PROBE ";
	static final String READY = PREFIX + "READY";
	static final String RESULT = PREFIX + "RESULT ";
	static final String CLOSED = PREFIX + "CLOSED";
	static final String KEY = "jvm-shutdown-put";

	/** How long the hook gives the CRT's own shutdown hook to run before the target answers. */
	private static final long CRT_HOOK_HEAD_START_MS = 500;
	/** How long the hook waits for the answered request: the engine's default stop grace. */
	private static final Duration RESULT_BUDGET = Duration.ofSeconds(5);
	private static final Duration REQUEST_BUDGET = Duration.ofSeconds(30);
	/** How long a probe in signal mode waits for its signal before it exits on its own. */
	private static final Duration SIGNAL_BUDGET = REQUEST_BUDGET.multipliedBy(2);

	private S3AwsJvmShutdownProbe() {}

	public static void main(final String[] args) throws Exception {
		final boolean waitForSignal = "signal".equals(args[0]);
		final Path home = Path.of(args[1]);
		final var received = new CountDownLatch(1);
		final var answer = new CountDownLatch(1);
		final HttpServer target = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
		target.createContext("/", exchange -> {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				if ("PUT".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getPath().endsWith("/" + KEY)) {
					received.countDown();
					answer.await(REQUEST_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
				}
				exchange.getResponseHeaders().add("ETag", "\"0\"");
				exchange.sendResponseHeaders(200, -1);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		target.setExecutor(Executors.newCachedThreadPool(task -> {
			final Thread thread = new Thread(task, "probe-target");
			thread.setDaemon(true);
			return thread;
		}));
		target.start();

		final S3AwsStorageDriver<DataItem, DataOperation<DataItem>> driver = S3AwsDriverHarness.driver(
						home, target.getAddress().getPort(), Map.of());
		final DataOperationImpl<DataItem> put = S3AwsDriverHarness.putOp(KEY);
		final var started = S3AwsDriverHarness.Started.<DataItem, DataOperation<DataItem>> start(driver, put);
		if (!received.await(REQUEST_BUDGET.toMillis(), TimeUnit.MILLISECONDS)) {
			report(PREFIX + "the target never got the PUT");
			System.exit(2);
		}

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				Thread.sleep(CRT_HOOK_HEAD_START_MS);
				answer.countDown();
				String outcome;
				try {
					outcome = String.valueOf(started.await(RESULT_BUDGET).status());
				} catch (final AssertionError notDone) {
					outcome = "none within " + RESULT_BUDGET;
				}
				report(RESULT + outcome);
				started.close();
				report(CLOSED);
			} catch (final Exception | Error e) {
				report(PREFIX + "hook failed: " + e);
			}
		}, "probe-shutdown-hook"));

		report(READY);
		if (waitForSignal) {
			// Bounded, so a probe whose test worker died does not stay on the host.
			Thread.sleep(SIGNAL_BUDGET.toMillis());
			report(PREFIX + "no signal within " + SIGNAL_BUDGET);
			System.exit(3);
		}
		System.exit(0);
	}

	private static void report(final String line) {
		System.out.println(line);
		System.out.flush();
	}
}
