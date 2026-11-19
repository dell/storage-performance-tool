package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.concurrent.DaemonRegistryView;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.base.storage.driver.bucket.TargetBucketProvisioning;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;

/** A real S3 driver whose constructor throws gives back its event loops, through the driver factory. */
@Timeout(120)
class S3FailedConstructionTest {

	private static final int CONSTRUCTIONS = 5;
	private static final int IO_WORKERS = 2;
	/** Well under what one leaked group holds; room for a descriptor the JVM opens meanwhile. */
	private static final long DESCRIPTOR_TOLERANCE = 2;

	private static final List<Extension> EXTENSIONS = List.of(new S3StorageDriverExtension<>());

	private static Config storage(final int authVersion, final String transport) {
		final var root = S3StorageDriverTest.baseConfig(false, authVersion, false, null, "127.0.0.1");
		root.val("storage-net-transport", transport);
		root.val("storage-driver-threads", IO_WORKERS);
		root.val("storage-net-timeoutMilliSec", 1_000);
		return root.configVal("storage");
	}

	private static StorageDriver<DataItem, Operation<DataItem>> instance(final Config storage, final String stepId)
					throws InterruptedException {
		return StorageDriver.instance(EXTENSIONS, storage, null, false, 32, stepId);
	}

	private static long openDescriptors() {
		try (final var entries = Files.list(Path.of("/proc/self/fd"))) {
			return entries.count();
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private static long ioWorkerThreads() {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(thread -> thread.isAlive() && thread.getName().startsWith("ioWorker"))
						.count();
	}

	/** A closed group's last thread can outlive its termination future by a moment. */
	private static long settleTo(final long limit, final LongSupplier count) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		long value = count.getAsLong();
		while (value > limit && System.nanoTime() < deadline) {
			Thread.sleep(20);
			value = count.getAsLong();
		}
		return value;
	}

	private static boolean driverRegistered(final String stepId) {
		return DaemonRegistryView.isRegistered(
						daemon -> daemon instanceof S3StorageDriver && stepId.equals(stepIdOf(daemon)));
	}

	/** The driver's string form does not name its step. */
	private static String stepIdOf(final Object driver) {
		try {
			final var field = StorageDriverBase.class.getDeclaredField("stepId");
			field.setAccessible(true);
			return (String) field.get(driver);
		} catch (final ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}

	/** The same configuration refused through {@code attempt}, repeatedly, leaves nothing behind. */
	private static void refusedConstructionsLeaveNothing(final String stepId, final Executable attempt)
					throws Exception {
		// The first failure loads classes and opens what the JVM then keeps.
		assertThrows(AssertionError.class, attempt);
		final long descriptors = openDescriptors();
		final long threads = ioWorkerThreads();

		for (int i = 0; i < CONSTRUCTIONS; i++) {
			final var failure = assertThrows(AssertionError.class, attempt);
			assertTrue(failure.getMessage().contains("Only 2 and 4 versions are supported"), failure.toString());
		}

		final long descriptorsAfter = settleTo(descriptors + DESCRIPTOR_TOLERANCE, S3FailedConstructionTest::openDescriptors);
		assertTrue(
						descriptorsAfter <= descriptors + DESCRIPTOR_TOLERANCE,
						() -> "open descriptors went from " + descriptors + " to " + descriptorsAfter);
		final long threadsAfter = settleTo(threads, S3FailedConstructionTest::ioWorkerThreads);
		assertTrue(threadsAfter <= threads, () -> "I/O threads went from " + threads + " to " + threadsAfter);
		assertFalse(driverRegistered(stepId));
	}

	private static void refusedThroughTheFactory(final String transport) throws Exception {
		final var stepId = "failed-construction-" + transport;
		final var storage = storage(3, transport);
		refusedConstructionsLeaveNothing(stepId, () -> instance(storage, stepId));
	}

	/** The target-bucket control client builds its driver without going through the factory call. */
	private static void refusedThroughTargetBucketControl(final String transport) throws Exception {
		final var stepId = "failed-control-" + transport;
		final Config config = S3StorageDriverTest.baseConfig(false, 3, false, null, "127.0.0.1");
		config.val("storage-driver-type", new S3StorageDriverExtension<>().id());
		config.val("storage-net-transport", transport);
		config.val("storage-driver-threads", IO_WORKERS);
		config.val("storage-net-timeoutMilliSec", 1_000);
		config.val("storage-targetBucket-provision", true);
		config.val("storage-auth-file", null);
		config.val("load-op-type", "create");
		config.val("item-type", "data");
		config.val("item-output-path", "/control");
		config.val("item-input-path", null);
		config.val("item-input-file", null);
		config.val("load-step-id", stepId);
		refusedConstructionsLeaveNothing(
						stepId, () -> TargetBucketProvisioning.provision(stepId, config, null, EXTENSIONS));
	}

	@Test
	void refusedConstructionsOnEpollLeaveNoDescriptorThreadOrRegistration() throws Exception {
		assumeTrue(Epoll.isAvailable(), "epoll is not available here");
		refusedThroughTheFactory("epoll");
	}

	@Test
	void refusedConstructionsOnNioLeaveNoDescriptorThreadOrRegistration() throws Exception {
		refusedThroughTheFactory("nio");
	}

	@Test
	void aRefusedControlClientOnEpollLeavesNoDescriptorThreadOrRegistration() throws Exception {
		assumeTrue(Epoll.isAvailable(), "epoll is not available here");
		refusedThroughTargetBucketControl("epoll");
	}

	@Test
	void aRefusedControlClientOnNioLeavesNoDescriptorThreadOrRegistration() throws Exception {
		refusedThroughTargetBucketControl("nio");
	}

	/** Live threads running an asynchronous expression, the highest of a few samples. */
	private static long expressionWorkers() {
		long max = 0;
		for (int i = 0; i < 5; i++) {
			final long count = Thread.getAllStackTraces().entrySet().stream()
							.filter(entry -> entry.getKey().isAlive() && Arrays.stream(entry.getValue())
											.anyMatch(frame -> frame.getClassName().endsWith(".AsyncExpressionInputImpl")))
							.count();
			max = Math.max(max, count);
			try {
				Thread.sleep(5);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		return max;
	}

	@Test
	void aRefusedDriverStopsItsUriArgumentsExpressionWorker() throws Exception {
		final var stepId = "failed-uri-args";
		final var storage = storage(3, "nio");
		storage.val("net-http-uri-args", Map.of("probe", "#{this.expr()}"));
		assertThrows(AssertionError.class, () -> instance(storage, stepId));
		final long workers = settleTo(0, S3FailedConstructionTest::expressionWorkers);
		for (int i = 0; i < CONSTRUCTIONS; i++) {
			assertThrows(AssertionError.class, () -> instance(storage, stepId));
		}
		final long workersAfter = settleTo(workers, S3FailedConstructionTest::expressionWorkers);
		assertTrue(workersAfter <= workers, () -> "expression workers went from " + workers + " to " + workersAfter);
		assertFalse(driverRegistered(stepId));
	}

	@Test
	void aDriverThatIsBuiltStaysRegisteredAndUsableUntilItCloses() throws Exception {
		final var stepId = "built-driver";
		final var driver = instance(storage(4, "nio"), stepId);
		try {
			assertTrue(driverRegistered(stepId));
			assertFalse(driver.isClosed());
		} finally {
			driver.close();
		}
		assertFalse(driverRegistered(stepId));
	}
}
