package com.dell.spt.base.load.generator;

import static com.dell.spt.base.Constants.APP_NAME;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.io.collection.IoBuffer;
import com.github.akurilov.commons.io.collection.LimitedQueueBuffer;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.math3.stat.Frequency;

public class LoadGeneratorBuilderImplTest {

	private static final Map<String, Object> CONFIG_SCHEMA;

	static {
		try {
			final var configSchemas = Extension.load(Thread.currentThread().getContextClassLoader()).stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(
											schemaProvider -> {
												try {
													return schemaProvider.schema();
												} catch (final Exception e) {
													throw new RuntimeException("Failed to get schema", e);
												}
											})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader()).stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			CONFIG_SCHEMA = TreeUtil.reduceForest(configSchemas);
		} catch (final Throwable cause) {
			throw new AssertionError(cause);
		}
	}

	@Test
	public void multiBucketPerUserTest() throws Exception {

		final var credentialsFilePath = Files.createTempFile(getClass().getSimpleName(), ".csv");
		credentialsFilePath.toFile().deleteOnExit();
		final var bucketCount = 100;
		final var opCount = 10000;
		final var prefixUid = "user-";
		final var prefixSecret = "secret-";
		final var prefixBucket = "bucket-";
		try (final var bw = Files.newBufferedWriter(credentialsFilePath)) {
			for (var i = 0; i < bucketCount; i++) {
				bw.append(prefixBucket)
								.append(Integer.toString(i))
								.append(',')
								.append(prefixUid)
								.append(Integer.toString(i))
								.append(',')
								.append(prefixSecret)
								.append(Integer.toString(i));
				bw.newLine();
			}
		}
		final var seed = 314159265;
		final Map<String, Object> options = new HashMap<>();
		options.put("item-data-ranges-concat", null);
		options.put("item-data-ranges-fixed", null);
		options.put("item-data-ranges-random", 0);
		options.put("item-data-ranges-threshold", 0);
		options.put("item-data-size", "1MB");
		options.put("item-input-path", null);
		options.put("item-naming-length", 12);
		options.put("item-naming-seed", 0L);
		options.put("item-naming-prefix", null);
		options.put("item-naming-radix", 36);
		options.put("item-naming-step", 1);
		options.put("item-naming-type", "random");
		options.put("item-output-path", prefixBucket + "${rnd.nextLong(100)}%{" + seed + "}");
		options.put("load-batch-size", opCount);
		options.put("load-op-limit-count", opCount);
		options.put("load-op-limit-recycle", 1_000_000);
		options.put("load-op-recycle-mode", false);
		options.put("load-op-recycle-content-update", false);
		options.put("load-op-retry", false);
		options.put("load-op-retryLimit", 10);
		options.put("load-op-shuffle", false);
		options.put("load-op-type", OpType.CREATE.name().toLowerCase(Locale.ROOT));
		options.put("storage-auth-file", credentialsFilePath.toAbsolutePath().toString());
		final var config = (Config) new BasicConfig("-", CONFIG_SCHEMA);
		options.forEach(config::val);
		final var itemFactory = (ItemFactory) new DataItemFactoryImpl();
		final List<DataOperation<DataItem>> ops = new ArrayList<>(opCount);

		try (final IoBuffer<DataOperation<DataItem>> opBuff = new LimitedQueueBuffer<>(new ArrayBlockingQueue<>(opCount));
						final LoadGenerator loadGenerator = new LoadGeneratorBuilderImpl()
										.authConfig(config.configVal("storage-auth"))
										.itemConfig(config.configVal("item"))
										.itemFactory(itemFactory)
										.itemType(ItemType.DATA)
										.loadConfig(config.configVal("load"))
										.loadOperationsOutput(opBuff)
										.originIndex(0)
										.build()) {
			loadGenerator.start();
			if (!loadGenerator.await(10, TimeUnit.SECONDS)) { // Currently fails, but if you increase time to 20 it passes (needs to be looked into)
				throw new AssertionError("Load generator await timeout");
			}
			assertEquals(opCount, loadGenerator.generatedOpCount());
			assertEquals(opCount, opBuff.size());
			assertEquals(opCount, opBuff.get(ops, opCount));
		}

		String bucket;
		Credential credential;
		String uid;
		String secret;
		String suffix;
		long n;
		final var freq = new Frequency();
		for (final Operation op : ops) {
			bucket = op.dstPath();
			suffix = bucket.substring(prefixBucket.length());
			n = Long.parseLong(suffix);
			assertTrue(n >= 0);
			assertTrue(n < bucketCount);
			freq.addValue(n);
			credential = op.credential();
			uid = credential.getUid();
			assertEquals(prefixUid + suffix, uid);
			secret = credential.getSecret();
			assertEquals(prefixSecret + suffix, secret);
		}
		ops.clear();
		final var expectedFreq = opCount / bucketCount;
		for (var i = 0; i < bucketCount; i++) {
			assertEquals(expectedFreq, (double) freq.getCount(i), expectedFreq / 2.0);
		}
	}

	private static Map<String, Object> baseSingleOpOptions() {
		final Map<String, Object> options = new HashMap<>();
		options.put("item-data-ranges-concat", null);
		options.put("item-data-ranges-fixed", null);
		options.put("item-data-ranges-random", 0);
		options.put("item-data-ranges-threshold", 0);
		options.put("item-data-size", "1KB");
		options.put("item-input-path", null);
		options.put("item-naming-length", 12);
		options.put("item-naming-seed", 0L);
		options.put("item-naming-prefix", null);
		options.put("item-naming-radix", 36);
		options.put("item-naming-step", 1);
		options.put("item-naming-type", "random");
		options.put("item-output-path", "test-bucket");
		options.put("load-batch-size", 1);
		options.put("load-op-limit-count", 1);
		options.put("load-op-limit-recycle", 1_000_000);
		options.put("load-op-recycle-mode", false);
		options.put("load-op-recycle-content-update", false);
		options.put("load-op-retryLimit", 10);
		options.put("load-op-shuffle", false);
		options.put("load-op-type", OpType.CREATE.name().toLowerCase(Locale.ROOT));
		options.put("storage-auth-uid", "test-uid");
		options.put("storage-auth-secret", "test-secret");
		return options;
	}

	/**
	 * Finding #1 (round 2 of the load-op-retry code review): LoadGeneratorImpl used to
	 * self-stop the instant its dispatch count reached load-op-limit-count, regardless of
	 * whether that dispatch had actually completed - so a later retry decision (made by
	 * whoever is watching completions, once the op actually resolves) could never happen in
	 * time, and the operation would be forced to a terminal failure with zero retry attempts
	 * used despite a configured retry budget. Drives the generator directly against a
	 * "black hole" output (an IoBuffer that just accumulates ops and never signals
	 * completion) to isolate exactly this: does reaching countLimit alone make the
	 * generator conclude it's done, or does it correctly wait for an external stop when
	 * load-op-retry is enabled?
	 */
	@Test
	public void retryEnabledGeneratorDoesNotSelfStopOnCountLimitWhileDispatchUnresolved() throws Exception {
		final Map<String, Object> options = baseSingleOpOptions();
		options.put("load-op-retry", true);
		final var config = (Config) new BasicConfig("-", CONFIG_SCHEMA);
		options.forEach(config::val);
		final var itemFactory = (ItemFactory) new DataItemFactoryImpl();

		try (final IoBuffer<DataOperation<DataItem>> opBuff = new LimitedQueueBuffer<>(new ArrayBlockingQueue<>(10));
						final LoadGenerator loadGenerator = new LoadGeneratorBuilderImpl()
										.authConfig(config.configVal("storage-auth"))
										.itemConfig(config.configVal("item"))
										.itemFactory(itemFactory)
										.itemType(ItemType.DATA)
										.loadConfig(config.configVal("load"))
										.loadOperationsOutput(opBuff)
										.originIndex(0)
										.build()) {
			loadGenerator.start();
			final long deadline = System.currentTimeMillis() + 2000;
			while (opBuff.size() < 1 && System.currentTimeMillis() < deadline) {
				Thread.sleep(5);
			}
			assertEquals(1, opBuff.size(), "the one op allowed by load-op-limit-count should have been dispatched");
			// Give the generator a bit more time to (incorrectly) decide it's done, if the
			// bug were present.
			Thread.sleep(200);
			assertTrue(
							!loadGenerator.isStopped(),
							"generator must not self-stop on count-limit while load-op-retry is enabled and the "
											+ "one dispatch is still unresolved (nobody is watching for its completion in this test)");
			loadGenerator.stop();
		}
	}

	/** Contrasts with the test above: without load-op-retry, reaching count-limit should still self-stop as before. */
	@Test
	public void retryDisabledGeneratorSelfStopsOnCountLimitAsBefore() throws Exception {
		final Map<String, Object> options = baseSingleOpOptions();
		options.put("load-op-retry", false);
		final var config = (Config) new BasicConfig("-", CONFIG_SCHEMA);
		options.forEach(config::val);
		final var itemFactory = (ItemFactory) new DataItemFactoryImpl();

		try (final IoBuffer<DataOperation<DataItem>> opBuff = new LimitedQueueBuffer<>(new ArrayBlockingQueue<>(10));
						final LoadGenerator loadGenerator = new LoadGeneratorBuilderImpl()
										.authConfig(config.configVal("storage-auth"))
										.itemConfig(config.configVal("item"))
										.itemFactory(itemFactory)
										.itemType(ItemType.DATA)
										.loadConfig(config.configVal("load"))
										.loadOperationsOutput(opBuff)
										.originIndex(0)
										.build()) {
			loadGenerator.start();
			final long deadline = System.currentTimeMillis() + 2000;
			while (!loadGenerator.isStopped() && System.currentTimeMillis() < deadline) {
				Thread.sleep(5);
			}
			assertTrue(loadGenerator.isStopped(), "generator should self-stop on count-limit when load-op-retry is off");
			assertEquals(1, opBuff.size());
		}
	}

	/**
	 * P2a (round 4 of the load-op-retry code review): the retry queue intentionally
	 * bypasses {@code load-op-limit-count} (a retry is a re-attempt of something already
	 * counted once, not new work), but should still respect configured rate/index
	 * throttles - otherwise a burst of retries during a failure storm could exceed the
	 * user's configured rate by roughly the original traffic plus retry traffic.
	 */
	@Test
	public void retryRedispatchRespectsThrottles() throws Exception {
		final Map<String, Object> options = baseSingleOpOptions();
		options.put("load-op-retry", true);
		final var config = (Config) new BasicConfig("-", CONFIG_SCHEMA);
		options.forEach(config::val);
		final var itemFactory = (ItemFactory) new DataItemFactoryImpl();

		final java.util.concurrent.atomic.AtomicBoolean allowDispatch = new java.util.concurrent.atomic.AtomicBoolean(true);
		final com.github.akurilov.commons.concurrent.throttle.Throttle throttle = new com.github.akurilov.commons.concurrent.throttle.Throttle() {
			@Override
			public boolean tryAcquire() {
				return allowDispatch.get();
			}

			@Override
			public int tryAcquire(final int n) {
				return allowDispatch.get() ? n : 0;
			}
		};

		try (final IoBuffer<DataOperation<DataItem>> opBuff = new LimitedQueueBuffer<>(new ArrayBlockingQueue<>(10));
						final LoadGenerator loadGenerator = new LoadGeneratorBuilderImpl()
										.authConfig(config.configVal("storage-auth"))
										.itemConfig(config.configVal("item"))
										.itemFactory(itemFactory)
										.itemType(ItemType.DATA)
										.loadConfig(config.configVal("load"))
										.loadOperationsOutput(opBuff)
										.addThrottle(throttle)
										.originIndex(0)
										.build()) {
			loadGenerator.start();
			// Let the one fresh item (load-op-limit-count=1, from baseSingleOpOptions())
			// dispatch normally while the throttle is open.
			final long dispatchDeadline = System.currentTimeMillis() + 2000;
			while (opBuff.size() < 1 && System.currentTimeMillis() < dispatchDeadline) {
				Thread.sleep(5);
			}
			assertEquals(1, opBuff.size(), "the one fresh op should have dispatched while the throttle was open");

			// Close the throttle, then simulate a failed operation's retry with a
			// freshly-constructed synthetic op (leaving the already-dispatched one in
			// opBuff alone, so the size assertions below stay simple).
			allowDispatch.set(false);
			final DataOperation<DataItem> retryOp = new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("retry-throttle-test", 0, 1024), null, "test-bucket", null, List.of(), 0);
			@SuppressWarnings("unchecked")
			final LoadGenerator<DataItem, Operation<DataItem>> typedGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) loadGenerator;
			typedGenerator.retry(retryOp);

			// Give the generator's work loop several iterations to (incorrectly) dispatch
			// the retry if the throttle were not being respected.
			Thread.sleep(200);
			assertEquals(
							1, opBuff.size(), "retry must not be dispatched while the throttle denies permits");

			// Re-open the throttle: the retry should now get through.
			allowDispatch.set(true);
			final long retryDeadline = System.currentTimeMillis() + 2000;
			while (opBuff.size() < 2 && System.currentTimeMillis() < retryDeadline) {
				Thread.sleep(5);
			}
			assertEquals(2, opBuff.size(), "retry should dispatch once the throttle allows it again");
			loadGenerator.stop();
		}
	}

	/** {@link IndexThrottle} counterpart to {@link #retryRedispatchRespectsThrottles()} above - same property, the other throttle type. */
	@Test
	public void retryRedispatchRespectsIndexThrottles() throws Exception {
		final Map<String, Object> options = baseSingleOpOptions();
		options.put("load-op-retry", true);
		final var config = (Config) new BasicConfig("-", CONFIG_SCHEMA);
		options.forEach(config::val);
		final var itemFactory = (ItemFactory) new DataItemFactoryImpl();

		final java.util.concurrent.atomic.AtomicBoolean allowDispatch = new java.util.concurrent.atomic.AtomicBoolean(true);
		final com.github.akurilov.commons.concurrent.throttle.IndexThrottle indexThrottle = new com.github.akurilov.commons.concurrent.throttle.IndexThrottle() {
			@Override
			public boolean tryAcquire(final int index) {
				return allowDispatch.get();
			}

			@Override
			public int tryAcquire(final int index, final int n) {
				return allowDispatch.get() ? n : 0;
			}
		};

		try (final IoBuffer<DataOperation<DataItem>> opBuff = new LimitedQueueBuffer<>(new ArrayBlockingQueue<>(10));
						final LoadGenerator loadGenerator = new LoadGeneratorBuilderImpl()
										.authConfig(config.configVal("storage-auth"))
										.itemConfig(config.configVal("item"))
										.itemFactory(itemFactory)
										.itemType(ItemType.DATA)
										.loadConfig(config.configVal("load"))
										.loadOperationsOutput(opBuff)
										.addThrottle(indexThrottle)
										.originIndex(0)
										.build()) {
			loadGenerator.start();
			final long dispatchDeadline = System.currentTimeMillis() + 2000;
			while (opBuff.size() < 1 && System.currentTimeMillis() < dispatchDeadline) {
				Thread.sleep(5);
			}
			assertEquals(1, opBuff.size(), "the one fresh op should have dispatched while the throttle was open");

			allowDispatch.set(false);
			final DataOperation<DataItem> retryOp = new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("retry-index-throttle-test", 0, 1024), null, "test-bucket", null, List.of(), 0);
			@SuppressWarnings("unchecked")
			final LoadGenerator<DataItem, Operation<DataItem>> typedGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) loadGenerator;
			typedGenerator.retry(retryOp);

			Thread.sleep(200);
			assertEquals(1, opBuff.size(), "retry must not be dispatched while the index throttle denies permits");

			allowDispatch.set(true);
			final long retryDeadline = System.currentTimeMillis() + 2000;
			while (opBuff.size() < 2 && System.currentTimeMillis() < retryDeadline) {
				Thread.sleep(5);
			}
			assertEquals(2, opBuff.size(), "retry should dispatch once the index throttle allows it again");
			loadGenerator.stop();
		}
	}
}
