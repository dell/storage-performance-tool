package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Drivers made by the factory for a {@link ScriptedS3Server}, and operations to hand them. */
final class S3AwsDriverHarness {

	static final Credential CREDENTIAL = Credential.getInstance("access-key", "secret-key");

	private S3AwsDriverHarness() {}

	@SuppressWarnings("unchecked")
	static <I extends Item, O extends Operation<I>> S3AwsStorageDriver<I, O> driver(
					final Path home, final ScriptedS3Server server, final Map<String, Object> overrides) throws Exception {
		final Config storage = S3AwsMergedConfig.create(home, server.port(), overrides).configVal("storage");
		return (S3AwsStorageDriver<I, O>) new S3AwsStorageDriverFactory<I, O>().create(
						"s3-aws-harness",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storage,
						false,
						16);
	}

	/** A PUT of an object large enough to be streamed, which the transfer client serves. */
	static DataOperationImpl<DataItem> putOp(final String key) throws Exception {
		final DataItem item = new DataItemImpl(key, 0, 10 * 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false));
		return new DataOperationImpl<DataItem>(0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0);
	}

	/**
	 * A request for an object; one that names a version is served by the exact-version client,
	 * one that does not by the transfer client.
	 */
	static DataOperationImpl<IntegrityManifestDataItem> op(final OpType type, final String key, final String versionId) {
		return new DataOperationImpl<IntegrityManifestDataItem>(
						0, type, new IntegrityManifestDataItem("bucket", key, 0, versionId), null, "/bucket", CREDENTIAL, null, 0);
	}

	/** A started driver, the results it outputs, and its close. */
	static final class Started<I extends Item, O extends Operation<I>> implements AutoCloseable {

		private final S3AwsStorageDriver<I, O> driver;
		private final LinkedBlockingQueue<O> results = new LinkedBlockingQueue<>();

		private Started(final S3AwsStorageDriver<I, O> driver) {
			this.driver = driver;
		}

		/** Starts the driver and hands it a first operation. */
		static <I extends Item, O extends Operation<I>> Started<I, O> start(final S3AwsStorageDriver<I, O> driver, final O op)
						throws Exception {
			final var started = new Started<I, O>(driver);
			driver.operationResultOutput(new ResultOutput<>(started.results));
			driver.start();
			assertTrue(driver.put(op));
			return started;
		}

		S3AwsStorageDriver<I, O> driver() {
			return driver;
		}

		O await(final Duration budget) throws InterruptedException {
			final O result = results.poll(budget.toMillis(), TimeUnit.MILLISECONDS);
			assertNotNull(result, "the request was still in flight after " + budget);
			return result;
		}

		O next(final O op, final Duration budget) throws Exception {
			assertTrue(driver.put(op));
			return await(budget);
		}

		@Override
		public void close() throws Exception {
			driver.close();
		}
	}

	private record ResultOutput<O>(LinkedBlockingQueue<O> results)implements Output<O>
	{

	@Override
	public boolean put(final O value) {
		return results.offer(value);
	}

	@Override
	public int put(final List<O> values, final int from, final int to) {
		int count = 0;
		for (int i = from; i < to; i++) {
			if (!results.offer(values.get(i))) {
				break;
			}
			count++;
		}
		return count;
	}

	@Override
	public int put(final List<O> values) {
		return put(values, 0, values.size());
	}

	@Override
	public Input<O> getInput() {
		return null;
	}

	@Override
	public void close() {
		results.clear();
	}
}}
