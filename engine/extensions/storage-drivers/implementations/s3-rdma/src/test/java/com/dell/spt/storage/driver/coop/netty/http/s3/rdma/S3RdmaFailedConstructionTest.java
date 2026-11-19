package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** With RDMA unavailable and no fallback, the shipped default, the constructor leaves nothing open. */
@Timeout(120)
class S3RdmaFailedConstructionTest {

	private static final int CONSTRUCTIONS = 5;
	private static final long DESCRIPTOR_TOLERANCE = 2;

	private static final class Probe extends S3RdmaStorageDriver<Item, Operation<Item>> {
		static RdmaPayloadCopier copier;
		static RdmaBufferPool pool;

		Probe(final DataInput input, final Config root, final RdmaTransport transport)
						throws IllegalConfigurationException, InterruptedException {
			super("rdma-failed-construction", input, root.configVal("storage"), false,
							root.intVal("load-batch-size"), ignored -> transport);
		}

		@Override
		protected void releaseAfterFailedConstruction() {
			copier = payloadCopier();
			pool = bufferPool();
			super.releaseAfterFailedConstruction();
		}
	}

	private static long openDescriptors() {
		try (final var entries = Files.list(Path.of("/proc/self/fd"))) {
			return entries.count();
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private static void constructAndFail(final RdmaConfig rdma, final Config root) throws Exception {
		final var transport = new FakeRdmaTransport(rdma);
		transport.setFailInit(true);
		try (final var input = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true);
						final var scope = DaemonBase.openConstructionScope()) {
			final var failure = assertThrows(
							IllegalConfigurationException.class, () -> new Probe(input, root, transport));
			assertTrue(failure.getMessage().contains("fallback is disabled"), failure.toString());
		}
	}

	@Test
	void aFailedInitWithoutFallbackReleasesTheCopierThePoolAndTheIoGroup() throws Exception {
		final var rdma = new RdmaConfig(true, 1024, false, "auto", "", "WARN", 30_000L, false, true);
		final var root = S3RdmaStorageDriverTestSupport.config(rdma, "/bucket", List.of("127.0.0.1"), 9020);
		root.val("storage-driver-threads", 2);
		constructAndFail(rdma, root);
		final long descriptors = openDescriptors();

		for (int i = 0; i < CONSTRUCTIONS; i++) {
			constructAndFail(rdma, root);
		}

		assertNotNull(Probe.copier);
		assertTrue(Probe.copier.isStopped());
		assertNotNull(Probe.pool);
		assertTrue(Probe.pool.isClosed());
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		long after = openDescriptors();
		while (after > descriptors + DESCRIPTOR_TOLERANCE && System.nanoTime() < deadline) {
			Thread.sleep(20);
			after = openDescriptors();
		}
		final long open = after;
		assertTrue(
						open <= descriptors + DESCRIPTOR_TOLERANCE,
						() -> "open descriptors went from " + descriptors + " to " + open);
	}
}
