package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitNoNicIoThreads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A verified split read of an item whose ranges were updated verifies each range against the
 * whole item's cells, as a verified whole read of the same item does. The loopback target holds
 * the item's layer with each updated cell at the next layer, as a target that applied the
 * item's ranged updates would.
 */
@Timeout(180)
final class S3SplitReadUpdatedItemVerifyTest {

	private static final int PART_SIZE = 1024;
	private static final int RING_SIZE = 64 * 1024;
	private static final long QUIET_NANOS = 1_000_000_000L;
	private static final long DEADLINE_NANOS = 20_000_000_000L;

	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<String> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final AtomicInteger cutsLeft = new AtomicInteger();
	private volatile byte[] content;
	private HttpServer server;

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		if (server != null) {
			server.stop(0);
		}
		serverThreads.shutdownNow();
		awaitNoNicIoThreads();
	}

	@Test
	void anItemWithNoUpdatedRangesVerifiesWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3 * PART_SIZE, 0, RING_SIZE);
	}

	@Test
	void anUpdatedCellInsideTheFirstRangeVerifiesWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3 * PART_SIZE, 0, RING_SIZE, 5);
	}

	@Test
	void anUpdatedCellAcrossARangeBoundaryVerifiesWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3 * PART_SIZE, 0, RING_SIZE, 10);
	}

	@Test
	void anUpdatedCellAcrossTheLastRangeBoundaryVerifiesWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3 * PART_SIZE, 0, RING_SIZE, 11);
	}

	@Test
	void twoUpdatedCellsVerifyWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3 * PART_SIZE, 0, RING_SIZE, 3, 10);
	}

	@Test
	void anUpdatedCellOfALayer1ItemVerifiesWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3 * PART_SIZE, 1, RING_SIZE, 5);
	}

	@Test
	void aTailRangeShorterThanTheRangeSizeVerifiesWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3000, 0, RING_SIZE, 5, 11);
	}

	@Test
	void anItemLargerThanItsDataRingVerifiesWholeAndSplit() throws Exception {
		assertVerifiesWholeAndSplit(3000, 0, 1000, 3, 10, 11);
	}

	/** The mask says cell 10 is updated, but the target holds the base layer's bytes. */
	@Test
	void staleContentOfAnUpdatedCellIsCorruptWholeAndSplit() throws Exception {
		final int size = 3 * PART_SIZE;
		content = content(item(size, 0, RING_SIZE), RING_SIZE);
		startServer();

		final var whole = read(item(size, 0, RING_SIZE, 10), RING_SIZE, false);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, whole.get(0).status(), whole::toString);

		final var split = read(item(size, 0, RING_SIZE, 10), RING_SIZE, true);
		final var parts = parts(split);
		// Cell 10 is bytes 1023..2046: the last byte of range 0 and most of range 1.
		for (final int corrupt : new int[]{0, 1
		}) {
			assertTrue(parts.stream().anyMatch(p -> p.partNumber() == corrupt && p.status() == Operation.Status.RESP_FAIL_CORRUPT),
							() -> "range " + corrupt + ": " + parts);
			assertFalse(parts.stream().anyMatch(p -> p.partNumber() == corrupt && p.status() == Operation.Status.SUCC),
							() -> "range " + corrupt + ": " + parts);
		}
		assertTrue(parts.stream().anyMatch(p -> p.partNumber() == 2 && p.status() == Operation.Status.SUCC), parts::toString);
	}

	/** The first range is cut mid-body once; its retry verifies from the range's start and succeeds. */
	@Test
	void aRetriedCutRangeOfAnUpdatedItemVerifiesAndSucceeds() throws Exception {
		final int size = 3 * PART_SIZE;
		content = content(item(size, 0, RING_SIZE, 5, 10), RING_SIZE);
		startServer();
		cutsLeft.set(1);

		final var split = read(item(size, 0, RING_SIZE, 5, 10), RING_SIZE, true);
		assertEquals(1, requests.stream().filter(r -> r.endsWith("-> CUT")).count(), requests::toString);
		assertSplitSucceeded(split, size);
		final var parts = parts(split);
		assertEquals(4, parts.size(), parts::toString);
		assertEquals(1, parts.stream().filter(p -> p.status() == Operation.Status.FAIL_IO).count(), parts::toString);
	}

	private void assertVerifiesWholeAndSplit(final long size, final int layer, final int ringSize, final int... cells)
					throws Exception {
		content = content(item(size, layer, ringSize, cells), ringSize);
		startServer();

		final var whole = read(item(size, layer, ringSize, cells), ringSize, false);
		assertEquals(Operation.Status.SUCC, whole.get(0).status(), () -> requests + " " + whole);
		assertEquals(size, ((DataOperationImpl<?>) whole.get(0)).countBytesDone(), whole::toString);

		assertSplitSucceeded(read(item(size, layer, ringSize, cells), ringSize, true), size);
	}

	private void assertSplitSucceeded(final List<Operation<?>> results, final long size) {
		final var composite = results.get(results.size() - 1);
		assertTrue(composite instanceof CompositeDataOperationImpl<?>, results::toString);
		assertEquals(Operation.Status.SUCC, composite.status(), () -> requests + " " + results);
		long bytes = 0;
		for (final var part : parts(results)) {
			assertTrue(part.status() == Operation.Status.SUCC || part.status() == Operation.Status.FAIL_IO,
							() -> "range #" + part.partNumber() + ": " + results);
			if (part.status() == Operation.Status.SUCC) {
				bytes += part.countBytesDone();
			}
		}
		assertEquals(size, bytes, results::toString);
	}

	private void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();
	}

	/** Every result of a verified read of {@code item}, whole or split, ending with its last result. */
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private List<Operation<?>> read(final DataItemImpl item, final int ringSize, final boolean split) throws Exception {
		requests.clear();
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", 5_000);
		config.val("storage-net-node-addrs", List.of("127.0.0.1:" + server.getAddress().getPort()));
		config.val("storage-net-endpoint-selection", "round-robin");
		config.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		final S3StorageDriver<DataItem, Operation<DataItem>> driver = (S3StorageDriver) new S3StorageDriverExtension().create(
						"split-read-updated-verify", input(ringSize), config.configVal("storage"), true, 16);
		resources.add(driver);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();

		final long deadline = System.nanoTime() + DEADLINE_NANOS;
		if (!split) {
			assertTrue(driver.put(new DataOperationImpl<DataItem>(0, OpType.READ, item, null, "/bucket", CREDENTIAL, null, 0)));
			while (System.nanoTime() < deadline) {
				final var result = results.poll(50);
				if (result != null) {
					return List.of(result);
				}
			}
			throw new AssertionError("the whole read did not finish: " + requests);
		}
		assertTrue(driver.put(new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", CREDENTIAL, null, 0, PART_SIZE)));
		// The composite's result comes last: wait for it, then for the driver to be idle and quiet.
		final List<Operation<?>> collected = new ArrayList<>();
		CompositeDataOperationImpl<?> composite = null;
		long quietSince = System.nanoTime();
		while (System.nanoTime() < deadline) {
			final var result = results.poll(50);
			if (result != null) {
				quietSince = System.nanoTime();
				collected.add(result);
				if (result instanceof CompositeDataOperationImpl<?> parent) {
					composite = parent;
				}
			} else if (composite != null && composite.allSubOperationsDone() && driver.activeOpCount() == 0
							&& System.nanoTime() - quietSince > QUIET_NANOS) {
				return collected;
			}
		}
		throw new AssertionError("the read did not finish: " + requests + " " + collected);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			if (!"GET".equals(exchange.getRequestMethod())) {
				exchange.sendResponseHeaders(200, -1);
				return;
			}
			final var range = exchange.getRequestHeaders().getFirst("Range");
			if (range == null) {
				requests.add("GET -> 200");
				exchange.sendResponseHeaders(200, content.length);
				exchange.getResponseBody().write(content);
				return;
			}
			final var bounds = range.substring("bytes=".length()).split("-", -1);
			final int first = Integer.parseInt(bounds[0]);
			final int last = Integer.parseInt(bounds[1]);
			final int length = last - first + 1;
			exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + content.length);
			exchange.sendResponseHeaders(206, length);
			if (first == 0 && cutsLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
				requests.add("GET " + range + " -> CUT");
				exchange.getResponseBody().write(content, first, length / 2);
				exchange.getResponseBody().flush();
				// Closing the exchange with its body short closes the connection.
				return;
			}
			requests.add("GET " + range + " -> 206");
			exchange.getResponseBody().write(content, first, length);
		}
	}

	private static List<PartialDataOperation<?>> parts(final List<Operation<?>> results) {
		return results.stream().filter(result -> result instanceof PartialDataOperation<?>)
						.<PartialDataOperation<?>> map(result -> (PartialDataOperation<?>) result).toList();
	}

	private static SeedDataInput input(final int ringSize) {
		return new SeedDataInput(1, ringSize, 3, true);
	}

	private static DataItemImpl item(final long size, final int layer, final int ringSize, final int... cells) {
		final var item = new DataItemImpl("updated", 9, size, layer);
		if (cells.length > 0) {
			final var mask = new BitSet();
			for (final int cell : cells) {
				mask.set(cell);
			}
			item.commitUpdatedRanges(new BitSet[]{mask, new BitSet()
			});
		}
		return item;
	}

	/** The object's bytes: the item's layer, with each updated cell at the next layer. */
	private static byte[] content(final DataItemImpl item, final int ringSize) throws IOException {
		item.dataInput(input(ringSize));
		final int size = (int) item.size();
		final byte[] all = read(item.slice(0, size), size);
		for (int i = 0; DataItem.rangeOffset(i) < size; i++) {
			if (item.isRangeUpdated(i)) {
				final int cellOffset = (int) DataItem.rangeOffset(i);
				final int cellSize = (int) item.rangeSize(i);
				final var cell = item.slice(cellOffset, cellSize);
				cell.layer(item.layer() + 1);
				System.arraycopy(read(cell, cellSize), 0, all, cellOffset, cellSize);
			}
		}
		return all;
	}

	private static byte[] read(final DataItemImpl item, final int size) {
		final ByteBuffer bytes = ByteBuffer.allocate(size);
		while (bytes.hasRemaining()) {
			item.read(bytes);
		}
		return bytes.array();
	}
}
