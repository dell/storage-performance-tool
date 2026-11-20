package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitNoNicIoThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.storage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * An extension built before 5.17.1 can subclass {@code CompositeDataOperationImpl} and already have a
 * {@code public void markRangesSentAsRequests()} of its own. That method is not the engine's: the
 * {@code s3} driver still marks the split read, so four range GETs give the step four duration and
 * four latency samples and the subclass's method is never called. The subclass is compiled against
 * the class's shape before the mark (a stub with its constructor only) and loaded against this build.
 */
@Timeout(60)
final class S3LegacyMarkSubclassTimingTest {

	private static final int RANGE_SIZE = 1024;
	private static final int RANGES = 4;

	/** {@code CompositeDataOperationImpl} as an extension compiled before the mark saw it. */
	private static final String PRE_MARK_IMPL = """
					package com.dell.spt.base.item.op.composite.data;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.data.DataOperationImpl;
					import com.dell.spt.base.storage.Credential;
					import com.github.akurilov.commons.collection.Range;
					import java.util.List;

					public class CompositeDataOperationImpl<I extends DataItem> extends DataOperationImpl<I> {
						public CompositeDataOperationImpl(final int originIndex, final OpType opType, final I item,
										final String srcPath, final String dstPath, final Credential credential,
										final List<Range> fixedRanges, final int randomRangesCount, final long sizeThreshold) {
							super(originIndex, opType, item, srcPath, dstPath, credential, fixedRanges, randomRangesCount);
						}
					}
					""";

	private static final String LEGACY_SUBCLASS = """
					package legacy.composite;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
					import com.dell.spt.base.storage.Credential;
					import java.util.concurrent.atomic.AtomicInteger;

					public class LegacyNamedMarker<I extends DataItem> extends CompositeDataOperationImpl<I> {

						public static final AtomicInteger CALLS = new AtomicInteger();

						public LegacyNamedMarker(final I item, final Credential credential, final long threshold) {
							super(0, OpType.READ, item, null, "/bucket", credential, null, 0, threshold);
						}

						/** The extension's own meaning, unrelated to the engine's mark. */
						public void markRangesSentAsRequests() {
							CALLS.incrementAndGet();
						}
					}
					""";

	private final NicSetLoopback stub = new NicSetLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();

	@TempDir
	Path dir;

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "the stub fixture needs the epoll transport");
		stub.absentObjectSize = (long) RANGES * RANGE_SIZE;
	}

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
		awaitNoNicIoThreads();
	}

	@Test
	void aSubclassWithItsOwnMarkerGetsOneSamplePerRange() throws Exception {
		assertOneSamplePerRange(false);
	}

	@Test
	void aSubclassWithItsOwnMarkerGetsOneSamplePerRangeWithBatchedResults() throws Exception {
		assertOneSamplePerRange(true);
	}

	private void assertOneSamplePerRange(final boolean batch) throws Exception {
		final URL classes = compileLegacySubclass().toUri().toURL();
		try (var loader = URLClassLoader.newInstance(List.of(classes).toArray(URL[]::new), getClass().getClassLoader())) {
			final var type = loader.loadClass("legacy.composite.LegacyNamedMarker");
			final var calls = (AtomicInteger) type.getField("CALLS").get(null);
			final var name = "legacy-marker-" + batch;
			@SuppressWarnings("unchecked")
			final var read = (Operation<DataItem>) type.getConstructor(DataItem.class, Credential.class, long.class)
							.newInstance(new DataItemImpl(name, 7, (long) RANGES * RANGE_SIZE), CREDENTIAL, RANGE_SIZE);

			final var results = read(read, name);
			final var ranges = results.stream()
							.filter(result -> result instanceof PartialOperation<?> && result.status() == Operation.Status.SUCC)
							.toList();
			assertEquals(RANGES, ranges.size(), results::toString);
			assertTrue(ranges.stream().allMatch(range -> range.duration() > 0 && range.latency() > 0), ranges::toString);

			try (var metrics = metricsContext()) {
				final var context = context(metrics);
				if (batch) {
					assertEquals(results.size(), context.put(results, 0, results.size()));
				} else {
					for (final var result : results) {
						assertTrue(context.put(result));
					}
				}
				final var snapshot = metrics.terminalSnapshot();
				assertEquals(1, snapshot.successSnapshot().count(), "reads");
				assertEquals((long) RANGES * RANGE_SIZE, snapshot.byteSnapshot().count(), "bytes");
				assertEquals(RANGES, snapshot.durationSnapshot().count(), "duration samples, one per range");
				assertEquals(RANGES, snapshot.latencySnapshot().count(), "latency samples, one per range");
				assertEquals(ranges.stream().mapToLong(Operation::duration).sum(), snapshot.durationSnapshot().sum(), "duration sum");
			}
			assertEquals(0, calls.get(), "the subclass's own method is not the engine's mark");
		}
	}

	/** Every result of the split read, in order, up to its finalization, once nothing is in flight. */
	private List<Operation<DataItem>> read(final Operation<DataItem> read, final String name) throws Exception {
		final var driver = new S3StorageDriver<DataItem, Operation<DataItem>>("legacy-mark-timing",
						new SeedDataInput(1, RANGE_SIZE, 1, true), storage(stub.listeners(1), List.of(), 2), false, 16);
		resources.add(driver);
		final var output = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(read));
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		final List<Operation<DataItem>> results = new ArrayList<>();
		while (System.nanoTime() < deadline) {
			@SuppressWarnings("unchecked")
			final var result = (Operation<DataItem>) output.poll();
			if (result != null) {
				assertTrue(result.item().name().endsWith("/" + name), result::toString);
				results.add(result);
			} else if (!results.isEmpty() && results.get(results.size() - 1) instanceof CompositeDataOperation<?> last
							&& (last.allSubOperationsDone() || last.status() != Operation.Status.SUCC) && driver.activeOpCount() == 0) {
				return results;
			}
		}
		throw new AssertionError("the read did not finish: " + results);
	}

	/** Compiles the subclass against the pre-mark stub; only the subclass is kept to be loaded. */
	private Path compileLegacySubclass() throws IOException {
		final var stubSrc = dir.resolve("stub-src/CompositeDataOperationImpl.java");
		final var stubClasses = dir.resolve("stub-classes");
		final var src = dir.resolve("src/LegacyNamedMarker.java");
		final var classes = dir.resolve("classes");
		Files.createDirectories(stubSrc.getParent());
		Files.createDirectories(stubClasses);
		Files.createDirectories(src.getParent());
		Files.createDirectories(classes);
		Files.writeString(stubSrc, PRE_MARK_IMPL);
		Files.writeString(src, LEGACY_SUBCLASS);
		final var classpath = System.getProperty("java.class.path");
		compile("-cp", classpath, "-d", stubClasses.toString(), stubSrc.toString());
		compile("-cp", stubClasses + File.pathSeparator + classpath, "-d", classes.toString(), src.toString());
		return classes;
	}

	private static void compile(final String... args) {
		final var compiler = ToolProvider.getSystemJavaCompiler();
		assertNotNull(compiler, "the tests run on a JDK");
		final var all = new ArrayList<>(List.of("-proc:none", "-nowarn"));
		all.addAll(List.of(args));
		assertEquals(0, compiler.run(null, null, null, all.toArray(String[]::new)), "compiles");
	}

	@SuppressWarnings("unchecked")
	private static LoadStepContextImpl<DataItem, Operation<DataItem>> context(final MetricsContext<AllMetricsSnapshot> metrics)
					throws Exception {
		final Config config = new BundledDefaultsProvider().config("-", InitialConfigSchemaProvider.provider().schema());
		config.val("load-op-retry", false);
		config.val("load-op-recycle-mode", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		return new LoadStepContextImpl<>("legacy-mark-timing", generator, driver, metrics, config.configVal("load"), false);
	}

	private static MetricsContext<AllMetricsSnapshot> metricsContext() {
		final MetricsContext<AllMetricsSnapshot> context = MetricsContextImpl.builder()
						.loadStepId("legacy-mark-timing")
						.opType(OpType.READ)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(RANGE_SIZE))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		context.start();
		return context;
	}
}
