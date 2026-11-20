package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A split read whose ranges are not the engine's own {@code PartialDataOperationImpl}, or are
 * subclasses of it built against 5.17.0, ends as it did in 5.17.0: a range out of retries gives the
 * read its failure, and the read ends once. The engine's mark that a range recorded its failure is
 * kept on {@code PartialDataOperationImpl} and reached only through its static methods, so an
 * extension's own methods of the same names neither break loading nor are called by the driver.
 */
@SuppressWarnings("unchecked")
@Timeout(60)
final class ExtensionRangeCompatibilityTest {

	private static final int RANGES = 2;
	private static final long RANGE_SIZE = 1024;
	private static final String FIXTURE_PACKAGE = "fixture.ext";

	/** Makes the ranges of an {@link OwnCompositeRead}. */
	@FunctionalInterface
	interface RangeFactory {
		PartialDataOperation<DataItem> make(int partNumber, DataItem part, CompositeDataOperation<DataItem> parent);
	}

	/** A range type an extension implements itself, as it could against 5.17.0, counting its own retries. */
	static final class OwnRange extends DataOperationImpl<DataItem> implements PartialDataOperation<DataItem> {

		private final int partNumber;
		private final CompositeDataOperation<DataItem> parent;
		private int retries;

		OwnRange(final int partNumber, final DataItem part, final CompositeDataOperation<DataItem> parent) {
			super(0, OpType.READ, part, null, "/bucket", null, null, 0);
			this.partNumber = partNumber;
			this.parent = parent;
		}

		private OwnRange(final OwnRange other) {
			super(other);
			this.partNumber = other.partNumber;
			this.parent = other.parent;
			this.retries = other.retries;
		}

		@Override
		public OwnRange result() {
			return new OwnRange(this);
		}

		@Override
		public int partNumber() {
			return partNumber;
		}

		@Override
		public CompositeDataOperation<DataItem> parent() {
			return parent;
		}

		@Override
		public int retryCount() {
			return retries;
		}

		@Override
		public void incrementRetryCount() {
			retries++;
		}
	}

	/** A composite read an extension implements itself, whose ranges a {@link RangeFactory} makes. */
	static final class OwnCompositeRead extends DataOperationImpl<DataItem> implements CompositeDataOperation<DataItem> {

		private final RangeFactory rangeFactory;
		private final Map<String, String> context = new ConcurrentHashMap<>();
		private final List<PartialDataOperation<DataItem>> ranges = new ArrayList<>();
		private final AtomicInteger pending = new AtomicInteger(-1);
		private final AtomicBoolean yielded = new AtomicBoolean();

		OwnCompositeRead(final DataItem item, final RangeFactory rangeFactory) {
			super(0, OpType.READ, item, null, "/bucket", null, null, 0);
			this.rangeFactory = rangeFactory;
		}

		private OwnCompositeRead(final OwnCompositeRead other) {
			super(other);
			this.rangeFactory = other.rangeFactory;
			this.pending.set(other.pending.get());
		}

		@Override
		public OwnCompositeRead result() {
			return new OwnCompositeRead(this);
		}

		@Override
		public String get(final String key) {
			return context.get(key);
		}

		@Override
		public void put(final String key, final String value) {
			context.put(key, value);
		}

		@Override
		public synchronized List<? extends PartialDataOperation<DataItem>> subOperations() {
			if (ranges.isEmpty()) {
				for (var i = 0; i < RANGES; i++) {
					ranges.add(rangeFactory.make(i, item.slice(i * RANGE_SIZE, RANGE_SIZE), this));
				}
				pending.set(ranges.size());
			}
			return ranges;
		}

		@Override
		public List<? extends PartialDataOperation<DataItem>> nextSubOperations(final int limit) {
			final var all = subOperations();
			return yielded.getAndSet(true) ? List.of() : all;
		}

		@Override
		public void markSubTaskCompleted() {
			pending.decrementAndGet();
		}

		@Override
		public void undoMarkSubTaskCompleted() {
			pending.incrementAndGet();
		}

		@Override
		public boolean allSubOperationsDone() {
			return pending.get() == 0;
		}

		@Override
		public long sizeThreshold() {
			return RANGE_SIZE;
		}
	}

	/** Ends a read as the {@code s3} driver does, and counts the reads it was asked to end. */
	private static final class FinishingDriver extends CoopStorageDriverMock<Item, Operation<Item>> {
		final AtomicInteger finished = new AtomicInteger();

		FinishingDriver(final DataInput in, final Config cfg) throws Exception {
			super("extension-range-compatibility", in, cfg, false, 1);
		}

		@Override
		protected boolean finishCompositeRead(final Operation<Item> read) {
			finished.incrementAndGet();
			final String rangeFailure = ((CompositeOperation<?>) read).get("mpuFailure");
			read.status(rangeFailure == null ? Operation.Status.SUCC : Operation.Status.valueOf(rangeFailure));
			read.finishResponse();
			handleCompleted(read);
			return true;
		}
	}

	/** {@code PartialOperation} as 5.17.0 shipped it, compiled with the next stub. */
	private static final String PARTIAL_OPERATION_5_17_0 = """
					package com.dell.spt.base.item.op.partial;

					import com.dell.spt.base.item.Item;
					import com.dell.spt.base.item.op.Operation;
					import com.dell.spt.base.item.op.composite.CompositeOperation;

					public interface PartialOperation<I extends Item> extends Operation<I> {
						int partNumber();
						CompositeOperation<I> parent();
						default int retryCount() {
							return 0;
						}
						default void incrementRetryCount() {}
					}
					""";

	/**
	 * {@code PartialDataOperationImpl} as 5.17.0 shipped it, with no failure mark: the fixtures are
	 * compiled against this shape and {@link #PARTIAL_OPERATION_5_17_0}, as an extension built against
	 * 5.17.0 was, and loaded against the engine's classes under test.
	 */
	private static final String PARTIAL_DATA_OPERATION_IMPL_5_17_0 = """
					package com.dell.spt.base.item.op.partial.data;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
					import com.dell.spt.base.item.op.data.DataOperationImpl;
					import com.dell.spt.base.storage.Credential;

					public class PartialDataOperationImpl<I extends DataItem> extends DataOperationImpl<I>
									implements PartialDataOperation<I> {
						private int partNumber;
						private CompositeDataOperation<I> parent;
						private int retryCount;
						public PartialDataOperationImpl() {}
						public PartialDataOperationImpl(final int originIndex, final OpType opType, final I part,
										final String srcPath, final String dstPath, final Credential credential,
										final int partNumber, final CompositeDataOperation<I> parent) {
							super(originIndex, opType, part, srcPath, dstPath, credential, null, 0);
							this.partNumber = partNumber;
							this.parent = parent;
						}
						protected PartialDataOperationImpl(final PartialDataOperationImpl<I> other) {
							super(other);
						}
						@Override public PartialDataOperationImpl<I> result() { return new PartialDataOperationImpl<>(this); }
						@Override public final int partNumber() { return partNumber; }
						@Override public final CompositeDataOperation<I> parent() { return parent; }
						@Override public final int retryCount() { return retryCount; }
						@Override public final void incrementRetryCount() { retryCount++; }
						@Override public final void finishResponse() {
							try {
								super.finishResponse();
							} finally {
								parent.markSubTaskCompleted();
							}
						}
					}
					""";

	/** A subclass whose own {@code failureRecorded()} means something else: that some attempt failed. */
	private static final String BOOLEAN_MARK_RANGE = """
					package fixture.ext;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
					import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;

					public class BooleanMarkRange<I extends DataItem> extends PartialDataOperationImpl<I> {
						public BooleanMarkRange(final int partNumber, final I part, final CompositeDataOperation<I> parent) {
							super(0, OpType.READ, part, null, "/bucket", null, partNumber, parent);
						}
						public boolean failureRecorded() {
							return true;
						}
					}
					""";

	/** A subclass with its own {@code failureRecorded()} of another return type. */
	private static final String STRING_MARK_RANGE = """
					package fixture.ext;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
					import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;

					public class StringMarkRange<I extends DataItem> extends PartialDataOperationImpl<I> {
						public StringMarkRange(final int partNumber, final I part, final CompositeDataOperation<I> parent) {
							super(0, OpType.READ, part, null, "/bucket", null, partNumber, parent);
						}
						public String failureRecorded() {
							return "the extension's own";
						}
					}
					""";

	/**
	 * An extension's own interface with default methods of the mark's names, taking the engine's range
	 * class, as an extension built against 5.17.0 may have.
	 */
	private static final String EXTENSION_MARKS = """
					package fixture.ext;

					import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;

					public interface ExtensionMarks {
						default void markFailureRecorded(final PartialDataOperationImpl<?> part) {}
						default boolean failureRecorded(final PartialDataOperationImpl<?> part) {
							return true;
						}
					}
					""";

	/**
	 * A subclass that takes the interface's methods, and calls them on itself, which compiles to calls
	 * on the subclass that the JVM resolves through its superclasses first.
	 */
	private static final String INTERFACE_MARK_RANGE = """
					package fixture.ext;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
					import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;

					public class InterfaceMarkRange<I extends DataItem> extends PartialDataOperationImpl<I>
									implements ExtensionMarks {
						public InterfaceMarkRange(final int partNumber, final I part, final CompositeDataOperation<I> parent) {
							super(0, OpType.READ, part, null, "/bucket", null, partNumber, parent);
						}
						public static boolean ownMark(final InterfaceMarkRange<?> range) {
							range.markFailureRecorded(range);
							return range.failureRecorded(range);
						}
					}
					""";

	private final List<Operation<Item>> results = Collections.synchronizedList(new ArrayList<>());
	private DataInput dataInput;
	private FinishingDriver driver;

	@TempDir
	Path tempDir;

	@BeforeEach
	void setUp() throws Exception {
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var cfg = CompositeReadFailureDriverTest.cfg();
		when(cfg.intVal("driver-limit-queue-input")).thenReturn(64);
		driver = new FinishingDriver(dataInput, cfg);
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenAnswer(invocation -> results.add(invocation.getArgument(0)));
		driver.operationResultOutput(out);
	}

	@AfterEach
	void closeDriver() throws Exception {
		driver.close();
	}

	private static Operation<Item> op(final Operation<?> op) {
		return (Operation<Item>) op;
	}

	private OwnCompositeRead read(final String name, final RangeFactory rangeFactory) {
		final var item = new DataItemImpl(name, 0, RANGES * RANGE_SIZE);
		item.dataInput(dataInput);
		return new OwnCompositeRead(item, rangeFactory);
	}

	/** The read's initial pass, which sends no request; its ranges are queued. */
	private List<PartialDataOperation<DataItem>> initialPass(final OwnCompositeRead read) {
		assertTrue(driver.operationLifecycle().driverQueued(op(read)));
		read.reset();
		assertTrue(driver.claimDispatchOwnership(op(read)));
		read.startRequest();
		read.finishRequest();
		read.startResponse();
		read.status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(read)));
		return (List<PartialDataOperation<DataItem>>) read.subOperations();
	}

	private void dispatch(final Operation<?> range) throws InterruptedException {
		assertSame(range, driver.childOpQueue.poll(5, TimeUnit.SECONDS));
		assertTrue(driver.claimDispatchOwnership(op(range)), () -> range + " crossed dispatch");
	}

	/** {@code range}'s attempt ends with {@code status} (it settles) and reaches the driver's completion. */
	private void complete(final OwnCompositeRead read, final Operation<?> range, final Operation.Status status) {
		range.status(status);
		read.markSubTaskCompleted();
		assertTrue(driver.handleCompleted(op(range)));
	}

	private List<Operation<Item>> resultsOf(final Operation<?> read) {
		synchronized (results) {
			return results.stream()
							.filter(result -> result instanceof CompositeOperation && result.item() == read.item())
							.toList();
		}
	}

	private void assertEndedOnce(final OwnCompositeRead read, final Operation.Status status) {
		assertEquals(1, driver.finished.get(), "the read is ended once");
		final var outcome = resultsOf(read);
		assertEquals(2, outcome.size(), () -> "the initial pass and the read's end: " + outcome);
		assertEquals(status, outcome.get(1).status());
		assertEquals(OperationLifecycleState.TERMINAL, driver.operationLifecycle().stateOf(op(read)));
		final var snapshot = driver.operationLifecycle().snapshot();
		assertEquals(0, snapshot.inFlight(), snapshot::toString);
		assertEquals(0, snapshot.unresolved(), snapshot::toString);
	}

	/** One range succeeds, the other fails every attempt: the read ends once, with that failure. */
	private void oneRangeFailsEveryAttempt(final OwnCompositeRead read) throws Exception {
		final var ranges = initialPass(read);
		for (final var range : ranges) {
			dispatch(range);
		}
		final var failing = ranges.get(1);
		complete(read, ranges.get(0), Operation.Status.SUCC);
		for (var retry = 0; retry < CoopStorageDriverBase.MAX_PART_RETRIES; retry++) {
			complete(read, failing, Operation.Status.RESP_FAIL_SVC);
			assertEquals(0, driver.finished.get(), "the range is retried");
			dispatch(failing);
		}
		complete(read, failing, Operation.Status.RESP_FAIL_SVC);

		assertEquals(CoopStorageDriverBase.MAX_PART_RETRIES, failing.retryCount());
		assertTrue(driver.childOpQueue.isEmpty(), () -> "queued: " + driver.childOpQueue);
		assertEndedOnce(read, Operation.Status.RESP_FAIL_SVC);
	}

	@Test
	void anExtensionsOwnRangeTypeThatCountsItsRetriesEndsItsFailedRead() throws Exception {
		oneRangeFailsEveryAttempt(read("own", OwnRange::new));
	}

	@Test
	void aSubclassWithItsOwnBooleanFailureRecordedLoadsRecompilesAndEndsItsFailedRead() throws Exception {
		final var range = fixtureRanges(compileAgainst5170()).apply("BooleanMarkRange");
		oneRangeFailsEveryAttempt(read("boolean", range));
		assertCompilesAgainstThisEngine(BOOLEAN_MARK_RANGE);
	}

	/**
	 * The subclass's {@code failureRecorded()} always says {@code true}; the driver must not take it for
	 * its own mark. Both ranges run out of retries, and the second's completion is held: the first's
	 * completion must not end the read.
	 */
	@Test
	void aSubclassesOwnFailureRecordedDoesNotEndTheReadEarly() throws Exception {
		final var read = read("held", fixtureRanges(compileAgainst5170()).apply("BooleanMarkRange"));
		final var ranges = initialPass(read);
		for (final var range : ranges) {
			dispatch(range);
		}
		final var refused = ranges.get(0);
		final var unavailable = ranges.get(1);
		for (var retry = 0; retry < CoopStorageDriverBase.MAX_PART_RETRIES; retry++) {
			complete(read, refused, Operation.Status.RESP_FAIL_AUTH);
			dispatch(refused);
			complete(read, unavailable, Operation.Status.RESP_FAIL_SVC);
			dispatch(unavailable);
		}
		refused.status(Operation.Status.RESP_FAIL_AUTH);
		read.markSubTaskCompleted();
		unavailable.status(Operation.Status.RESP_FAIL_SVC);
		read.markSubTaskCompleted();
		assertTrue(driver.handleCompleted(op(refused)));
		assertEquals(0, driver.finished.get(), "the held range has not recorded its failure");

		assertTrue(driver.handleCompleted(op(unavailable)));

		assertEndedOnce(read, Operation.Status.RESP_FAIL_SVC);
	}

	@Test
	void aSubclassWithItsOwnStringFailureRecordedLoadsRecompilesAndEndsItsFailedRead() throws Exception {
		final var range = fixtureRanges(compileAgainst5170()).apply("StringMarkRange");
		oneRangeFailsEveryAttempt(read("string", range));

		assertCompilesAgainstThisEngine(STRING_MARK_RANGE);
	}

	/**
	 * The engine's range class, its supertypes and the interfaces they implement have no method of the
	 * mark's names, so none can be resolved in place of an extension's own.
	 */
	@Test
	void theEnginesRangeTypesHaveNoMethodOfTheMarksNames() {
		final var names = java.util.Set.of("markFailureRecorded", "failureRecorded");
		final List<Class<?>> types = new ArrayList<>();
		for (Class<?> type = PartialDataOperationImpl.class; type != null; type = type.getSuperclass()) {
			types.add(type);
		}
		for (var i = 0; i < types.size(); i++) {
			for (final var implemented : types.get(i).getInterfaces()) {
				if (!types.contains(implemented)) {
					types.add(implemented);
				}
			}
		}
		for (final var type : types) {
			for (final var method : type.getDeclaredMethods()) {
				assertTrue(!names.contains(method.getName()), () -> type.getName() + " declares " + method);
			}
		}
	}

	/**
	 * A subclass that takes default methods of the mark's names from its own interface, compiled
	 * against 5.17.0, still calls them, and its read ends as in 5.17.0.
	 */
	@Test
	void aSubclassWhoseInterfaceHasMethodsOfTheMarksNamesStillCallsThem() throws Exception {
		final var classes = compileAgainst5170();
		final var range = fixtureRanges(classes).apply("InterfaceMarkRange");
		final var read = read("interface", range);
		oneRangeFailsEveryAttempt(read);

		final var part = read.subOperations().get(0);
		final var ownMark = part.getClass().getMethod("ownMark", part.getClass());
		assertEquals(Boolean.TRUE, ownMark.invoke(null, part), "the interface's own method answers");
		assertCompilesAgainstThisEngine(EXTENSION_MARKS, INTERFACE_MARK_RANGE);
	}

	private void assertCompilesAgainstThisEngine(final String... sources) throws IOException {
		final var diagnostics = compile(tempDir.resolve("current"), List.of(sources));
		assertEquals("", diagnostics, "the subclass compiles against this engine");
	}

	/** Compiles the fixtures against the 5.17.0 shape; returns the directory of their classes. */
	private Path compileAgainst5170() throws IOException {
		final var out = tempDir.resolve("against-5.17.0");
		final var diagnostics = compile(
						out,
						List.of(
										PARTIAL_OPERATION_5_17_0,
										PARTIAL_DATA_OPERATION_IMPL_5_17_0,
										BOOLEAN_MARK_RANGE,
										STRING_MARK_RANGE,
										EXTENSION_MARKS,
										INTERFACE_MARK_RANGE));
		assertEquals("", diagnostics, "the fixtures compile against 5.17.0");
		return out;
	}

	/** Compiles {@code sources} on the test's class path into {@code out}; returns javac's diagnostics. */
	private static String compile(final Path out, final List<String> sources) throws IOException {
		Files.createDirectories(out);
		final var compiler = ToolProvider.getSystemJavaCompiler();
		final List<JavaFileObject> units = new ArrayList<>();
		for (final var source : sources) {
			final var pkg = source.lines().findFirst().orElseThrow().replace("package ", "").replace(";", "").strip();
			final var name = source.lines()
							.filter(line -> line.startsWith("public class ") || line.startsWith("public interface "))
							.findFirst()
							.orElseThrow()
							.split(" ")[2]
							.split("<")[0];
			units.add(new SimpleJavaFileObject(
							URI.create("string:///" + (pkg + "." + name).replace('.', '/') + ".java"),
							JavaFileObject.Kind.SOURCE) {
				@Override
				public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
					return source;
				}
			});
		}
		final var diagnostics = new StringWriter();
		final var options = List.of(
						"-proc:none", "-classpath", System.getProperty("java.class.path"), "-d", out.toString());
		final boolean ok = compiler.getTask(diagnostics, null, null, options, null, units).call();
		return ok ? "" : diagnostics.toString();
	}

	/**
	 * Loads the fixture classes compiled into {@code classes}, and no other class from there, over the
	 * engine's classes under test; returns a range factory for a fixture by simple name.
	 */
	private static java.util.function.Function<String, RangeFactory> fixtureRanges(final Path classes) {
		final var loader = new ClassLoader(ExtensionRangeCompatibilityTest.class.getClassLoader()) {
			@Override
			protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
				if (!name.startsWith(FIXTURE_PACKAGE + ".")) {
					return super.loadClass(name, resolve);
				}
				synchronized (getClassLoadingLock(name)) {
					var loaded = findLoadedClass(name);
					if (loaded == null) {
						try {
							final var bytes = Files.readAllBytes(classes.resolve(name.replace('.', '/') + ".class"));
							loaded = defineClass(name, bytes, 0, bytes.length);
						} catch (final IOException e) {
							throw new UncheckedIOException(e);
						}
					}
					return loaded;
				}
			}
		};
		return simpleName -> {
			final Constructor<?> constructor;
			try {
				constructor = Class.forName(FIXTURE_PACKAGE + "." + simpleName, true, loader)
								.getConstructor(int.class, DataItem.class, CompositeDataOperation.class);
			} catch (final ReflectiveOperationException e) {
				throw new AssertionError(e);
			}
			return (partNumber, part, parent) -> {
				try {
					return (PartialDataOperation<DataItem>) constructor.newInstance(partNumber, part, parent);
				} catch (final ReflectiveOperationException e) {
					throw new AssertionError(e);
				}
			};
		};
	}
}
