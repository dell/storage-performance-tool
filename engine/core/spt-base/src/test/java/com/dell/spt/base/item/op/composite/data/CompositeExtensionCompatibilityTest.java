package com.dell.spt.base.item.op.composite.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.system.SizeInBytes;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The split-read mark adds nothing an extension built before it can collide with. Two such providers
 * had their own {@code requestless()}: one in a subclass of {@link CompositeDataOperationImpl}, one as
 * an interface default on its own {@link CompositeDataOperation}. A third subclass has its own
 * {@code boolean rangesSentAsRequests()}, which the engine's mark does not consult. Two more subclasses
 * implement an interface of their own with a default of the name and descriptor of one of
 * {@link SplitReadMarks}' methods, and call it on the subclass, which {@code javac} links to the
 * subclass; on {@link CompositeDataOperationImpl} itself such a method resolved to the engine's static
 * one and failed with {@link IncompatibleClassChangeError}. Each provider calls its own method while it
 * loads, and is compiled here against {@link CompositeDataOperationImpl}'s shape before the mark (a stub with only
 * its constructor, ahead of this build on the class path), packed as an extension jar, loaded the way
 * the engine loads extensions, and its finished, unmarked read is given to the load step, which keeps
 * its sample as in 5.17.0.
 */
final class CompositeExtensionCompatibilityTest {

	private static final long PART_SIZE = 1024;
	private static final int PARTS = 4;
	private static final long PAUSE_NANOS = 5_000;

	/**
	 * The methods the two interfaces declare at this branch's base, as name/parameter count: 5.17.0's,
	 * and {@code resetSubOperations()}, added after it.
	 */
	private static final Map<Class<?>, List<String>> INTERFACES_AT_BRANCH_BASE = Map.of(
					CompositeOperation.class, List.of("allSubOperationsDone/0", "get/1", "item/0",
									"markSubTaskCompleted/0", "nextSubOperations/1", "put/2", "resetSubOperations/0",
									"subOperations/0", "undoMarkSubTaskCompleted/0"),
					CompositeDataOperation.class, List.of("sizeThreshold/0", "subOperations/0"));

	/** {@link CompositeDataOperationImpl} as an extension compiled before the mark saw it. */
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

	/** A subclass whose own getter has the mark's name and answers true. */
	private static final String NAMED_GETTER = """
					package legacy.composite;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;

					public class LegacyNamedGetter<I extends DataItem> extends CompositeDataOperationImpl<I> {

						public LegacyNamedGetter(final I item, final long threshold) {
							super(0, OpType.READ, item, null, "/bucket", null, null, 0, threshold);
						}

						/** The extension's own meaning, unrelated to the engine's mark. */
						public boolean rangesSentAsRequests() {
							return true;
						}
					}
					""";

	private static final String SUBCLASS = """
					package legacy.composite;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;

					public class LegacySubclass<I extends DataItem> extends CompositeDataOperationImpl<I> {

						public LegacySubclass(final I item, final long threshold) {
							super(0, OpType.READ, item, null, "/bucket", null, null, 0, threshold);
						}

						/** The extension's own meaning, unrelated to the engine's mark. */
						public boolean requestless() {
							return true;
						}

						public void markRequestless() {}
					}
					""";

	private static final String OWN_TYPE = """
					package legacy.composite;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
					import com.dell.spt.base.item.op.data.DataOperationImpl;
					import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
					import java.util.List;

					public class LegacyOwnComposite<I extends DataItem> extends DataOperationImpl<I>
									implements CompositeDataOperation<I>, LegacyMarks {


						private final long threshold;

						public LegacyOwnComposite(final I item, final long threshold) {
							super(0, OpType.READ, item, null, "/bucket", null, null, 0);
							this.threshold = threshold;
						}

						@Override
						public String get(final String key) {
							return null;
						}

						@Override
						public void put(final String key, final String value) {}

						@Override
						public List<? extends PartialDataOperation<I>> subOperations() {
							return List.of();
						}

						@Override
						public void markSubTaskCompleted() {}

						@Override
						public boolean allSubOperationsDone() {
							return true;
						}

						@Override
						public long sizeThreshold() {
							return threshold;
						}
					}
					""";

	/** The second provider's own interface, with its own defaults. */
	private static final String MARKS = """
					package legacy.composite;

					public interface LegacyMarks {
						default boolean requestless() {
							return true;
						}

						default void markRequestless() {}
					}
					""";

	/** A subclass that takes a getter of the name and descriptor of the engine's from its own interface. */
	private static final String DEFAULT_GETTER = """
					package legacy.composite;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;

					public class LegacyDefaultGetter<I extends DataItem> extends CompositeDataOperationImpl<I>
									implements LegacyMarkQueries {

						public LegacyDefaultGetter(final I item, final long threshold) {
							super(0, OpType.READ, item, null, "/bucket", null, null, 0, threshold);
						}
					}
					""";

	private static final String MARK_QUERIES = """
					package legacy.composite;

					import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;

					public interface LegacyMarkQueries {
						/** The extension's own meaning, unrelated to the engine's mark. */
						default boolean rangesSentAsRequests(final CompositeDataOperationImpl<?> op) {
							return true;
						}
					}
					""";

	/** A subclass that takes a setter of the name and descriptor of the engine's from its own interface. */
	private static final String DEFAULT_MARKER = """
					package legacy.composite;

					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.op.OpType;
					import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;

					public class LegacyDefaultMarker<I extends DataItem> extends CompositeDataOperationImpl<I>
									implements LegacyMarkCommands {

						public LegacyDefaultMarker(final I item, final long threshold) {
							super(0, OpType.READ, item, null, "/bucket", null, null, 0, threshold);
						}
					}
					""";

	private static final String MARK_COMMANDS = """
					package legacy.composite;

					import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
					import java.util.concurrent.atomic.AtomicInteger;

					public interface LegacyMarkCommands {
						AtomicInteger CALLS = new AtomicInteger();

						/** The extension's own meaning: it counts calls and leaves the engine's mark alone. */
						default void markRangesSentAsRequests(final CompositeDataOperationImpl<?> op) {
							CALLS.incrementAndGet();
						}
					}
					""";

	private static final String PROVIDER = """
					package legacy.composite;

					import com.dell.spt.base.env.Extension;
					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.DataItemImpl;
					import com.dell.spt.base.item.op.Operation;
					import com.github.akurilov.confuse.Config;
					import com.github.akurilov.confuse.SchemaProvider;
					import java.nio.file.Path;

					/** Builds its composite and calls its own method when it is loaded, so that loading links both. */
					public class %1$sExtension implements Extension {

						private final Object probe = ownAnswer(composite(new DataItemImpl("/bucket/probe", 0, 4096), 1024));

						public Operation<DataItem> composite(final DataItem item, final long threshold) {
							return new %1$s<>(item, threshold);
						}

						/** Calls the composite's own method through the composite's own type. */
						@SuppressWarnings("unchecked")
						public Object ownAnswer(final Operation<DataItem> op) {
							final %1$s<DataItem> c = (%1$s<DataItem>) op;
							%2$s
						}

						@Override
						public String id() {
							return "legacy-%1$s";
						}

						@Override
						public Config defaults(final Path appHomePath) {
							return null;
						}

						@Override
						public SchemaProvider schemaProvider() {
							return null;
						}

						@Override
						public void install(final Path dstPath) {}
					}
					""";

	@TempDir
	Path dir;

	@Test
	void theCompositeInterfacesDeclareOnlyTheirBranchBaseMethods() {
		for (final var entry : INTERFACES_AT_BRANCH_BASE.entrySet()) {
			final var declared = Arrays.stream(entry.getKey().getDeclaredMethods())
							.filter(method -> !method.isSynthetic())
							.map(method -> method.getName() + "/" + method.getParameterCount())
							.collect(Collectors.toCollection(TreeSet::new));
			assertEquals(new TreeSet<>(entry.getValue()), declared, entry.getKey().getSimpleName());
		}
	}

	/**
	 * The mark is reached through {@link SplitReadMarks} only: {@link CompositeDataOperationImpl} and its
	 * supertypes declare no method of either name, so no method an extension's subclass, or an interface
	 * of the subclass, already has under either name, whatever its descriptor, overrides, replaces or
	 * links to the engine's.
	 */
	@Test
	void theMarkIsReachedOnlyThroughItsHelperClass() throws Exception {
		final var names = List.of("rangesSentAsRequests", "markRangesSentAsRequests");
		final var types = new ArrayList<Class<?>>();
		final var pending = new ArrayList<Class<?>>(List.of(CompositeDataOperationImpl.class));
		while (!pending.isEmpty()) {
			final Class<?> type = pending.remove(pending.size() - 1);
			if (type == null || types.contains(type)) {
				continue;
			}
			types.add(type);
			pending.add(type.getSuperclass());
			pending.addAll(List.of(type.getInterfaces()));
		}
		assertTrue(types.contains(CompositeOperation.class), "the walk reaches the interfaces: " + types);
		for (final var type : types) {
			for (final var method : type.getDeclaredMethods()) {
				assertFalse(names.contains(method.getName()), type.getName() + " declares " + method);
			}
		}

		assertTrue(Modifier.isFinal(SplitReadMarks.class.getModifiers()), "the helper class is final");
		for (final var constructor : SplitReadMarks.class.getDeclaredConstructors()) {
			assertTrue(Modifier.isPrivate(constructor.getModifiers()), "its constructor is private: " + constructor);
		}
		final var declared = Arrays.stream(SplitReadMarks.class.getDeclaredMethods())
						.filter(method -> !method.isSynthetic())
						.toList();
		assertEquals(new TreeSet<>(names), declared.stream().map(Method::getName).collect(Collectors.toCollection(TreeSet::new)),
						"its methods: " + declared);
		assertEquals(2, declared.size(), "its methods: " + declared);
		for (final Method method : declared) {
			assertTrue(Modifier.isPublic(method.getModifiers()), method + " is public");
			assertTrue(Modifier.isStatic(method.getModifiers()), method + " is static");
			assertEquals(List.of(CompositeDataOperationImpl.class), List.of(method.getParameterTypes()), method + " takes the operation");
		}
		assertFalse(Modifier.isFinal(CompositeDataOperationImpl.class.getMethod("reset").getModifiers()), "reset() stays overridable");
	}

	@Test
	void aSubclassWithItsOwnRequestlessLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample("LegacySubclass", "return c.requestless();", true, Map.of("LegacySubclass", SUBCLASS));
	}

	@Test
	void anOwnCompositeWithItsOwnRequestlessDefaultLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample("LegacyOwnComposite", "return c.requestless();", true,
						Map.of("LegacyOwnComposite", OWN_TYPE, "LegacyMarks", MARKS));
	}

	/** Its own {@code rangesSentAsRequests()} answers true; the engine's mark is unset, so the parent keeps its sample. */
	@Test
	void aSubclassWithItsOwnMarkGetterLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample(
						"LegacyNamedGetter", "return c.rangesSentAsRequests();", true, Map.of("LegacyNamedGetter", NAMED_GETTER));
	}

	/**
	 * Its interface's default {@code rangesSentAsRequests(CompositeDataOperationImpl)}, called on the
	 * subclass, answers true; the engine's mark is unset, so the parent keeps its sample.
	 */
	@Test
	void aSubclassWithAnInterfaceDefaultMarkGetterLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample("LegacyDefaultGetter", "return c.rangesSentAsRequests(c);", true,
						Map.of("LegacyDefaultGetter", DEFAULT_GETTER, "LegacyMarkQueries", MARK_QUERIES));
	}

	/**
	 * Its interface's default {@code markRangesSentAsRequests(CompositeDataOperationImpl)}, called on the
	 * subclass, runs once per call and does not mark the read, so the parent keeps its sample.
	 */
	@Test
	void aSubclassWithAnInterfaceDefaultMarkSetterLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample("LegacyDefaultMarker", "c.markRangesSentAsRequests(c); return LegacyMarkCommands.CALLS.get();",
						2, Map.of("LegacyDefaultMarker", DEFAULT_MARKER, "LegacyMarkCommands", MARK_COMMANDS));
	}

	/**
	 * {@code ownCall} is the body of the provider's {@code ownAnswer}, which has the composite as
	 * {@code c}, and {@code expected} what the test's own call of it returns, after the provider's call
	 * while it loaded.
	 */
	private void assertLoadsAndKeepsItsSample(
					final String type, final String ownCall, final Object expected, final Map<String, String> sources)
					throws Exception {
		final var jarDir = dir.resolve("ext");
		Files.createDirectories(jarDir);
		buildExtensionJar(type, ownCall, sources, jarDir.resolve(type + ".jar"));
		try (var loader = Extension.extClassLoader(jarDir.toFile())) {
			final var extension = Extension.load(loader).stream()
							.filter(loaded -> ("legacy-" + type).equals(loaded.id()))
							.findFirst()
							.orElse(null);
			assertNotNull(extension, "the provider loaded");
			final DataItem item = new DataItemImpl("/bucket/" + type, 0, PARTS * PART_SIZE);
			@SuppressWarnings("unchecked")
			final var op = (Operation<DataItem>) extension.getClass()
							.getMethod("composite", DataItem.class, long.class)
							.invoke(extension, item, PART_SIZE);
			assertEquals(expected, extension.getClass().getMethod("ownAnswer", Operation.class).invoke(extension, op),
							"its own method answers");
			finish(op);

			try (var metrics = metricsContext()) {
				assertTrue(context(metrics).put(op));
				final var snapshot = metrics.terminalSnapshot();
				assertEquals(1, snapshot.successSnapshot().count(), "reads");
				assertEquals(1, snapshot.durationSnapshot().count(), "its parent sample, as in 5.17.0");
				assertEquals(op.duration(), snapshot.durationSnapshot().sum(), "duration sum");
			}
		}
	}

	/**
	 * Compiles the composite and its provider against the pre-mark stub ahead of this classpath, and
	 * packs them, without the stub, with a service entry.
	 */
	private void buildExtensionJar(final String type, final String ownCall, final Map<String, String> sources, final Path jar)
					throws IOException {
		final var src = dir.resolve("src/" + type + "/legacy/composite");
		final var classes = dir.resolve("classes/" + type);
		final var stubSrc = dir.resolve("stub-src/" + type + "/CompositeDataOperationImpl.java");
		final var stubClasses = dir.resolve("stub-classes/" + type);
		Files.createDirectories(src);
		Files.createDirectories(classes);
		Files.createDirectories(stubSrc.getParent());
		Files.createDirectories(stubClasses);
		Files.writeString(stubSrc, PRE_MARK_IMPL);
		final var classpath = System.getProperty("java.class.path");
		final var compiler = ToolProvider.getSystemJavaCompiler();
		assertNotNull(compiler, "the tests run on a JDK");
		assertEquals(0, compiler.run(null, null, null, "-proc:none", "-nowarn", "-cp", classpath, "-d", stubClasses.toString(),
						stubSrc.toString()), "the pre-mark stub compiles");
		final List<String> args = new ArrayList<>(List.of("-proc:none", "-nowarn", "-cp",
						stubClasses + File.pathSeparator + classpath, "-d", classes.toString()));
		for (final var source : sources.entrySet()) {
			final var file = src.resolve(source.getKey() + ".java");
			Files.writeString(file, source.getValue());
			args.add(file.toString());
		}
		final var provider = src.resolve(type + "Extension.java");
		Files.writeString(provider, PROVIDER.formatted(type, ownCall));
		args.add(provider.toString());
		final int status = compiler.run(null, null, null, args.toArray(String[]::new));
		assertEquals(0, status, "the pre-mark source compiles");
		try (var out = new JarOutputStream(Files.newOutputStream(jar));
						var files = Files.walk(classes)) {
			for (final var file : files.filter(Files::isRegularFile).toList()) {
				out.putNextEntry(new JarEntry(classes.relativize(file).toString().replace(File.separatorChar, '/')));
				out.write(Files.readAllBytes(file));
				out.closeEntry();
			}
			out.putNextEntry(new JarEntry("META-INF/services/" + Extension.class.getName()));
			out.write(("legacy.composite." + type + "Extension\n").getBytes(StandardCharsets.UTF_8));
			out.closeEntry();
		}
	}

	/** Runs the read's own pass, settles its parts and ends it, each step timed as a request. */
	private static void finish(final Operation<DataItem> op) {
		op.startRequest();
		pause();
		op.finishRequest();
		pause();
		op.startResponse();
		final var composite = (CompositeOperation<?>) op;
		for (var part = composite.subOperations().size(); part > 0; part--) {
			composite.markSubTaskCompleted();
		}
		pause();
		op.finishResponse();
		op.status(Status.SUCC);
		((DataOperationImpl<?>) op).countBytesDone(PARTS * PART_SIZE);
		assertTrue(composite.allSubOperationsDone(), "the read is finished");
	}

	private static void pause() {
		final long start = System.nanoTime();
		while (System.nanoTime() - start < PAUSE_NANOS) {
			Thread.onSpinWait();
		}
	}

	@SuppressWarnings("unchecked")
	private static LoadStepContextImpl<DataItem, Operation<DataItem>> context(
					final MetricsContext<AllMetricsSnapshot> metrics) {
		final var config = TestConfigBuilder.config();
		config.val("load-op-retry", false);
		config.val("load-op-recycle-mode", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		return new LoadStepContextImpl<>("extension-compatibility", generator, driver, metrics, config.configVal("load"), false);
	}

	private static MetricsContext<AllMetricsSnapshot> metricsContext() {
		final MetricsContext<AllMetricsSnapshot> context = MetricsContextImpl.builder()
						.loadStepId("extension-compatibility")
						.opType(OpType.READ)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(PART_SIZE))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		context.start();
		return context;
	}
}
