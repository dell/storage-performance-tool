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
 * {@code boolean rangesSentAsRequests()}, which the engine's static mark does not consult. Each is
 * compiled here against {@link CompositeDataOperationImpl}'s shape before the mark (a stub with only
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

	private static final String PROVIDER = """
					package legacy.composite;

					import com.dell.spt.base.env.Extension;
					import com.dell.spt.base.item.DataItem;
					import com.dell.spt.base.item.DataItemImpl;
					import com.dell.spt.base.item.op.Operation;
					import com.github.akurilov.confuse.Config;
					import com.github.akurilov.confuse.SchemaProvider;
					import java.nio.file.Path;

					/** Builds its composite when it is loaded, so that loading links the class. */
					public class %1$sExtension implements Extension {

						private final Class<?> type = new %1$s<DataItem>(new DataItemImpl("/bucket/probe", 0, 4096), 1024).getClass();

						public Operation<DataItem> composite(final DataItem item, final long threshold) {
							return new %1$s<>(item, threshold);
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
	 * The mark is reached through static methods only, which take the operation: no subclass method
	 * overrides them, and none an extension already has, of any name, is called in their place.
	 */
	@Test
	void theMarkIsReachedOnlyThroughStaticMethods() throws Exception {
		for (final var name : List.of("rangesSentAsRequests", "markRangesSentAsRequests")) {
			final var methods = Arrays.stream(CompositeDataOperationImpl.class.getDeclaredMethods())
							.filter(method -> method.getName().equals(name))
							.toList();
			assertEquals(1, methods.size(), name + ": " + methods);
			final Method method = methods.get(0);
			assertTrue(Modifier.isPublic(method.getModifiers()), name + " is public");
			assertTrue(Modifier.isStatic(method.getModifiers()), name + " is static");
			assertEquals(List.of(CompositeDataOperationImpl.class), List.of(method.getParameterTypes()), name + " takes the operation");
		}
		assertFalse(Modifier.isFinal(CompositeDataOperationImpl.class.getMethod("reset").getModifiers()), "reset() stays overridable");
	}

	@Test
	void aSubclassWithItsOwnRequestlessLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample("LegacySubclass", "requestless", Map.of("LegacySubclass", SUBCLASS));
	}

	@Test
	void anOwnCompositeWithItsOwnRequestlessDefaultLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample(
						"LegacyOwnComposite", "requestless", Map.of("LegacyOwnComposite", OWN_TYPE, "LegacyMarks", MARKS));
	}

	/** Its own {@code rangesSentAsRequests()} answers true; the engine's mark is unset, so the parent keeps its sample. */
	@Test
	void aSubclassWithItsOwnMarkGetterLoadsAndKeepsItsSample() throws Exception {
		assertLoadsAndKeepsItsSample("LegacyNamedGetter", "rangesSentAsRequests", Map.of("LegacyNamedGetter", NAMED_GETTER));
	}

	private void assertLoadsAndKeepsItsSample(final String type, final String ownMethod, final Map<String, String> sources)
					throws Exception {
		final var jarDir = dir.resolve("ext");
		Files.createDirectories(jarDir);
		buildExtensionJar(type, sources, jarDir.resolve(type + ".jar"));
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
			assertEquals(Boolean.TRUE, op.getClass().getMethod(ownMethod).invoke(op), "its own method answers");
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
	private void buildExtensionJar(final String type, final Map<String, String> sources, final Path jar) throws IOException {
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
		Files.writeString(provider, PROVIDER.formatted(type));
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
