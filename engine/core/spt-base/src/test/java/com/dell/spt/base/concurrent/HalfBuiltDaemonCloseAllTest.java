package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.AsyncRunnable.State;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/**
 * A signal while the engine builds a daemon runs the exit hook's {@link DaemonBase#closeAll()} on a
 * daemon whose fields are not all set yet. It must not close it, so it logs no close failure; once
 * built, the daemon closes normally.
 */
class HalfBuiltDaemonCloseAllTest {

	private static final long WAIT_SECONDS = 30;

	/** Holds the thread that builds a daemon at one point of its constructor until the test releases it. */
	private static final class Hold {
		private static final ThreadLocal<Hold> ARMED = new ThreadLocal<>();

		private final CountDownLatch reached = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private volatile Object held;

		/** Blocks the first time it is called on a thread that {@link #build} armed, then never again. */
		static void here(final Object daemon) {
			final var hold = ARMED.get();
			if (hold == null) {
				return;
			}
			ARMED.remove();
			hold.held = daemon;
			hold.reached.countDown();
			try {
				if (!hold.release.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
					throw new IllegalStateException("the test never released the constructor");
				}
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		}
	}

	/** A daemon built on its own thread, held in its constructor at {@link Hold#here}. */
	private static final class Building<T> {
		private final Hold hold = new Hold();
		private final AtomicReference<T> built = new AtomicReference<>();
		private final AtomicReference<Throwable> failure = new AtomicReference<>();
		private final Thread thread;

		private Building(final Supplier<T> constructor) {
			thread = new Thread(() -> {
				Hold.ARMED.set(hold);
				try {
					built.set(constructor.get());
				} catch (final Throwable e) {
					failure.set(e);
				} finally {
					Hold.ARMED.remove();
				}
			}, "test-half-built-daemon");
			thread.setDaemon(true);
			thread.start();
		}

		/** The object as its constructor saw it when it reached the hold. */
		Object awaitHeld() throws InterruptedException {
			assertTrue(hold.reached.await(WAIT_SECONDS, TimeUnit.SECONDS), "the constructor never reached the hold");
			return hold.held;
		}

		T releaseAndAwait() throws InterruptedException {
			hold.release.countDown();
			thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
			assertFalse(thread.isAlive(), "the constructor did not finish");
			if (failure.get() != null) {
				throw new AssertionError("the constructor failed", failure.get());
			}
			return built.get();
		}
	}

	private static <T> Building<T> build(final Supplier<T> constructor) {
		return new Building<>(constructor);
	}

	private static boolean registered(final Object daemon) {
		return DaemonBase.isRegistered(candidate -> candidate == daemon);
	}

	private static final class HeldClient extends LinearLoadStepClient {
		HeldClient(final Config config) {
			super(config, List.of(), null, mock(MetricsManager.class));
		}

		@Override
		protected LinearLoadStepClient copyInstance(final Config config, final List<Config> ctxConfigs) {
			return new HeldClient(config);
		}

		/** The base constructor calls this to log the config, before the client's own fields are set. */
		@Override
		public String getTypeName() {
			Hold.here(this);
			return super.getTypeName();
		}
	}

	private static final class HeldLocal extends LinearLoadStepLocal {
		HeldLocal(final Config config) {
			super(config, List.of(), null, mock(MetricsManager.class));
		}

		@Override
		public String getTypeName() {
			Hold.here(this);
			return super.getTypeName();
		}
	}

	/** Built with {@link LoadStepBase}'s own registration, not a subclass's deferred one. */
	private static final class DirectStep extends LoadStepBase {
		DirectStep(final Config config) {
			super(config, Collections.emptyList(), Collections.emptyList(), mock(MetricsManager.class));
		}

		@Override
		protected void provisionTargetBuckets() {}

		@Override
		protected void init() {}

		@Override
		protected void doStartWrapped() {}

		@Override
		protected void initMetrics(
						final int originIndex,
						final OpType opType,
						final int concurrency,
						final Config metricsConfig,
						final SizeInBytes itemDataSize,
						final boolean outputColorFlag) {}

		@Override
		public boolean await(final long timeout, final TimeUnit timeUnit) {
			return true;
		}

		@Override
		public String getTypeName() {
			Hold.here(this);
			return "test";
		}
	}

	/** Registers at once, as a storage driver does, and reads a field its constructor sets after the hold. */
	private static final class ScopedDaemon extends DaemonBase {
		private final List<String> closes;

		ScopedDaemon() {
			Hold.here(this);
			closes = new ArrayList<>();
		}

		@Override
		protected void doClose() {
			closes.add("close");
		}
	}

	/** A step id of its own: a closed local step publishes artifacts that a later run must not find. */
	private static Config stepConfig() {
		final var config = TestConfigBuilder.config();
		config.val("load-step-id", "half-built-" + UUID.randomUUID());
		config.val("load-step-idAutoGenerated", false);
		return config;
	}

	private static Config standaloneDelete() {
		final var config = stepConfig();
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-step-limit-time", 0);
		return config;
	}

	/** Runs closeAll and returns the close failures it logged for this class's daemons. */
	private static List<String> closeAllFailures() throws InterruptedException {
		final var sentinel = new Throwable("closeAll test sentinel", null, false, false) {};
		final var appender = new CloseFailureAppender(sentinel);
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		logger.addAppender(appender);
		try {
			DaemonBase.closeAll();
			// The engine's loggers are asynchronous but keep their order: a record closeAll logged arrives first.
			Loggers.ERR.warn("closeAll test sentinel", sentinel);
			assertTrue(appender.awaitSentinel(WAIT_SECONDS, TimeUnit.SECONDS), "the sentinel record never arrived");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
		return appender.failures();
	}

	private static final class CloseFailureAppender extends AbstractAppender {
		private final List<String> failures = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch sentinelSeen = new CountDownLatch(1);
		private final Throwable sentinel;

		CloseFailureAppender(final Throwable sentinel) {
			super("testHalfBuiltDaemonCloseFailures", null, null, true, Property.EMPTY_ARRAY);
			this.sentinel = sentinel;
		}

		@Override
		public void append(final LogEvent event) {
			if (event.getThrown() == sentinel) {
				sentinelSeen.countDown();
				return;
			}
			final var message = event.getMessage().getFormattedMessage();
			// Another test class may leave its own daemons registered for closeAll.
			if (event.getLevel() == Level.WARN && message.startsWith("Failed to close the daemon instance")
							&& names(message)) {
				failures.add(message + ": " + event.getThrown());
			}
		}

		private static boolean names(final String message) {
			return message.contains(HalfBuiltDaemonCloseAllTest.class.getName())
							|| message.contains(LoadStepContextImpl.class.getName());
		}

		boolean awaitSentinel(final long timeout, final TimeUnit unit) throws InterruptedException {
			return sentinelSeen.await(timeout, unit);
		}

		List<String> failures() {
			return List.copyOf(failures);
		}
	}

	@Test
	void aClientHeldInItsConstructorIsLeftAloneByCloseAllAndClosesOnceBuilt() throws Exception {
		final var building = build(() -> new HeldClient(stepConfig()));
		final var held = building.awaitHeld();

		assertFalse(registered(held));
		assertEquals(List.of(), closeAllFailures());

		final var client = building.releaseAndAwait();
		assertSame(held, client);
		assertEquals(State.INITIAL, client.state());
		assertTrue(registered(client));
		client.close();
		assertTrue(client.isClosed());
		assertFalse(registered(client));
	}

	/** Its stop also reads the standalone DELETE stop coordination, which the client's own fields hold. */
	@Test
	void aStandaloneDeleteClientHeldInItsConstructorIsLeftAloneByCloseAll() throws Exception {
		final var building = build(() -> new HeldClient(standaloneDelete()));
		final var held = building.awaitHeld();

		assertEquals(List.of(), closeAllFailures());

		final var client = building.releaseAndAwait();
		assertSame(held, client);
		assertEquals(State.INITIAL, client.state());
		assertTrue(registered(client));
		// Not closed here: a never-started standalone DELETE client fails its failure budget at close, a
		// separate defect (delete-prototype-close-integrity-error), and would stay registered for the next test.
		client.abandonRegistration();
	}

	@Test
	void aLocalStepHeldInItsConstructorIsLeftAloneByCloseAllAndClosesOnceBuilt() throws Exception {
		final var building = build(() -> new HeldLocal(stepConfig()));
		final var held = building.awaitHeld();

		assertFalse(registered(held));
		assertEquals(List.of(), closeAllFailures());

		final var step = building.releaseAndAwait();
		assertSame(held, step);
		assertEquals(State.INITIAL, step.state());
		assertTrue(registered(step));
		step.close();
		assertTrue(step.isClosed());
	}

	@Test
	void aDirectlyBuiltStepIsLeftAloneWhileHeldAndClosedByCloseAllOnceBuilt() throws Exception {
		final var building = build(() -> new DirectStep(stepConfig()));
		final var held = building.awaitHeld();

		assertFalse(registered(held));
		assertEquals(List.of(), closeAllFailures());

		final var step = building.releaseAndAwait();
		assertEquals(State.INITIAL, step.state());
		assertTrue(registered(step));
		assertEquals(List.of(), closeAllFailures());
		assertTrue(step.isClosed());
	}

	@Test
	void aDaemonHeldInItsConstructionScopeIsLeftAloneByCloseAllAndClosedOnceTheScopeSucceeds() throws Exception {
		final var building = build(() -> {
			try (final var scope = DaemonBase.openConstructionScope()) {
				final var daemon = new ScopedDaemon();
				scope.succeeded();
				return daemon;
			}
		});
		final var held = building.awaitHeld();

		assertTrue(registered(held), "a daemon without deferred registration registers at once");
		assertEquals(List.of(), closeAllFailures());

		final var daemon = building.releaseAndAwait();
		assertSame(held, daemon);
		assertEquals(State.INITIAL, daemon.state());
		assertTrue(registered(daemon), "closeAll keeps a daemon it left alone");
		assertEquals(List.of(), closeAllFailures());
		assertTrue(daemon.isClosed());
		assertEquals(List.of("close"), daemon.closes);
	}

	/** Built, but its enclosing scope is still open: the construction around it may still fail. */
	@Test
	void aDaemonInANestedScopeWaitsForTheOutermostScopeToSucceed() throws Exception {
		final ScopedDaemon daemon;
		try (final var outer = DaemonBase.openConstructionScope()) {
			try (final var inner = DaemonBase.openConstructionScope()) {
				daemon = new ScopedDaemon();
				inner.succeeded();
			}
			assertEquals(List.of(), closeAllFailures());
			assertFalse(daemon.isClosed());
			outer.succeeded();
		}

		assertEquals(List.of(), closeAllFailures());
		assertTrue(daemon.isClosed());
	}

	/** The contexts registered now; a context has no id accessor, so the tests compare these sets. */
	private static Set<Daemon> registeredContexts() {
		final Set<Daemon> contexts = Collections.newSetFromMap(new IdentityHashMap<>());
		DaemonBase.isRegistered(daemon -> {
			if (daemon instanceof LoadStepContextImpl<?, ?>) {
				contexts.add(daemon);
			}
			return false;
		});
		return contexts;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static StorageDriver<Item, Operation<Item>> driver() {
		final StorageDriver<Item, Operation<Item>> driver = mock(StorageDriver.class);
		when(driver.operationLifecycle()).thenReturn((OperationLifecycleTracker) new OperationLifecycleTracker<>());
		return driver;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static LoadStepContextImpl<Item, Operation<Item>> context(
					final LoadGenerator generator, final Config config) {
		return new LoadStepContextImpl<>(
						"half-built-context", generator, driver(), mock(MetricsContext.class), config.configVal("load"), false, null);
	}

	@Test
	@SuppressWarnings("rawtypes")
	void aContextHeldInItsConstructorIsLeftAloneByCloseAllAndClosesOnceBuilt() throws Exception {
		final LoadGenerator generator = mock(LoadGenerator.class);
		// The constructor hands the generator its exhaustion callback before it sets most of its fields.
		doAnswer(invocation -> {
			Hold.here(invocation.getArgument(0));
			return null;
		}).when(generator).onSchedulingExhaustion(any());
		final var before = registeredContexts();
		final var building = build(() -> context(generator, TestConfigBuilder.config()));
		building.awaitHeld();

		assertEquals(before, registeredContexts());
		assertEquals(List.of(), closeAllFailures());

		final var context = building.releaseAndAwait();
		assertEquals(State.INITIAL, context.state());
		assertTrue(registered(context));
		context.close();
		assertTrue(context.isClosed());
	}

	@Test
	void aContextRefusedForItsConfigurationIsNotRegistered() {
		final var config = TestConfigBuilder.config();
		config.val("load-op-retry", true);
		config.val("load-op-retryLimit", -1);
		final var before = registeredContexts();

		assertThrows(IllegalConfigurationException.class, () -> context(mock(LoadGenerator.class), config));

		assertEquals(before, registeredContexts());
	}

	@Test
	void aDaemonClosedBeforeItRegistersStaysUnregistered() throws IOException {
		final var daemon = new DaemonBase(true) {};
		daemon.close();
		daemon.register();

		assertFalse(registered(daemon));
	}

	@Test
	void registeringTwiceRegistersOnce() throws IOException {
		final var daemon = new DaemonBase(true) {};
		daemon.register();
		daemon.register();
		assertTrue(registered(daemon));

		daemon.close();
		assertFalse(registered(daemon));
	}

	@Test
	void anAbandonedRegistrationCannotBeRegisteredAgain() {
		final var daemon = new DaemonBase(true) {};
		daemon.abandonRegistration();
		daemon.register();

		assertFalse(registered(daemon));
	}
}
