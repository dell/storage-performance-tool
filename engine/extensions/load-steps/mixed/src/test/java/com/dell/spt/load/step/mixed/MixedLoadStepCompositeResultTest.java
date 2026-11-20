package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.ThreadContext;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The mixed step's result wrapper retires a multipart (composite) operation once, at its last
 * result, on the cooperative mock: 16 MiB objects in 4 MiB parts at concurrency 8. The generator's
 * permits never exceed its limit and return to it after each operation, and the operation counts
 * as one in flight from its dispatch to its last result.
 */
@Timeout(120)
@SuppressWarnings({"rawtypes", "unchecked"
})
class MixedLoadStepCompositeResultTest {

	private static final int CONCURRENCY = 8;
	private static final long SIZE = 16L << 20;
	private static final String CHILD_ENQUEUE_TIMEOUT = "spt.mpu.child.enqueue.timeout.millis";

	@TempDir
	Path home;

	@BeforeEach
	void setUp() {
		ThreadContext.put("home_dir", home.toString());
	}

	@AfterEach
	void tearDown() {
		ThreadContext.remove("home_dir");
		System.clearProperty(CHILD_ENQUEUE_TIMEOUT);
	}

	enum Mode {
		/** A clean upload: initiation, four parts, completion. */
		CLEAN_UPLOAD(false),
		/** The initiation fails: its one result ends it. */
		FAILED_INITIATION(false),
		/** The completion fails. */
		FAILED_COMPLETION(false),
		/** Part 1 runs out of retries; the abort ends the upload with a failure, as the S3 drivers do. */
		PART_OUT_OF_RETRIES_ABORT_FAILS(false),
		/** As above, but the abort ends as a success, as the cooperative mock and {@code fs} leave it. */
		PART_OUT_OF_RETRIES_ABORT_SUCCEEDS(false),
		/**
		 * A control for an accepted limit: as above, with part 3 still in flight when the abort ends
		 * as a success. That unfinished success looks like an initiation, and the cooperative base
		 * never finalizes the upload, so each such upload keeps one permit and one in-flight count.
		 */
		UNFINISHED_ABORT_SUCCEEDS(false),
		/** Part 1 fails once and its retry succeeds. */
		PART_RETRY(false),
		/** The initiation ends {@code INTERRUPTED}: the driver sends nothing more for it. */
		INTERRUPTED_INITIATION(false),
		/** The initiation ends {@code PENDING}: the driver sends nothing more for it. */
		PENDING_INITIATION(false),
		/**
		 * The child queue holds two operations, the dispatcher takes one at a time, and the parts stay
		 * unsent until the upload's failure
		 * is published: the enqueue of the rest times out ({@code failChildOpsEnqueue}), the upload's
		 * abort cannot be queued either and times out too, so the upload is published as a
		 * {@code FAIL_TIMEOUT} failure without its completion or abort being sent.
		 */
		CHILD_ENQUEUE_TIMEOUT(false),
		/** A clean range read. */
		RANGE_READ(true),
		/** A range read whose range 1 runs out of retries; its finalization reports the failure. */
		RANGE_READ_FAILED_PART(true);

		final boolean read;

		Mode(final boolean read) {
			this.read = read;
		}
	}

	@TestFactory
	Stream<DynamicTest> permitsReturnToTheLimitAndTheOperationCountsAsOneUntilItsLastResult() {
		return Arrays.stream(Mode.values())
						.map(mode -> DynamicTest.dynamicTest(mode.name(), () -> runMode(mode)));
	}

	private void runMode(final Mode mode) throws Exception {
		ThreadContext.put("home_dir", home.toString());
		if (mode == Mode.CHILD_ENQUEUE_TIMEOUT) {
			System.setProperty(CHILD_ENQUEUE_TIMEOUT, "200");
		}
		final ScriptedFactory factory = new ScriptedFactory(mode, null);
		final MixedLoadStepLocal step = newStep(mode, factory);
		final ErrorLogCapture errors = new ErrorLogCapture();
		try {
			step.init();
			final MixedLoadGenerator generator = generator(step);
			final Semaphore permits = permits(generator);
			factory.observe(generator, permits);
			factory.recordResults();
			factory.driver.get().start();
			final OpType type = mode.read ? OpType.READ : OpType.CREATE;
			assertEquals(CONCURRENCY, permits.availablePermits(), "precondition");
			for (int n = 1; n <= 2; n++) {
				factory.startNext();
				dispatchComposite(generator, factory, n);
				awaitQuiet(factory);
				// The control keeps one permit and one count for each upload; every other mode none.
				final int kept = mode == Mode.UNFINISHED_ABORT_SUCCEEDS ? n : 0;
				awaitSettled(generator, permits, type, kept);
				assertEquals(CONCURRENCY - kept, permits.availablePermits(),
								mode + ": permits after operation " + n + ": " + factory.sequence);
				assertEquals(kept, generator.inFlightCount(type), mode + ": in flight after operation " + n + ": " + factory.sequence);
				if (mode == Mode.CHILD_ENQUEUE_TIMEOUT) {
					// Each upload: the enqueue of its parts times out, then that of its abort, and the
					// driver publishes the upload as a failure.
					assertEquals(n, errors.errorsContaining("timed out enqueueing MPU part operation").size(),
									"upload " + n + "'s parts timed out: " + factory.results);
					assertEquals(n, errors.errorsContaining("timed out enqueueing MPU abort operation").size(),
									"upload " + n + "'s abort timed out: " + factory.results);
					assertEquals(n, factory.timeoutFailures.get(),
									"upload " + n + "'s FAIL_TIMEOUT result was published: " + factory.results);
					assertTrue(factory.partsHeldForTimeout.get(), "its parts were held until the enqueue timed out");
				}
			}
			if (mode == Mode.CHILD_ENQUEUE_TIMEOUT) {
				assertTrue(!factory.sequence.toString().contains("Cfinal") && !factory.sequence.toString().contains("Cabort"),
								"no upload's completion or abort was sent: " + factory.sequence);
			}
			assertTrue(factory.maxPermits.get() <= CONCURRENCY, mode + ": permits peaked at " + factory.maxPermits.get());
			// In the control the first upload's kept count adds one while the second one's parts run.
			// With an enqueue timeout, the parts held in the dispatcher reach the driver only after their
			// upload's failure has retired it.
			final int partsOfFirst = factory.partsOfFirst.get();
			for (int i = 0; i < factory.inFlightDuringParts.size(); i++) {
				final int expected = mode == Mode.CHILD_ENQUEUE_TIMEOUT
								? 0
								: mode == Mode.UNFINISHED_ABORT_SUCCEEDS && i >= partsOfFirst ? 2 : 1;
				assertEquals(expected, factory.inFlightDuringParts.get(i),
								mode + ": in flight while its parts ran: " + factory.inFlightDuringParts);
			}
			if (mode == Mode.UNFINISHED_ABORT_SUCCEEDS) {
				assertTrue(factory.unfinishedAbort.get(), "the abort ended with a part in flight: " + factory.sequence);
			}
			if (mode == Mode.PART_RETRY) {
				assertTrue(factory.sequence.toString().contains("P1:FAIL_IO"), "a part attempt failed: " + factory.sequence);
			}
		} finally {
			factory.releaseAll();
			step.close();
			errors.close();
			System.clearProperty(CHILD_ENQUEUE_TIMEOUT);
		}
	}

	/**
	 * The ticket's reproduction: right after the initiation's submit returns, with the parts held
	 * behind a latch, the upload still counts as one CREATE in flight.
	 */
	@Test
	void anUploadCountsAsOneInFlightRightAfterItsInitiation() throws Exception {
		final CountDownLatch partsRelease = new CountDownLatch(1);
		final ScriptedFactory factory = new ScriptedFactory(Mode.CLEAN_UPLOAD, partsRelease);
		final MixedLoadStepLocal step = newStep(Mode.CLEAN_UPLOAD, factory);
		try {
			step.init();
			final MixedLoadGenerator generator = generator(step);
			factory.observe(generator, permits(generator));
			factory.driver.get().start();
			factory.startNext();
			dispatchComposite(generator, factory, 1);
			assertTrue(factory.initiated.await(10, TimeUnit.SECONDS), "the initiation was published");
			assertEquals(1, factory.inFlightAfterInitiation.get(), "one multipart CREATE still has its parts to run");
			partsRelease.countDown();
			awaitQuiet(factory);
			assertEquals(0, generator.inFlightCount(OpType.CREATE), "retired at its completion: " + factory.sequence);
		} finally {
			partsRelease.countDown();
			factory.releaseAll();
			step.close();
		}
	}

	private MixedLoadStepLocal newStep(final Mode mode, final ScriptedFactory factory) throws Exception {
		final Config config = MixedLoadStepCompositeStopTest.defaultConfig();
		config.val("load-step-id", "mixed-composite-result-" + UUID.randomUUID());
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-type", "create");
		config.val("load-op-wait-limit", 0);
		config.val("load-op-weights-get", mode.read ? 99 : 0);
		config.val("load-op-weights-put", mode.read ? 0 : 99);
		config.val("load-op-weights-delete", 0);
		config.val("load-op-weights-stat", 1);
		config.val("storage-driver-type", ScriptedFactory.TYPE);
		config.val("storage-driver-limit-concurrency", CONCURRENCY);
		if (mode == Mode.CHILD_ENQUEUE_TIMEOUT) {
			// The child queue holds two operations, and the dispatcher's buffer one, so that queued
			// parts it cannot send keep the queue full.
			config.val("storage-driver-limit-queue-input", 2);
			config.val("load-batch-size", 1);
		}
		config.val("storage-targetBucket-provision", false);
		config.val("item-data-size", "16MiB");
		config.val("item-data-ranges-threshold", "4MiB");
		config.val("item-output-path", "/bucket");
		config.val("item-output-file", home.resolve("created-" + UUID.randomUUID() + ".csv").toString());
		final Path seeds = home.resolve("seeds-" + UUID.randomUUID() + ".csv");
		Files.writeString(seeds, new DataItemImpl("/bucket/seed", 0, SIZE).toString() + "\n");
		config.val("item-input-file", seeds.toString());
		return new MixedLoadStepLocal(config, List.of(new MixedLoadStepExtension(), factory), List.of(),
						mock(MetricsManager.class));
	}

	private static MixedLoadGenerator generator(final MixedLoadStepLocal step) throws Exception {
		final Field field = MixedLoadStepLocal.class.getDeclaredField("mixedGenerator");
		field.setAccessible(true);
		return (MixedLoadGenerator) field.get(step);
	}

	private static Semaphore permits(final MixedLoadGenerator generator) throws Exception {
		final Field field = MixedLoadGenerator.class.getDeclaredField("concurrencyThrottle");
		field.setAccessible(true);
		return (Semaphore) field.get(generator);
	}

	/** Runs the generator until it has dispatched composite number {@code n}; a STAT may come first. */
	private static void dispatchComposite(final MixedLoadGenerator generator, final ScriptedFactory factory, final int n)
					throws Exception {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (factory.compositesSubmitted.get() < n) {
			assertTrue(System.nanoTime() < deadline, "composite " + n + " was dispatched");
			generator.doWork();
			awaitQuiet(factory);
		}
	}

	/**
	 * Waits, bounded, for the permits to return and the in-flight count to clear: an enqueue timeout
	 * publishes its upload from the enqueue thread, outside the mock's submit.
	 */
	private static void awaitSettled(
					final MixedLoadGenerator generator, final Semaphore permits, final OpType type, final int kept)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (System.nanoTime() < deadline
						&& (permits.availablePermits() != CONCURRENCY - kept || generator.inFlightCount(type) != kept)) {
			Thread.sleep(10);
		}
	}

	/** Waits until the driver has published nothing new for a while. */
	private static void awaitQuiet(final ScriptedFactory factory) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		int last = -1;
		long stableSince = System.nanoTime();
		while (System.nanoTime() < deadline) {
			final int now = factory.published.get();
			if (now != last) {
				last = now;
				stableSince = System.nanoTime();
			} else if (System.nanoTime() - stableSince > TimeUnit.MILLISECONDS.toNanos(400)) {
				return;
			}
			Thread.sleep(10);
		}
	}

	/** The cooperative mock, publishing each composite's results as the mode scripts them. */
	static final class ScriptedFactory implements StorageDriverFactory {

		static final String TYPE = "composite-result-scripted-mock";

		final Mode mode;
		private final CountDownLatch partsRelease;
		final AtomicReference<CoopStorageDriverMock> driver = new AtomicReference<>();
		final AtomicInteger published = new AtomicInteger();
		final AtomicInteger compositesSubmitted = new AtomicInteger();
		final AtomicInteger maxPermits = new AtomicInteger();
		final AtomicInteger inFlightAfterInitiation = new AtomicInteger(-1);
		final CountDownLatch initiated = new CountDownLatch(1);
		final List<Integer> inFlightDuringParts = new CopyOnWriteArrayList<>();
		final StringBuffer sequence = new StringBuffer();
		private volatile long holdPartsUntilNanos;
		// Opened when the current upload's FAIL_TIMEOUT result is published (CHILD_ENQUEUE_TIMEOUT).
		private volatile CountDownLatch enqueueTimedOut = new CountDownLatch(0);
		final AtomicBoolean partsHeldForTimeout = new AtomicBoolean();
		/** Composite results published with {@code FAIL_TIMEOUT}. */
		final AtomicInteger timeoutFailures = new AtomicInteger();
		/** Every result the driver published, as tag and status. */
		final StringBuffer results = new StringBuffer();
		private final AtomicBoolean holdPart3 = new AtomicBoolean();
		private final AtomicReference<Operation> inFlightPart = new AtomicReference<>();
		/** Whether an abort was published unfinished, that is with a part still in flight. */
		final AtomicBoolean unfinishedAbort = new AtomicBoolean();
		private final AtomicInteger part1Failures = new AtomicInteger();
		private volatile MixedLoadGenerator generator;
		private volatile Semaphore permits;

		ScriptedFactory(final Mode mode, final CountDownLatch partsRelease) {
			this.mode = mode;
			this.partsRelease = partsRelease;
		}

		void observe(final MixedLoadGenerator generator, final Semaphore permits) {
			this.generator = generator;
			this.permits = permits;
		}

		/** How many part attempts the first operation made, when the second starts. */
		final AtomicInteger partsOfFirst = new AtomicInteger(Integer.MAX_VALUE);

		void startNext() {
			if (compositesSubmitted.get() == 1) {
				partsOfFirst.set(inFlightDuringParts.size());
			}
			part1Failures.set(0);
			// Held until the upload's FAIL_TIMEOUT result is published, past both 200 ms enqueue
			// timeouts, of the parts and then of the abort; bounded, so a run that never times out
			// fails its assertions instead of hanging.
			if (mode == Mode.CHILD_ENQUEUE_TIMEOUT) {
				enqueueTimedOut = new CountDownLatch(1);
				holdPartsUntilNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			} else {
				holdPartsUntilNanos = System.nanoTime();
			}
			holdPart3.set(mode == Mode.UNFINISHED_ABORT_SUCCEEDS);
		}

		void releaseAll() {
			holdPartsUntilNanos = System.nanoTime();
			enqueueTimedOut.countDown();
			holdPart3.set(false);
		}

		/** Records every result the driver publishes, ahead of the step's result output. */
		void recordResults() throws ReflectiveOperationException {
			final Field field = StorageDriverBase.class.getDeclaredField("opResultOut");
			field.setAccessible(true);
			final Output output = (Output) field.get(driver.get());
			field.set(driver.get(), new Output() {
				@Override
				public boolean put(final Object op) {
					final boolean accepted = output.put(op);
					record((Operation) op);
					return accepted;
				}

				@Override
				public int put(final List ops, final int from, final int to) {
					final int accepted = output.put(ops, from, to);
					for (int i = from; i < to; i++) {
						record((Operation) ops.get(i));
					}
					return accepted;
				}

				@Override
				public int put(final List ops) {
					return put(ops, 0, ops.size());
				}

				@Override
				public Input getInput() {
					return output.getInput();
				}

				@Override
				public void close() throws Exception {
					output.close();
				}
			});
		}

		private void record(final Operation op) {
			results.append(tagOf(op)).append(':').append(op.status()).append(' ');
			if (op instanceof CompositeOperation && op.status() == Operation.Status.FAIL_TIMEOUT) {
				timeoutFailures.incrementAndGet();
				enqueueTimedOut.countDown();
			}
		}

		@Override
		public String id() {
			return TYPE;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		public void install(final Path dstPath) {}

		@Override
		public Config defaults(final Path appHomePath) {
			return null;
		}

		@Override
		public StorageDriver create(
						final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
						final int batchSize) {
			final List<Object> seen = new ArrayList<>();
			final CoopStorageDriverMock created = new CoopStorageDriverMock(stepId, dataInput, storageConfig, verifyFlag,
							batchSize) {
				@Override
				protected boolean submit(final Operation op) {
					final OpType type = mode.read ? OpType.READ : OpType.CREATE;
					if (op instanceof PartialOperation part) {
						if (System.nanoTime() < holdPartsUntilNanos && enqueueTimedOut.getCount() > 0) {
							if (mode == Mode.CHILD_ENQUEUE_TIMEOUT) {
								partsHeldForTimeout.set(true);
							}
							LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
							return false;
						}
						if (holdPart3.get() && part.partNumber() == 3) {
							// Sent, and answered only after the upload's abort ends.
							if (!beginDispatch(op)) {
								return false;
							}
							op.startRequest();
							op.finishRequest();
							inFlightPart.set(op);
							return true;
						}
						if (partsRelease != null) {
							try {
								partsRelease.await(10, TimeUnit.SECONDS);
							} catch (final InterruptedException e) {
								Thread.currentThread().interrupt();
								return false;
							}
						}
						inFlightDuringParts.add(generator.inFlightCount(type));
					}
					if (!beginDispatch(op)) {
						return false;
					}
					synchronized (seen) {
						if (seen.stream().anyMatch(known -> known == op)) {
							op.resetTiming();
						} else {
							seen.add(op);
						}
					}
					op.startRequest();
					op.finishRequest();
					op.startResponse();
					if (op instanceof DataOperation dataOp) {
						try {
							if (dataOp.type() == OpType.CREATE) {
								dataOp.countBytesDone(((DataItem) dataOp.item()).size());
							} else if (dataOp.type() == OpType.READ) {
								dataOp.startDataResponse();
							}
						} catch (final Exception ignored) {
							// The mock keeps its default byte count.
						}
					}
					op.finishResponse();
					final Operation.Status status = statusOf(op);
					if (op instanceof CompositeOperation composite && composite.get("mpuAbort") != null
									&& !composite.allSubOperationsDone()) {
						unfinishedAbort.set(true);
					}
					op.status(status);
					sequence.append(tagOf(op)).append(':').append(status).append(' ');
					final boolean initiation = op instanceof CompositeOperation composite
									&& !composite.allSubOperationsDone() && composite.get("mpuAbort") == null;
					if (initiation) {
						compositesSubmitted.incrementAndGet();
					}
					final boolean abort = op instanceof CompositeOperation composite && composite.get("mpuAbort") != null;
					published.incrementAndGet();
					handleCompleted(op);
					final Operation heldPart = abort ? inFlightPart.getAndSet(null) : null;
					if (heldPart != null) {
						// The part still in flight when the abort ended answers now.
						heldPart.startResponse();
						heldPart.finishResponse();
						heldPart.status(Operation.Status.SUCC);
						sequence.append(tagOf(heldPart)).append(":SUCC ");
						published.incrementAndGet();
						handleCompleted(heldPart);
					}
					maxPermits.accumulateAndGet(permits.availablePermits(), Math::max);
					if (initiation && inFlightAfterInitiation.get() < 0) {
						inFlightAfterInitiation.set(generator.inFlightCount(type));
						initiated.countDown();
					}
					return true;
				}

				@Override
				protected int submit(final List ops, final int from, final int to) {
					var i = from;
					while (i < to && submit((Operation) ops.get(i))) {
						i++;
					}
					return i - from;
				}
			};
			driver.set(created);
			return created;
		}

		private Operation.Status statusOf(final Operation op) {
			if (op instanceof CompositeOperation composite) {
				final boolean abort = composite.get("mpuAbort") != null;
				final boolean done = composite.allSubOperationsDone();
				if (abort) {
					return mode == Mode.PART_OUT_OF_RETRIES_ABORT_FAILS ? Operation.Status.FAIL_UNKNOWN : Operation.Status.SUCC;
				}
				if (!done) {
					return switch (mode) {
					case FAILED_INITIATION -> Operation.Status.RESP_FAIL_CLIENT;
					case INTERRUPTED_INITIATION -> Operation.Status.INTERRUPTED;
					case PENDING_INITIATION -> Operation.Status.PENDING;
					default -> Operation.Status.SUCC;
					};
				}
				return mode == Mode.FAILED_COMPLETION ? Operation.Status.RESP_FAIL_CLIENT : Operation.Status.SUCC;
			}
			if (op instanceof PartialOperation part && part.partNumber() == 1) {
				switch (mode) {
				case PART_OUT_OF_RETRIES_ABORT_FAILS, PART_OUT_OF_RETRIES_ABORT_SUCCEEDS, UNFINISHED_ABORT_SUCCEEDS,
								RANGE_READ_FAILED_PART:
					return Operation.Status.FAIL_IO;
				case PART_RETRY:
					return part1Failures.getAndIncrement() == 0 ? Operation.Status.FAIL_IO : Operation.Status.SUCC;
				default:
					break;
				}
			}
			return Operation.Status.SUCC;
		}

		private static String tagOf(final Operation op) {
			if (op instanceof CompositeOperation composite) {
				return composite.get("mpuAbort") != null ? "Cabort" : composite.allSubOperationsDone() ? "Cfinal" : "Cinit";
			}
			if (op instanceof PartialOperation part) {
				return "P" + part.partNumber();
			}
			return "F";
		}
	}
}
