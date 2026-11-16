package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.logging.Loggers;
import java.io.BufferedWriter;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeleteInventoryVerifierTest {
	private static final int LARGE_SELECTION_COUNT = 20_000;
	private static final int INTERRUPT_AFTER_PROBES = 100;

	@Test
	void postVerificationMakesOneFullPassThenRetriesOnlyPresentOrUnresolved(
					@TempDir final Path temp) throws Exception {
		final Path manifest = manifest(temp);
		final Map<String, Queue<DeleteVerificationProbe.Presence>> answers = new HashMap<>();
		answers.put("current", queue(
						DeleteVerificationProbe.Presence.PRESENT,
						DeleteVerificationProbe.Presence.ABSENT));
		answers.put("exact", queue(DeleteVerificationProbe.Presence.ABSENT));
		answers.put("uncertain", queue(
						DeleteVerificationProbe.Presence.UNRESOLVED,
						DeleteVerificationProbe.Presence.UNRESOLVED,
						DeleteVerificationProbe.Presence.ABSENT));
		final Map<String, AtomicInteger> calls = new HashMap<>();

		try (final DeleteVerificationReport report = DeleteInventoryVerifier.verify(
						manifest,
						3,
						DeleteVerificationPhase.POST_DELETE,
						Duration.ofSeconds(1),
						target -> {
							calls.computeIfAbsent(target.key(), ignored -> new AtomicInteger()).incrementAndGet();
							final var values = answers.get(target.key());
							return values.size() == 1 ? values.peek() : values.remove();
						})) {

			assertTrue(report.completePass());
			assertEquals(3, report.absent());
			assertEquals(0, report.present());
			assertEquals(0, report.unresolved());
			assertEquals(2, calls.get("current").get());
			assertEquals(1, calls.get("exact").get(), "already-absent exact version was retried");
			assertEquals(3, calls.get("uncertain").get());
		}
	}

	@Test
	void preValidationRequiresEveryCurrentAndExactIdentityToBePresent(
					@TempDir final Path temp) throws Exception {
		try (final DeleteVerificationReport report = DeleteInventoryVerifier.verify(
						manifest(temp),
						3,
						DeleteVerificationPhase.PRE_DELETE,
						Duration.ZERO,
						target -> switch (target.key()) {
						case "current" -> DeleteVerificationProbe.Presence.PRESENT;
						case "exact" -> DeleteVerificationProbe.Presence.ABSENT;
						default -> DeleteVerificationProbe.Presence.UNRESOLVED;
						})) {

			assertTrue(report.completePass());
			assertFalse(report.successful());
			assertEquals(1, report.present());
			assertEquals(1, report.absent());
			assertEquals(1, report.unresolved());
			assertEquals(2, report.failureCount());
			assertEquals(DeleteVerificationProbe.Presence.PRESENT, report.presence(0));
			assertEquals(DeleteVerificationProbe.Presence.ABSENT, report.presence(1));
			assertEquals(DeleteVerificationProbe.Presence.UNRESOLVED, report.presence(2));
		}
	}

	@Test
	void reportDoesNotRetainASelectionSizedHeapArray() {
		assertFalse(Arrays.stream(DeleteVerificationReport.class.getDeclaredFields())
						.filter(field -> !Modifier.isStatic(field.getModifiers()))
						.anyMatch(field -> field.getType().isArray()));
	}

	@Test
	void retryPauseForwardsExternalInterruption(@TempDir final Path temp) throws Exception {
		final CountDownLatch retryPassCompleted = new CountDownLatch(1);
		final AtomicInteger calls = new AtomicInteger();
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final AtomicBoolean returnedReport = new AtomicBoolean();
		final AtomicBoolean interruptedStatus = new AtomicBoolean();
		final Thread worker = Thread.ofPlatform().start(() -> {
			try (final var ignored = DeleteInventoryVerifier.verify(
							manifest(temp),
							3,
							DeleteVerificationPhase.POST_DELETE,
							Duration.ofSeconds(10),
							target -> {
								if (calls.incrementAndGet() > 3) {
									retryPassCompleted.countDown();
								}
								return DeleteVerificationProbe.Presence.PRESENT;
							})) {
				returnedReport.set(true);
			} catch (final Throwable thrown) {
				failure.set(thrown);
				interruptedStatus.set(Thread.currentThread().isInterrupted());
			}
		});

		assertTrue(retryPassCompleted.await(5, TimeUnit.SECONDS), "verification never reached its retry pause");
		final long interruptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (worker.isAlive() && failure.get() == null && System.nanoTime() < interruptDeadline) {
			worker.interrupt();
			Thread.onSpinWait();
		}
		worker.join(TimeUnit.SECONDS.toMillis(5));

		assertFalse(worker.isAlive(), "interrupted verification did not terminate");
		assertTrue(failure.get() instanceof InterruptedException,
						() -> "expected original interruption, got " + failure.get());
		assertTrue(interruptedStatus.get(), "verification cleared the interrupt status");
		assertFalse(returnedReport.get(), "interrupted verification returned a classified report");
	}

	@Test
	void preInterruptedCompletePassStopsBeforeAnyProbe(@TempDir final Path temp) throws Exception {
		final AtomicInteger calls = new AtomicInteger();
		Thread.currentThread().interrupt();
		try {
			assertThrows(
							InterruptedException.class,
							() -> DeleteInventoryVerifier.verify(
											manifest(temp),
											3,
											DeleteVerificationPhase.PRE_DELETE,
											Duration.ZERO,
											target -> {
												calls.incrementAndGet();
												return DeleteVerificationProbe.Presence.PRESENT;
											}));
			assertTrue(Thread.currentThread().isInterrupted(), "verification cleared interrupt status");
			assertEquals(0, calls.get(), "pre-interrupted verification probed inventory");
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void interruptionDuringLargeFastCompletePassStopsWithoutMoreProbesOrReport(
					@TempDir final Path temp) throws Exception {
		final AtomicInteger calls = new AtomicInteger();
		final VerificationThreadOutcome outcome = runVerification(
						largeManifest(temp, LARGE_SELECTION_COUNT),
						LARGE_SELECTION_COUNT,
						DeleteVerificationPhase.PRE_DELETE,
						Duration.ZERO,
						target -> {
							final int call = calls.incrementAndGet();
							if (call == INTERRUPT_AFTER_PROBES) {
								Thread.currentThread().interrupt();
							}
							return DeleteVerificationProbe.Presence.PRESENT;
						});

		assertInterrupted(outcome);
		assertEquals(
						INTERRUPT_AFTER_PROBES,
						calls.get(),
						"complete pass continued probing after cancellation");
	}

	@Test
	void interruptionDuringLargeFastRetryPassStopsWithoutMoreProbesOrReport(
					@TempDir final Path temp) throws Exception {
		final AtomicInteger calls = new AtomicInteger();
		final VerificationThreadOutcome outcome = runVerification(
						largeManifest(temp, LARGE_SELECTION_COUNT),
						LARGE_SELECTION_COUNT,
						DeleteVerificationPhase.POST_DELETE,
						Duration.ofMinutes(1),
						target -> {
							final int call = calls.incrementAndGet();
							if (call == LARGE_SELECTION_COUNT + INTERRUPT_AFTER_PROBES) {
								Thread.currentThread().interrupt();
							}
							return DeleteVerificationProbe.Presence.PRESENT;
						});

		assertInterrupted(outcome);
		assertEquals(
						LARGE_SELECTION_COUNT + INTERRUPT_AFTER_PROBES,
						calls.get(),
						"retry pass continued probing after cancellation");
	}

	@Test
	void interruptionBeforeLargeFastEvidenceScanStopsWithoutReturningReport(
					@TempDir final Path temp) throws Exception {
		final AtomicInteger calls = new AtomicInteger();
		final VerificationThreadOutcome outcome = runVerification(
						largeManifest(temp, LARGE_SELECTION_COUNT),
						LARGE_SELECTION_COUNT,
						DeleteVerificationPhase.POST_DELETE,
						Duration.ZERO,
						target -> {
							if (calls.incrementAndGet() == LARGE_SELECTION_COUNT) {
								Thread.currentThread().interrupt();
							}
							return DeleteVerificationProbe.Presence.ABSENT;
						});

		assertInterrupted(outcome);
		assertEquals(LARGE_SELECTION_COUNT, calls.get(), "evidence cancellation triggered extra probes");
	}

	@Test
	void mandatoryCompletePassMayConsumeTheRetryWindow(@TempDir final Path temp) throws Exception {
		final AtomicInteger calls = new AtomicInteger();
		try (final DeleteVerificationReport report = DeleteInventoryVerifier.verify(
						manifest(temp),
						3,
						DeleteVerificationPhase.POST_DELETE,
						Duration.ofMillis(20),
						target -> {
							calls.incrementAndGet();
							try {
								TimeUnit.MILLISECONDS.sleep(25);
							} catch (final InterruptedException interrupted) {
								Thread.currentThread().interrupt();
								throw new AssertionError(interrupted);
							}
							return DeleteVerificationProbe.Presence.PRESENT;
						})) {
			assertTrue(report.completePass());
			assertEquals(3, calls.get(), "complete pass was truncated or retry deadline was reset");
			assertEquals(3, report.present());
		}
	}

	@Test
	void ordinaryProbeFailureRemainsUnresolvedEvidence(@TempDir final Path temp) throws Exception {
		try (final DeleteVerificationReport report = DeleteInventoryVerifier.verify(
						manifest(temp),
						3,
						DeleteVerificationPhase.PRE_DELETE,
						Duration.ZERO,
						target -> {
							throw new IllegalStateException("ordinary probe failure");
						})) {
			assertEquals(3, report.unresolved());
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void retryPauseStartsAtTenMillisDoublesAndCapsAtOneSecond() {
		assertEquals(10, DeleteInventoryVerifier.retryPauseMillis(1));
		assertEquals(20, DeleteInventoryVerifier.retryPauseMillis(2));
		assertEquals(40, DeleteInventoryVerifier.retryPauseMillis(3));
		assertEquals(640, DeleteInventoryVerifier.retryPauseMillis(7));
		assertEquals(1_000, DeleteInventoryVerifier.retryPauseMillis(8));
		assertEquals(1_000, DeleteInventoryVerifier.retryPauseMillis(64));
		assertEquals(1_000, DeleteInventoryVerifier.retryPauseMillis(Integer.MAX_VALUE));
		assertEquals(10, DeleteInventoryVerifier.retryPauseMillis(0));
	}

	@Test
	void probeThatFailsAtOnceIsRetriedABoundedNumberOfPassesUntilTheDeadline(
					@TempDir final Path temp) throws Exception {
		final Duration timeout = Duration.ofSeconds(5);
		final Map<String, AtomicInteger> calls = new HashMap<>();
		try (final DeleteVerificationReport report = DeleteInventoryVerifier.verify(
						manifest(temp),
						3,
						DeleteVerificationPhase.POST_DELETE,
						timeout,
						target -> {
							calls.computeIfAbsent(target.key(), ignored -> new AtomicInteger()).incrementAndGet();
							throw new IllegalStateException("connection closed at once");
						})) {

			assertEquals(3, report.unresolved());
			assertTrue(report.elapsed().compareTo(timeout.minusMillis(DeleteInventoryVerifier.FIRST_RETRY_PAUSE_MILLIS)) >= 0,
							"verification ended before its final pass");
			for (final String key : List.of("current", "exact", "uncertain")) {
				final int probes = calls.get(key).get();
				// Pauses of 10, 20, 40 ... ms capped at 1 s allow about 12 passes in 5 s; a fixed
				// 10 ms pause allowed hundreds.
				assertTrue(probes <= 15, () -> key + " was probed " + probes + " times in " + timeout);
				assertTrue(probes >= 2, () -> key + " was never retried");
			}
		}
	}

	@Test
	void targetThatSettlesJustBeforeTheDeadlineIsSeenByAFinalPass(@TempDir final Path temp)
					throws Exception {
		final Duration timeout = Duration.ofSeconds(3);
		final AtomicLong settlesAt = new AtomicLong();
		final AtomicLong lastProbe = new AtomicLong();
		settlesAt.set(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2_800));
		try (final DeleteVerificationReport report = DeleteInventoryVerifier.verify(
						singleTargetManifest(temp),
						1,
						DeleteVerificationPhase.POST_DELETE,
						timeout,
						target -> {
							lastProbe.set(System.nanoTime());
							return System.nanoTime() < settlesAt.get()
											? DeleteVerificationProbe.Presence.PRESENT
											: DeleteVerificationProbe.Presence.ABSENT;
						})) {
			// Back-off places retry passes at about 0, 10, 30 ... 1270 and 2270 ms; without a reserved
			// final pass the last check came at about 2270 ms plus pass overhead, before the target settled.
			assertTrue(report.successful(), () -> "the target was still present at its last check, "
							+ TimeUnit.NANOSECONDS.toMillis(lastProbe.get() - settlesAt.get()) + " ms from settling");
			assertTrue(report.elapsed().compareTo(timeout) <= 0, "the deadline moved");
		}
	}

	@Test
	void probeThatSucceedsOnTheThirdPassResolvesWithoutWaitingForTheDeadline(
					@TempDir final Path temp) throws Exception {
		final AtomicInteger calls = new AtomicInteger();
		final Duration timeout = Duration.ofSeconds(30);
		try (final DeleteVerificationReport report = DeleteInventoryVerifier.verify(
						singleTargetManifest(temp),
						1,
						DeleteVerificationPhase.POST_DELETE,
						timeout,
						target -> calls.incrementAndGet() < 3
										? DeleteVerificationProbe.Presence.UNRESOLVED
										: DeleteVerificationProbe.Presence.ABSENT)) {

			assertTrue(report.successful());
			assertEquals(1, report.absent());
			assertEquals(3, calls.get());
			assertTrue(report.elapsed().compareTo(timeout) < 0, "a resolved target waited for the deadline");
		}
	}

	@Test
	void unresolvedTargetsAreReportedOnceAtWarnWithTheirCountAndCause(
					@TempDir final Path temp) throws Exception {
		final List<String> warnings = captureWarnings("Standalone DELETE pre-validation left ", () -> {
			try (final var resolved = DeleteInventoryVerifier.verify(
							manifest(temp),
							3,
							DeleteVerificationPhase.PRE_DELETE,
							Duration.ofSeconds(1),
							target -> DeleteVerificationProbe.Presence.PRESENT)) {
				assertTrue(resolved.successful());
			}
			try (final var unresolved = DeleteInventoryVerifier.verify(
							manifest(temp),
							3,
							DeleteVerificationPhase.PRE_DELETE,
							Duration.ofMillis(300),
							new DeleteVerificationProbe() {
								@Override
								public Presence presence(final DeleteTarget target) {
									throw new AssertionError("an observe() override must be used as it is");
								}

								@Override
								public Observation observe(final DeleteTarget target) {
									return "current".equals(target.key())
													? Observation.of(Presence.PRESENT)
													: Observation.unresolved(DeleteVerificationCause.noResponse());
								}
							})) {
				assertEquals(2, unresolved.unresolved());
			}
		});
		assertEquals(1, warnings.size(), warnings::toString);
		final String message = warnings.get(0);
		assertTrue(message.startsWith("Standalone DELETE pre-validation left 2 selected identity/identities "
						+ "unresolved after "), message);
		assertTrue(message.endsWith(
						"; last cause: no HEAD response (the connection closed or the response timed out)"), message);
	}

	@Test
	void causeOfATargetThatLaterResolvedIsNotReportedForTheOneStillUnresolved(
					@TempDir final Path temp) throws Exception {
		final Path manifest = temp.resolve("stale-cause-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nbucket,stuck,1,\nbucket,transient,1,\n");
		final AtomicInteger transientCalls = new AtomicInteger();
		final List<String> warnings = captureWarnings("Standalone DELETE pre-validation left ", () -> {
			try (final var report = DeleteInventoryVerifier.verify(
							manifest,
							2,
							DeleteVerificationPhase.PRE_DELETE,
							Duration.ofMillis(300),
							target -> {
								if ("stuck".equals(target.key())) {
									return DeleteVerificationProbe.Presence.UNRESOLVED;
								}
								if (transientCalls.incrementAndGet() == 1) {
									DeleteVerificationCause.report(DeleteVerificationCause.httpStatus(503));
									return DeleteVerificationProbe.Presence.UNRESOLVED;
								}
								return DeleteVerificationProbe.Presence.PRESENT;
							})) {
				assertEquals(1, report.unresolved());
				assertEquals(1, report.present());
			}
		});
		assertEquals(1, warnings.size(), warnings::toString);
		assertTrue(warnings.get(0).startsWith("Standalone DELETE pre-validation left 1 "), warnings::toString);
		assertTrue(warnings.get(0).endsWith("; last cause: not reported by the storage driver"), warnings::toString);
	}

	@Test
	void exceptionTextFromAProbeNeverReachesTheWarning(@TempDir final Path temp) throws Exception {
		final String signedUrl = "https://bucket.example.invalid/key?X-Amz-Credential=SYNTHETICKEY"
						+ "&X-Amz-Signature=0123456789abcdef";
		final List<String> warnings = captureWarnings("Standalone DELETE post-verification left ", () -> {
			try (final var report = DeleteInventoryVerifier.verify(
							singleTargetManifest(temp),
							1,
							DeleteVerificationPhase.POST_DELETE,
							Duration.ofMillis(200),
							target -> {
								throw new IllegalStateException("HEAD failed for " + signedUrl + "\nsecond line");
							})) {
				assertEquals(1, report.unresolved());
			}
		});
		assertEquals(1, warnings.size(), warnings::toString);
		final String message = warnings.get(0);
		assertTrue(message.endsWith("; last cause: probe error (IllegalStateException)"), message);
		assertFalse(message.contains("example.invalid") || message.contains("X-Amz") || message.contains("\n"),
						message);
	}

	@Test
	void unresolvedWarningNamesAPhaseWithoutAReportedCause() {
		assertEquals(
						"Standalone DELETE post-verification left 4 selected identity/identities unresolved "
										+ "after 12 pass(es) in 30001 ms; last cause: not reported by the storage driver",
						DeleteInventoryVerifier.unresolvedWarning(
										DeleteVerificationPhase.POST_DELETE,
										4,
										12,
										TimeUnit.MILLISECONDS.toNanos(30_001),
										null));
	}

	private static Path manifest(final Path temp) throws Exception {
		final Path manifest = temp.resolve("verify-input.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\n"
										+ "bucket,current,1,\n"
										+ "bucket,exact,1,version-1\n"
										+ "bucket,uncertain,1,version-2\n");
		return manifest;
	}

	private static Path singleTargetManifest(final Path temp) throws Exception {
		final Path manifest = temp.resolve("single-verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nbucket,settling,1,\n");
		return manifest;
	}

	/**
	 * Runs {@code body} and returns the messages starting with {@code prefix} logged between two
	 * markers. Engine loggers are asynchronous, so the end marker drains what the body logged.
	 */
	private static List<String> captureWarnings(final String prefix, final ThrowingRunnable body)
					throws Exception {
		final var appender = new CapturingAppender(prefix);
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		logger.addAppender(appender);
		try {
			Loggers.MSG.warn(appender.startMarker);
			awaitTrue(() -> appender.armed, "the start marker was not logged");
			body.run();
			Loggers.MSG.warn(appender.endMarker);
			awaitTrue(() -> appender.drained, "the end marker was not logged");
			return List.copyOf(appender.messages);
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}

	private static void awaitTrue(final java.util.function.BooleanSupplier condition, final String message)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), message);
	}

	private static final class CapturingAppender extends AbstractAppender {
		final List<String> messages = Collections.synchronizedList(new ArrayList<>());
		final String startMarker = "DeleteInventoryVerifierTest start " + System.nanoTime();
		final String endMarker = "DeleteInventoryVerifierTest end " + System.nanoTime();
		final String prefix;
		volatile boolean armed;
		volatile boolean drained;

		CapturingAppender(final String prefix) {
			super("testDeleteVerificationWarning", null, null, true, Property.EMPTY_ARRAY);
			this.prefix = prefix;
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (message.equals(startMarker)) {
				armed = true;
			} else if (message.equals(endMarker)) {
				drained = true;
			} else if (armed && !drained && message.startsWith(prefix)) {
				messages.add(Level.WARN.equals(event.getLevel()) ? message : "not at WARN: " + message);
			}
		}
	}

	private static Path largeManifest(final Path temp, final int count) throws Exception {
		final Path manifest = temp.resolve("large-verify-input.csv");
		try (BufferedWriter output = Files.newBufferedWriter(manifest)) {
			output.write("bucket,key,size,version_id\n");
			for (int index = 0; index < count; index++) {
				output.write("bucket,key-");
				output.write(Integer.toString(index));
				output.write(",1,\n");
			}
		}
		return manifest;
	}

	private static VerificationThreadOutcome runVerification(
					final Path manifest,
					final long selectedCount,
					final DeleteVerificationPhase phase,
					final Duration timeout,
					final DeleteVerificationProbe probe) throws Exception {
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final AtomicBoolean returnedReport = new AtomicBoolean();
		final AtomicBoolean interruptedStatus = new AtomicBoolean();
		final Thread worker = Thread.ofPlatform().start(() -> {
			try (final var ignored = DeleteInventoryVerifier.verify(
							manifest, selectedCount, phase, timeout, probe)) {
				returnedReport.set(true);
			} catch (final Throwable thrown) {
				failure.set(thrown);
				interruptedStatus.set(Thread.currentThread().isInterrupted());
			}
		});
		worker.join(TimeUnit.SECONDS.toMillis(5));
		assertFalse(worker.isAlive(), "interrupted verification did not terminate promptly");
		return new VerificationThreadOutcome(
						failure.get(), returnedReport.get(), interruptedStatus.get());
	}

	private static void assertInterrupted(final VerificationThreadOutcome outcome) {
		assertInstanceOf(
						InterruptedException.class,
						outcome.failure(),
						() -> "expected clear cancellation classification, got " + outcome.failure());
		assertTrue(outcome.interruptedStatus(), "verification cleared the interrupt status");
		assertFalse(outcome.returnedReport(), "interrupted verification returned a classified report");
	}

	private record VerificationThreadOutcome(
					Throwable failure, boolean returnedReport, boolean interruptedStatus) {}

	@SafeVarargs
	private static <T> Queue<T> queue(final T... values) {
		return new ArrayDeque<>(java.util.List.of(values));
	}
}
