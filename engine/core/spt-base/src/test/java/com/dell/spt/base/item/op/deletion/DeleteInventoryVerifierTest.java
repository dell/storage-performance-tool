package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
