package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeleteInventoryVerifierTest {

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

	@SafeVarargs
	private static <T> Queue<T> queue(final T... values) {
		return new ArrayDeque<>(java.util.List.of(values));
	}
}
