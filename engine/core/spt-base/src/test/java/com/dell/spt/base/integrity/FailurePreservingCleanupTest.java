package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class FailurePreservingCleanupTest {

	@Test
	void alwaysSuppressesCleanupBehindPrimaryFailure() {
		final IOException primary = new IOException("primary");
		final IOException cleanup = new IOException("cleanup");

		final IOException thrown = assertThrows(
						IOException.class,
						() -> FailurePreservingCleanup.always(
										() -> {
											throw primary;
										},
										() -> {
											throw cleanup;
										}));

		assertSame(primary, thrown);
		assertEquals(1, thrown.getSuppressed().length);
		assertSame(cleanup, thrown.getSuppressed()[0]);
	}

	@Test
	void alwaysReportsCleanupOnlyFailure() {
		final IOException cleanup = new IOException("cleanup");
		final IOException thrown = assertThrows(
						IOException.class,
						() -> FailurePreservingCleanup.always(() -> "complete", () -> {
							throw cleanup;
						}));
		assertSame(cleanup, thrown);
	}

	@Test
	void onFailureSuppressesCleanupBehindPrimaryFailure() {
		final IOException primary = new IOException("primary");
		final IOException cleanup = new IOException("cleanup");

		final IOException thrown = assertThrows(
						IOException.class,
						() -> FailurePreservingCleanup.onFailure(
										() -> {
											throw primary;
										},
										() -> {
											throw cleanup;
										}));

		assertSame(primary, thrown);
		assertEquals(1, thrown.getSuppressed().length);
		assertSame(cleanup, thrown.getSuppressed()[0]);
	}

	@Test
	void onFailureSkipsCleanupAfterSuccess() throws Exception {
		final AtomicBoolean cleaned = new AtomicBoolean();
		final String result = FailurePreservingCleanup.onFailure(
						() -> "complete", () -> cleaned.set(true));
		assertEquals("complete", result);
		assertEquals(false, cleaned.get());
	}
}
