package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class CsvArtifactAggregatorFailureTest {

	@Test
	void emissionCountMismatchCleanupFailureIsSuppressedBehindTerminalFailure() {
		final IntegrityTerminalException primary = new IntegrityTerminalException(
						IntegrityTerminalException.Category.AGGREGATION,
						"list-step",
						"LIST emitted 2 records but parsed 1 manifest rows",
						null);
		final IOException cleanup = new IOException("staging delete failed");

		final IntegrityTerminalException thrown = assertThrows(
						IntegrityTerminalException.class,
						() -> CsvArtifactAggregator.preserveEmissionCountFailure(primary, () -> {
							throw cleanup;
						}));

		assertSame(primary, thrown);
		assertEquals(IntegrityTerminalException.Category.AGGREGATION, thrown.category());
		assertTrue(thrown.getMessage().contains("LIST emitted 2 records"));
		assertEquals(1, thrown.getSuppressed().length);
		assertSame(cleanup, thrown.getSuppressed()[0]);
	}

	@Test
	void emissionCountMismatchSuccessfulCleanupPreservesTerminalFailure() {
		final IntegrityTerminalException primary = new IntegrityTerminalException(
						IntegrityTerminalException.Category.AGGREGATION,
						"list-step",
						"LIST emitted 2 records but parsed 1 manifest rows",
						null);

		final IntegrityTerminalException thrown = assertThrows(
						IntegrityTerminalException.class,
						() -> CsvArtifactAggregator.preserveEmissionCountFailure(primary, () -> {}));

		assertSame(primary, thrown);
		assertEquals(0, thrown.getSuppressed().length);
	}
}
