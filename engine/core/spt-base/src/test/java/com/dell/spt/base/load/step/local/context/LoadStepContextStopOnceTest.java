package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteArtifactRecorder;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A standalone DELETE context whose stop or shutdown keeps failing: outside duration mode each runs once,
 * so the close that repeats them reaches the context's own close and closes its generator, recorder
 * and driver. Duration mode retries its stop until the slice gives up.
 */
class LoadStepContextStopOnceTest {

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	@Test
	void aStopWhoseEvidenceKeepsFailingRunsOnceAndTheCloseReleasesTheContext(@TempDir final Path temp)
					throws Exception {
		final var fixture = new Fixture(temp, false, false);
		final IllegalStateException persistFailure = new IllegalStateException(
						"Failed to persist DELETE recovery evidence");
		fixture.finishAlwaysThrows(persistFailure);
		final var context = fixture.context();
		context.start();

		assertSame(persistFailure, assertThrows(IllegalStateException.class, context::stop));
		verify(fixture.driver, times(1)).stop();

		// The stop that close() repeats returns: the close reaches the context's own close.
		assertDoesNotThrow(context::close);
		assertTrue(context.isClosed());
		fixture.verifyFinishCalls(1);
		verify(fixture.driver, times(1)).stop();
		verify(fixture.generator, times(1)).close();
		verify(fixture.recorder, times(1)).close();
		verify(fixture.driver, times(1)).close();

		// A later stop or close does nothing again.
		assertDoesNotThrow(context::stop);
		assertDoesNotThrow(context::close);
		fixture.verifyFinishCalls(1);
		verify(fixture.driver, times(1)).close();
	}

	@Test
	void aDriverWhoseStopKeepsFailingIsStoppedOnceAndStillClosed(@TempDir final Path temp) throws Exception {
		final var fixture = new Fixture(temp, false, false);
		final IllegalStateException driverFailure = new IllegalStateException(
						"Timed out waiting for NIO worker termination");
		when(fixture.driver.stop()).thenThrow(driverFailure);
		final var context = fixture.context();
		context.start();

		assertSame(driverFailure, assertThrows(IllegalStateException.class, context::stop));
		assertDoesNotThrow(context::close);

		assertTrue(context.isClosed());
		fixture.verifyFinishCalls(1);
		verify(fixture.driver, times(1)).stop();
		verify(fixture.generator, times(1)).close();
		verify(fixture.recorder, times(1)).close();
		verify(fixture.driver, times(1)).close();
	}

	@Test
	void bothFailuresOfTheFirstStopAreThrownOnce(@TempDir final Path temp) throws Exception {
		final var fixture = new Fixture(temp, false, false);
		final IllegalStateException persistFailure = new IllegalStateException(
						"Failed to persist DELETE recovery evidence");
		final IllegalStateException driverFailure = new IllegalStateException("injected driver stop failure");
		fixture.finishAlwaysThrows(persistFailure);
		when(fixture.driver.stop()).thenThrow(driverFailure);
		final var context = fixture.context();
		context.start();

		final var thrown = assertThrows(IllegalStateException.class, context::stop);
		assertSame(persistFailure, thrown);
		assertSame(driverFailure, thrown.getSuppressed()[0]);
		assertDoesNotThrow(context::close);
		verify(fixture.driver, times(1)).stop();
		verify(fixture.driver, times(1)).close();
	}

	@Test
	void aShutdownWhosePostVerificationKeepsFailingRunsOnceAndTheCloseReleasesTheContext(
					@TempDir final Path temp) throws Exception {
		final var fixture = new Fixture(temp, false, true);
		final var context = fixture.context();
		fixture.failEveryVerificationPass();
		context.start();

		final var thrown = assertThrows(IntegrityTerminalException.class, context::stop);
		assertEquals(
						"Standalone DELETE post-verification could not complete its full inventory pass",
						thrown.getMessage());
		// The stop fixed the verification summary before it released the outcome storage, so the
		// context can still be read.
		assertDoesNotThrow(context::deleteObjectLifecycle);

		assertDoesNotThrow(context::close);
		assertDoesNotThrow(context::deleteObjectLifecycle);
		assertTrue(context.isClosed());
		// The verification did not complete: no evidence is finished, and the driver is stopped.
		fixture.verifyFinishCalls(0);
		verify(fixture.driver, never()).shutdown();
		verify(fixture.driver, times(1)).stop();
		verify(fixture.generator, times(1)).close();
		verify(fixture.recorder, times(1)).close();
		verify(fixture.driver, times(1)).close();
	}

	@Test
	void aDriverShutdownThatFailsOnceAfterTheDrainStillFinishesTheEvidence(@TempDir final Path temp)
					throws Exception {
		final var fixture = new Fixture(temp, false, false);
		final IllegalStateException shutdownFailure = new IllegalStateException(
						"one-off driver shutdown failure");
		when(fixture.driver.shutdown()).thenThrow(shutdownFailure).thenReturn(fixture.driver);
		final var context = fixture.context();
		context.start();

		assertSame(shutdownFailure, assertThrows(IllegalStateException.class, context::stop));
		assertDoesNotThrow(context::close);

		// Only the driver failed: the stop that follows still finishes the DELETE evidence.
		assertTrue(context.isClosed());
		fixture.verifyFinishCalls(1);
		verify(fixture.driver, times(1)).stop();
		verify(fixture.driver, times(1)).close();
	}

	@Test
	void durationModeRetriesItsStopUntilTheSliceGivesUp(@TempDir final Path temp) throws Exception {
		final var fixture = new Fixture(temp, true, false);
		final IllegalStateException persistFailure = new IllegalStateException(
						"Failed to persist DELETE recovery evidence");
		fixture.finishAlwaysThrows(persistFailure);
		final var context = fixture.context();
		context.start();

		assertSame(persistFailure, assertThrows(IllegalStateException.class, context::stop));
		// The retry calls finish again, as duration mode persists its evidence that way.
		assertSame(persistFailure, assertThrows(IllegalStateException.class, context::stop));
		fixture.verifyFinishCalls(2);
		verify(fixture.driver, never()).close();

		// The slice gave up: the context closes without a third finish.
		assertDoesNotThrow(context::releaseAfterFailedStop);
		assertTrue(context.isClosed());
		fixture.verifyFinishCalls(2);
		verify(fixture.generator, times(1)).close();
		verify(fixture.recorder, times(1)).close();
		verify(fixture.driver, times(1)).close();
	}

	@Test
	void durationModeShutdownThatKeepsFailingIsReleasedWhenTheSliceGivesUp(@TempDir final Path temp)
					throws Exception {
		final var fixture = new Fixture(temp, true, true);
		final var context = fixture.context();
		fixture.failEveryVerificationPass();
		context.start();

		assertThrows(IntegrityTerminalException.class, context::shutdown);
		// Duration mode verifies again on a retry, and it fails again.
		assertThrows(IntegrityTerminalException.class, context::shutdown);

		assertDoesNotThrow(context::releaseAfterFailedStop);
		assertTrue(context.isClosed());
		fixture.verifyFinishCalls(0);
		verify(fixture.driver, never()).shutdown();
		verify(fixture.driver, times(1)).stop();
		verify(fixture.generator, times(1)).close();
		verify(fixture.recorder, times(1)).close();
		verify(fixture.driver, times(1)).close();
	}

	@Test
	void aCleanStopFinishesOnceAndCloses(@TempDir final Path temp) throws Exception {
		final var fixture = new Fixture(temp, false, false);
		final var context = fixture.context();
		context.start();

		assertDoesNotThrow(context::stop);
		assertDoesNotThrow(context::close);

		fixture.verifyFinishCalls(1);
		verify(fixture.driver, times(1)).shutdown();
		verify(fixture.driver, times(1)).stop();
		verify(fixture.driver, times(1)).close();
		verify(fixture.recorder, times(1)).close();
	}

	private static final class Fixture {
		private final Config config;
		private final StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver;
		private final LoadGenerator<IntegrityManifestDataItem, DeleteRequestOperation> generator;
		private final DeleteArtifactRecorder recorder = mock(DeleteArtifactRecorder.class);
		private final Path selection;

		@SuppressWarnings("unchecked")
		Fixture(final Path temp, final boolean durationMode, final boolean postVerification) throws Exception {
			selection = temp.resolve("delete-input.csv");
			Files.writeString(selection, "bucket,key,size,version_id\nbucket,key,1,\n");
			config = TestConfigBuilder.config();
			config.val("item-type", "data");
			config.val("item-output-file", "");
			config.val("item-input-file", selection.toString());
			config.val("load-batch-size", 4);
			config.val("load-op-type", "delete");
			config.val("load-op-delete-standalone", true);
			config.val("load-op-delete-batchSize", 1);
			config.val("load-op-delete-duration", durationMode);
			config.val("load-op-delete-selected", 1L);
			config.val("load-op-delete-selectedCurrentKey", 1L);
			config.val("load-op-delete-selectedExactVersion", 0L);
			config.val("load-op-delete-selectedBuckets", List.of("bucket=1"));
			config.val("load-op-delete-preValidation", false);
			config.val("load-op-delete-postVerification", postVerification);
			config.val("load-op-delete-verificationTimeoutMillis", 1L);
			config.val("load-step-limit-time", durationMode ? "60s" : "0s");
			config.val("load-op-recycle-mode", false);
			config.val("load-op-retry", false);
			config.val("load-op-limit-count", 0L);
			config.val("load-op-wait-finish", true);
			config.val("load-op-wait-limit", 1);
			driver = postVerification
							? mock(StorageDriver.class, withSettings().extraInterfaces(DeleteVerificationProbe.class))
							: mock(StorageDriver.class);
			when(driver.supportsStandaloneDeleteRequests()).thenReturn(true);
			when(driver.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
			generator = mock(LoadGenerator.class);
		}

		LoadStepContextImpl<IntegrityManifestDataItem, DeleteRequestOperation> context() {
			return new LoadStepContextImpl<>(
							"stop-once-" + System.nanoTime(),
							generator,
							driver,
							mock(MetricsContext.class),
							null,
							config.configVal("load"),
							false,
							com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
							null,
							config.configVal("item"),
							(ignoredId, ignoredSelection, ignoredCount) -> recorder);
		}

		void finishAlwaysThrows(final RuntimeException failure) {
			doThrow(failure).when(recorder).finish(any(), any(), any(), any());
		}

		void verifyFinishCalls(final int calls) {
			verify(recorder, times(calls)).finish(any(), any(), any(), any());
		}

		/**
		 * Every verification pass fails with an I/O error, as it does on the packaged engine while
		 * java.io.tmpdir is read-only: the verifier cannot read the selection.
		 */
		void failEveryVerificationPass() throws java.io.IOException {
			Files.delete(selection);
		}
	}
}
