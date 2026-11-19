package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.local.LoadStepLocalBase;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.svc.ServiceUtil;
import com.dell.spt.testing.tags.IntegrationTest;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A long-lived worker over repeated standalone DELETE slices whose context stop or shutdown keeps
 * failing, as on the packaged engine when the step folder or java.io.tmpdir is read-only: each slice is
 * unexported and each context closes its storage driver, so no driver thread stays on the worker.
 *
 * <p>The contexts are real; their storage driver is a stand-in that owns one thread, as the HTTP
 * drivers' date-header refresher does, and ends it only when the driver is closed.
 */
@IntegrationTest
class RemoteStepSliceReleaseDeleteContextIntegrationTest {

	private static final String STEP_TYPE = "release-delete-context";
	private static final String DRIVER_THREAD_PREFIX = "release-delete-driver-";
	private static final int FAILING_STEPS = 3;

	private enum Fault {
		NONE, STEP_FOLDER_READ_ONLY, VERIFICATION_FAILS
	}

	private final List<String> stepIds = new ArrayList<>();
	private final List<Path> readOnlyFolders = new ArrayList<>();

	@Test
	void aWorkerReleasesEveryDeleteSliceWhoseEvidenceCannotBePersisted(@TempDir final Path temp)
					throws Exception {
		runWorker(temp, Fault.STEP_FOLDER_READ_ONLY, "Failed to persist DELETE recovery evidence");
	}

	@Test
	void aWorkerReleasesEveryDeleteSliceWhosePostVerificationCannotRun(@TempDir final Path temp)
					throws Exception {
		runWorker(
						temp,
						Fault.VERIFICATION_FAILS,
						"Standalone DELETE post-verification could not complete its full inventory pass");
	}

	private void runWorker(final Path temp, final Fault fault, final String expectedFailure) throws Exception {
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		final int port = freePort();
		final MetricsManager workerMetrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		final LoadStepManagerServiceImpl worker = new LoadStepManagerServiceImpl(
						port, List.of(deleteFactory(fault == Fault.VERIFICATION_FAILS)), workerMetrics);
		try {
			worker.start();
			final LoadStepManagerService manager = ServiceUtil.resolve(
							"127.0.0.1", port, LoadStepManagerService.SVC_NAME, LoadStepManagerService.class);

			// A clean step first: its context closes its driver as always.
			runStep(manager, port, temp, Fault.NONE, null);
			assertReleased(workerMetrics, port, "the clean step");

			final List<Integer> driverThreads = new ArrayList<>();
			for (var step = 1; step <= FAILING_STEPS; step++) {
				runStep(manager, port, temp, fault, expectedFailure);
				assertReleased(workerMetrics, port, "failing step " + step);
				driverThreads.add(liveDriverThreads());
			}
			System.out.printf("DELETE context release (%s): driver threads after each failing step %s%n", fault, driverThreads);
		} finally {
			try {
				worker.close();
			} finally {
				ServiceUtil.shutdown();
				if (originalHost == null) {
					System.clearProperty("java.rmi.server.hostname");
				} else {
					System.setProperty("java.rmi.server.hostname", originalHost);
				}
				for (final Path folder : readOnlyFolders) {
					Files.setPosixFilePermissions(folder, PosixFilePermissions.fromString("rwxr-xr-x"));
				}
				for (final String stepId : stepIds) {
					removeStepFolder(stepId);
				}
			}
		}
	}

	/** Runs one slice as the Entry Node does: start, stop, close, all over RMI. */
	private void runStep(
					final LoadStepManagerService manager,
					final int port,
					final Path temp,
					final Fault fault,
					final String expectedFailure) throws Exception {
		final String stepId = "delete-context-release-" + UUID.randomUUID();
		stepIds.add(stepId);
		final Path selection = temp.resolve(stepId + ".csv");
		Files.writeString(selection, "bucket,key,size,version_id\nbucket,key,1,\n");
		final Config config = deleteConfig(stepId, selection, fault == Fault.VERIFICATION_FAILS);
		final String serviceName = manager.newStepService(STEP_TYPE, config, List.of());
		final LoadStepService slice = ServiceUtil.resolve("127.0.0.1", port, serviceName, LoadStepService.class);
		slice.start();
		switch (fault) {
		case NONE -> {
			slice.stop();
			slice.close();
			return;
		}
		case STEP_FOLDER_READ_ONLY -> {
			final Path folder = Path.of(
							FileManager.INSTANCE.logFileName(Loggers.DELETE_OBJECTS.getName(), stepId)).getParent();
			Files.setPosixFilePermissions(folder, PosixFilePermissions.fromString("r-xr-xr-x"));
			readOnlyFolders.add(folder);
		}
		// Every verification pass then fails with an I/O error, as while java.io.tmpdir is read-only.
		case VERIFICATION_FAILS -> Files.delete(selection);
		}
		assertCause(assertThrows(Exception.class, slice::stop), expectedFailure, "stop");
		assertCause(assertThrows(Exception.class, slice::close), expectedFailure, "close");
		// A released slice answers no further remote call.
		assertThrows(java.rmi.RemoteException.class, slice::loadStepId);
	}

	private void assertReleased(final MetricsManager workerMetrics, final int port, final String after)
					throws Exception {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (liveDriverThreads() > 0 && System.nanoTime() < deadline) {
			Thread.sleep(20);
		}
		assertEquals(0, liveDriverThreads(), "storage driver threads alive on the worker after " + after);
		assertEquals(List.of(LoadStepManagerService.SVC_NAME),
						List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()),
						"services bound on the worker after " + after);
		assertTrue(workerMetrics.getAllContexts().isEmpty(),
						"metrics contexts registered on the worker after " + after);
	}

	private static int liveDriverThreads() {
		return (int) Thread.getAllStackTraces().keySet().stream()
						.filter(Thread::isAlive)
						.filter(thread -> thread.getName().startsWith(DRIVER_THREAD_PREFIX))
						.count();
	}

	private static void assertCause(final Throwable failure, final String expected, final String call) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			if (cause.getMessage() != null && cause.getMessage().contains(expected)) {
				return;
			}
		}
		throw new AssertionError("the " + call + " did not report \"" + expected + "\"", failure);
	}

	private static Config deleteConfig(final String stepId, final Path selection, final boolean postVerification) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("item-type", "data");
		config.val("item-output-file", "");
		config.val("item-input-file", selection.toString());
		config.val("load-batch-size", 4);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-batchSize", 1);
		config.val("load-op-delete-duration", false);
		config.val("load-step-limit-time", "0s");
		config.val("load-op-delete-selected", 1L);
		config.val("load-op-delete-selectedCurrentKey", 1L);
		config.val("load-op-delete-selectedExactVersion", 0L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=1"));
		config.val("load-op-delete-preValidation", false);
		config.val("load-op-delete-postVerification", postVerification);
		config.val("load-op-delete-verificationTimeoutMillis", 1L);
		config.val("load-op-recycle-mode", false);
		config.val("load-op-retry", false);
		config.val("load-op-limit-count", 0L);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 1);
		return config;
	}

	private LoadStepFactory<LoadStep, ?> deleteFactory(final boolean verificationProbe) {
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class, withSettings().stubOnly());
		when(factory.id()).thenReturn(STEP_TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenAnswer(invocation -> new DeleteSlice(
						invocation.getArgument(0), invocation.getArgument(3), verificationProbe));
		return factory;
	}

	/** A storage driver that owns one thread until it is closed, as the HTTP drivers do. */
	@SuppressWarnings("unchecked")
	private static StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> threadOwningDriver(
					final String stepId, final boolean verificationProbe) throws IOException {
		final var settings = withSettings().stubOnly();
		final StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver = verificationProbe
						? mock(StorageDriver.class, settings.extraInterfaces(DeleteVerificationProbe.class))
						: mock(StorageDriver.class, settings);
		when(driver.supportsStandaloneDeleteRequests()).thenReturn(true);
		when(driver.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
		final Thread poller = Thread.ofPlatform().daemon().name(DRIVER_THREAD_PREFIX + stepId).start(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
			}
		});
		doAnswer(invocation -> {
			poller.interrupt();
			poller.join(TimeUnit.SECONDS.toMillis(5));
			return null;
		}).when(driver).close();
		return driver;
	}

	private static int freePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static void removeStepFolder(final String stepId) throws IOException {
		final Path dir = Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId)).getParent();
		if (dir == null || !Files.exists(dir)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(dir)) {
			for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(path);
			}
		}
	}

	/** A worker's standalone DELETE slice over one real context. */
	private static final class DeleteSlice extends LoadStepLocalBase {
		private final boolean verificationProbe;

		DeleteSlice(final Config config, final MetricsManager metricsMgr, final boolean verificationProbe) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsMgr);
			this.verificationProbe = verificationProbe;
		}

		@Override
		public String getTypeName() {
			return STEP_TYPE;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void init() {
			initMetrics(0, OpType.DELETE, 1, config.configVal("output").configVal("metrics"), new SizeInBytes(0L), false);
			try {
				final String stepId = loadStepId();
				stepContexts.add(new LoadStepContextImpl<>(
								stepId,
								mock(LoadGenerator.class, withSettings().stubOnly()),
								threadOwningDriver(stepId, verificationProbe),
								metricsContexts.getFirst(),
								null,
								config.configVal("load"),
								false,
								ListShardMetricsRecorder.NO_OP,
								null,
								config.configVal("item")));
			} catch (final IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
