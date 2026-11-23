package com.dell.spt.base.load.step.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.StepIdInUseException;
import com.dell.spt.base.load.step.client.CsvLoggerArtifactAggregator;
import com.dell.spt.base.load.step.client.StepIdReuseAtStartTest;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.linear.LinearLoadStepExtension;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A worker engine (its step manager over RMI on the loopback interface, the real linear step and the
 * dummy mock driver) whose log directory holds a step's lifecycle file from an earlier run, as the
 * legacy harnesses leave it: the first CREATE step of that id in the next run moves the file aside,
 * warns once and runs, and the step's lifecycle evidence is this run's alone. A second step of the id
 * in the same run is refused at its start.
 */
@Timeout(120)
class WorkerEarlierRunStepTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private static final long RUN_ID = 1_763_892_000_000L;
	private static final long COUNT = 5;
	private static final String EARLIER = OperationLifecycleArtifact.HEADER + "\n"
					+ "1,1762300000000,%s,worker-of-an-earlier-run,1,true,1032767,0,0,1032767,0,0,0,0,0\n";

	private final String stepId = "MAXT-W-10KB-C-T10-" + UUID.randomUUID();
	private final MetricsManagerImpl metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private String originalRmiHostname;
	private int port;
	private LoadStepManagerServiceImpl worker;

	@BeforeEach
	void startTheWorker() throws Exception {
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			port = socket.getLocalPort();
		}
		worker = new LoadStepManagerServiceImpl(port, extensions(), metrics);
		worker.start();
	}

	@AfterEach
	void stopTheWorker() throws Exception {
		try {
			if (!worker.isClosed()) {
				worker.close();
			}
		} finally {
			ServiceUtil.shutdown();
			if (originalRmiHostname == null) {
				System.clearProperty(RMI_HOSTNAME);
			} else {
				System.setProperty(RMI_HOSTNAME, originalRmiHostname);
			}
			try (Stream<Path> siblings = Files.list(lifecycle().toAbsolutePath().getParent().getParent())) {
				for (final Path dir : siblings.filter(dir -> dir.getFileName().toString().startsWith(stepId)).toList()) {
					StepIdReuseAtStartTest.removeFolder(dir);
				}
			}
		}
	}

	@Test
	void theFirstCreateStepOfTheIdInARunMovesTheEarlierRunsFileAsideAndPublishesOnlyThisRunsRow() throws Exception {
		final Path lifecycle = lifecycle();
		Files.createDirectories(lifecycle.toAbsolutePath().getParent());
		final String earlier = EARLIER.formatted(stepId);
		Files.writeString(lifecycle, earlier);

		final List<String> warnings = warnings(() -> runSlice());

		final Path moved = movedFolder();
		assertEquals(1, warnings.size(), warnings.toString());
		assertTrue(warnings.get(0).contains(moved.toString()), warnings.toString());
		assertEquals(earlier, Files.readString(moved.resolve(OperationLifecycleArtifact.FILE_NAME)), "the earlier file changed");
		final List<String> sliceRows = Files.readAllLines(lifecycle);
		assertEquals(OperationLifecycleArtifact.HEADER, sliceRows.get(0));
		assertEquals(1, sliceRows.size() - 1, sliceRows.toString());
		assertEquals(String.valueOf(RUN_ID), runIdOf(sliceRows.get(1)), sliceRows.toString());

		// The Entry Node's merge of the step's lifecycle files then holds this run's row alone.
		new CsvLoggerArtifactAggregator(stepId, List.of(FileManager.INSTANCE), Loggers.OPERATION_LIFECYCLE.getName(),
						OperationLifecycleArtifact.FILE_NAME, OperationLifecycleArtifact.HEADER).close();
		final List<String> merged = Files.readAllLines(lifecycle);
		assertEquals(2, merged.size(), merged.toString());
		assertEquals(String.valueOf(RUN_ID), runIdOf(merged.get(1)), merged.toString());
		assertFalse(String.join("\n", merged).contains("worker-of-an-earlier-run"), merged.toString());
	}

	@Test
	void aSecondStepOfTheIdInTheSameRunIsRefusedAtItsStartOnTheWorker() throws Exception {
		runSlice();
		final String published = Files.readString(lifecycle());

		final LoadStepService second = newSlice();
		final var thrown = assertThrows(Exception.class, second::start);

		assertTrue(causedBy(thrown, StepIdInUseException.class), thrown.toString());
		second.close();
		assertEquals(published, Files.readString(lifecycle()));
		assertNoMovedFolder();
	}

	@Test
	void theWorkerStillServesTheIdInItsNextRun() throws Exception {
		runSlice();
		final String published = Files.readString(lifecycle());

		runSlice(newSlice(RUN_ID + 1));

		assertEquals(published, Files.readString(movedFolder().resolve(OperationLifecycleArtifact.FILE_NAME)));
		assertEquals(String.valueOf(RUN_ID + 1), runIdOf(Files.readAllLines(lifecycle()).get(1)));
	}

	private void runSlice() throws Exception {
		runSlice(newSlice());
	}

	/** Runs {@code slice} as the Entry Node does: start, wait for its count's operations, stop and close. */
	private static void runSlice(final LoadStepService slice) throws Exception {
		slice.start();
		assertTrue(slice.await(60, TimeUnit.SECONDS), "the slice did not end its count");
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
		while (successes(slice) < COUNT) {
			assertTrue(System.nanoTime() < deadline, "the slice's operations did not finish");
			Thread.sleep(10);
		}
		slice.stop();
		slice.close();
	}

	private static long successes(final LoadStepService slice) throws Exception {
		return slice.metricsSnapshots().stream().mapToLong(snapshot -> snapshot.successSnapshot().count()).sum();
	}

	private LoadStepService newSlice() throws Exception {
		return newSlice(RUN_ID);
	}

	private LoadStepService newSlice(final long runId) throws Exception {
		final String name = ((LoadStepManagerService) worker).newStepService(LinearLoadStepExtension.TYPE, config(runId), List.of());
		return ServiceUtil.resolve("127.0.0.1", port, name, LoadStepService.class);
	}

	private Config config(final long runId) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("run-id", runId);
		config.val("load-step-limit-time", 0);
		config.val("storage-driver-type", "dummy-mock");
		config.val("output-metrics-timing-persist", false);
		config.val("output-metrics-trace-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		config.val("item-input-file", null);
		config.val("item-input-path", null);
		config.val("item-output-file", "");
		config.val("load-op-type", "create");
		config.val("load-op-limit-count", COUNT);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 1);
		return config;
	}

	private Path lifecycle() throws Exception {
		return Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId));
	}

	private Path movedFolder() throws Exception {
		final List<Path> moved = movedFolders();
		assertEquals(1, moved.size(), moved.toString());
		return moved.get(0);
	}

	private void assertNoMovedFolder() throws Exception {
		assertEquals(List.of(), movedFolders());
	}

	private List<Path> movedFolders() throws Exception {
		try (Stream<Path> siblings = Files.list(lifecycle().toAbsolutePath().getParent().getParent())) {
			return siblings.filter(dir -> dir.getFileName().toString().startsWith(stepId + ".earlier-")).toList();
		}
	}

	/** The engine_run_id of a lifecycle row. */
	private static String runIdOf(final String row) {
		return row.split(",", -1)[1];
	}

	private static boolean causedBy(final Throwable thrown, final Class<? extends Throwable> type) {
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (type.isInstance(cause)) {
				return true;
			}
		}
		return false;
	}

	/** The warnings {@code action} logs for this step to errors.log and the console. */
	private List<String> warnings(final ThrowingAction action) throws Exception {
		final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
		final String end = "worker earlier run step end " + UUID.randomUUID();
		final var ended = new CountDownLatch(1);
		final var appender = new AbstractAppender("workerEarlierRunStep-" + stepId, null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				final String message = event.getMessage().getFormattedMessage();
				if (message.equals(end)) {
					ended.countDown();
				} else if ("WARN".equals(event.getLevel().name()) && message.contains(stepId)) {
					warnings.add(message);
				}
			}
		};
		appender.start();
		final var logger = (org.apache.logging.log4j.core.Logger) LoggerContext.getContext(false)
						.getLogger(Loggers.ERR.getName());
		logger.addAppender(appender);
		try {
			action.run();
			Loggers.ERR.info(end);
			assertTrue(ended.await(10, TimeUnit.SECONDS), "the end marker never arrived");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
		synchronized (warnings) {
			return List.copyOf(warnings);
		}
	}

	@FunctionalInterface
	private interface ThrowingAction {
		void run() throws Exception;
	}

	private static List<Extension> extensions() {
		return List.of(new LinearLoadStepExtension<>(), new DummyStorageDriverMockExtension<>());
	}
}
