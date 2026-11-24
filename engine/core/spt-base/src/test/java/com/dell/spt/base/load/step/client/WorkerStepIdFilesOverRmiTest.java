package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.StepIdCheckException;
import com.dell.spt.base.load.step.StepIdFiles;
import com.dell.spt.base.load.step.StepIdInUseException;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.linear.LinearLoadStepExtension;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The Entry Node's check of another node's step files, against a real worker step manager over RMI
 * on the loopback interface: an earlier run's file there is moved aside before any slice of the step
 * exists, and a worker that cannot move it, or that had the id in this run already, refuses the
 * step there and then, in the Entry Node's words for another node, with no worker path.
 */
@Timeout(120)
class WorkerStepIdFilesOverRmiTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private static final String EARLIER = OperationLifecycleArtifact.HEADER + "\n"
					+ "1,1762300000000,%s,worker-of-an-earlier-run,1,true,10,10,0,0,0,10,0,0,0\n";

	private final String stepId = "worker-step-id-files-rmi-" + UUID.randomUUID();
	private final MetricsManagerImpl metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private String originalRmiHostname;
	private String nodeAddr;
	private LoadStepManagerServiceImpl worker;

	@BeforeEach
	void startTheWorker() throws Exception {
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			port = socket.getLocalPort();
		}
		nodeAddr = "127.0.0.1:" + port;
		final List<Extension> extensions = List.of(new LinearLoadStepExtension<>(), new DummyStorageDriverMockExtension<>());
		worker = new LoadStepManagerServiceImpl(port, extensions, metrics);
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
			try (Stream<Path> siblings = Files.list(stepDir().getParent())) {
				for (final Path dir : siblings.filter(dir -> dir.getFileName().toString().startsWith(stepId)).toList()) {
					Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
					StepIdReuseAtStartTest.removeFolder(dir);
				}
			}
		}
	}

	@Test
	void anEarlierRunsFileIsMovedAsideBeforeAnySliceExists() throws Exception {
		final Path lifecycle = earlierFile();

		check(1_763_892_000_001L);

		assertFalse(Files.exists(lifecycle));
		final List<Path> moved = movedFolders();
		assertEquals(1, moved.size(), moved.toString());
		assertEquals(EARLIER.formatted(stepId), Files.readString(moved.get(0).resolve(lifecycle.getFileName())));
	}

	@Test
	void aReadOnlyWorkerFolderRefusesTheStepAndKeepsTheEarlierFileInPlace() throws Exception {
		assumeFalse("root".equals(System.getProperty("user.name")), "root writes a read-only folder");
		final Path lifecycle = earlierFile();
		Files.setPosixFilePermissions(stepDir(), PosixFilePermissions.fromString("r-xr-xr-x"));
		final StepIdCheckException failure;
		try {
			failure = assertThrows(StepIdCheckException.class, () -> check(1_763_892_000_002L));
		} finally {
			Files.setPosixFilePermissions(stepDir(), PosixFilePermissions.fromString("rwxr-xr-x"));
		}

		assertEquals("engine 2 of 2: step id \"" + stepId + "\" was used by an earlier run in that node's log directory, "
						+ "and its files could not be moved aside; see the entry's log", failure.getMessage());
		assertFalse(failure.getMessage().contains(stepDir().toString()), failure.getMessage());
		assertEquals(EARLIER.formatted(stepId), Files.readString(lifecycle));
		assertEquals(List.of(), movedFolders());
	}

	@Test
	void aRepeatedRunIdOnALongLivedWorkerRefusesTheStepAndKeepsTheFirstRunsFile() throws Exception {
		final long runId = 777_001L;
		check(runId);
		// The first run's slice there published its file.
		final Path lifecycle = Files.writeString(
						Files.createDirectories(stepDir()).resolve(OperationLifecycleArtifact.FILE_NAME),
						OperationLifecycleArtifact.HEADER + "\n1," + runId + "," + stepId + ",first-run,1,true,5,5,0,0,0,5,0,0,0\n");
		final String published = Files.readString(lifecycle);

		// A fresh Entry Node of the next run, given the same run id: its own record has no step yet.
		final var failure = assertThrows(StepIdInUseException.class, () -> check(runId));

		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
		assertEquals(published, Files.readString(lifecycle));
		assertEquals(List.of(), movedFolders());
	}

	/** The Entry Node's check of the worker's files for the first CREATE step of the id in run {@code runId}. */
	private void check(final long runId) throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-op-type", "create");
		final var merged = StepIdFiles.merged(config, List.of());
		// One engine: the worker's file manager is this process's, as its step manager is.
		LoadStepClientBase.refuseNodeFiles(
						stepId, List.of(FileManager.INSTANCE, FileManager.INSTANCE), List.of(nodeAddr), merged,
						List.of(false, false), runId,
						node -> ServiceUtil.resolve(node, LoadStepManagerService.SVC_NAME, LoadStepManagerService.class));
	}

	private Path earlierFile() throws Exception {
		final Path lifecycle = Files.createDirectories(stepDir()).resolve(OperationLifecycleArtifact.FILE_NAME);
		Files.writeString(lifecycle, EARLIER.formatted(stepId));
		return lifecycle;
	}

	private Path stepDir() throws Exception {
		return Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId))
						.toAbsolutePath()
						.getParent();
	}

	private List<Path> movedFolders() throws Exception {
		try (Stream<Path> siblings = Files.list(stepDir().getParent())) {
			return siblings.filter(dir -> dir.getFileName().toString().startsWith(stepId + ".earlier-")).toList();
		}
	}
}
