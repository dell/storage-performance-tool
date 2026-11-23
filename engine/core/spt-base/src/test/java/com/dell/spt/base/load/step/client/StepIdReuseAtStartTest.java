package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.buildinfo.EngineBuildInfoPublisher;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.StepIdCheckException;
import com.dell.spt.base.load.step.StepIdFiles;
import com.dell.spt.base.load.step.StepIdInUseException;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.ServerException;
import java.rmi.UnmarshalException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;

/**
 * A step whose id an earlier step already used in this log directory is refused at its start, before
 * any slice starts, when it would refuse the earlier step's files at close; the earlier step's files
 * stay as they were, even through the refused step's close. A step that publishes none of them runs
 * as before.
 */
public class StepIdReuseAtStartTest {

	private static final String NODE = "127.0.0.1:1";
	private static final String NODE_FILE = "/w/operation.lifecycle.csv";
	private final String stepId = "step-id-reuse-" + UUID.randomUUID();
	private LoadStepFactory<LoadStep, ?> factory;
	private Path stepDir;

	@TempDir
	Path tempDir;

	@AfterEach
	void removeTheStepFolder() throws IOException {
		if (stepDir != null) {
			removeFolder(stepDir);
		}
	}

	/**
	 * Best effort: a step's asynchronous log can add a file while the folder is removed, so the walk is
	 * retried; the step id is unique, so a folder left behind affects no other test.
	 */
	public static void removeFolder(final Path dir) throws IOException {
		for (int attempt = 0; attempt < 5 && Files.isDirectory(dir); attempt++) {
			try (Stream<Path> files = Files.walk(dir)) {
				for (final Path file : files.sorted(Comparator.reverseOrder()).toList()) {
					Files.deleteIfExists(file);
				}
			} catch (final DirectoryNotEmptyException | NoSuchFileException raced) {
				// A file appeared or went during the walk: walk again.
			}
		}
	}

	@Test
	void aReadStepIsRefusedWhenAnEarlierStepsNodeSourceExists() throws Exception {
		final Path stale = earlierFile(nodeSource(Loggers.OPERATION_LIFECYCLE.getName(), 0));

		final var failure = refused(client(config("read")));

		assertEquals(stale.toString(), failure.getMessage().replaceAll(".*\\((.*) exists\\).*", "$1"));
		assertEquals("earlier", Files.readString(stale));
		verify(factory, never()).createLocal(any(), anyList(), anyList(), any());
	}

	@Test
	void aSingleNodeStepIsRefusedWhenALargerEarlierStepLeftOnlyAWorkersNodeSource() throws Exception {
		// An earlier 3-node step whose entry file was missing keeps its workers' files only.
		final Path stale = earlierFile(nodeSource(Loggers.OPERATION_LIFECYCLE.getName(), 2));

		final var failure = refused(client(config("create")));

		assertEquals(stale.toAbsolutePath().toString(),
						Path.of(failure.getMessage().replaceAll(".*\\((.*) exists\\).*", "$1")).toAbsolutePath().toString());
		assertEquals("earlier", Files.readString(stale));
		verify(factory, never()).createLocal(any(), anyList(), anyList(), any());
	}

	@Test
	void aCreateStepIsRefusedWhenTheCanonicalFileExistsAndItsCloseLeavesItAlone() throws Exception {
		final Path canonical = earlierFile(canonical(Loggers.OPERATION_LIFECYCLE.getName()));
		final TestLoadStepClient client = client(config("create"));

		final var failure = refused(client);
		assertTrue(failure.getMessage().contains(canonical.toString()), failure.getMessage());
		closeQuietly(client);

		assertEquals("earlier", Files.readString(canonical));
		assertFalse(Files.exists(nodeSource(Loggers.OPERATION_LIFECYCLE.getName(), 0)),
						"the refused step's close moved the earlier step's file");
		try (Stream<Path> files = Files.list(stepDir)) {
			// The refused step logs its messages there, and adds no result file.
			assertEquals(List.of(canonical.toAbsolutePath()), files.map(Path::toAbsolutePath)
							.filter(file -> file.getFileName().toString().endsWith(".csv")).toList());
		}
	}

	@Test
	void aPartialReadIsRefusedWhenAnEarlierRangeReadSourceExists() throws Exception {
		final Path stale = earlierFile(nodeSource(Loggers.RANGE_READ.getName(), 0));
		final Config config = config("read");
		config.val("load-op-read-range-size", "1KiB");
		// Only its name is checked before the step starts; this fixture never loads a driver.
		config.val("storage-driver-type", "s3");

		final var failure = refused(client(config));

		assertTrue(failure.getMessage().contains(stale.toString()), failure.getMessage());
		assertEquals("earlier", Files.readString(stale));
	}

	@Test
	void aReadWithoutRangesIgnoresAnEarlierRangeReadFile() throws Exception {
		earlierFile(nodeSource(Loggers.RANGE_READ.getName(), 0));

		final TestLoadStepClient client = client(config("read"));
		try {
			client.start();
		} catch (final RuntimeException other) {
			assertFalse(RunTerminalException.find(other) instanceof StepIdInUseException, other.toString());
		} finally {
			closeQuietly(client);
		}
	}

	@Test
	void updateDeleteAndNoopStepsStillRunAfterAnEarlierStepWithTheSameId() throws Exception {
		earlierFile(canonical(Loggers.OPERATION_LIFECYCLE.getName()));
		earlierFile(nodeSource(Loggers.OPERATION_LIFECYCLE.getName(), 0));
		for (final String opType : List.of("update", "delete", "noop")) {
			final TestLoadStepClient client = client(config(opType));
			try {
				client.start();
			} catch (final RuntimeException other) {
				assertFalse(RunTerminalException.find(other) instanceof StepIdInUseException, opType + ": " + other);
			} finally {
				closeQuietly(client);
			}
		}
	}

	@Test
	void aMetadataModeStepIsRefusedWhenItsManifestWasAlreadyPublished() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		final Path marker = IntegrityManifestCompletion.completionPath(manifest);
		Files.writeString(marker, "earlier");
		final Config config = config("create");
		config.val("item-output-file", manifest.toString());
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-driver-type", "s3");

		final TestLoadStepClient client = client(config);
		final var thrown = assertThrows(RuntimeException.class, client::start);
		closeQuietly(client);

		final var failure = IntegrityTerminalException.find(thrown);
		assertEquals(IntegrityTerminalException.Category.PUBLICATION, failure.category(), thrown.toString());
		assertTrue(failure.getMessage().contains("item output file is already published by an earlier step"),
						failure.getMessage());
		assertEquals("earlier", Files.readString(marker));
	}

	@Test
	void theStartCheckTestsTheFilesTheLoggerAggregatorRefusesAtClose() throws Exception {
		final var entry = FileManager.INSTANCE;
		final var paths = CsvLoggerArtifactAggregator.publishedPaths(
						entry, Loggers.OPERATION_LIFECYCLE.getName(), stepId, 3);
		final Path canonical = canonical(Loggers.OPERATION_LIFECYCLE.getName());
		assertEquals(List.of(canonical, CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 0),
						CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 1),
						CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 2)), paths);
	}

	@Test
	void theStartCheckTestsTheFilesTheManifestAggregatorRefusesAtClose() {
		final Path manifest = tempDir.resolve("verify-input.csv");
		final Path node0 = CsvArtifactAggregator.nodeSourcePath(manifest, 0);
		final Path node1 = CsvArtifactAggregator.nodeSourcePath(manifest, 1);
		assertEquals(List.of(IntegrityManifestCompletion.completionPath(manifest), node0, node1),
						CsvArtifactAggregator.publishedPaths(manifest.toString(), 2, OpType.CREATE));
		assertEquals(List.of(IntegrityManifestCompletion.completionPath(manifest),
						node0, IntegrityManifestCompletion.emissionCountPath(node0),
						IntegrityManifestCompletion.deleteMarkerCountPath(node0),
						node1, IntegrityManifestCompletion.emissionCountPath(node1),
						IntegrityManifestCompletion.deleteMarkerCountPath(node1)),
						CsvArtifactAggregator.publishedPaths(manifest.toString(), 2, OpType.LIST));
	}

	@Test
	void theEntryNodeRefusesTheStepWhenAnotherNodeHoldsAnEarlierFileOfIt() throws Exception {
		final var merged = StepIdFiles.merged(config("create"), List.of());
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), "/w/operation.lifecycle.csv", 120);

		final var failure = assertThrows(StepIdInUseException.class, () -> LoadStepClientBase.refuseNodeFiles(
						stepId, List.of(FileManager.INSTANCE, worker), List.of("10.0.0.2:1100"), merged, List.of(false, false), 0L, null));

		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
	}

	@Test
	void theEntryNodeAsksAWorkerToMoveAsideItsEarlierRunFileForTheFirstStepOfItsIdInTheRun() throws Exception {
		final var merged = StepIdFiles.merged(config("create"), List.of());
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);
		when(manager.prepareStepIdFiles(anyLong(), anyString(), anyList())).thenReturn(true);

		LoadStepClientBase.refuseNodeFiles(
						stepId, List.of(FileManager.INSTANCE, worker), List.of("10.0.0.2:1100"), merged, List.of(false, false),
						77L, node -> manager);

		verify(worker).fileSize(NODE_FILE);
		verify(manager).prepareStepIdFiles(77L, stepId, List.of(Loggers.OPERATION_LIFECYCLE.getName()));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("uncheckableNodes")
	void aNodeThatCannotTellIsRefusedForTheFirstStepOfItsIdInTheRunToo(final String name, final Fault fault)
					throws Exception {
		final var merged = StepIdFiles.merged(config("create"), List.of());
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 0);
		fault.apply(worker, stepId);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);

		final var failure = assertThrows(StepIdCheckException.class, () -> LoadStepClientBase.refuseNodeFiles(
						stepId, List.of(FileManager.INSTANCE, worker), List.of("n2"), merged, List.of(false, false),
						77L, node -> manager));

		assertEquals("engine 2 of 2: step id \"" + stepId + "\" could not be checked in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
		verify(manager, never()).prepareStepIdFiles(anyLong(), anyString(), anyList());
	}

	@Test
	void aWorkersEarlierRunFileIsRefusedForTheSecondStepOfItsIdInTheSameRunBeforeAnySlice() throws Exception {
		final long runId = runId();
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);
		when(manager.prepareStepIdFiles(anyLong(), anyString(), anyList())).thenReturn(true);
		final Config firstConfig = config("create");
		firstConfig.val("run-id", runId);
		firstConfig.val("load-step-node-addrs", List.of(NODE));
		final TestLoadStepClient first = client(firstConfig);
		first.withNodeFileManagers(Map.of(NODE, worker)).withNodeStepManagers(Map.of(NODE, manager));
		try {
			// The node 127.0.0.1:1 serves no step, so the first step fails later, at its slice there.
			final var thrown = assertThrows(RuntimeException.class, first::start);
			final var cause = RunTerminalException.find(thrown);
			assertFalse(cause instanceof StepIdInUseException || cause instanceof StepIdCheckException, thrown.toString());
		} finally {
			closeQuietly(first);
		}
		verify(manager).prepareStepIdFiles(runId, stepId, List.of(Loggers.OPERATION_LIFECYCLE.getName()));

		final Config second = config("create");
		second.val("run-id", runId);
		final var failure = refusedAtStart(second, worker);

		assertInstanceOf(StepIdInUseException.class, failure, failure.toString());
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
		verify(manager, org.mockito.Mockito.times(1)).prepareStepIdFiles(anyLong(), anyString(), anyList());
	}

	@Test
	void aWorkerThatCannotMoveAsideItsEarlierRunFileRefusesTheStepBeforeAnySliceExists() throws Exception {
		// A read-only step folder on the worker: its step manager throws as the real one does.
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);
		when(manager.prepareStepIdFiles(anyLong(), anyString(), anyList())).thenThrow(StepIdCheckException.notMovedAside(
						stepId, NODE_FILE, new AccessDeniedException(NODE_FILE + " -> /w.earlier-20261123T101530Z")));
		final Config config = config("create");
		config.val("run-id", runId());

		final var failure = refusedAtStart(config, worker, manager);

		assertInstanceOf(StepIdCheckException.class, failure, failure.toString());
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" was used by an earlier run in that node's log directory, "
						+ "and its files could not be moved aside; see the entry's log", failure.getMessage());
		assertFalse(failure.getMessage().contains("/w"), failure.getMessage());
	}

	@Test
	void aWorkerThatHadTheIdInThisRunAlreadyRefusesTheStepBeforeAnySliceExists() throws Exception {
		// A repeated run id on a worker that outlived its run: the worker, not this Entry Node, had the id.
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);
		when(manager.prepareStepIdFiles(anyLong(), anyString(), anyList())).thenReturn(false);
		final Config config = config("create");
		config.val("run-id", runId());

		final var failure = refusedAtStart(config, worker, manager);

		assertInstanceOf(StepIdInUseException.class, failure, failure.toString());
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
	}

	@Test
	void anOlderWorkerHoldingAnEarlierRunFileIsRefusedAsBefore() throws Exception {
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);
		when(manager.prepareStepIdFiles(anyLong(), anyString(), anyList())).thenThrow(new ServerException(
						"RemoteException occurred in server thread",
						new UnmarshalException("unrecognized method hash: method not supported by remote object")));
		final Config config = config("create");
		config.val("run-id", runId());

		final var failure = refusedAtStart(config, worker, manager);

		assertInstanceOf(StepIdInUseException.class, failure, failure.toString());
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
	}

	@Test
	void aNodeWhoseStepManagerCannotBeReachedIsRefusedAsOneThatCannotTell() throws Exception {
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);
		when(manager.prepareStepIdFiles(anyLong(), anyString(), anyList()))
						.thenThrow(new java.rmi.ConnectException("Connection refused to host: 10.0.0.2"));
		final Config config = config("create");
		config.val("run-id", runId());

		final var failure = refusedAtStart(config, worker, manager);

		assertInstanceOf(StepIdCheckException.class, failure, failure.toString());
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" could not be checked in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
	}

	@Test
	void aStepWithNoneOfTheFilesLeavesItsIdToALaterStepOfTheRun() throws Exception {
		// NOOP X, then CREATE X, in one run, on a worker that holds an earlier run's file for X.
		final long runId = runId();
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);
		final LoadStepManagerService manager = mock(LoadStepManagerService.class);
		when(manager.prepareStepIdFiles(anyLong(), anyString(), anyList())).thenReturn(true);
		for (final String opType : List.of("noop", "create")) {
			final Config config = config(opType);
			config.val("run-id", runId);
			config.val("load-step-node-addrs", List.of(NODE));
			final TestLoadStepClient client = client(config);
			client.withNodeFileManagers(Map.of(NODE, worker)).withNodeStepManagers(Map.of(NODE, manager));
			try {
				// The node 127.0.0.1:1 serves no step, so each step fails later, at its slice there.
				final var thrown = assertThrows(RuntimeException.class, client::start);
				final var cause = RunTerminalException.find(thrown);
				assertFalse(cause instanceof StepIdInUseException || cause instanceof StepIdCheckException,
								opType + ": " + thrown);
			} finally {
				closeQuietly(client);
			}
		}
		verify(manager, org.mockito.Mockito.times(1))
						.prepareStepIdFiles(runId, stepId, List.of(Loggers.OPERATION_LIFECYCLE.getName()));
	}

	@Test
	void aWorkersRefusalAtItsSliceStartReadsAsTheEntryNodesNodeMessage() throws Exception {
		final TestLoadStepClient client = client(config("create"));
		final String workerFile = "/srv/worker/log/" + stepId + "/operation.lifecycle.csv";
		final var inUse = new StepIdInUseException(stepId, Path.of(workerFile));
		final var unchecked = StepIdCheckException.here(stepId, workerFile, new AccessDeniedException(workerFile));
		final String inUseMessage = "engine 2 of 3: step id \"" + stepId + "\" is already used in that node's log "
						+ "directory; see the entry's log";
		final String uncheckedMessage = "engine 2 of 3: step id \"" + stepId + "\" could not be checked in that node's log "
						+ "directory; see the entry's log";

		final List<RunTerminalException> failures = List.of(
						client.sliceStartFailure(new ServerException("RemoteException occurred in server thread", inUse), 1, 3),
						client.metadataSliceStartFailure(new RuntimeException(inUse), 1, 3),
						client.barrierStartFailure(new ServerException("RemoteException occurred in server thread", inUse),
										"10.0.0.2:1100", 1, 3),
						client.sliceStartFailure(unchecked, 1, 3),
						client.barrierStartFailure(new ServerException("RemoteException occurred in server thread", unchecked),
										"10.0.0.2:1100", 1, 3));

		assertEquals(List.of(inUseMessage, inUseMessage, inUseMessage, uncheckedMessage, uncheckedMessage),
						failures.stream().map(RunTerminalException::getMessage).toList());
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failures.get(0).category());
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failures.get(3).category());
		// The Entry Node's own slice keeps its own file in the message, as #320 has it.
		assertEquals("engine 1 of 3: " + inUse.getMessage(), client.sliceStartFailure(inUse, 0, 3).getMessage());
	}

	@Test
	void theEntryNodesOwnEarlierFileIsRefusedForTheFirstStepOfItsIdInARun() throws Exception {
		final Path canonical = earlierFile(canonical(Loggers.OPERATION_LIFECYCLE.getName()));
		final Config config = config("create");
		config.val("run-id", runId());

		final var failure = refusedAtStart(config, node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 0));

		assertEquals(StepIdInUseException.class, failure.getClass());
		assertEquals("step id \"" + stepId + "\" is already used in this log directory (" + canonical
						+ " exists); give each step its own load.step.id", failure.getMessage());
		assertEquals("earlier", Files.readString(canonical));
	}

	@Test
	void aLocalRunsEarlierFileIsRefusedForTheFirstStepOfItsIdInARun() throws Exception {
		final Path canonical = earlierFile(canonical(Loggers.OPERATION_LIFECYCLE.getName()));
		final Config config = config("create");
		config.val("run-id", runId());

		final var failure = refused(client(config));

		assertTrue(failure.getMessage().contains("(" + canonical + " exists)"), failure.getMessage());
		assertEquals("earlier", Files.readString(canonical));
	}

	/** A run id of its own, so that no other test's step was in its run. */
	private static long runId() {
		return (UUID.randomUUID().getMostSignificantBits() >>> 1) | 1L;
	}

	@Test
	void anotherNodesEarlierIntegrityLogAndEmptyFilesAreRefusedToo() throws Exception {
		final Config config = config("create");
		config.val("storage-integrity-mode", "metadata");
		final var merged = StepIdFiles.merged(config, List.of());
		assertEquals(List.of(Loggers.OPERATION_LIFECYCLE.getName(), Loggers.INTEGRITY_PERFORMANCE.getName()),
						StepIdFiles.sliceLoggerNames(merged, false));
		final FileManager worker = node(Loggers.INTEGRITY_PERFORMANCE.getName(), "/w/integrity.performance.csv", 0);

		final var failure = assertThrows(StepIdInUseException.class, () -> LoadStepClientBase.refuseNodeFiles(
						stepId, List.of(FileManager.INSTANCE, worker), List.of("n2"), merged, List.of(false, false), 0L, null));

		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
		verify(worker).readFromFile(eq("/w/integrity.performance.csv"), anyLong());
	}

	@Test
	void anotherNodesNicFilesAreTestedOnlyWhenItsSliceRequestedANicSet() throws Exception {
		final var merged = StepIdFiles.merged(config("update"), List.of());
		final FileManager worker = node(Loggers.NIC_SET.getName(), "/w/nic.set.json", 10);
		final List<FileManager> fileMgrs = List.of(FileManager.INSTANCE, worker);

		LoadStepClientBase.refuseNodeFiles(stepId, fileMgrs, List.of("n2"), merged, List.of(true, false), 0L, null);
		final var failure = assertThrows(StepIdInUseException.class,
						() -> LoadStepClientBase.refuseNodeFiles(stepId, fileMgrs, List.of("n2"), merged, List.of(false, true), 0L, null));

		assertTrue(failure.getMessage().startsWith("engine 2 of 2: "), failure.getMessage());
		verify(worker, org.mockito.Mockito.atLeastOnce()).fileSize("/w/nic.set.json");
	}

	@Test
	void theStartRefusesAStepWhoseOtherNodeHoldsAnEarlierFileBeforeAnySliceOrBuildRecord() throws Exception {
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 120);

		final var failure = refusedAtStart(config("create"), worker);

		assertInstanceOf(StepIdInUseException.class, failure, failure.toString());
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", failure.getMessage());
		assertFalse(failure.getMessage().contains(NODE) || failure.getMessage().contains(NODE_FILE), failure.getMessage());
		verify(worker).fileSize(NODE_FILE);
	}

	@Test
	void theStartCheckLogsNoSliceInterfacesOfItsOwn() throws Exception {
		final Config config = config("update");
		config.val("storage-net-nodeInterfaces-entry", "none");
		final String line = "Load step slice #0 (entry) interfaces: none (host override)";
		final String end = "stepIdReuseSliceLines end " + UUID.randomUUID();
		final List<String> messages = Collections.synchronizedList(new ArrayList<>());
		final var ended = new CountDownLatch(1);
		final var appender = new AbstractAppender("stepIdReuseSliceLines", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				final String message = event.getMessage().getFormattedMessage();
				if (message.contains(stepId) || message.startsWith("Load step slice #")) {
					messages.add(message);
				}
				if (message.equals(end)) {
					ended.countDown();
				}
			}
		};
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		logger.addAppender(appender);
		final TestLoadStepClient client = client(config);
		try {
			try {
				client.start();
			} catch (final RuntimeException other) {
				assertFalse(RunTerminalException.find(other) instanceof StepIdInUseException, other.toString());
			}
			// One thread's messages arrive in order, so once this marker logged after the start has
			// arrived, every line the start logged has arrived before it.
			Loggers.MSG.info(end);
			assertTrue(ended.await(10, TimeUnit.SECONDS), "the end marker never arrived");
			final List<String> snapshot;
			synchronized (messages) {
				snapshot = new ArrayList<>(messages);
			}
			assertEquals(1, snapshot.stream().filter(line::equals).count(), snapshot.toString());
		} finally {
			closeQuietly(client);
			logger.removeAppender(appender);
			appender.stop();
		}
	}

	/** The faults a node's file manager can answer the reuse check with, none of them a definite answer. */
	static Stream<Arguments> uncheckableNodes() {
		return Stream.of(
						Arguments.of("a denied file on readFromFile", (Fault) (node, stepId) -> when(node.readFromFile(eq(NODE_FILE), anyLong()))
										.thenThrow(new AccessDeniedException(NODE_FILE))),
						Arguments.of("an RMI failure on fileSize", (Fault) (node, stepId) -> when(node.fileSize(NODE_FILE))
										.thenThrow(new RemoteException("connection reset"))),
						Arguments.of("an RMI failure on readFromFile", (Fault) (node, stepId) -> when(node.readFromFile(eq(NODE_FILE), anyLong()))
										.thenThrow(new RemoteException("connection reset"))),
						Arguments.of("an RMI failure on logFileName", (Fault) (node, stepId) -> when(node.logFileName(
										Loggers.OPERATION_LIFECYCLE.getName(), stepId)).thenThrow(new RemoteException("connection reset"))));
	}

	@FunctionalInterface
	interface Fault {
		void apply(FileManager node, String stepId) throws IOException;
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("uncheckableNodes")
	void aNodeThatCannotTellIsRefusedBeforeAnySliceStarts(final String name, final Fault fault) throws Exception {
		final FileManager worker = node(Loggers.OPERATION_LIFECYCLE.getName(), NODE_FILE, 0);
		fault.apply(worker, stepId);

		final var failure = refusedAtStart(config("create"), worker);

		final var unchecked = assertInstanceOf(StepIdCheckException.class, failure, failure.toString());
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" could not be checked in that node's log directory; "
						+ "see the entry's log", unchecked.getMessage());
		assertEquals(IntegrityTerminalException.Category.EXECUTION, unchecked.category());
	}

	@Test
	void anEntryFileThatCannotBeTestedRefusesTheStepBeforeAnySliceStarts() throws Exception {
		final Path lifecycle = canonical(Loggers.OPERATION_LIFECYCLE.getName());
		Files.createDirectories(stepDir);
		// A link to itself: the file system cannot tell whether an earlier step left the file.
		Files.createSymbolicLink(lifecycle, lifecycle.getFileName());

		final var failure = refusedAtStart(config("create"), null);

		assertInstanceOf(StepIdCheckException.class, failure, failure.toString());
		assertEquals("step id \"" + stepId + "\" could not be checked in this log directory; see the engine log",
						failure.getMessage());
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(Files.isSymbolicLink(lifecycle));
	}

	@Test
	void anEntryManifestThatCannotBeTestedRefusesTheStepBeforeAnySliceStarts() throws Exception {
		assumeFalse("root".equals(System.getProperty("user.name")), "root reads a denied directory");
		final Path denied = Files.createDirectory(tempDir.resolve("denied"));
		final Config config = config("create");
		config.val("item-output-file", denied.resolve("written.csv").toString());
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-driver-type", "s3");
		Files.setPosixFilePermissions(denied, PosixFilePermissions.fromString("---------"));
		try {
			final var failure = refusedAtStart(config, null);

			assertInstanceOf(StepIdCheckException.class, failure, failure.toString());
			assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		} finally {
			Files.setPosixFilePermissions(denied, PosixFilePermissions.fromString("rwx------"));
		}
	}

	/**
	 * Starts a step of {@code config}, on this node and, with {@code worker}, on one other node, and
	 * returns why it was refused: before any slice started and before its build record was written.
	 */
	private RunTerminalException refusedAtStart(final Config config, final FileManager worker) throws Exception {
		return refusedAtStart(config, worker, null);
	}

	/** As {@link #refusedAtStart(Config, FileManager)}, the other node's step manager {@code manager} when given. */
	private RunTerminalException refusedAtStart(
					final Config config, final FileManager worker, final LoadStepManagerService manager) throws Exception {
		final Path buildRecord = canonical(Loggers.OPERATION_LIFECYCLE.getName()).resolveSibling(EngineBuildInfoPublisher.FILE_NAME);
		if (worker != null) {
			config.val("load-step-node-addrs", List.of(NODE));
		}
		final TestLoadStepClient client = client(config);
		if (worker != null) {
			client.withNodeFileManagers(Map.of(NODE, worker));
		}
		if (manager != null) {
			client.withNodeStepManagers(Map.of(NODE, manager));
		}
		final List<String> before = resultFiles();
		final RuntimeException thrown;
		// The build record goes to the log home of the thread context, which the loggers' folder is under here.
		try (var home = CloseableThreadContext.put(Constants.KEY_HOME_DIR, stepDir.getParent().getParent().toString())) {
			thrown = assertThrows(RuntimeException.class, client::start);
		} finally {
			closeQuietly(client);
		}
		verify(factory, never()).createLocal(any(), anyList(), anyList(), any());
		assertFalse(Files.exists(buildRecord, LinkOption.NOFOLLOW_LINKS), "the refused step wrote its build record");
		assertEquals(before, resultFiles(), "the refused step published a result file");
		final var failure = RunTerminalException.find(thrown);
		assertTrue(failure instanceof StepIdCheckException || failure instanceof StepIdInUseException, thrown.toString());
		assertEquals(stepId, failure.stepId());
		return failure;
	}

	@Test
	void aMetadataSlicesRefusalKeepsItsCategoryAndMessage() throws Exception {
		final TestLoadStepClient client = client(config("create"));
		final var refusal = new StepIdInUseException(stepId, Path.of("/w/operation.lifecycle.csv"));
		final var remote = new java.rmi.ServerException("remote call failed", refusal);

		final var single = client.metadataSliceStartFailure(refusal, 0, 1);
		final var distributed = client.metadataSliceStartFailure(remote, 1, 2);
		final var other = client.metadataSliceStartFailure(new IllegalStateException("boom"), 1, 2);

		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, single.category());
		assertEquals(refusal.getMessage(), single.getMessage());
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, distributed.category());
		// Another node's refusal reads as the Entry Node's node message: its file goes to the log.
		assertEquals("engine 2 of 2: step id \"" + stepId + "\" is already used in that node's log directory; "
						+ "see the entry's log", distributed.getMessage());
		assertInstanceOf(IntegrityTerminalException.class, other);
		assertEquals(IntegrityTerminalException.Category.EXECUTION, other.category());
		assertEquals("failed to start metadata-mode step slice 1", other.getMessage());
	}

	/** A node whose file of {@code loggerName} exists with {@code size} bytes; its other files are missing. */
	private FileManager node(final String loggerName, final String file, final long size) throws IOException {
		final FileManager node = mock(FileManager.class);
		when(node.logFileName(anyString(), eq(stepId))).thenAnswer(call -> "/w/other-" + call.getArgument(0));
		when(node.logFileName(loggerName, stepId)).thenReturn(file);
		when(node.fileSize(anyString())).thenReturn(0L);
		when(node.fileSize(file)).thenReturn(size);
		when(node.readFromFile(anyString(), anyLong())).thenAnswer(call -> {
			throw new NoSuchFileException(call.getArgument(0));
		});
		if (size == 0) {
			when(node.readFromFile(eq(file), anyLong())).thenThrow(new EOFException());
		}
		return node;
	}

	private StepIdInUseException refused(final TestLoadStepClient client) {
		try {
			final var thrown = assertThrows(RuntimeException.class, client::start);
			final var failure = assertInstanceOf(StepIdInUseException.class, RunTerminalException.find(thrown),
							thrown.toString());
			assertEquals(stepId, failure.stepId());
			assertTrue(failure.getMessage().startsWith(
							"step id \"" + stepId + "\" is already used in this log directory ("), failure.getMessage());
			assertTrue(failure.getMessage().endsWith(" exists); give each step its own load.step.id"),
							failure.getMessage());
			assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
			return failure;
		} finally {
			closeQuietly(client);
		}
	}

	/** The step folder's result files, without its logs. */
	private List<String> resultFiles() throws IOException {
		if (!Files.isDirectory(stepDir)) {
			return List.of();
		}
		try (Stream<Path> files = Files.list(stepDir)) {
			return files.map(file -> file.getFileName().toString())
							.filter(file -> file.endsWith(".csv") || file.endsWith(".json"))
							.sorted()
							.toList();
		}
	}

	private static void closeQuietly(final TestLoadStepClient client) {
		try {
			client.close();
		} catch (final Exception ignored) {
			// This fixture has no real slice, so the close of a started step fails at collection.
		}
	}

	private Config config(final String opType) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-op-type", opType);
		config.val("output-metrics-timing-persist", false);
		config.val("load-step-node-addrs", List.of());
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		return config;
	}

	@SuppressWarnings("unchecked")
	private TestLoadStepClient client(final Config config) throws Exception {
		factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(mock(LoadStep.class));
		return new TestLoadStepClient(config, List.of(factory), null, mock(MetricsManager.class));
	}

	private Path canonical(final String loggerName) throws IOException {
		final Path path = Path.of(FileManager.INSTANCE.logFileName(loggerName, stepId));
		stepDir = path.toAbsolutePath().getParent();
		return path;
	}

	private Path nodeSource(final String loggerName, final int index) throws IOException {
		return CsvLoggerArtifactAggregator.nodeSourcePath(canonical(loggerName), index);
	}

	private static Path earlierFile(final Path path) throws IOException {
		Files.createDirectories(path.toAbsolutePath().getParent());
		Files.writeString(path, "earlier");
		return path;
	}
}
