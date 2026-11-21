package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.deletion.DeleteArtifacts;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class SliceLifecycleCloseTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private final MetricsManager metricsMgr = mock(MetricsManager.class);
	private final List<AutoCloseable> toClose = new ArrayList<>();
	private Path stepDir;
	private String originalRmiHostname;

	@AfterEach
	void tearDown() throws Exception {
		Thread.interrupted();
		JvmExitTestSupport.reset();
		for (int i = toClose.size() - 1; i >= 0; i--) {
			try {
				toClose.get(i).close();
			} catch (final Exception e) {
				// A failed close here is the case under test; the step is left to the next test's reset.
			}
		}
		ServiceUtil.shutdown();
		if (originalRmiHostname == null) {
			System.clearProperty(RMI_HOSTNAME);
		} else {
			System.setProperty(RMI_HOSTNAME, originalRmiHostname);
		}
		if (stepDir != null) {
			StepIdReuseAtStartTest.removeFolder(stepDir);
		}
	}

	@Test
	void aRetriedCloseAfterAStoppedStartStillRefusesAPartialDelete(@TempDir final Path temp) throws Exception {
		final String stepId = "retried-close-del-" + UUID.randomUUID();
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection, "bucket,key,size,version_id\na,a,1,\nb,b,1,\nc,c,1,\nd,d,1,\n");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 4, 4, 4).publish(selection);

		final Path localMetrics = Path.of(FileManager.INSTANCE.logFileName(
						Loggers.DELETE_METRICS_TOTAL.getName(), stepId)).toAbsolutePath();
		stepDir = localMetrics.getParent();

		final int port = freePort();
		final String nodeAddr = "127.0.0.1:" + port;
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");

		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenReturn(1L);
		when(entry.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(entry.deleteObjectLifecycle()).thenReturn(
						new com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot(
										2, 2, 2, 0, 0, 0, 0, 1, 0, 0, 0, true));
		doAnswer(inv -> {
			writeLocalDeleteArtifacts(stepDir);
			return null;
		}).when(entry).close();

		final Thread starter = Thread.currentThread();
		final LoadStep worker = mock(LoadStep.class);
		when(worker.runId()).thenReturn(1L);
		when(worker.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(worker.deleteObjectLifecycle()).thenReturn(
						com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot.empty());
		doAnswer(inv -> {
			starter.interrupt();
			throw new IllegalStateException("stopped during worker start");
		}).when(worker).start();

		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		final AtomicInteger created = new AtomicInteger();
		when(factory.createLocal(any(), anyList(), anyList(), any()))
						.thenAnswer(inv -> created.getAndIncrement() == 0 ? entry : worker);

		final LoadStepManagerServiceImpl node = new LoadStepManagerServiceImpl(port, List.of(factory), metricsMgr);
		node.start();
		toClose.add(node);

		final Config config = TestConfigBuilder.config();
		config.val("run-id", 1L);
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-step-limit-time", 0);
		config.val("storage-driver-type", "dummy-mock");
		config.val("load-step-node-addrs", List.of(nodeAddr));
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		config.val("item-input-file", selection.toString());
		config.val("load-op-limit-count", 4L);

		final FileManager workerFiles = mock(FileManager.class);
		when(workerFiles.logFileName(anyString(), eq(stepId))).thenAnswer(inv -> "/w/" + inv.getArgument(0));
		when(workerFiles.fileSize(anyString())).thenReturn(0L);
		when(workerFiles.readFromFile(anyString(), anyLong())).thenThrow(new EOFException());

		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(
						factory, new com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension<>()), null, metricsMgr) {
			@Override
			protected boolean supportsStandaloneDeleteArtifactAggregation() {
				return true;
			}
		};
		client.withNodeFileManagers(Map.of(nodeAddr, workerFiles));
		toClose.add(client);

		// Start stops after slice 0 (entry) started, while slice 1 (worker) is starting
		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		Thread.interrupted();

		// First close MUST fail with IntegrityTerminalException ("DELETE frozen selection count does not match terminal evidence")
		assertThrows(IntegrityTerminalException.class, client::close);
		assertFalse(client.isClosed(), "client must remain unclosed after failed close");

		// Second close (what exit hook DaemonBase.closeAll() or explicit retry runs)
		Throwable secondFailure = null;
		try {
			client.close();
		} catch (final Throwable t) {
			secondFailure = t;
		}
		assertInstanceOf(IntegrityTerminalException.class, secondFailure,
						"second close succeeded cleanly (isClosed=" + client.isClosed()
										+ ") because startedSliceCount was zeroed before DeleteArtifactAggregator failed!");
	}

	@Test
	void theExitHookCloseAfterAFailedRunCloseStillRefusesAPartialDelete(@TempDir final Path temp) throws Exception {
		final String stepId = "hook-close-del-" + UUID.randomUUID();
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection, "bucket,key,size,version_id\na,a,1,\nb,b,1,\nc,c,1,\nd,d,1,\n");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 4, 4, 4).publish(selection);

		final Path localMetrics = Path.of(FileManager.INSTANCE.logFileName(
						Loggers.DELETE_METRICS_TOTAL.getName(), stepId)).toAbsolutePath();
		stepDir = localMetrics.getParent();

		final int port = freePort();
		final String nodeAddr = "127.0.0.1:" + port;
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");

		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenReturn(1L);
		when(entry.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(entry.deleteObjectLifecycle()).thenReturn(
						new com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot(
										2, 2, 2, 0, 0, 0, 0, 1, 0, 0, 0, true));
		doAnswer(inv -> {
			writeLocalDeleteArtifacts(stepDir);
			return null;
		}).when(entry).close();

		final Thread starter = Thread.currentThread();
		final LoadStep worker = mock(LoadStep.class);
		when(worker.runId()).thenReturn(1L);
		when(worker.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(worker.deleteObjectLifecycle()).thenReturn(
						com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot.empty());
		doAnswer(inv -> {
			starter.interrupt();
			throw new IllegalStateException("stopped during worker start");
		}).when(worker).start();

		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		final AtomicInteger created = new AtomicInteger();
		when(factory.createLocal(any(), anyList(), anyList(), any()))
						.thenAnswer(inv -> created.getAndIncrement() == 0 ? entry : worker);

		final LoadStepManagerServiceImpl node = new LoadStepManagerServiceImpl(port, List.of(factory), metricsMgr);
		node.start();
		toClose.add(node);

		final Config config = TestConfigBuilder.config();
		config.val("run-id", 1L);
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-step-limit-time", 0);
		config.val("storage-driver-type", "dummy-mock");
		config.val("load-step-node-addrs", List.of(nodeAddr));
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		config.val("item-input-file", selection.toString());
		config.val("load-op-limit-count", 4L);

		final FileManager workerFiles = mock(FileManager.class);
		when(workerFiles.logFileName(anyString(), eq(stepId))).thenAnswer(inv -> "/w/" + inv.getArgument(0));
		when(workerFiles.fileSize(anyString())).thenReturn(0L);
		when(workerFiles.readFromFile(anyString(), anyLong())).thenThrow(new EOFException());

		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(
						factory, new com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension<>()), null, metricsMgr) {
			@Override
			protected boolean supportsStandaloneDeleteArtifactAggregation() {
				return true;
			}
		};
		client.withNodeFileManagers(Map.of(nodeAddr, workerFiles));
		toClose.add(client);

		// Scenario thread runs client.run(): start stops after slice 0; run()'s finally closes client,
		// fails DeleteArtifactAggregator silently behind InterruptedException, leaves client unclosed!
		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::run));
		Thread.interrupted();
		assertFalse(client.isClosed(), "after run() close failed, client must still be unclosed");

		// Exit hook runs DaemonBase.closeAll():
		JvmExit.markBegun();
		DaemonBase.closeAll();
		assertFalse(client.isClosed(), "DaemonBase.closeAll() falsely closed partial DELETE cleanly!");
	}

	@Test
	void anAwaitEndingWhileTheExitHookClosesSlicesThrowsInterrupted() throws Exception {
		final String stepId = "mid-close-await-" + UUID.randomUUID();
		final int port1 = freePort();
		final int port2 = freePort();
		final String w1 = "127.0.0.1:" + port1;
		final String w2 = "127.0.0.1:" + port2;
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");

		final LoadStep entry = mock(LoadStep.class);
		when(entry.await(anyLong(), any())).thenReturn(true);
		final LoadStep w1Local = mock(LoadStep.class);
		when(w1Local.await(anyLong(), any())).thenReturn(false);
		final LoadStep w2Local = mock(LoadStep.class);
		when(w2Local.await(anyLong(), any())).thenReturn(true);

		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory1 = mock(LoadStepFactory.class);
		when(factory1.id()).thenReturn("test-load-step");
		when(factory1.createLocal(any(), anyList(), anyList(), any())).thenReturn(w1Local);
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory2 = mock(LoadStepFactory.class);
		when(factory2.id()).thenReturn("test-load-step");
		when(factory2.createLocal(any(), anyList(), anyList(), any())).thenReturn(w2Local);
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> entryFactory = mock(LoadStepFactory.class);
		when(entryFactory.id()).thenReturn("test-load-step");
		when(entryFactory.createLocal(any(), anyList(), anyList(), any())).thenReturn(entry);

		final LoadStepManagerServiceImpl node1 = new LoadStepManagerServiceImpl(port1, List.of(factory1), metricsMgr);
		node1.start();
		toClose.add(node1);
		final LoadStepManagerServiceImpl node2 = new LoadStepManagerServiceImpl(port2, List.of(factory2), metricsMgr);
		node2.start();
		toClose.add(node2);

		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-type", "noop");
		config.val("load-step-node-addrs", List.of(w1, w2));
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");

		final FileManager f1 = mock(FileManager.class);
		final FileManager f2 = mock(FileManager.class);
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(entryFactory), null, metricsMgr);
		client.withNodeFileManagers(Map.of(w1, f1, w2, f2));
		toClose.add(client);

		final CountDownLatch w1AwaitPaused = new CountDownLatch(1);
		final CountDownLatch releaseW1Await = new CountDownLatch(1);
		final CountDownLatch w2CloseEntered = new CountDownLatch(1);
		final CountDownLatch releaseW2Close = new CountDownLatch(1);
		final AtomicInteger w1Awaits = new AtomicInteger();
		final AtomicReference<Throwable> w1AwaitErr = new AtomicReference<>();

		try (MockedStatic<LoadStepSliceUtil> util = mockStatic(LoadStepSliceUtil.class, CALLS_REAL_METHODS)) {
			util.when(() -> LoadStepSliceUtil.resolveRemote(any(), anyList(), anyString(), anyString()))
							.thenAnswer(inv -> {
								final LoadStepService stub = (LoadStepService) inv.callRealMethod();
								final String addr = inv.getArgument(3);
								return (LoadStepService) Proxy.newProxyInstance(
												LoadStepService.class.getClassLoader(),
												new Class<?>[]{LoadStepService.class
								},
												(proxy, method, args) -> {
													final String name = method.getName();
													if (method.getDeclaringClass() == Object.class) {
														return switch (name) {
														case "equals" -> proxy == args[0];
														case "hashCode" -> System.identityHashCode(proxy);
														default -> "proxy@" + addr;
														};
													}
													if (w1.equals(addr) && "await".equals(name) && w1Awaits.incrementAndGet() == 1) {
														w1AwaitPaused.countDown();
														releaseW1Await.await(15, TimeUnit.SECONDS);
													}
													if (w2.equals(addr) && "close".equals(name)) {
														w2CloseEntered.countDown();
														releaseW2Close.await(15, TimeUnit.SECONDS);
													}
													try {
														return method.invoke(stub, args);
													} catch (final InvocationTargetException e) {
														if (w1.equals(addr) && "await".equals(name)) {
															w1AwaitErr.compareAndSet(null, e.getCause());
														}
														throw e.getCause();
													}
												});
							});
			client.start();
		}

		final CompletableFuture<Object> awaited = new CompletableFuture<>();
		Thread.ofPlatform().daemon(true).name("scenario").start(() -> {
			try {
				awaited.complete(client.await(1, TimeUnit.MINUTES));
			} catch (final Throwable t) {
				awaited.complete(t);
			}
		});
		assertTrue(w1AwaitPaused.await(15, TimeUnit.SECONDS));

		JvmExit.markBegun();
		final CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
			try {
				client.close();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		});
		// Wait until exit hook has ALREADY closed+unexported w1, and is now paused inside w2.close()
		// (slicesClosing==true, stepSlices NOT yet cleared, stopCloseDone==false, isClosed()==false)
		assertTrue(w2CloseEntered.await(15, TimeUnit.SECONDS));
		releaseW1Await.countDown();
		// Give scenario thread time to hit NoSuchObjectException on w1, exit awaitSlices, evaluate closedAtJvmExit,
		// and queue on stopCloseLock in stopAfterAwait -> doStop()
		Thread.sleep(300);
		releaseW2Close.countDown();

		closing.get(15, TimeUnit.SECONDS);
		final Object outcome = awaited.get(15, TimeUnit.SECONDS);
		assertInstanceOf(NoSuchObjectException.class, w1AwaitErr.get());
		assertInstanceOf(InterruptedException.class, outcome,
						"await() returned " + outcome + " instead of InterruptedException during mid-doClose()!");
	}

	private static void writeLocalDeleteArtifacts(final Path dir) throws IOException {
		Files.createDirectories(dir);
		Files.writeString(dir.resolve(DeleteArtifacts.METRICS_FILE_NAME),
						String.join(",", DeleteArtifacts.METRICS_HEADER) + "\n"
										+ "1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1,0,1,0,0,2,2,1,1,0,0,1,2,1,0,true\n");
		Files.writeString(dir.resolve(DeleteArtifacts.REQUESTS_FILE_NAME),
						String.join(",", DeleteArtifacts.REQUESTS_HEADER) + "\n"
										+ "1,req-0,req-0,2,partial,local,1,2,1\n");
		Files.writeString(dir.resolve(DeleteArtifacts.OBJECTS_FILE_NAME),
						String.join(",", DeleteArtifacts.OBJECTS_HEADER) + "\n"
										+ "1,req-0,t-0-a,0,a,a,1,,accepted,none,\n"
										+ "1,req-0,t-0-b,1,b,b,1,,failed,operational,error\n");
		Files.writeString(dir.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME),
						"bucket,key,size,version_id\nb,b,1,\n");
		Files.writeString(dir.resolve(DeleteArtifacts.VERIFICATION_FILE_NAME),
						String.join(",", DeleteArtifacts.VERIFICATION_HEADER) + "\n"
										+ "1,t-0-a,0,a,a,1,,accepted,false,disabled,false,disabled,false,false,false\n"
										+ "1,t-0-b,1,b,b,1,,failed,false,disabled,false,disabled,false,false,true\n");
	}

	private static int freePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}
}
