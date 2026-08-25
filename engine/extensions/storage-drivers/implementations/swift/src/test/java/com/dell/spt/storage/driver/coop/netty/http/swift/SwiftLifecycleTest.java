package com.dell.spt.storage.driver.coop.netty.http.swift;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SwiftLifecycleTest {
	private static final class TestSwiftDriver extends SwiftStorageDriver<DataItem, Operation<DataItem>> {
		private TestSwiftDriver() throws Exception {
			super(
							"swift-lifecycle-step",
							DataInput.instance(
											null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true),
							storageConfig(),
							false,
							4);
		}

		private boolean beginTransport(final Operation<DataItem> op) {
			return beginDispatch(op);
		}
	}

	@Test
	void compositeAdmissionClosureRecoversAllChildrenWithoutSubmissionSpin() throws Exception {
		try (final var driver = new TestSwiftDriver()) {
			driver.start();
			final var dataItem = new DataItemImpl("closed-dlo", 0, 2048);
			final CompositeDataOperation<DataItem> dloOp = new CompositeDataOperationImpl<>(
							hashCode(), OpType.CREATE, dataItem, null, "/container", null,
							null, 0, 1024);
			final Operation<DataItem> parent = dloOp;
			assertTrue(driver.operationLifecycle().driverQueued(parent));

			driver.closeAdmission();
			assertFalse(driver.submit(parent));

			assertEquals(OperationLifecycleState.UNATTEMPTED, parent.lifecycle().state(),
							"closed admission must reconcile the parent as well as its generated children");
			assertTrue(dloOp.subOperations().stream()
							.allMatch(child -> child.lifecycle().state() == OperationLifecycleState.UNATTEMPTED));
			assertEquals(3, driver.operationLifecycle().snapshot().unattempted());
		}
	}

	@Test
	void partialTransportKeepsSwiftParentInFlightAndRemainingSiblingRecoverable() throws Exception {
		try (final var driver = new TestSwiftDriver()) {
			driver.start();
			final var dataItem = new DataItemImpl("partial-dlo", 0, 2048);
			final CompositeDataOperation<DataItem> dloOp = new CompositeDataOperationImpl<>(
							hashCode(), OpType.CREATE, dataItem, null, "/container", null,
							null, 0, 1024);
			final Operation<DataItem> parent = dloOp;
			assertTrue(driver.operationLifecycle().driverQueued(parent));
			assertTrue(driver.submit(parent), "Swift expansion should claim and enqueue both segments");

			final var children = dloOp.subOperations();
			assertTrue(driver.beginTransport(children.get(0)));
			assertEquals(OperationLifecycleState.DISPATCHED, parent.lifecycle().state(),
							"the first segment transport must make its composite parent in flight");

			driver.closeAdmission();
			final var recovered = driver.recoverQueuedOperations();
			assertEquals(1, recovered.size());
			assertTrue(recovered.get(0) == children.get(1));
			assertEquals(2, driver.operationLifecycle().resolveOutstandingAsUnresolved(),
							"the transported child and its parent must both outlive the bound");

			assertEquals(OperationLifecycleState.UNRESOLVED, parent.lifecycle().state());
			assertEquals(OperationLifecycleState.UNRESOLVED, children.get(0).lifecycle().state());
			assertEquals(OperationLifecycleState.UNATTEMPTED, children.get(1).lifecycle().state());
			assertEquals(2, driver.operationLifecycle().snapshot().unresolved());
			assertEquals(1, driver.operationLifecycle().snapshot().unattempted());
		}
	}

	private static Config storageConfig() {
		final var storageConfig = mock(Config.class);
		final var driverConfig = mock(Config.class);
		final var limitConfig = mock(Config.class);
		final var authConfig = mock(Config.class);
		final var integrityConfig = mock(Config.class);
		final var integrityInputConfig = mock(Config.class);
		final var netConfig = mock(Config.class);
		final var sslConfig = mock(Config.class);
		final var nodeConfig = mock(Config.class);
		final var httpConfig = mock(Config.class);

		org.mockito.Mockito.when(storageConfig.configVal("driver")).thenReturn(driverConfig);
		org.mockito.Mockito.when(driverConfig.configVal("limit")).thenReturn(limitConfig);
		org.mockito.Mockito.when(limitConfig.intVal("concurrency")).thenReturn(0);
		org.mockito.Mockito.when(driverConfig.intVal("threads")).thenReturn(1);
		org.mockito.Mockito.when(storageConfig.stringVal("namespace")).thenReturn("container");
		org.mockito.Mockito.when(storageConfig.stringVal("driver-type")).thenReturn("swift");
		org.mockito.Mockito.when(storageConfig.configVal("auth")).thenReturn(authConfig);
		org.mockito.Mockito.when(authConfig.stringVal("uid")).thenReturn("user");
		org.mockito.Mockito.when(authConfig.stringVal("secret")).thenReturn("secret");
		org.mockito.Mockito.when(authConfig.stringVal("token")).thenReturn(null);
		org.mockito.Mockito.when(storageConfig.configVal("integrity")).thenReturn(integrityConfig);
		org.mockito.Mockito.when(integrityConfig.stringVal("mode")).thenReturn("none");
		org.mockito.Mockito.when(integrityConfig.stringVal("algorithm")).thenReturn("sha256");
		org.mockito.Mockito.when(integrityConfig.configVal("input")).thenReturn(integrityInputConfig);
		org.mockito.Mockito.when(integrityInputConfig.stringVal("provenance")).thenReturn("none");
		org.mockito.Mockito.when(integrityInputConfig.stringVal("expectedProducerId")).thenReturn("");
		org.mockito.Mockito.when(storageConfig.intVal("driver-limit-queue-input")).thenReturn(16);
		org.mockito.Mockito.when(storageConfig.intVal("driver-limit-multipart-objects")).thenReturn(0);
		org.mockito.Mockito.when(storageConfig.intVal("driver-limit-multipart-parts")).thenReturn(0);
		org.mockito.Mockito.when(storageConfig.intVal("driver-threads")).thenReturn(1);

		org.mockito.Mockito.when(storageConfig.configVal("net")).thenReturn(netConfig);
		org.mockito.Mockito.when(netConfig.configVal("ssl")).thenReturn(sslConfig);
		org.mockito.Mockito.when(sslConfig.boolVal("enabled")).thenReturn(false);
		org.mockito.Mockito.when(netConfig.intVal("timeoutMilliSec")).thenReturn(1000);
		org.mockito.Mockito.when(netConfig.configVal("node")).thenReturn(nodeConfig);
		org.mockito.Mockito.when(nodeConfig.intVal("port")).thenReturn(9024);
		org.mockito.Mockito.when(nodeConfig.intVal("connAttemptsLimit")).thenReturn(0);
		org.mockito.Mockito.when(nodeConfig.<String> listVal("addrs")).thenReturn(List.of("127.0.0.1"));
		org.mockito.Mockito.when(netConfig.stringVal("transport")).thenReturn("nio");
		org.mockito.Mockito.when(netConfig.intVal("ioRatio")).thenReturn(50);
		org.mockito.Mockito.when(netConfig.intVal("writeSpinCount")).thenReturn(1);
		org.mockito.Mockito.when(netConfig.boolVal("keepAlive")).thenReturn(true);
		org.mockito.Mockito.when(netConfig.intVal("linger")).thenReturn(0);
		org.mockito.Mockito.when(netConfig.boolVal("reuseAddr")).thenReturn(true);
		org.mockito.Mockito.when(netConfig.boolVal("tcpNoDelay")).thenReturn(true);

		org.mockito.Mockito.when(storageConfig.configVal("net-http")).thenReturn(httpConfig);
		org.mockito.Mockito.when(httpConfig.<String> mapVal("headers")).thenReturn(Map.of());
		org.mockito.Mockito.when(httpConfig.boolVal("read-metadata-only")).thenReturn(false);
		org.mockito.Mockito.when(httpConfig.intVal("max-chunk-size")).thenReturn(0);
		org.mockito.Mockito.when(httpConfig.<String> mapVal("uri-args")).thenReturn(Map.of());
		org.mockito.Mockito.when(httpConfig.boolVal("versioning")).thenReturn(false);
		return storageConfig;
	}
}
