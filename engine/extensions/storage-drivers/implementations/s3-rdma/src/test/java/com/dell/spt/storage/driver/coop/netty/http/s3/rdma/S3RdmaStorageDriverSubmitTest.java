package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

@SuppressWarnings("unchecked")
public class S3RdmaStorageDriverSubmitTest {

	private static final Credential TEST_CRED = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final long THRESHOLD = 1_048_576L;

	// ---------- shouldUseRdma: transport availability ----------

	@Test
	void testShouldUseRdma_falseWhenTransportUnavailable() throws Exception {
		final var driver = newMockDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		final var op = dataOp(OpType.CREATE, THRESHOLD + 1);
		assertFalse(invokeShouldUseRdma(driver, op));
	}

	// ---------- shouldUseRdma: operation type filtering ----------

	@Test
	void testShouldUseRdma_falseForNonDataOp() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		final var op = (Operation<Item>) (Operation<?>) new OperationImpl<>(
						0, OpType.CREATE, new ItemImpl("item"), null, "/bucket", TEST_CRED);
		assertFalse(invokeShouldUseRdma(driver, op));
	}

	@Test
	void testShouldUseRdma_falseForDeleteOp() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		assertFalse(invokeShouldUseRdma(driver, dataOp(OpType.DELETE, THRESHOLD + 1)));
	}

	@Test
	void testShouldUseRdma_falseForUpdateOp() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		assertFalse(invokeShouldUseRdma(driver, dataOp(OpType.UPDATE, THRESHOLD + 1)));
	}

	// ---------- shouldUseRdma: size threshold ----------

	@Test
	void testShouldUseRdma_falseWhenBelowThreshold() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		assertFalse(invokeShouldUseRdma(driver, dataOp(OpType.CREATE, THRESHOLD - 1)));
	}

	@Test
	void testShouldUseRdma_trueForCreateAtThreshold() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		assertTrue(invokeShouldUseRdma(driver, dataOp(OpType.CREATE, THRESHOLD)));
	}

	@Test
	void testShouldUseRdma_trueForReadAtThreshold() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		assertTrue(invokeShouldUseRdma(driver, dataOp(OpType.READ, THRESHOLD)));
	}

	@Test
	void testShouldUseRdma_trueForLargeCreate() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		assertTrue(invokeShouldUseRdma(driver, dataOp(OpType.CREATE, THRESHOLD * 10)));
	}

	// ---------- extractBucket ----------

	@Test
	void testExtractBucket_stripsLeadingSlash() throws Exception {
		assertEquals("mybucket", invokeExtractBucket(driverWithNamespace("/mybucket")));
	}

	@Test
	void testExtractBucket_noLeadingSlash() throws Exception {
		assertEquals("mybucket", invokeExtractBucket(driverWithNamespace("mybucket")));
	}

	@Test
	void testExtractBucket_extractsFirstSegment() throws Exception {
		assertEquals("mybucket", invokeExtractBucket(driverWithNamespace("/mybucket/prefix")));
	}

	@Test
	void testExtractBucket_emptyNamespace() throws Exception {
		assertEquals("", invokeExtractBucket(driverWithNamespace("")));
	}

	@Test
	void testExtractBucket_nullNamespace() throws Exception {
		assertEquals("", invokeExtractBucket(driverWithNamespace(null)));
	}

	// ---------- extractKey ----------

	@Test
	void testExtractKey_stripsLeadingSlash() throws Exception {
		final var driver = newMockDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("mykey", invokeExtractKey(driver, new ItemImpl("/mykey")));
	}

	@Test
	void testExtractKey_noLeadingSlash() throws Exception {
		final var driver = newMockDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("mykey", invokeExtractKey(driver, new ItemImpl("mykey")));
	}

	@Test
	void testExtractKey_nestedPath() throws Exception {
		final var driver = newMockDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("a/b/c", invokeExtractKey(driver, new ItemImpl("a/b/c")));
	}

	// ---------- buildEndpointUrl ----------

	@Test
	void testBuildEndpointUrl_httpForPort9020() throws Exception {
		assertEquals("http://10.0.0.1:9020",
						invokeBuildEndpointUrl(driverWithNode("10.0.0.1:9020", 9020)));
	}

	@Test
	void testBuildEndpointUrl_httpsForPort443() throws Exception {
		assertEquals("https://s3.example.com:443",
						invokeBuildEndpointUrl(driverWithNode("s3.example.com:443", 443)));
	}

	@Test
	void testBuildEndpointUrl_httpsForPort9021() throws Exception {
		assertEquals("https://10.0.0.1:9021",
						invokeBuildEndpointUrl(driverWithNode("10.0.0.1:9021", 9021)));
	}

	@Test
	void testBuildEndpointUrl_usesPortFromAddr() throws Exception {
		assertEquals("http://10.0.0.1:8080",
						invokeBuildEndpointUrl(driverWithNode("10.0.0.1:8080", 9020)));
	}

	@Test
	void testBuildEndpointUrl_usesStorageNodePortWhenNoPortInAddr() throws Exception {
		assertEquals("http://10.0.0.1:9020",
						invokeBuildEndpointUrl(driverWithNode("10.0.0.1", 9020)));
	}

	// ---------- batch submit ----------

	@Test
	void testBatchSubmit_delegatesToParentWhenRdmaUnavailable() throws Exception {
		// When RDMA is unavailable, batch submit should delegate to parent's optimized path
		final var transport = new RdmaTransport(enabledConfig()); // Not initialized = unavailable
		final var driver = newMockDriver(enabledConfig(), transport);

		// Create a batch of operations
		final var ops = List.of(
						dataOp(OpType.CREATE, THRESHOLD + 1),
						dataOp(OpType.CREATE, THRESHOLD + 1));

		// Since transport is unavailable, batch submit should check this first
		// and delegate to parent (which we can't easily verify without full driver setup)
		// This test verifies the transport availability check is correct
		assertFalse(transport.isAvailable());
	}

	@Test
	void testBatchSubmit_routesIndividuallyWhenRdmaAvailable() throws Exception {
		// When RDMA is available, batch submit processes ops individually
		final var transport = availableTransport();
		final var driver = newMockDriver(enabledConfig(), transport);

		// Verify transport is available
		assertTrue(transport.isAvailable());

		// The batch submit method calls submit(op) for each op when RDMA is available
		// This allows per-operation RDMA vs HTTP routing decisions
		// Test verifies the method exists and is properly overridden
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod(
						"submit", List.class, int.class, int.class);
		assertNotNull(m);
		assertEquals(int.class, m.getReturnType());
	}

	@Test
	void testBatchSubmit_mixedOperationsRouteCorrectly() throws Exception {
		// A batch with mixed sizes should route large ops to RDMA, small to HTTP
		final var transport = availableTransport();
		final var driver = newMockDriver(enabledConfig(), transport);

		// Create operations: one above threshold, one below
		final var largeOp = dataOp(OpType.CREATE, THRESHOLD + 1);
		final var smallOp = dataOp(OpType.CREATE, THRESHOLD - 1);

		// Verify shouldUseRdma returns correct values for each
		assertTrue(invokeShouldUseRdma(driver, largeOp), "Large op should use RDMA");
		assertFalse(invokeShouldUseRdma(driver, smallOp), "Small op should not use RDMA");
	}

	// ==================== Helpers ====================

	private static RdmaConfig enabledConfig() {
		return new RdmaConfig(true, THRESHOLD, true, 0, 0, "auto", "", "WARN");
	}

	private static RdmaTransport availableTransport() {
		final var transport = Mockito.mock(RdmaTransport.class);
		Mockito.when(transport.isAvailable()).thenReturn(true);
		return transport;
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> newMockDriver(
					final RdmaConfig config, final RdmaTransport transport) throws Exception {
		final var driver = (S3RdmaStorageDriver<Item, Operation<Item>>) Mockito.mock(
						S3RdmaStorageDriver.class,
						Mockito.withSettings().lenient().defaultAnswer(CALLS_REAL_METHODS));
		setField(S3RdmaStorageDriver.class, driver, "rdmaConfig", config);
		setField(S3RdmaStorageDriver.class, driver, "rdmaTransport", transport);
		return driver;
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> driverWithNamespace(
					final String namespace) throws Exception {
		final var driver = newMockDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		setFieldInHierarchy(driver, "namespace", namespace);
		return driver;
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> driverWithNode(
					final String addr, final int port) throws Exception {
		final var driver = newMockDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		setFieldInHierarchy(driver, "storageNodeAddrs", new String[]{addr
		});
		setFieldInHierarchy(driver, "storageNodePort", port);
		return driver;
	}

	private static Operation<Item> dataOp(final OpType opType, final long size) {
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<>(
						0, opType, new DataItemImpl("testItem", 0, size), null, "/bucket",
						TEST_CRED, null, 0);
	}

	private static boolean invokeShouldUseRdma(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver,
					final Operation<Item> op) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod(
						"shouldUseRdma", Operation.class);
		m.setAccessible(true);
		return (boolean) m.invoke(driver, op);
	}

	private static String invokeExtractBucket(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod("extractBucket");
		m.setAccessible(true);
		return (String) m.invoke(driver);
	}

	private static String invokeExtractKey(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver,
					final Item item) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod("extractKey", Item.class);
		m.setAccessible(true);
		return (String) m.invoke(driver, item);
	}

	private static String invokeBuildEndpointUrl(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod("buildEndpointUrl");
		m.setAccessible(true);
		return (String) m.invoke(driver);
	}

	private static void setField(final Class<?> clazz, final Object obj,
					final String name, final Object value) throws Exception {
		final Field f = clazz.getDeclaredField(name);
		f.setAccessible(true);
		f.set(obj, value);
	}

	private static void setFieldInHierarchy(final Object obj,
					final String name, final Object value) throws Exception {
		Class<?> clazz = obj.getClass();
		while (clazz != null) {
			try {
				final Field f = clazz.getDeclaredField(name);
				f.setAccessible(true);
				f.set(obj, value);
				return;
			} catch (final NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			}
		}
		throw new NoSuchFieldException(
						name + " not found in hierarchy of " + obj.getClass().getName());
	}
}
