package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
public class S3RdmaStorageDriverSubmitTest {

	private static final Credential TEST_CRED = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final long THRESHOLD = 1_048_576L;

	@AfterEach
	void closeDrivers() throws Exception {
		S3RdmaStorageDriverTestSupport.closeCreatedDrivers();
	}

	// ---------- shouldUseRdma: transport availability ----------

	@Test
	void testShouldUseRdma_falseWhenTransportUnavailable() throws Exception {
		final var driver = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		final var op = dataOp(OpType.CREATE, THRESHOLD + 1);
		assertFalse(invokeShouldUseRdma(driver, op));
	}

	// ---------- shouldUseRdma: operation type filtering ----------

	@Test
	void testShouldUseRdma_falseForNonDataOp() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		final var op = (Operation<Item>) (Operation<?>) new OperationImpl<>(
						0, OpType.CREATE, new ItemImpl("item"), null, "/bucket", TEST_CRED);
		assertFalse(invokeShouldUseRdma(driver, op));
	}

	@Test
	void testShouldUseRdma_falseForDeleteOp() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertFalse(invokeShouldUseRdma(driver, dataOp(OpType.DELETE, THRESHOLD + 1)));
	}

	@Test
	void testShouldUseRdma_falseForUpdateOp() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertFalse(invokeShouldUseRdma(driver, dataOp(OpType.UPDATE, THRESHOLD + 1)));
	}

	// ---------- shouldUseRdma: size threshold ----------

	@Test
	void testShouldUseRdma_falseWhenBelowThreshold() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertFalse(invokeShouldUseRdma(driver, dataOp(OpType.CREATE, THRESHOLD - 1)));
	}

	@Test
	void testShouldUseRdma_trueForCreateAtThreshold() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertTrue(invokeShouldUseRdma(driver, dataOp(OpType.CREATE, THRESHOLD)));
	}

	@Test
	void testShouldUseRdma_trueForReadAtThreshold() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertTrue(invokeShouldUseRdma(driver, dataOp(OpType.READ, THRESHOLD)));
	}

	@Test
	void testShouldUseRdma_trueForLargeCreate() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
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
		final var driver = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("mykey", invokeExtractKey(driver, new ItemImpl("/mykey")));
	}

	@Test
	void testExtractKey_noLeadingSlash() throws Exception {
		final var driver = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("mykey", invokeExtractKey(driver, new ItemImpl("mykey")));
	}

	@Test
	void testExtractKey_nestedPath() throws Exception {
		final var driver = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("a/b/c", invokeExtractKey(driver, new ItemImpl("a/b/c")));
	}

	// ---------- buildEndpointAddrs ----------

	@Test
	void testBuildEndpointAddrs_httpForPort9020() throws Exception {
		assertEquals("http://10.0.0.1:9020",
						invokeBuildEndpointAddrs(driverWithNode("10.0.0.1:9020", 9020)));
	}

	@Test
	void testBuildEndpointAddrs_httpsForPort443() throws Exception {
		assertEquals("https://s3.example.com:443",
						invokeBuildEndpointAddrs(driverWithNode("s3.example.com:443", 443)));
	}

	@Test
	void testBuildEndpointAddrs_httpsForPort9021() throws Exception {
		assertEquals("https://10.0.0.1:9021",
						invokeBuildEndpointAddrs(driverWithNode("10.0.0.1:9021", 9021)));
	}

	@Test
	void testBuildEndpointAddrs_usesPortFromAddr() throws Exception {
		assertEquals("http://10.0.0.1:8080",
						invokeBuildEndpointAddrs(driverWithNode("10.0.0.1:8080", 9020)));
	}

	@Test
	void testBuildEndpointAddrs_usesStorageNodePortWhenNoPortInAddr() throws Exception {
		assertEquals("http://10.0.0.1:9020",
						invokeBuildEndpointAddrs(driverWithNode("10.0.0.1", 9020)));
	}

	@Test
	void testBuildEndpointAddrs_multipleNodes() throws Exception {
		final var config = enabledConfig();
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(
						config,
						new RdmaTransport(config),
						"/bucket",
						List.of("10.0.0.1:9020", "10.0.0.2:9020", "10.0.0.3:9020"),
						9020);
		assertEquals("http://10.0.0.1:9020,http://10.0.0.2:9020,http://10.0.0.3:9020",
						invokeBuildEndpointAddrs(driver));
	}

	// ---------- buildEndpointAddrs: edge cases ----------

	@Test
	void testBuildEndpointAddrs_singleNode() throws Exception {
		assertEquals("http://localhost:9020",
						invokeBuildEndpointAddrs(driverWithNode("localhost:9020", 9020)));
	}

	@Test
	void testBuildEndpointAddrs_httpsForPort443WithoutPortInAddr() throws Exception {
		// When addr has no port and storageNodePort=443, scheme should be https
		assertEquals("https://s3.example.com:443",
						invokeBuildEndpointAddrs(driverWithNode("s3.example.com", 443)));
	}

	// ---------- extractBucket: edge cases ----------

	@Test
	void testExtractBucket_slashOnly() throws Exception {
		// Namespace "/" → after stripping leading slash, empty → return ""
		assertEquals("", invokeExtractBucket(driverWithNamespace("/")));
	}

	@Test
	void testExtractBucket_trailingSlash() throws Exception {
		// "/mybucket/" → strips leading slash → "mybucket/" → indexOf('/') at 8 → "mybucket"
		assertEquals("mybucket", invokeExtractBucket(driverWithNamespace("/mybucket/")));
	}

	// ---------- extractKey: edge cases ----------

	@Test
	void testExtractKey_leadingSlashWithNestedPath() throws Exception {
		final var driver = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("a/b/c", invokeExtractKey(driver, new ItemImpl("/a/b/c")));
	}

	@Test
	void testExtractKey_slashOnly() throws Exception {
		final var driver = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals("", invokeExtractKey(driver, new ItemImpl("/")));
	}

	// ---------- shouldUseRdma: zero threshold ----------

	@Test
	void testShouldUseRdma_zeroThresholdAcceptsAnySizeCreate() throws Exception {
		final var config = new RdmaConfig(true, 0L, true, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());
		assertTrue(invokeShouldUseRdma(driver, dataOp(OpType.CREATE, 1)));
	}

	@Test
	void testShouldUseRdma_zeroThresholdAcceptsZeroSizeCreate() throws Exception {
		final var config = new RdmaConfig(true, 0L, true, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());
		assertTrue(invokeShouldUseRdma(driver, dataOp(OpType.CREATE, 0)));
	}

	// ---------- shouldUseRdma: below-threshold warning ----------

	@Test
	void testShouldUseRdma_warnsBelowThresholdOnce() throws Exception {
		final var config = new RdmaConfig(true, THRESHOLD, false, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());

		// Before any sub-threshold op, the guard should be false
		assertFalse(getBelowThresholdWarned(driver), "belowThresholdWarned should start false");

		// First sub-threshold call should trip the guard
		final var smallOp = dataOp(OpType.CREATE, THRESHOLD - 1);
		assertFalse(invokeShouldUseRdma(driver, smallOp));
		assertTrue(getBelowThresholdWarned(driver), "belowThresholdWarned should be true after first sub-threshold op");

		// Second call — guard stays true (log-once behaviour)
		assertFalse(invokeShouldUseRdma(driver, smallOp));
		assertTrue(getBelowThresholdWarned(driver));
	}

	@Test
	void testShouldUseRdma_noWarningWhenAboveThreshold() throws Exception {
		final var config = new RdmaConfig(true, THRESHOLD, false, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());

		// Above-threshold op should not trip the guard
		final var largeOp = dataOp(OpType.CREATE, THRESHOLD + 1);
		assertTrue(invokeShouldUseRdma(driver, largeOp));
		assertFalse(getBelowThresholdWarned(driver), "belowThresholdWarned should remain false for above-threshold ops");
	}

	// ==================== Helpers ====================

	private static RdmaConfig enabledConfig() {
		return new RdmaConfig(true, THRESHOLD, true, "auto", "", "WARN");
	}

	private static RdmaTransport availableTransport() {
		final var transport = Mockito.mock(RdmaTransport.class);
		Mockito.when(transport.init(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
						.thenReturn(true);
		Mockito.when(transport.isAvailable()).thenReturn(true);
		return transport;
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> newDriver(
					final RdmaConfig config, final RdmaTransport transport) throws Exception {
		return S3RdmaStorageDriverTestSupport.newDriver(config, transport);
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> driverWithNamespace(
					final String namespace) throws Exception {
		final var config = enabledConfig();
		return S3RdmaStorageDriverTestSupport.newDriver(
						config, new RdmaTransport(config), namespace, List.of("127.0.0.1"), 9020);
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> driverWithNode(
					final String addr, final int port) throws Exception {
		final var config = enabledConfig();
		return S3RdmaStorageDriverTestSupport.newDriver(
						config, new RdmaTransport(config), "/bucket", List.of(addr), port);
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

	private static String invokeBuildEndpointAddrs(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod("buildEndpointAddrs");
		m.setAccessible(true);
		return (String) m.invoke(driver);
	}

	private static boolean getBelowThresholdWarned(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver) throws Exception {
		final Field f = S3RdmaStorageDriver.class.getDeclaredField("belowThresholdWarned");
		f.setAccessible(true);
		return ((java.util.concurrent.atomic.AtomicBoolean) f.get(driver)).get();
	}

}
