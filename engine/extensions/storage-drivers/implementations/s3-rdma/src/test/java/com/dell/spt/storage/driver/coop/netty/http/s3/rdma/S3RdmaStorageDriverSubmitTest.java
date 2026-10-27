package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.S3RdmaStorageDriver.PathSelection;
import com.github.akurilov.commons.collection.Range;
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

	// ---------- selectPath: transport availability ----------

	@Test
	void testSelectPath_falseWhenTransportUnavailable() throws Exception {
		final var driver = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		final var op = dataOp(OpType.CREATE, THRESHOLD + 1);
		assertFalse(selectsRdma(driver, op));
	}

	// ---------- selectPath: operation type filtering ----------

	@Test
	void testSelectPath_falseForNonDataOp() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		final var op = (Operation<Item>) (Operation<?>) new OperationImpl<>(
						0, OpType.CREATE, new ItemImpl("item"), null, "/bucket", TEST_CRED);
		assertFalse(selectsRdma(driver, op));
	}

	@Test
	void testSelectPath_falseForDeleteOp() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertFalse(selectsRdma(driver, dataOp(OpType.DELETE, THRESHOLD + 1)));
	}

	@Test
	void testSelectPath_falseForUpdateOp() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertFalse(selectsRdma(driver, dataOp(OpType.UPDATE, THRESHOLD + 1)));
	}

	// ---------- selectPath: size threshold ----------

	@Test
	void testSelectPath_falseWhenBelowThreshold() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertFalse(selectsRdma(driver, dataOp(OpType.CREATE, THRESHOLD - 1)));
	}

	@Test
	void testSelectPath_trueForCreateAtThreshold() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertTrue(selectsRdma(driver, dataOp(OpType.CREATE, THRESHOLD)));
	}

	@Test
	void testSelectPath_trueForReadAtThreshold() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertTrue(selectsRdma(driver, dataOp(OpType.READ, THRESHOLD)));
	}

	@Test
	void testSelectPath_trueForLargeCreate() throws Exception {
		final var driver = newDriver(enabledConfig(), availableTransport());
		assertTrue(selectsRdma(driver, dataOp(OpType.CREATE, THRESHOLD * 10)));
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

	// ---------- selectPath: the reason a request goes over HTTP ----------

	@Test
	void testSelectPath_namesTheReasonForHttp() throws Exception {
		final var unavailable = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		assertEquals(PathSelection.HTTP_RDMA_UNAVAILABLE, unavailable.selectPath(dataOp(OpType.CREATE, THRESHOLD + 1)));
		assertEquals(PathSelection.HTTP_RDMA_UNAVAILABLE, unavailable.selectPath(dataOp(OpType.READ, 1)));
		assertEquals(PathSelection.HTTP_UNCOUNTED, unavailable.selectPath(dataOp(OpType.DELETE, THRESHOLD + 1)),
						"only a CREATE or READ carries a payload");

		final var driver = newDriver(enabledConfig(), availableTransport());
		assertEquals(PathSelection.RDMA, driver.selectPath(dataOp(OpType.READ, THRESHOLD)));
		assertEquals(PathSelection.HTTP_BELOW_THRESHOLD, driver.selectPath(dataOp(OpType.READ, THRESHOLD - 1)));
		assertEquals(PathSelection.HTTP_INELIGIBLE, driver.selectPath(rangedRead(THRESHOLD + 1)));
		assertEquals(PathSelection.HTTP_UNCOUNTED, driver.selectPath(dataOp(OpType.DELETE, THRESHOLD + 1)));
		assertEquals(PathSelection.HTTP_UNCOUNTED, driver.selectPath(dataOp(OpType.UPDATE, THRESHOLD + 1)));
	}

	@Test
	void testSelectPath_countsNothing() throws Exception {
		// A request is counted when it is sent; choosing its path may happen more than once.
		final var unavailable = newDriver(enabledConfig(), new RdmaTransport(enabledConfig()));
		unavailable.selectPath(dataOp(OpType.CREATE, THRESHOLD + 1));
		unavailable.driverStats().counters().forEach((name, value) -> assertEquals(0L, value, name));

		final var driver = newDriver(enabledConfig(), availableTransport());
		for (final var op : List.of(
						dataOp(OpType.READ, THRESHOLD),
						dataOp(OpType.READ, THRESHOLD - 1),
						rangedRead(THRESHOLD + 1),
						dataOp(OpType.DELETE, THRESHOLD + 1))) {
			driver.selectPath(op);
		}
		driver.driverStats().counters().forEach((name, value) -> assertEquals(0L, value, name));
	}

	// ---------- selectPath: zero threshold ----------

	@Test
	void testSelectPath_zeroThresholdAcceptsAnySizeCreate() throws Exception {
		final var config = new RdmaConfig(true, 0L, true, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());
		assertTrue(selectsRdma(driver, dataOp(OpType.CREATE, 1)));
	}

	@Test
	void testSelectPath_zeroThresholdAcceptsZeroSizeCreate() throws Exception {
		final var config = new RdmaConfig(true, 0L, true, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());
		assertTrue(selectsRdma(driver, dataOp(OpType.CREATE, 0)));
	}

	// ---------- selectPath: below-threshold warning ----------

	@Test
	void testSelectPath_warnsBelowThresholdOnce() throws Exception {
		final var config = new RdmaConfig(true, THRESHOLD, false, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());

		// Before any sub-threshold op, the guard should be false
		assertFalse(getBelowThresholdWarned(driver), "belowThresholdWarned should start false");

		// First sub-threshold call should trip the guard
		final var smallOp = dataOp(OpType.CREATE, THRESHOLD - 1);
		assertFalse(selectsRdma(driver, smallOp));
		assertTrue(getBelowThresholdWarned(driver), "belowThresholdWarned should be true after first sub-threshold op");

		// Second call — guard stays true (log-once behaviour)
		assertFalse(selectsRdma(driver, smallOp));
		assertTrue(getBelowThresholdWarned(driver));
	}

	@Test
	void testSelectPath_noWarningWhenAboveThreshold() throws Exception {
		final var config = new RdmaConfig(true, THRESHOLD, false, "auto", "", "WARN");
		final var driver = newDriver(config, availableTransport());

		// Above-threshold op should not trip the guard
		final var largeOp = dataOp(OpType.CREATE, THRESHOLD + 1);
		assertTrue(selectsRdma(driver, largeOp));
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

	private static boolean selectsRdma(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver,
					final Operation<Item> op) {
		return driver.selectPath(op) == PathSelection.RDMA;
	}

	private static Operation<Item> rangedRead(final long size) {
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<>(
						0, OpType.READ, new DataItemImpl("testItem", 0, size), null, "/bucket",
						TEST_CRED, List.of(new Range(0L, size / 2, -1L)), 0);
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
