package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.storage.driver.CompositeOperationsUnsupported;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.io.collection.ListInput;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * S3 Tables extends the {@code s3} driver but runs every operation through its configured opMode,
 * never through the inherited S3 multipart route, so it has no composite (multipart) support and a
 * step refuses a ranges threshold on it at start.
 */
@SuppressWarnings({"rawtypes", "unchecked"
})
class S3TablesCompositeSupportTest {

	/**
	 * With the threshold at 0 a step builds no composite, and its operation runs as a table write, as
	 * before: one data operation on one driver, with no multipart request and no range read. It
	 * builds the operation the way a step's builder does, so the builder's accepting the threshold
	 * of 0 is checked too.
	 */
	@Test
	void anOperationAtThresholdZeroIsRunAsATableWrite() throws Exception {
		final Config driverConfig = S3TablesStorageDriverTest.baseConfig("tableWrite", "127.0.0.1");
		driverConfig.val("storage-driver-type", "s3-tables");
		final var drv = new S3TablesStorageDriverTest.TestDriver(driverConfig);
		try {
			for (final String opType : List.of("create", "read")) {
				try (var generator = builder(drv, stepConfig(opType, 0)).build()) {
					assertNotNull(generator, opType);
				}
			}
			final DataItem item = new DataItemImpl("obj1", 0, 4096);
			item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
			final var op = new DataOperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null, null, 0);

			assertTrue(drv.submit((Operation) op));

			assertEquals(Operation.Status.SUCC, op.status());
			boolean dataPut = false;
			for (final FullHttpRequest request : drv.log()) {
				final String uri = request.uri();
				assertFalse(uri.contains("uploads") || uri.contains("uploadId") || uri.contains("partNumber"),
								request.method() + " " + uri);
				assertNull(request.headers().get(HttpHeaderNames.RANGE), request.method() + " " + uri);
				dataPut |= HttpMethod.PUT.equals(request.method()) && uri.endsWith(".parquet");
			}
			assertTrue(dataPut, "a Parquet data file is written: " + uris(drv));
		} finally {
			drv.close();
		}
	}

	@Test
	void createAndReadStepsWithAThresholdFailAtStart() throws Exception {
		final Config driverConfig = S3TablesStorageDriverTest.baseConfig("tableWrite", "127.0.0.1");
		driverConfig.val("storage-driver-type", "s3-tables");
		final var drv = new S3TablesStorageDriverTest.TestDriver(driverConfig);
		try {
			assertInstanceOf(CompositeOperationsUnsupported.class, drv);
			for (final String opType : List.of("create", "read")) {
				final var failure = assertThrows(
								IllegalConfigurationException.class, () -> builder(drv, stepConfig(opType, "1MB")).build(), opType);
				assertEquals(
								"The storage driver type \"" + drv.driverType() + "\" does not support composite (multipart) operations,"
												+ " but item.data.ranges.threshold=1048576 would make composite " + opType.toUpperCase(Locale.ROOT)
												+ " operations; set item.data.ranges.threshold to 0 for this driver",
								failure.getMessage());
			}
		} finally {
			drv.close();
		}
	}

	private static List<String> uris(final S3TablesStorageDriverTest.TestDriver drv) {
		final List<String> uris = new ArrayList<>();
		for (final FullHttpRequest request : drv.log()) {
			uris.add(request.method() + " " + request.uri());
		}
		return uris;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl builder(final S3TablesStorageDriverTest.TestDriver drv, final Config config) {
		return new com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl()
						.itemConfig(config.configVal("item"))
						.loadConfig(config.configVal("load"))
						.itemType(ItemType.DATA)
						.itemFactory(new DataItemFactoryImpl())
						.authConfig(config.configVal("storage-auth"))
						.originIndex(0)
						.itemInput(new ListInput<>(new ArrayList<>()))
						.loadOperationsOutput((Output) drv);
	}

	/** The engine's shipped defaults, with the op type and the ranges threshold set. */
	private static Config stepConfig(final String opType, final Object threshold) throws Exception {
		final String defaults;
		try (InputStream stream = InitialConfigSchemaProvider.class.getResourceAsStream("/config/defaults.yaml")) {
			defaults = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
		final Config config = ConfigUtil.loadConfig(
						defaults, ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		config.val("load-op-type", opType);
		config.val("item-data-ranges-threshold", threshold);
		config.val("item-data-ranges-concat", null);
		return config;
	}
}
