package com.dell.spt.storage.driver.coop.netty.http.atmos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.github.akurilov.commons.io.collection.ListInput;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Atmos has no composite (multipart) handling: it sends a composite and each of its parts as an
 * ordinary request, so a step refuses a ranges threshold on it at start.
 */
class AtmosCompositeSupportTest {

	private static final long SIZE = 4096;
	private static final long THRESHOLD = 1024;

	/** Each part of a composite CREATE is its own new object, as the composite itself is. */
	@Test
	void aCompositeCreateAndEachOfItsPartsAreEachSentAsANewObject() throws Exception {
		final var drv = new AtmosUriArgsSigningTest.TestAtmosDriver(AtmosUriArgsSigningTest.config(Map.of()));
		try {
			final var composite = composite(OpType.CREATE);
			final HttpRequest initiation = drv.requestFor(composite);
			assertEquals(HttpMethod.POST, initiation.method());
			assertEquals("/rest/objects", initiation.uri());
			assertEquals(Long.toString(SIZE), initiation.headers().get(HttpHeaderNames.CONTENT_LENGTH));
			final var parts = composite.subOperations();
			assertEquals(4, parts.size());
			for (final var part : parts) {
				final HttpRequest request = drv.requestFor((DataOperation<DataItem>) (DataOperation<?>) part);
				assertEquals(HttpMethod.POST, request.method(), "a part creates a new object");
				assertEquals("/rest/objects", request.uri(), "a part names no object, upload or part number");
				assertEquals(Long.toString(THRESHOLD), request.headers().get(HttpHeaderNames.CONTENT_LENGTH));
			}
		} finally {
			drv.close();
		}
	}

	/** Each part of a composite READ asks for the whole object: no range. */
	@Test
	void eachPartOfACompositeReadAsksForTheWholeObject() throws Exception {
		final var drv = new AtmosUriArgsSigningTest.TestAtmosDriver(AtmosUriArgsSigningTest.config(Map.of()));
		try {
			final var composite = composite(OpType.READ);
			final var parts = composite.subOperations();
			assertEquals(4, parts.size());
			for (final var part : parts) {
				final HttpRequest request = drv.requestFor((DataOperation<DataItem>) (DataOperation<?>) part);
				assertEquals(HttpMethod.GET, request.method());
				assertEquals("/rest/objects/bucket/obj1", request.uri());
				assertNull(request.headers().get(HttpHeaderNames.RANGE), "part " + part.partNumber());
			}
		} finally {
			drv.close();
		}
	}

	@Test
	void createAndReadStepsWithAThresholdFailAtStart() throws Exception {
		final var drv = new AtmosUriArgsSigningTest.TestAtmosDriver(AtmosUriArgsSigningTest.config(Map.of()));
		try {
			assertFalse(drv.supportsCompositeOperations());
			for (final String opType : List.of("create", "read")) {
				final var failure = assertThrows(
								IllegalConfigurationException.class, () -> builder(drv, stepConfig(opType, "1MB")).build(), opType);
				assertEquals(
								"The storage driver type \"atmos\" does not support composite (multipart) operations, but"
												+ " item.data.ranges.threshold=1048576 would make composite " + opType.toUpperCase(Locale.ROOT)
												+ " operations; set item.data.ranges.threshold to 0 for this driver",
								failure.getMessage());
			}
		} finally {
			drv.close();
		}
	}

	@SuppressWarnings("unchecked")
	private static CompositeDataOperationImpl<DataItem> composite(final OpType opType) throws Exception {
		final DataItem item = new DataItemImpl("obj1", 0, SIZE);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return new CompositeDataOperationImpl<>(
						0, opType, item, null, "/bucket", null, null, 0, THRESHOLD);
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static LoadGeneratorBuilderImpl<DataItem, DataOperation<DataItem>, LoadGeneratorImpl<DataItem, DataOperation<DataItem>>> builder(
					final AtmosUriArgsSigningTest.TestAtmosDriver drv, final Config config) {
		return new LoadGeneratorBuilderImpl<DataItem, DataOperation<DataItem>, LoadGeneratorImpl<DataItem, DataOperation<DataItem>>>()
						.itemConfig(config.configVal("item"))
						.loadConfig(config.configVal("load"))
						.itemType(ItemType.DATA)
						.itemFactory(new DataItemFactoryImpl())
						.authConfig(config.configVal("storage-auth"))
						.originIndex(0)
						.itemInput(new ListInput<>(new ArrayList<>()))
						.loadOperationsOutput((com.github.akurilov.commons.io.Output) drv);
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
