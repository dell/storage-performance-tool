package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Placement of a mixed step's PUTs in a target bucket set, through the slice's PUT builder wiring. */
class MixedTargetBucketPlacementTest {

	private static Config config(final int count) throws Exception {
		final Config config = defaultConfig();
		config.val("storage-targetBucket-count", count);
		config.val("item-output-path", "/bench");
		return config;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static List<String> putDestinations(final Config config, final long... ids) throws Exception {
		final var builder = MixedLoadStepLocal.makePutBuilder(
						0, null, config.stringVal("item-output-path"), new ConstantValueInputImpl<>(Credential.NONE),
						Long.MAX_VALUE, config.configVal("storage"), config.configVal("item"));
		assertEquals(OpType.CREATE, builder.opType());
		final List<DataItem> items = new ArrayList<>();
		for (final long id : ids) {
			items.add(new DataItemImpl(Long.toString(id, 36), id, 1024));
		}
		final List<Operation> ops = new ArrayList<>();
		builder.buildOps(items, ops);
		final List<String> destinations = new ArrayList<>();
		for (final Operation op : ops) {
			destinations.add(op.dstPath());
		}
		destinations.add(((Operation) builder.buildOp(items.get(0))).dstPath());
		return destinations;
	}

	@Test
	void putsGoToTheMembersOfTheBucketSet() throws Exception {
		assertEquals(List.of("/bench-0", "/bench-1", "/bench-2", "/bench-3", "/bench-1", "/bench-0"),
						putDestinations(config(4), 0, 1, 2, 3, 5));
	}

	@Test
	void putsKeepShardLabelsIndependentOfTheirBucket() throws Exception {
		final var config = config(4);
		config.val("item-naming-shards", 64);

		assertEquals(List.of("/bench-0", "/bench-0", "/bench-1", "/bench-3", "/bench-0"),
						putDestinations(config, 0, 63, 64, 255));
	}

	@Test
	void oneBucketKeepsTheOutputPath() throws Exception {
		assertEquals(List.of("/bench", "/bench", "/bench"), putDestinations(config(1), 0, 1));
	}

	/** The shipped defaults of the engine and of this extension, as the engine merges them. */
	private static Config defaultConfig() throws Exception {
		final Config core = ConfigUtil.loadConfig(
						resource("/config/defaults.yaml"), ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		final Config mixed = ConfigUtil.loadConfig(
						resource("/config/defaults-load-op-mixed.yaml"),
						ConfigFormat.YAML,
						new MixedLoadStepExtension().schemaProvider().schema());
		return ConfigUtil.merge(core.pathSep(), List.of(mixed, core));
	}

	private static String resource(final String name) throws IOException {
		try (InputStream stream = MixedTargetBucketPlacementTest.class.getResourceAsStream(name)) {
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
