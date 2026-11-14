package com.dell.spt.storage.driver.coop.netty.mock;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The mock completes each operation inside {@code sendRequest}, so completion-driven direct
 * dispatch would recurse until the stack overflows. It must keep the dispatcher path.
 */
class NettyStorageDriverMockDispatchTest {

	private static final String DIRECT_DISPATCH_PROPERTY = "spt.dispatch.direct";

	@Test
	void theMockKeepsTheDispatcherPathWhileTheJvmSwitchAllowsDirectDispatch() throws Exception {
		final var prior = System.setProperty(DIRECT_DISPATCH_PROPERTY, "true");
		try (final var driver = new NettyStorageDriverMock<Item, Operation<Item>>(
						"netty-mock-dispatch",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1KB"), 16, false, 0.0, true),
						storageConfig(),
						false,
						1)) {
			final Method enabled = CoopStorageDriverBase.class.getDeclaredMethod("directDispatchEnabled");
			enabled.setAccessible(true);
			assertFalse((Boolean) enabled.invoke(driver),
							"the mock completes inside sendRequest, so direct dispatch would recurse until the stack overflows");
		} finally {
			if (prior == null) {
				System.clearProperty(DIRECT_DISPATCH_PROPERTY);
			} else {
				System.setProperty(DIRECT_DISPATCH_PROPERTY, prior);
			}
		}
	}

	private static Config storageConfig() throws Exception {
		final var schemas = new ArrayList<Map<String, Object>>();
		for (final var extension : Extension.load(Thread.currentThread().getContextClassLoader())) {
			final var provider = extension.schemaProvider();
			if (provider != null && provider.schema() != null) {
				schemas.add(provider.schema());
			}
		}
		SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream().findFirst().ifPresent(schemas::add);
		final Config config = new BasicConfig("-", TreeUtil.reduceForest(schemas));

		config.val("storage-namespace", "");
		config.val("storage-auth-uid", "test-user");
		config.val("storage-auth-secret", "test-secret");
		config.val("storage-auth-token", null);

		config.val("storage-driver-threads", 1);
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 16);
		config.val("storage-integrity-mode", "none");
		config.val("storage-integrity-algorithm", "sha256");
		config.val("storage-integrity-input-provenance", "none");
		config.val("storage-integrity-input-expectedProducerId", "");

		config.val("storage-net-bindBacklogSize", 0);
		config.val("storage-net-interestOpQueued", false);
		config.val("storage-net-keepAlive", true);
		config.val("storage-net-linger", 0);
		config.val("storage-net-reuseAddr", true);
		config.val("storage-net-rcvBuf", 0);
		config.val("storage-net-sndBuf", 0);
		config.val("storage-net-tcpNoDelay", true);
		config.val("storage-net-timeoutMilliSec", 1000);
		config.val("storage-net-ioRatio", 50);
		config.val("storage-net-transport", "nio");
		config.val("storage-net-writeSpinCount", 1);
		config.val("storage-net-node-addrs", List.of("127.0.0.1"));
		config.val("storage-net-node-port", 9020);
		config.val("storage-net-node-connAttemptsLimit", 0);
		config.val("storage-net-node-slice", false);

		config.val("storage-net-ssl-enabled", false);
		config.val("storage-net-ssl-provider", "JDK");
		config.val("storage-net-ssl-protocols", List.of("TLSv1.3", "TLSv1.2"));
		config.val("storage-net-ssl-ciphers", List.of());
		config.val("storage-net-ssl-pqcMode", "off");
		config.val("storage-net-ssl-jsseProvider", "");
		config.val("storage-net-ssl-namedGroups", List.of());

		return config.configVal("storage");
	}
}
