package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.base.Constants.APP_NAME;

import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Constructor-faithful S3-RDMA driver fixtures shared by hardware-free unit tests. */
final class S3RdmaStorageDriverTestSupport {

	private static final Credential CREDENTIAL = Credential.getInstance("user1", "test-secret");
	private static final ThreadLocal<List<S3RdmaStorageDriver<?, ?>>> CREATED_DRIVERS = ThreadLocal.withInitial(ArrayList::new);

	private S3RdmaStorageDriverTestSupport() {}

	static S3RdmaStorageDriver<Item, Operation<Item>> newDriver(
					final RdmaConfig rdmaConfig, final RdmaTransport transport) throws Exception {
		return newDriver(rdmaConfig, transport, "/bucket", List.of("127.0.0.1"), 9020);
	}

	static S3RdmaStorageDriver<Item, Operation<Item>> newDriver(
					final RdmaConfig rdmaConfig,
					final RdmaTransport transport,
					final String namespace,
					final List<String> nodeAddrs,
					final int nodePort) throws Exception {
		final Config config = config(rdmaConfig, namespace, nodeAddrs, nodePort);
		final DataInput dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true);
		try {
			final var driver = new S3RdmaStorageDriver<Item, Operation<Item>>(
							"s3-rdma-constructor-fixture",
							dataInput,
							config.configVal("storage"),
							false,
							config.intVal("load-batch-size"),
							ignored -> transport);
			CREATED_DRIVERS.get().add(driver);
			return driver;
		} catch (final Throwable failure) {
			dataInput.close();
			throw failure;
		}
	}

	static void closeCreatedDrivers() throws Exception {
		final List<S3RdmaStorageDriver<?, ?>> drivers = CREATED_DRIVERS.get();
		Exception closeFailure = null;
		for (int i = drivers.size() - 1; i >= 0; i--) {
			try {
				drivers.get(i).close();
			} catch (final Exception failure) {
				if (closeFailure == null) {
					closeFailure = failure;
				} else {
					closeFailure.addSuppressed(failure);
				}
			}
		}
		drivers.clear();
		CREATED_DRIVERS.remove();
		if (closeFailure != null) {
			throw closeFailure;
		}
	}

	private static Config config(
					final RdmaConfig rdmaConfig,
					final String namespace,
					final List<String> nodeAddrs,
					final int nodePort) {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(provider -> {
								try {
									return provider.schema();
								} catch (final Exception e) {
									throw new IllegalStateException(e);
								}
							})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			configSchemas.add(0, InitialConfigSchemaProvider.provider().schema());
			SchemaProvider
							.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			final Config config = new BasicConfig("-", TreeUtil.reduceForest(configSchemas));
			config.val("load-batch-size", 16);
			config.val("storage-driver-limit-concurrency", 1);
			config.val("storage-driver-threads", 1);
			config.val("storage-driver-limit-queue-input", 16);
			config.val("storage-namespace", namespace);
			config.val("storage-net-transport", "nio");
			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl-enabled", false);
			config.val("storage-net-ssl-protocols", List.of());
			config.val("storage-net-ssl-provider", "OPENSSL");
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-writeSpinCount", 1);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 2_000);
			config.val("storage-net-ioRatio", 50);
			config.val("storage-net-node-addrs", nodeAddrs);
			config.val("storage-net-node-port", nodePort);
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-net-http-headers",
							Map.of("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}"));
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65_536);
			config.val("storage-net-http-uri-args", Map.of());
			config.val("storage-object-fsAccess", true);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", Map.of());
			config.val("storage-object-versioning", false);
			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());
			config.val("storage-auth-version", 4);
			config.val("storage-checksum-enabled", false);
			config.val("storage-integrity-mode", "none");
			config.val("storage-integrity-algorithm", "sha256");
			config.val("storage-integrity-input-provenance", "none");
			config.val("storage-integrity-input-expectedProducerId", "");
			config.val("storage-integrity-selection-maxCount", 0L);
			config.val("storage-rdma-enabled", rdmaConfig.isEnabled());
			config.val("storage-rdma-thresholdBytes", rdmaConfig.getThresholdBytes());
			config.val("storage-rdma-fallback", rdmaConfig.isFallbackEnabled());
			config.val("storage-rdma-device", rdmaConfig.getDevice());
			config.val("storage-rdma-localIp", rdmaConfig.getLocalIp());
			config.val("storage-rdma-logLevel", rdmaConfig.getLogLevel());
			config.val("storage-rdma-timeoutMs", rdmaConfig.getTimeoutMs());
			return config;
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
