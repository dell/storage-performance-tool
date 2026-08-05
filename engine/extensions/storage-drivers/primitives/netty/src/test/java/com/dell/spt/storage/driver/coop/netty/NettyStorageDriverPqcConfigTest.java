package com.dell.spt.storage.driver.coop.netty;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.storage.driver.coop.netty.mock.NettyStorageDriverMock;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

@SuppressWarnings("unchecked")
class NettyStorageDriverPqcConfigTest {

	private static final String TEST_SEED = "7a42d9c483244167";
	private static final String MISSING_JSSE_PROVIDER = "NoSuchJsseProvider";
	private static final String JSSE_PROVIDER_WARNING_FRAGMENT = "PQC TLS provider";
	private static final String NAMED_GROUPS_WARNING_FRAGMENT = "failed to apply SSL named groups";

	@Test
	void requireModeMissingJsseProviderFailsFast() {
		final var storageConfig = storageConfig("require", MISSING_JSSE_PROVIDER);
		assertThrows(IllegalConfigurationException.class, () -> newDriver(storageConfig));
	}

	@Test
	void resolveBcjsseAliasCreatesProviderWithoutGlobalRegistration() throws Exception {
		final var resolverMethod = NettyStorageDriverBase.class.getDeclaredMethod("resolveJsseProvider", String.class);
		resolverMethod.setAccessible(true);
		final var provider = (Provider) resolverMethod.invoke(null, "BCJSSE");
		assertNotNull(provider, "BCJSSE alias should resolve to a provider instance");
		assertEquals("BCJSSE", provider.getName());
		assertNotNull(provider.getService("SSLContext", "TLS"));
	}

	@Test
	void resolveBcjsseClassNameCreatesProviderWithoutGlobalRegistration() throws Exception {
		final var resolverMethod = NettyStorageDriverBase.class.getDeclaredMethod("resolveJsseProvider", String.class);
		resolverMethod.setAccessible(true);
		final var provider = (Provider) resolverMethod.invoke(null, "org.bouncycastle.jsse.provider.BouncyCastleJsseProvider");
		assertNotNull(provider, "BCJSSE class name should resolve to a provider instance");
		assertEquals("BCJSSE", provider.getName());
		assertNotNull(provider.getService("SSLContext", "TLS"));
	}

	@Test
	void bcJsseProviderBuildsRealNettyJdkSslContext() throws Exception {
		final var resolverMethod = NettyStorageDriverBase.class.getDeclaredMethod("resolveJsseProvider", String.class);
		resolverMethod.setAccessible(true);
		final var provider = (Provider) resolverMethod.invoke(null, "BCJSSE");
		assertNotNull(provider);
		final var sslCtx = SslContextBuilder
						.forClient()
						.trustManager(InsecureTrustManagerFactory.INSTANCE)
						.sslProvider(SslProvider.JDK)
						.sslContextProvider(provider)
						.protocols("TLSv1.3", "TLSv1.2")
						.ciphers(List.of())
						.build();
		assertNotNull(sslCtx);
	}

	@Test
	void preferModeMissingJsseProviderFallsBackAndLogsWarning() throws Exception {
		final var capture = attachCapture();
		try {
			final var storageConfig = storageConfig("prefer", MISSING_JSSE_PROVIDER);
			try (final var driver = newDriver(storageConfig)) {
				assertNotNull(sslContext(driver));
			}
			assertTrue(
							capture.awaitMessageContaining(JSSE_PROVIDER_WARNING_FRAGMENT, 1000),
							"prefer mode should warn and fall back when JSSE provider is unavailable");
		} finally {
			detachCapture(capture);
		}
	}

	@Test
	void offModeIgnoresMissingJsseProviderWithoutWarning() throws Exception {
		final var capture = attachCapture();
		try {
			final var storageConfig = storageConfig("off", MISSING_JSSE_PROVIDER);
			assertDoesNotThrow(() -> {
				try (final var driver = newDriver(storageConfig)) {
					assertNotNull(sslContext(driver));
				}
			});
			assertFalse(
							capture.awaitMessageContaining(JSSE_PROVIDER_WARNING_FRAGMENT, 250),
							"off mode should not attempt PQC provider resolution");
		} finally {
			detachCapture(capture);
		}
	}

	@Test
	void preferModeDoesNotMutateJvmGlobalProviderOrder() throws Exception {
		final var providersBefore = providerNames();
		try (final var driver = newDriver(storageConfig("prefer", MISSING_JSSE_PROVIDER))) {
			assertNotNull(driver);
		}
		assertEquals(providersBefore, providerNames());
	}

	@Test
	void negotiatedTlsLogContainsProtocolAndCipher() throws Exception {
		final var capture = attachCapture();
		try {
			final var driver = loggingDriver("tls-step");
			final var sslHandler = mock(SslHandler.class);
			final SSLEngine sslEngine = mock(SSLEngine.class);
			final SSLSession sslSession = mock(SSLSession.class);
			when(sslHandler.engine()).thenReturn(sslEngine);
			when(sslEngine.getSession()).thenReturn(sslSession);
			when(sslSession.getProtocol()).thenReturn("TLSv1.3");
			when(sslSession.getCipherSuite()).thenReturn("TLS_AES_128_GCM_SHA256");

			driver.logNegotiatedTls(sslHandler);
			driver.logNegotiatedTls(sslHandler);
			assertTrue(
							capture.awaitMessageContaining(
											"negotiated TLS protocol=TLSv1.3, cipher=TLS_AES_128_GCM_SHA256", 1000),
							"negotiated protocol/cipher should be emitted to SPT logs");
			assertEquals(
							1,
							capture.countMessagesContaining("negotiated TLS protocol=TLSv1.3, cipher=TLS_AES_128_GCM_SHA256"),
							"negotiated handshake log should be emitted once per driver instance");
		} finally {
			detachCapture(capture);
		}
	}

	@Test
	void applyNamedGroupsPreferModeLogsWarningAndDoesNotThrow() throws Exception {
		final var capture = attachCapture();
		try {
			final var driver = namedGroupsDriver("named-groups-step", "prefer", new String[]{"X25519MLKEM768"
			});
			final SSLEngine sslEngine = mock(SSLEngine.class);
			final SSLParameters sslParameters = mock(SSLParameters.class);
			when(sslEngine.getSSLParameters()).thenReturn(sslParameters);
			doThrow(new IllegalArgumentException("unsupported named group")).when(sslParameters).setNamedGroups(org.mockito.ArgumentMatchers.any());

			assertDoesNotThrow(() -> driver.applyNamedGroups(sslEngine));
			assertDoesNotThrow(() -> driver.applyNamedGroups(sslEngine));
			assertTrue(
							capture.awaitMessageContaining(NAMED_GROUPS_WARNING_FRAGMENT, 1000),
							"prefer mode should warn when named groups cannot be applied");
			assertEquals(
							1,
							capture.countMessagesContaining(NAMED_GROUPS_WARNING_FRAGMENT),
							"named-groups warning should be emitted once per driver instance");
		} finally {
			detachCapture(capture);
		}
	}

	@Test
	void applyNamedGroupsRequireModeThrows() throws Exception {
		final var driver = namedGroupsDriver("named-groups-step", "require", new String[]{"X25519MLKEM768"
		});
		final SSLEngine sslEngine = mock(SSLEngine.class);
		final SSLParameters sslParameters = mock(SSLParameters.class);
		when(sslEngine.getSSLParameters()).thenReturn(sslParameters);
		doThrow(new IllegalArgumentException("unsupported named group")).when(sslParameters).setNamedGroups(org.mockito.ArgumentMatchers.any());

		assertThrows(IllegalArgumentException.class, () -> driver.applyNamedGroups(sslEngine));
	}

	private static List<String> providerNames() {
		final var providers = Security.getProviders();
		final var names = new ArrayList<String>(providers.length);
		for (final Provider provider : providers) {
			names.add(provider.getName());
		}
		return names;
	}

	private static SslContext sslContext(final NettyStorageDriverMock<Item, Operation<Item>> driver) {
		try {
			final var sslCtxField = NettyStorageDriverBase.class.getDeclaredField("sslCtx");
			sslCtxField.setAccessible(true);
			return (SslContext) sslCtxField.get(driver);
		} catch (final ReflectiveOperationException e) {
			throw new AssertionError(e);
		}
	}

	private static NettyStorageDriverMock<Item, Operation<Item>> newDriver(final Config storageConfig)
					throws Exception {
		return new NettyStorageDriverMock<>(
						"test-step",
						DataInput.instance(null, TEST_SEED, new SizeInBytes("64KB"), 16, false, 0.0, true),
						storageConfig,
						false,
						1);
	}

	private static NettyStorageDriverBase<Item, Operation<Item>> loggingDriver(final String stepId) throws Exception {
		final var driver = mock(NettyStorageDriverBase.class, org.mockito.Mockito.withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var stepIdField = StorageDriverBase.class.getDeclaredField("stepId");
		stepIdField.setAccessible(true);
		stepIdField.set(driver, stepId);
		final var namedGroupsWarnedField = NettyStorageDriverBase.class.getDeclaredField("namedGroupsWarned");
		namedGroupsWarnedField.setAccessible(true);
		namedGroupsWarnedField.set(driver, new java.util.concurrent.atomic.AtomicBoolean(false));
		final var tlsHandshakeLoggedField = NettyStorageDriverBase.class.getDeclaredField("tlsHandshakeLogged");
		tlsHandshakeLoggedField.setAccessible(true);
		tlsHandshakeLoggedField.set(driver, new java.util.concurrent.atomic.AtomicBoolean(false));
		return driver;
	}

	private static NettyStorageDriverBase<Item, Operation<Item>> namedGroupsDriver(
					final String stepId, final String pqcMode, final String[] namedGroups)
					throws Exception {
		final var driver = loggingDriver(stepId);
		final var pqcModeField = NettyStorageDriverBase.class.getDeclaredField("sslPqcMode");
		pqcModeField.setAccessible(true);
		pqcModeField.set(driver, pqcMode);
		final var namedGroupsField = NettyStorageDriverBase.class.getDeclaredField("sslNamedGroups");
		namedGroupsField.setAccessible(true);
		namedGroupsField.set(driver, namedGroups);
		return driver;
	}

	private static Config storageConfig(final String pqcMode, final String jsseProvider) {
		try {
			final var configSchema = mergedSchema();
			final Config config = new BasicConfig("-", configSchema);

			config.val("storage-namespace", "");
			config.val("storage-auth-uid", "test-user");
			config.val("storage-auth-secret", "test-secret");
			config.val("storage-auth-token", null);

			config.val("storage-driver-threads", 1);
			config.val("storage-driver-limit-concurrency", 0);
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

			config.val("storage-net-ssl-enabled", true);
			config.val("storage-net-ssl-provider", "JDK");
			config.val("storage-net-ssl-protocols", List.of("TLSv1.3", "TLSv1.2"));
			config.val("storage-net-ssl-ciphers", List.of());
			config.val("storage-net-ssl-pqcMode", pqcMode);
			config.val("storage-net-ssl-jsseProvider", jsseProvider);
			config.val("storage-net-ssl-namedGroups", List.of("X25519MLKEM768", "x25519", "secp256r1"));

			return config.configVal("storage");
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static Map<String, Object> mergedSchema() {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(sp -> {
								try {
									return sp.schema();
								} catch (final Exception e) {
									throw new RuntimeException(e);
								}
							})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			SchemaProvider
							.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			return TreeUtil.reduceForest(configSchemas);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static CapturingAppender attachCapture() {
		final var appender = new CapturingAppender();
		appender.start();
		final var msgLogger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.MSG.getName());
		final var errLogger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.ERR.getName());
		msgLogger.addAppender(appender);
		errLogger.addAppender(appender);
		msgLogger.setLevel(Level.INFO);
		errLogger.setLevel(Level.WARN);
		return appender;
	}

	private static void detachCapture(final CapturingAppender appender) throws InterruptedException {
		LogUtil.flushAll();
		Thread.sleep(25);
		final var msgLogger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.MSG.getName());
		final var errLogger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.ERR.getName());
		msgLogger.removeAppender(appender);
		errLogger.removeAppender(appender);
		appender.stop();
	}

	private static final class CapturingAppender extends AbstractAppender {

		private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

		private CapturingAppender() {
			super("netty-pqc-capture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			messages.add(event.getMessage().getFormattedMessage());
		}

		boolean awaitMessageContaining(final String fragment, final long timeoutMs) throws InterruptedException {
			final var deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
			while (System.nanoTime() < deadline) {
				if (contains(fragment)) {
					return true;
				}
				Thread.sleep(10);
			}
			return contains(fragment);
		}

		private boolean contains(final String fragment) {
			synchronized (messages) {
				for (final var msg : messages) {
					if (msg.contains(fragment)) {
						return true;
					}
				}
			}
			return false;
		}

		int countMessagesContaining(final String fragment) {
			var n = 0;
			synchronized (messages) {
				for (final var msg : messages) {
					if (msg.contains(fragment)) {
						n++;
					}
				}
			}
			return n;
		}
	}
}
