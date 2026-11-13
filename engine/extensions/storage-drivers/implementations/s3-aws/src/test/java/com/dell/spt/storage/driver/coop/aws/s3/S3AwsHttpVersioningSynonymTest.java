package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityMetadataCodec;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * The Mongoose synonym {@code storage.net.http.versioning} with {@code s3-aws}. On this driver
 * {@code storage.object.versioning} only splits {@code key~versionId} input names: it sends no bucket versioning
 * request, and its written items keep plain names. The synonym gives exactly that, and nothing more.
 */
final class S3AwsHttpVersioningSynonymTest {

	private static final String SYNONYM = "storage-net-http-versioning";
	private static final String TARGET = "storage-object-versioning";

	/** The schema the extensions on this test classpath merge into, as the bundle merges its own. */
	private static Config mergedConfig() throws Exception {
		final ClassLoader loader = Thread.currentThread().getContextClassLoader();
		final List<Map<String, Object>> schemas = Extension.load(loader)
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
		SchemaProvider.resolve(APP_NAME, loader).stream().findFirst().ifPresent(schemas::add);
		return new BasicConfig("-", TreeUtil.reduceForest(schemas));
	}

	@SuppressWarnings("unchecked")
	private static Object leaf(final Map<String, Object> schema, final String... path) {
		Object node = schema;
		for (final var key : path) {
			node = ((Map<String, Object>) node).get(key);
		}
		return node;
	}

	@Test
	void theTestClasspathDeclaresTheSynonymWithoutTheSwiftJar() throws Exception {
		final var ids = Extension.load(Thread.currentThread().getContextClassLoader())
						.stream().map(Extension::id).collect(Collectors.toSet());
		assertFalse(ids.contains("swift"), ids::toString);

		final var schema = mergedConfig().schema();

		assertEquals(boolean.class, leaf(schema, "storage", "net", "http", "versioning"));
		assertEquals(boolean.class, leaf(schema, "storage", "object", "versioning"));
	}

	@Test
	void theSynonymSplitsVersionedInputNames() throws Exception {
		final S3AsyncClient client = mock(S3AsyncClient.class);
		final var driver = driver(true, client);

		assertEquals(Arrays.asList("bench/k0", "v000001"), Arrays.asList(driver.extractVersionId("bench/k0~v000001")));
		assertEquals(Arrays.asList("bench/k1", null), Arrays.asList(driver.extractVersionId("bench/k1")));
		verifyNoInteractions(client);
	}

	@Test
	void theSynonymFalseKeepsNamesWhole() throws Exception {
		final var driver = driver(false, mock(S3AsyncClient.class));

		assertEquals(
						Arrays.asList("bench/k0~v000001", null), Arrays.asList(driver.extractVersionId("bench/k0~v000001")));
	}

	private static S3AwsStorageDriver<Item, Operation<Item>> driver(final boolean synonym, final S3AsyncClient client)
					throws Exception {
		final Config config = mergedConfig();
		config.val("storage-driver-type", "s3-aws");
		config.val(TARGET, false);
		config.val(SYNONYM, synonym);
		// What the step's constructor does before any driver exists.
		HttpVersioningSynonym.resolve(config, null);
		assertEquals(synonym, config.boolVal(TARGET));
		return new S3AwsStorageDriver<>(
						"http-versioning-synonym",
						null,
						storageConfig(config.configVal("storage").configVal("object")),
						false,
						1,
						client,
						100 * 1024L,
						8 * 1024 * 1024L);
	}

	/** The driver's other settings, mocked as {@code S3AwsStorageDriverTest} does, around the real object config. */
	private static Config storageConfig(final Config objectConfig) {
		final Config limitConfig = mock(Config.class);
		when(limitConfig.intVal("concurrency")).thenReturn(1);
		final Config driverConfig = mock(Config.class);
		when(driverConfig.configVal("limit")).thenReturn(limitConfig);
		final Config authConfig = mock(Config.class);
		when(authConfig.stringVal("uid")).thenReturn("test-user");
		when(authConfig.stringVal("secret")).thenReturn("test-secret");
		final Config integrityConfig = mock(Config.class);
		final Config integrityInputConfig = mock(Config.class);
		when(integrityConfig.stringVal("mode")).thenReturn("none");
		when(integrityConfig.stringVal("algorithm")).thenReturn(IntegrityMetadataCodec.ALGORITHM_SHA256);
		when(integrityConfig.configVal("input")).thenReturn(integrityInputConfig);
		when(integrityInputConfig.stringVal("provenance")).thenReturn("none");
		when(integrityInputConfig.stringVal("expectedProducerId")).thenReturn("");

		final Config config = mock(Config.class);
		when(config.configVal("integrity")).thenReturn(integrityConfig);
		when(config.configVal("driver")).thenReturn(driverConfig);
		when(config.stringVal("namespace")).thenReturn("test-ns");
		when(config.configVal("auth")).thenReturn(authConfig);
		when(config.intVal("driver-limit-queue-input")).thenReturn(100);
		when(config.configVal("object")).thenReturn(objectConfig);
		when(config.configVal("item")).thenThrow(new RuntimeException("no item config"));
		when(config.configVal("checksum")).thenThrow(new RuntimeException("no checksum config"));
		when(config.listVal("net.node.addrs")).thenThrow(new RuntimeException("no net config"));
		return config;
	}
}
