package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class S3AwsStorageDriverFactoryTest {

	/**
	 * When neither "endpoint" nor nested "net.node.addrs" resolve to a value,
	 * the factory must throw {@link IllegalConfigurationException} with a clear
	 * message instead of crashing with NPE inside {@code URI.create(null)}.
	 */
	@Test
	void create_missingEndpoint_throwsIllegalConfigurationException() {
		Config storageConfig = mock(Config.class);

		// auth-uid and auth-secret resolve (so we get past credential checks)
		when(storageConfig.stringVal("auth-uid")).thenReturn("access");
		when(storageConfig.stringVal("auth-secret")).thenReturn("secret");
		when(storageConfig.stringVal("region")).thenReturn("us-east-1");
		when(storageConfig.stringVal("bucket")).thenReturn("test-bucket");

		// endpoint is null — primary lookup throws, fallback also throws
		when(storageConfig.stringVal("endpoint")).thenReturn(null);
		when(storageConfig.configVal("net")).thenThrow(new RuntimeException("no net config"));

		// optional params — defaults are fine
		when(storageConfig.boolVal("path-style-access")).thenReturn(true);
		when(storageConfig.intVal("max-connections")).thenThrow(new RuntimeException("missing"));
		when(storageConfig.intVal("socket-timeout-ms")).thenThrow(new RuntimeException("missing"));
		when(storageConfig.intVal("connection-timeout-ms")).thenThrow(new RuntimeException("missing"));

		S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

		var ex = assertThrows(IllegalConfigurationException.class,
						() -> factory.create("step-1", null, storageConfig, false, 1));
		assertTrue(ex.getMessage().contains("endpoint"),
						"Error message should mention 'endpoint', got: " + ex.getMessage());
	}

	/**
	 * When endpoint is set to an empty string, same guard should fire.
	 */
	@Test
	void create_emptyEndpoint_throwsIllegalConfigurationException() {
		Config storageConfig = mock(Config.class);

		when(storageConfig.stringVal("auth-uid")).thenReturn("access");
		when(storageConfig.stringVal("auth-secret")).thenReturn("secret");
		when(storageConfig.stringVal("region")).thenReturn("us-east-1");
		when(storageConfig.stringVal("bucket")).thenReturn("test-bucket");

		// endpoint resolves to empty string
		when(storageConfig.stringVal("endpoint")).thenReturn("");
		when(storageConfig.configVal("net")).thenThrow(new RuntimeException("no net config"));

		when(storageConfig.boolVal("path-style-access")).thenReturn(true);
		when(storageConfig.intVal("max-connections")).thenThrow(new RuntimeException("missing"));
		when(storageConfig.intVal("socket-timeout-ms")).thenThrow(new RuntimeException("missing"));
		when(storageConfig.intVal("connection-timeout-ms")).thenThrow(new RuntimeException("missing"));

		S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

		var ex = assertThrows(IllegalConfigurationException.class,
						() -> factory.create("step-1", null, storageConfig, false, 1));
		assertTrue(ex.getMessage().contains("endpoint"),
						"Error message should mention 'endpoint', got: " + ex.getMessage());
	}
}
