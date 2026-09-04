package com.dell.spt.storage.driver.coop.aws.s3;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.integrity.IntegrityMetadataCodec;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;

final class S3AwsDeleteRequestTestFixture {

	static final Credential CREDENTIAL = Credential.getInstance("access-key", "secret-key");

	private S3AwsDeleteRequestTestFixture() {}

	static DeleteTarget target(final String key, final String versionId) {
		return new DeleteTarget(new IntegrityManifestDataItem("bucket", key, 0, versionId));
	}

	static S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver(
					final S3AsyncClient primaryClient,
					final S3AsyncClient exactVersionClient,
					final S3AsyncClient standaloneDeleteClient,
					final SdkAsyncHttpClient standaloneDeleteHttpClient)
					throws Exception {
		return new S3AwsStorageDriver<>(
						"aws-delete-test",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storageConfig(),
						false,
						16,
						primaryClient,
						() -> exactVersionClient,
						standaloneDeleteClient,
						standaloneDeleteHttpClient,
						100 * 1024L,
						8 * 1024 * 1024L);
	}

	static DeleteRequestOperation operation(final DeleteTarget... targets) {
		return new DeleteRequestOperationImpl(
						0, new DeleteRequest("bucket", CREDENTIAL, Arrays.asList(targets)));
	}

	static S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver(
					final S3AsyncClient primaryClient, final S3AsyncClient exactVersionClient)
					throws Exception {
		return new S3AwsStorageDriver<>(
						"aws-delete-test",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storageConfig(),
						false,
						16,
						primaryClient,
						exactVersionClient,
						100 * 1024L,
						8 * 1024 * 1024L);
	}

	static S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> fallthroughDriver(
					final S3AsyncClient primaryClient, final S3AsyncClient exactVersionClient)
					throws Exception {
		return new S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation>(
						"aws-delete-fallthrough-test",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storageConfig(),
						false,
						16,
						primaryClient,
						exactVersionClient,
						100 * 1024L,
						8 * 1024 * 1024L) {
			@Override
			CompletableFuture<Void> execute(final DeleteRequestOperation operation) {
				return deleteObject(operation);
			}
		};
	}

	private static Config storageConfig() {
		final Config storage = mock(Config.class);
		final Config driver = mock(Config.class);
		final Config limits = mock(Config.class);
		final Config auth = mock(Config.class);
		final Config integrity = mock(Config.class);
		final Config integrityInput = mock(Config.class);

		when(storage.configVal("driver")).thenReturn(driver);
		when(driver.configVal("limit")).thenReturn(limits);
		when(storage.stringVal("namespace")).thenReturn("bucket");
		when(storage.stringVal("driver-type")).thenReturn("s3-aws");
		when(limits.intVal("concurrency")).thenReturn(1);
		when(driver.intVal("threads")).thenReturn(1);
		when(storage.intVal("driver-threads")).thenReturn(1);
		when(storage.intVal("driver-limit-queue-input")).thenReturn(8);

		when(storage.configVal("auth")).thenReturn(auth);
		when(auth.stringVal("uid")).thenReturn(CREDENTIAL.getUid());
		when(auth.stringVal("secret")).thenReturn(CREDENTIAL.getSecret());
		when(auth.stringVal("token")).thenReturn(null);

		when(storage.configVal("integrity")).thenReturn(integrity);
		when(integrity.stringVal("mode")).thenReturn("none");
		when(integrity.stringVal("algorithm"))
						.thenReturn(IntegrityMetadataCodec.ALGORITHM_SHA256);
		when(integrity.configVal("input")).thenReturn(integrityInput);
		when(integrityInput.stringVal("provenance")).thenReturn("none");
		when(integrityInput.stringVal("expectedProducerId")).thenReturn("");
		return storage;
	}
}
