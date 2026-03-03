package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergCommitterTest {

	private S3TablesStorageDriver<?, ?> mockDriver;
	private S3TablesControlPlane mockControlPlane;

	@BeforeEach
	void setUp() throws Exception {
		mockDriver = mock(S3TablesStorageDriver.class);
		mockControlPlane = mock(S3TablesControlPlane.class);
		when(mockDriver.getStepId()).thenReturn("test-step");
		when(mockDriver.getControlPlane()).thenReturn(mockControlPlane);
		doNothing().when(mockDriver).executeDataPlaneRequest(anyString(), any(), anyString());
		doNothing().when(mockDriver).recordCommitRetry();
	}

	@Test
	void commit_successFirstAttempt() throws Exception {
		when(mockControlPlane.updateTableMetadataLocation(anyString(), anyString()))
			.thenReturn(200);

		final IcebergCommitter committer = new IcebergCommitter(
			mockDriver, "s3://test-bucket/prefix", "token-v1");

		final byte[] parquetBytes = new byte[1024];
		final long snapshotId = committer.commit(parquetBytes, parquetBytes.length);

		assertTrue(snapshotId > 0, "snapshotId should be positive");
		// 3 data-plane PUTs: parquet + manifest + manifest-list + metadata JSON = 4
		verify(mockDriver, times(4)).executeDataPlaneRequest(anyString(), any(), anyString());
		verify(mockControlPlane, times(1)).updateTableMetadataLocation(anyString(), anyString());
	}

	@Test
	void commit_dataFilePutToCorrectPath() throws Exception {
		when(mockControlPlane.updateTableMetadataLocation(anyString(), anyString()))
			.thenReturn(200);

		final List<String> capturedPaths = new ArrayList<>();
		doAnswer(inv -> {
			capturedPaths.add(inv.getArgument(0));
			return null;
		}).when(mockDriver).executeDataPlaneRequest(anyString(), any(), anyString());

		final IcebergCommitter committer = new IcebergCommitter(
			mockDriver, "s3://test-bucket/warehouse", "token-v1");

		committer.commit(new byte[512], 512);

		// Data file should be under /test-bucket/warehouse/data/
		assertTrue(capturedPaths.stream().anyMatch(p -> p.contains("/data/") && p.endsWith(".parquet")),
			"Expected a data file PUT under /data/: " + capturedPaths);
		// Manifest under /metadata/ ending .avro
		assertTrue(capturedPaths.stream().anyMatch(p -> p.contains("/metadata/") && p.endsWith("-m0.avro")),
			"Expected a manifest PUT under /metadata/: " + capturedPaths);
		// Manifest list under /metadata/ starting snap-
		assertTrue(capturedPaths.stream().anyMatch(p -> p.contains("/metadata/snap-") && p.endsWith(".avro")),
			"Expected a manifest list PUT: " + capturedPaths);
		// Metadata JSON under /metadata/
		assertTrue(capturedPaths.stream().anyMatch(p -> p.contains("/metadata/") && p.endsWith(".metadata.json")),
			"Expected a metadata JSON PUT: " + capturedPaths);
	}

	@Test
	void commit_retriesOn409ThenSucceeds() throws Exception {
		final IcebergCommitter.MetadataLocationResult freshLoc = new IcebergCommitter.MetadataLocationResult("s3://b/w/metadata/v2.metadata.json", "token-v2");
		when(mockControlPlane.getTableMetadataLocation()).thenReturn(freshLoc);
		// First call: 409; second call: 200
		when(mockControlPlane.updateTableMetadataLocation(anyString(), anyString()))
						.thenReturn(409)
						.thenReturn(200);

		final IcebergCommitter committer = new IcebergCommitter(
						mockDriver, "s3://b/w", "token-v1");

		final long snapshotId = committer.commit(new byte[256], 256);

		assertTrue(snapshotId > 0);
		verify(mockControlPlane, times(2)).updateTableMetadataLocation(anyString(), anyString());
		verify(mockControlPlane, times(1)).getTableMetadataLocation();
		verify(mockDriver, times(1)).recordCommitRetry();
	}

	@Test
	void commit_throwsAfterMaxRetries() throws Exception {
		final IcebergCommitter.MetadataLocationResult freshLoc = new IcebergCommitter.MetadataLocationResult("s3://b/w/metadata/v2.metadata.json", "token-v2");
		when(mockControlPlane.getTableMetadataLocation()).thenReturn(freshLoc);
		// Always conflict
		when(mockControlPlane.updateTableMetadataLocation(anyString(), anyString()))
						.thenReturn(409);

		final IcebergCommitter committer = new IcebergCommitter(
						mockDriver, "s3://b/w", "token-v1");

		assertThrows(Exception.class, () -> committer.commit(new byte[64], 64),
						"Should throw after exhausting retries");
	}

	@Test
	void commit_throwsOnHttpError() throws Exception {
		when(mockControlPlane.updateTableMetadataLocation(anyString(), anyString()))
			.thenReturn(500);

		final IcebergCommitter committer = new IcebergCommitter(
			mockDriver, "s3://b/w", "token-v1");

		assertThrows(Exception.class, () -> committer.commit(new byte[64], 64),
			"Should throw on non-200/non-409 HTTP status");
	}

	@Test
	void commit_throwsWhenDataPlanePutFails() throws Exception {
		doThrow(new Exception("PUT failed"))
						.when(mockDriver).executeDataPlaneRequest(anyString(), any(), anyString());

		final IcebergCommitter committer = new IcebergCommitter(
						mockDriver, "s3://b/w", "token-v1");

		assertThrows(Exception.class, () -> committer.commit(new byte[64], 64),
						"Should propagate data-plane PUT failure");
	}

	@Test
	void metadataLocationResult_storesFields() {
		final IcebergCommitter.MetadataLocationResult result = new IcebergCommitter.MetadataLocationResult("s3://b/w/metadata/v1.json", "tok-abc");
		assertEquals("s3://b/w/metadata/v1.json", result.metadataLocation);
		assertEquals("tok-abc", result.versionToken);
	}

	@Test
	void commit_versionTokenUpdatedAfter409() throws Exception {
		final IcebergCommitter.MetadataLocationResult freshLoc = new IcebergCommitter.MetadataLocationResult("s3://b/w/metadata/v2.metadata.json", "new-token");
		when(mockControlPlane.getTableMetadataLocation()).thenReturn(freshLoc);
		final AtomicInteger callCount = new AtomicInteger(0);
		when(mockControlPlane.updateTableMetadataLocation(anyString(), anyString()))
						.thenAnswer(inv -> callCount.incrementAndGet() == 1 ? 409 : 200);

		final IcebergCommitter committer = new IcebergCommitter(
						mockDriver, "s3://b/w", "old-token");

		committer.commit(new byte[128], 128);

		// Second UpdateTableMetadataLocation call should use the refreshed token "new-token"
		verify(mockControlPlane, atLeast(1)).updateTableMetadataLocation(eq("new-token"), anyString());
	}
}
