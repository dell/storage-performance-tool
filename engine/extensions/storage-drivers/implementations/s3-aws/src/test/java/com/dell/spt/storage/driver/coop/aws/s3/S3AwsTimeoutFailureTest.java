package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** How a request failed by the stall monitor is classified and how often it is logged. */
final class S3AwsTimeoutFailureTest {

	private static final String CRT_CLIENT_FAILURE = "Failed to send the request: Http connection channel shut down due to failure to meet throughput minimum";
	private static final String HTTP_CLIENT_FAILURE = "software.amazon.awssdk.crt.http.HttpException: Http connection channel shut down due to failure to meet"
					+ " throughput minimum";

	@Test
	void theWordingTheClientsReportIsWhatTheDriverLooksFor() {
		assertTrue(CRT_CLIENT_FAILURE.contains(S3AwsClientTimeouts.STALL_FAILURE_MESSAGE));
		assertTrue(HTTP_CLIENT_FAILURE.contains(S3AwsClientTimeouts.STALL_FAILURE_MESSAGE));
	}

	@Test
	void aConnectionClosedByTheStallMonitorIsATimeout() {
		// The CRT transfer client reports the native error as the message of a client exception.
		final var crt = SdkClientException.create(CRT_CLIENT_FAILURE);
		// An ordinary SDK client wraps it as the cause of an I/O failure.
		final var http = SdkClientException.create(
						"Unable to execute HTTP request: " + HTTP_CLIENT_FAILURE, new IOException(HTTP_CLIENT_FAILURE));

		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(crt));
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(crt)));
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(http));
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(http)));
	}

	@Test
	void otherClientAndIoFailuresKeepTheirStatus() {
		assertEquals(Status.FAIL_IO, S3AwsStorageDriver.classifyFailure(
						SdkClientException.create("Unable to execute HTTP request", new IOException("Connection reset"))));
		assertEquals(Status.FAIL_UNKNOWN, S3AwsStorageDriver.classifyFailure(
						SdkClientException.create("Failed to send the request: some other native error")));
		assertEquals(Status.FAIL_IO, S3AwsStorageDriver.classifyFailure(new IOException("broken pipe")));
	}

	@Test
	void onlyTheFirstTimeoutIsLoggedAboveDebug() throws Exception {
		final var driver = S3AwsDeleteRequestTestFixture.driver(mock(S3AsyncClient.class), mock(S3AsyncClient.class));
		final var op = mock(DeleteRequestOperation.class);
		when(op.type()).thenReturn(OpType.CREATE);
		when(op.item()).thenReturn(new IntegrityManifestDataItem("bucket", "key", 0, null));
		final var failure = SdkClientException.create(CRT_CLIENT_FAILURE);

		assertTrue(driver.logTimeoutFailure(op, failure));
		for (int repeat = 0; repeat < 100; repeat++) {
			assertFalse(driver.logTimeoutFailure(op, failure));
		}
	}
}
