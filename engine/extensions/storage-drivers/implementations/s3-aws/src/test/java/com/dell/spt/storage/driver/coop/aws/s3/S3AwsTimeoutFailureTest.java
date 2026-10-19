package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.storage.driver.coop.aws.s3.DriverLogCapture.count;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** How a request that timed out is classified, and what the driver logs about it. */
final class S3AwsTimeoutFailureTest {

	private static final Credential CREDENTIAL = Credential.getInstance("access-key", "secret-key");

	/** What the CRT transfer client reports when the stall monitor closes a connection. */
	private static final String CRT_STALL = "Failed to send the request: Http connection channel shut down due to failure to meet throughput minimum";

	/** What an ordinary SDK client over the CRT HTTP client reports for the same. */
	private static final String HTTP_STALL = "software.amazon.awssdk.crt.http.HttpException: Http connection channel shut down due to failure to meet"
					+ " throughput minimum";

	/** The transfer client's report of a connection that could not be established in time. */
	private static final String CRT_SOCKET_TIMEOUT = "Failed to send the request: socket operation timed out.";

	/** The same for an ordinary SDK client over the CRT HTTP client. */
	private static final String HTTP_SOCKET_TIMEOUT = "software.amazon.awssdk.crt.http.HttpException: socket operation timed out.";

	private DriverLogCapture log;

	@BeforeEach
	void captureTheDriverLog() {
		log = new DriverLogCapture();
	}

	@AfterEach
	void releaseTheDriverLog() {
		log.close();
	}

	@Test
	void theWordingTheClientsReportIsWhatTheDriverLooksFor() {
		assertTrue(S3AwsClientTimeouts.isStall(SdkClientException.create(CRT_STALL)));
		assertTrue(CRT_STALL.contains(S3AwsClientTimeouts.STALL_FAILURE_MESSAGE));
		assertTrue(HTTP_STALL.contains(S3AwsClientTimeouts.STALL_FAILURE_MESSAGE));
		assertTrue(CRT_SOCKET_TIMEOUT.contains(S3AwsClientTimeouts.SOCKET_TIMEOUT_MESSAGE));
		assertTrue(HTTP_SOCKET_TIMEOUT.contains(S3AwsClientTimeouts.SOCKET_TIMEOUT_MESSAGE));
	}

	@Test
	void aConnectionClosedByTheStallMonitorIsATimeout() {
		final var crt = SdkClientException.create(CRT_STALL);
		final var http = SdkClientException.create("Unable to execute HTTP request: " + HTTP_STALL, new IOException(HTTP_STALL));

		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(crt));
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(crt)));
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(http));
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(http)));
	}

	@Test
	void aConnectionThatCouldNotBeEstablishedInTimeIsATimeout() {
		final var crt = SdkClientException.create(CRT_SOCKET_TIMEOUT);
		final var http = SdkClientException.create(
						"Unable to execute HTTP request: " + HTTP_SOCKET_TIMEOUT, new ConnectException(HTTP_SOCKET_TIMEOUT));

		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(crt)));
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(http)));
	}

	@Test
	void otherClientAndIoFailuresKeepTheirStatus() {
		assertEquals(Status.FAIL_IO, S3AwsStorageDriver.classifyFailure(
						SdkClientException.create("Unable to execute HTTP request", new IOException("Connection reset"))));
		assertEquals(Status.FAIL_IO, S3AwsStorageDriver.classifyFailure(SdkClientException.create(
						"Unable to execute HTTP request: Connection refused", new ConnectException("Connection refused"))));
		assertEquals(Status.FAIL_UNKNOWN, S3AwsStorageDriver.classifyFailure(
						SdkClientException.create("Failed to send the request: some other native error")));
		assertEquals(Status.FAIL_IO, S3AwsStorageDriver.classifyFailure(new IOException("broken pipe")));
	}

	@Test
	void aGatewayTimeoutIsLoggedAsATimeoutThatDoesNotBlameTheStallMonitor() throws Exception {
		final var primary = mock(S3AsyncClient.class);
		when(primary.headObject(any(HeadObjectRequest.class))).thenReturn(CompletableFuture.failedFuture(
						S3Exception.builder().statusCode(504).message("gateway timeout").build()));
		final var driver = S3AwsDeleteRequestTestFixture.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(primary, mock(S3AsyncClient.class));

		final var first = invoke(driver);
		final var second = invoke(driver);

		assertEquals(Status.FAIL_TIMEOUT, first.status());
		assertEquals(Status.FAIL_TIMEOUT, second.status());
		final var events = log.events();
		assertEquals(1, count(events, Level.WARN), events.toString());
		assertEquals(0, count(events, Level.ERROR), events.toString());
		assertFalse(events.stream().anyMatch(event -> event.message().contains("socketTimeoutMs")), events.toString());
	}

	@Test
	void aStalledConnectionIsLoggedOnceAndNamesTheSettingThatBoundsIt() throws Exception {
		final var primary = mock(S3AsyncClient.class);
		when(primary.headObject(any(HeadObjectRequest.class))).thenReturn(
						CompletableFuture.failedFuture(SdkClientException.create(CRT_STALL)));
		final var driver = S3AwsDeleteRequestTestFixture.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(primary, mock(S3AsyncClient.class));

		for (int repeat = 0; repeat < 20; repeat++) {
			assertEquals(Status.FAIL_TIMEOUT, invoke(driver).status());
		}

		final var events = log.events();
		assertEquals(1, count(events, Level.WARN), events.toString());
		assertEquals(0, count(events, Level.ERROR), events.toString());
		assertTrue(events.stream().anyMatch(event -> event.level() == Level.WARN
						&& event.message().contains("storage.crt.socketTimeoutMs")), events.toString());
	}

	@Test
	void eachKindOfTimeoutHasItsOwnWarning() throws Exception {
		final var primary = mock(S3AsyncClient.class);
		when(primary.headObject(any(HeadObjectRequest.class))).thenReturn(
						CompletableFuture.failedFuture(S3Exception.builder().statusCode(504).build()),
						CompletableFuture.failedFuture(SdkClientException.create(CRT_STALL)),
						CompletableFuture.failedFuture(SdkClientException.create(CRT_SOCKET_TIMEOUT)),
						CompletableFuture.failedFuture(SdkClientException.create(CRT_STALL)));
		final var driver = S3AwsDeleteRequestTestFixture.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(primary, mock(S3AsyncClient.class));

		for (int request = 0; request < 4; request++) {
			assertEquals(Status.FAIL_TIMEOUT, invoke(driver).status());
		}

		// A gateway timeout and a socket timeout share one warning; a stall has its own.
		final var events = log.events();
		assertEquals(2, count(events, Level.WARN), events.toString());
		assertEquals(1, events.stream().filter(event -> event.level() == Level.WARN
						&& event.message().contains("storage.crt.socketTimeoutMs")).count(), events.toString());
	}

	@Test
	void aFailureThatIsNotATimeoutIsStillLoggedAsAnError() throws Exception {
		final var primary = mock(S3AsyncClient.class);
		when(primary.headObject(any(HeadObjectRequest.class))).thenReturn(CompletableFuture.failedFuture(
						S3Exception.builder().statusCode(403).message("denied").build()));
		final var driver = S3AwsDeleteRequestTestFixture.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(primary, mock(S3AsyncClient.class));

		assertEquals(Status.RESP_FAIL_AUTH, invoke(driver).status());

		final var events = log.events();
		assertEquals(1, count(events, Level.ERROR), events.toString());
		assertEquals(0, count(events, Level.WARN), events.toString());
	}

	private static DataOperationImpl<IntegrityManifestDataItem> invoke(
					final S3AwsStorageDriver<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> driver) {
		final var item = new IntegrityManifestDataItem("bucket", "key", 0, null);
		final var op = new DataOperationImpl<IntegrityManifestDataItem>(
						0, OpType.STAT, item, null, "/bucket", CREDENTIAL, null, 0);
		op.startRequest();
		op.finishRequest();
		driver.invokeNio(op);
		return op;
	}
}
