package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.http.s3.VersioningConfigurationXmlHandler.Kind;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Characterizes the Netty driver's own first-use bucket initialization, which the controller's
 * provisioning does not replace and the multi-bucket feature keeps unchanged
 * ({@code S3StorageDriver.requestNewPath} through {@code StorageDriverBase.prepare}):
 *
 * <ul>
 *   <li>a successful check, an HTTP error status and a response timeout return the path, which is
 *       cached, so that driver instance never checks the bucket again;
 *   <li>only a connection failure of the initial HEAD returns null and caches nothing; the object
 *       operation is still submitted and its own request decides its outcome;
 *   <li>with versioning, initialization adds GET ?versioning and, only for a bucket that has never been
 *       versioned, PUT ?versioning, signed and built like the GET; each outcome logs one bucket versioning
 *       notice, and none throws, so the path is cached;
 *   <li>an interrupt ends a pending helper request.
 * </ul>
 */
@Timeout(60)
final class S3FirstUseBucketCheckTest {

	private record Captured(String method, String path, String query) {}

	/** A request with the headers the versioning tests check. */
	private record Seen(String method, String path, String query, String auth, String probe, int dates) {}

	private static final String ENABLED = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
	private static final String SUSPENDED = "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
	private static final String SUSPENDED_MFA = "<VersioningConfiguration><Status>Suspended</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>";
	private static final String NEVER = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>";
	private static final String S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/";
	/** Sol's review probe: a legal namespace declaration on the Status hid it from the old regex. */
	private static final String SUSPENDED_NS_DECLARED = "<VersioningConfiguration xmlns=\"" + S3_NS + "\">"
					+ "<s3:Status xmlns:s3=\"" + S3_NS + "\">Suspended</s3:Status></VersioningConfiguration>";
	private static final String ENABLED_ATTRIBUTED = "<VersioningConfiguration xmlns=\"" + S3_NS + "\">"
					+ "<Status note=\"x\">Enabled</Status></VersioningConfiguration>";

	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<HttpServer> servers = new ArrayList<>();
	private volatile int headStatus = 200;
	private volatile long headDelayMillis;
	private volatile CountDownLatch headArrived = new CountDownLatch(1);
	private final List<Seen> seen = new CopyOnWriteArrayList<>();
	private volatile int getVersioningStatus = 200;
	private volatile String getVersioningBody = "<VersioningConfiguration/>";
	private volatile int putVersioningStatus = 200;
	private volatile long putVersioningDelayMillis;
	private volatile long getVersioningDelay;
	private final BucketVersioningNotices notices = BucketVersioningNotices.attach();

	@AfterEach
	void tearDown() throws Exception {
		notices.close();
		for (final var resource : resources) {
			resource.close();
		}
		servers.forEach(server -> server.stop(0));
		serverThreads.shutdownNow();
	}

	@Test
	void aSuccessfulCheckReturnsThePathAndIsCachedForLaterOperations() throws Exception {
		final var driver = driver(List.of(listener()), "default", 5_000, false);

		assertEquals("/bench-0/p", driver.requestNewPath("/bench-0/p"));
		requests.clear();
		putTwice(driver, "/bench-0/p");

		assertEquals(1, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
	}

	@Test
	void anHttpErrorStatusIsCachedAndNotRetried() throws Exception {
		headStatus = 403;
		final var driver = driver(List.of(listener()), "default", 5_000, false);

		assertEquals("/bench-1/p", driver.requestNewPath("/bench-1/p"));
		requests.clear();
		final var outcomes = putTwice(driver, "/bench-1/p");

		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.SUCC), outcomes);
		assertEquals(1, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
	}

	@Test
	void aResponseTimeoutReturnsThePathAtTheHelperTimeout() throws Exception {
		headDelayMillis = 3_000;
		final var driver = driver(List.of(listener()), "default", 300, false);

		final long start = System.nanoTime();
		assertEquals("/bench-2/p", driver.requestNewPath("/bench-2/p"));
		final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

		assertTrue(millis < 2_500, "the wait ends at the helper timeout, not the response: " + millis + " ms");
	}

	@Test
	void aResponseTimeoutIsCachedAndNotRetried() throws Exception {
		headDelayMillis = 3_000;
		final var driver = driver(List.of(listener()), "default", 300, false);

		final var outcomes = putTwice(driver, "/bench-2/p");

		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.SUCC), outcomes);
		assertEquals(1, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aConnectionFailureOfTheVersioningCheckIsCached() throws Exception {
		// Round robin over a listener and a closed port: the HEAD goes to the listener, the versioning
		// GET to the closed port, and the first object request to the listener again.
		final var driver = driver(List.of(listener(), "127.0.0.1:" + closedPort()), "round-robin", 5_000, true);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput((com.github.akurilov.commons.io.Output) results);

		for (int i = 0; i < 2; i++) {
			final Operation<DataItem> op = new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("v" + i, i, ITEM_SIZE), null, "/bench-1/p", CREDENTIAL, null, 0);
			assertTrue(driver.put(op));
			final var result = results.await();
			if (i == 0) {
				assertEquals(Operation.Status.SUCC, result.status(), "the first object request reaches the listener");
			}
		}

		assertEquals(1, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
		assertEquals(0, requests.stream().filter(r -> "versioning".equals(r.query())).count(), requests::toString);
		notices.awaitOnly("bench-1", Level.WARN, "could not read the versioning state (the connection failed); SPT sent"
						+ " no request to enable it");
	}

	@Test
	void aConnectionFailureOfThePutCouldNotEnable() throws Exception {
		// Round robin over two listeners and a closed port: the HEAD and the GET reach the listeners, the
		// PUT ?versioning the closed port.
		final var driver = driver(List.of(listener(), listener(), "127.0.0.1:" + closedPort()), "round-robin", 5_000, true);

		assertEquals("/ver-putrefused/p", assertDoesNotThrow(() -> driver.requestNewPath("/ver-putrefused/p")));

		assertEquals(List.of(new Captured("HEAD", "/ver-putrefused", null),
						new Captured("GET", "/ver-putrefused", "versioning")), requests);
		notices.awaitOnly("ver-putrefused", Level.WARN, "could not enable versioning (the connection failed)");
	}

	@Test
	void aConnectionFailureOfTheHeadIsRetriedButDoesNotFailTheOperation() throws Exception {
		// Round robin alternates between a closed port and a listener: every bucket check lands on the
		// closed port, every object request on the listener.
		final var driver = driver(List.of("127.0.0.1:" + closedPort(), listener()), "round-robin", 5_000, false);

		final var outcomes = putTwice(driver, "/bench-3/p");

		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.SUCC), outcomes,
						"each operation's own request decides its outcome");
		assertEquals(0, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
		assertEquals(2, requests.stream().filter(r -> "PUT".equals(r.method())).count(), requests::toString);
		final var counters = ((S3EndpointSelectionDriver<?, ?>) driver).counters().snapshot();
		assertEquals(2, counters.connectsFailed(), "the uncached check was attempted again: " + counters);
	}

	@Test
	void versioningAddsTheVersioningCheckAndReconciliation() throws Exception {
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		assertEquals("/bench-0/p", driver.requestNewPath("/bench-0/p"));

		assertEquals(List.of(
						new Captured("HEAD", "/bench-0", null),
						new Captured("GET", "/bench-0", "versioning"),
						new Captured("PUT", "/bench-0", "versioning")), requests);
		notices.awaitOnly("bench-0", Level.INFO, "versioning enabled by this run");
	}

	@Test
	void anEnabledBucketGetsNoPut() throws Exception {
		getVersioningBody = ENABLED;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		assertEquals("/ver-enabled/p", driver.requestNewPath("/ver-enabled/p"));

		assertEquals(List.of(new Captured("HEAD", "/ver-enabled", null), new Captured("GET", "/ver-enabled", "versioning")),
						requests);
		notices.awaitOnly("ver-enabled", Level.INFO, "versioning is already enabled");
	}

	@Test
	void aNeverVersionedBucketIsEnabledWithANotice() throws Exception {
		getVersioningBody = NEVER;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		assertEquals("/ver-never/p", driver.requestNewPath("/ver-never/p"));

		assertEquals(1, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-never", Level.INFO, "versioning enabled by this run");
	}

	@Test
	void aRefusedPutCouldNotEnable() throws Exception {
		putVersioningStatus = 403;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		assertEquals("/ver-put403/p", driver.requestNewPath("/ver-put403/p"));

		assertEquals(1, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-put403", Level.WARN, "could not enable versioning (PUT ?versioning returned 403 Forbidden)");
	}

	@Test
	void aTimedOutPutCouldNotEnableAndIsCached() throws Exception {
		putVersioningDelayMillis = 3_000;
		final var driver = driver(List.of(listener()), "default", 400, true);

		final var outcomes = putTwice(driver, "/ver-puttimeout/p");

		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.SUCC), outcomes, "no exception reached driver.put");
		assertEquals(List.of(
						new Captured("HEAD", "/ver-puttimeout", null),
						new Captured("GET", "/ver-puttimeout", "versioning"),
						new Captured("PUT", "/ver-puttimeout", "versioning"),
						new Captured("PUT", "/ver-puttimeout/p/k0", null),
						new Captured("PUT", "/ver-puttimeout/p/k1", null)), requests,
						"the path is cached: the second operation sends no HEAD, GET or PUT ?versioning");
		notices.awaitOnly("ver-puttimeout", Level.WARN, "could not enable versioning (PUT ?versioning timed out after 400 ms)");
	}

	@Test
	void aTimedOutPutReturnsThePath() throws Exception {
		putVersioningDelayMillis = 3_000;
		final var driver = driver(List.of(listener()), "default", 400, true);

		assertEquals("/ver-puttimeout2/p", assertDoesNotThrow(() -> driver.requestNewPath("/ver-puttimeout2/p")));
	}

	@Test
	void aSuspendedBucketIsLeftAlone() throws Exception {
		getVersioningBody = SUSPENDED;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		assertEquals("/ver-suspended/p", driver.requestNewPath("/ver-suspended/p"));

		assertEquals(0, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-suspended", Level.WARN, "versioning is suspended; SPT left it alone");
	}

	@Test
	void aSuspendedBucketWithMfaDeleteEnabledIsLeftAlone() throws Exception {
		getVersioningBody = SUSPENDED_MFA;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		assertEquals("/ver-suspendedmfa/p", driver.requestNewPath("/ver-suspendedmfa/p"));

		assertEquals(0, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-suspendedmfa", Level.WARN, "versioning is suspended; SPT left it alone");
	}

	@Test
	void aFailedVersioningCheckSendsNoPut() throws Exception {
		for (final int status : new int[]{403, 404, 500
		}) {
			getVersioningStatus = status;
			final var driver = driver(List.of(listener()), "default", 5_000, true);
			final var bucket = "ver-get" + status;

			assertEquals("/" + bucket + "/p", driver.requestNewPath("/" + bucket + "/p"));

			assertEquals(0, versioningPuts(), requests::toString);
			notices.awaitOnly(bucket, Level.WARN, "could not read the versioning state (GET ?versioning returned " + status);
			notices.awaitOnly(bucket, Level.WARN, "SPT sent no request to enable it, and the run goes on un-versioned");
		}
	}

	@Test
	void aTimedOutVersioningCheckSendsNoPut() throws Exception {
		final var driver = driver(List.of(listener()), "default", 400, true);
		getVersioningDelay = 3_000;

		assertEquals("/ver-gettimeout/p", driver.requestNewPath("/ver-gettimeout/p"));

		assertEquals(0, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-gettimeout", Level.WARN, "could not read the versioning state (GET ?versioning timed out after 400 ms)");
	}

	@Test
	void theMongooseSynonymLeavesASuspendedBucketAlone() throws Exception {
		getVersioningBody = SUSPENDED;
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val(HttpVersioningSynonym.SYNONYM_PATH, true);
		HttpVersioningSynonym.resolve(config, null);
		final var driver = driver(config, listener(), 5_000);

		assertEquals("/ver-synonym/p", driver.requestNewPath("/ver-synonym/p"));

		assertEquals(List.of(new Captured("HEAD", "/ver-synonym", null), new Captured("GET", "/ver-synonym", "versioning")),
						requests);
		notices.awaitOnly("ver-synonym", Level.WARN, "versioning is suspended; SPT left it alone");
	}

	@Test
	void thePutIsSignedWithTheBucketsCredentialAndCarriesTheConfiguredHeaders() throws Exception {
		signedWithTheBucketsCredential(2);
		signedWithTheBucketsCredential(4);
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private void signedWithTheBucketsCredential(final int authVersion) throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(true, authVersion, false, null, "127.0.0.1");
		final var headers = new HashMap<String, String>();
		headers.put("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}");
		headers.put("x-probe", "shared");
		config.val("storage-net-http-headers", headers);
		final var driver = driver(config, listener(), 5_000);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput((com.github.akurilov.commons.io.Output) results);
		final var pathCredential = Credential.getInstance("pathuser", "pathsecretpathsecretpathsecretpathsecret0");
		final var bucket = "/ver-sign" + authVersion;
		final Operation<DataItem> op = new DataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("k0", 0, ITEM_SIZE), null, bucket + "/p", pathCredential, null, 0);

		assertTrue(driver.put(op));
		assertEquals(Operation.Status.SUCC, results.await().status());

		final var bucketRequests = seen.stream().filter(r -> bucket.equals(r.path())).toList();
		assertEquals(List.of("HEAD", "GET", "PUT"), bucketRequests.stream().map(Seen::method).toList(), seen::toString);
		final var put = bucketRequests.get(2);
		assertEquals("versioning", put.query());
		assertTrue(put.auth().contains("pathuser"), "the PUT names the bucket's user: " + put.auth());
		assertFalse(put.auth().contains(CREDENTIAL.getUid()), put.auth());
		assertEquals("shared", put.probe(), "the PUT carries the shared header");
		// V4 signs x-amz-date and drops Date, as for every request.
		assertEquals(authVersion == 2 ? 1 : 0, put.dates(), "exactly one Date under V2, none under V4");
		for (final var request : bucketRequests) {
			assertTrue(request.auth().contains("pathuser"), request.toString());
			assertEquals("shared", request.probe(), request.toString());
		}
	}

	@Test
	void thePutHasADateWhenTheConfigurationGivesNone() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(true, 2, false, null, "127.0.0.1");
		config.val("storage-net-http-headers", new HashMap<String, String>());
		final var driver = driver(config, listener(), 5_000);

		driver.requestNewPath("/ver-nodate/p");

		final var put = seen.stream().filter(r -> "PUT".equals(r.method()) && "versioning".equals(r.query())).findFirst();
		assertTrue(put.isPresent(), seen::toString);
		assertEquals(1, put.get().dates(), seen::toString);
	}

	@Test
	void theStatusIsReadExactly() {
		assertEquals(state(Kind.ENABLED), read(ENABLED));
		assertEquals(state(Kind.SUSPENDED), read(SUSPENDED));
		assertEquals(state(Kind.SUSPENDED), read(SUSPENDED_MFA), "MfaDelete never counts");
		assertEquals(state(Kind.ENABLED), read("<s3:VersioningConfiguration xmlns:s3=\"" + S3_NS + "\">"
						+ "<s3:Status> Enabled </s3:Status></s3:VersioningConfiguration>"));
		assertEquals(state(Kind.NEVER), read(NEVER));
		assertEquals(state(Kind.NEVER), read("<VersioningConfiguration><Status></Status></VersioningConfiguration>"));
		assertEquals(state(Kind.NEVER), read("<VersioningConfiguration><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>"));
		assertEquals(new VersioningConfigurationXmlHandler.State(Kind.OTHER, "Disabled"),
						read("<VersioningConfiguration><Status>Disabled</Status></VersioningConfiguration>"));
	}

	@Test
	void aStatusIsReadWhateverItsPrefixNamespaceOrAttributes() {
		assertEquals(state(Kind.SUSPENDED), read(SUSPENDED_NS_DECLARED));
		assertEquals(state(Kind.ENABLED), read(ENABLED_ATTRIBUTED));
		assertEquals(state(Kind.SUSPENDED), read("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<x:VersioningConfiguration"
						+ " xmlns:x=\"urn:other\"><x:Status a=\"1\">Suspended</x:Status></x:VersioningConfiguration>"));
		assertEquals(state(Kind.NEVER), read("<VersioningConfiguration><MfaDelete><Status>Enabled</Status></MfaDelete>"
						+ "</VersioningConfiguration>"), "only the root's own Status counts");
	}

	@Test
	void aBodyThatIsNotAReadableVersioningConfigurationIsUnrecognized() {
		for (final var body : List.of(
						"",
						"not xml",
						"<VersioningConfiguration><Status>Enabled</Status>",
						"<Error><Code>AccessDenied</Code></Error>",
						"<Other><Status>Enabled</Status></Other>",
						"<s3:VersioningConfiguration><s3:Status>Enabled</s3:Status></s3:VersioningConfiguration>",
						"<VersioningConfiguration><Status>Enabled</Status><Status>Suspended</Status></VersioningConfiguration>",
						"<VersioningConfiguration><Status><b>Enabled</b></Status></VersioningConfiguration>",
						"<!DOCTYPE VersioningConfiguration [<!ENTITY s \"Suspended\">]>"
										+ "<VersioningConfiguration><Status>&s;</Status></VersioningConfiguration>",
						"<!DOCTYPE VersioningConfiguration SYSTEM \"http://127.0.0.1:1/x.dtd\"><VersioningConfiguration/>")) {
			assertEquals(state(Kind.UNRECOGNIZED), read(body), body);
		}
	}

	@Test
	void aSuspendedStatusWithANamespaceDeclarationIsLeftAlone() throws Exception {
		getVersioningBody = SUSPENDED_NS_DECLARED;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		driver.requestNewPath("/ver-suspended-ns/p");

		assertEquals(0, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-suspended-ns", Level.WARN, "versioning is suspended; SPT left it alone");
	}

	@Test
	void anAttributedEnabledStatusGetsNoPut() throws Exception {
		getVersioningBody = ENABLED_ATTRIBUTED;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		driver.requestNewPath("/ver-enabled-attr/p");

		assertEquals(0, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-enabled-attr", Level.INFO, "versioning is already enabled");
	}

	@Test
	void anUnparseableBodySendsNoPut() throws Exception {
		unrecognizedBodySendsNoPut("ver-unparseable", "<VersioningConfiguration><Status>Enabled");
		unrecognizedBodySendsNoPut("ver-emptybody", "");
	}

	@Test
	void aBodyWithAnotherRootSendsNoPut() throws Exception {
		unrecognizedBodySendsNoPut("ver-otherroot", "<ListBucketResult><Name>x</Name></ListBucketResult>");
	}

	private void unrecognizedBodySendsNoPut(final String bucket, final String body) throws Exception {
		getVersioningBody = body;
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		driver.requestNewPath("/" + bucket + "/p");

		assertEquals(0, versioningPuts(), requests::toString);
		notices.awaitOnly(bucket, Level.WARN, "could not read the versioning state (GET ?versioning returned a body that is"
						+ " not a VersioningConfiguration SPT can read); SPT sent no request to enable it");
	}

	private static VersioningConfigurationXmlHandler.State read(final String body) {
		return S3StorageDriver.versioningState(body.getBytes(StandardCharsets.UTF_8));
	}

	private static VersioningConfigurationXmlHandler.State state(final Kind kind) {
		return new VersioningConfigurationXmlHandler.State(kind, null);
	}

	@Test
	void anUnknownStatusIsLeftAlone() throws Exception {
		getVersioningBody = "<VersioningConfiguration><Status>Disabled</Status></VersioningConfiguration>";
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		driver.requestNewPath("/ver-unknown/p");

		assertEquals(0, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-unknown", Level.WARN, "the versioning status \"Disabled\" is neither Enabled nor Suspended");
	}

	@Test
	void aStatusWithALineBreakStaysOneNotice() throws Exception {
		getVersioningBody = "<VersioningConfiguration><Status>X\nINFO bucket b9: versioning is already enabled</Status>"
						+ "</VersioningConfiguration>";
		final var driver = driver(List.of(listener()), "default", 5_000, true);
		// The driver's step file is shared with earlier tests and runs: read only what this test adds.
		final var file = Path.of(FileManager.INSTANCE.logFileName(Loggers.BUCKET_VERSIONING.getName(), "first-use"));
		final long start = Files.exists(file) ? Files.size(file) : 0;

		driver.requestNewPath("/ver-forge/p");

		assertEquals(0, versioningPuts(), requests::toString);
		final var notice = notices.awaitOnly("ver-forge", Level.WARN, "is neither Enabled nor Suspended");
		assertFalse(notice.text().contains("\n"), notice::toString);
		assertTrue(notice.text().contains("\"X INFO bucket b9: versioning is already enabled\""), notice::toString);
		// What the Entry Node would relay: the file has the bucket's line and no line for b9.
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while ((!Files.exists(file) || !added(file, start).contains("ver-forge")) && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		final var lines = added(file, start).lines().toList();
		assertTrue(lines.stream().anyMatch(line -> line.startsWith("WARN bucket ver-forge:")), lines::toString);
		assertFalse(lines.stream().anyMatch(line -> line.startsWith("INFO bucket b9")), lines::toString);
	}

	private static String added(final Path file, final long start) throws IOException {
		final byte[] bytes = Files.readAllBytes(file);
		return new String(bytes, (int) Math.min(start, bytes.length), (int) Math.max(0, bytes.length - start),
						StandardCharsets.UTF_8);
	}

	@Test
	void aLongStatusIsCutShort() {
		assertEquals("a".repeat(64) + "...", S3StorageDriver.quotedStatus("a".repeat(100)));
		assertEquals("x y z", S3StorageDriver.quotedStatus(" x\r\n\ty \u2028z "));
	}

	@Test
	void anOverrideOfTheResponseHookStillDecides() throws Exception {
		final var driver = hookDriver(true, false);

		driver.requestNewPath("/ver-hook1/p");

		assertEquals(1, driver.responses, "the first-use check calls the old protected response hook");
		assertEquals(0, driver.enables, "the override decided not to enable");
		assertEquals(0, versioningPuts(), requests::toString);
		Thread.sleep(200);
		assertEquals(List.of(), notices.forBucket("ver-hook1"), "the override logged nothing, so SPT logs nothing");
	}

	@Test
	void anOverrideOfTheEnableHookThatSendsNothingSuppressesThePut() throws Exception {
		final var driver = hookDriver(false, true);

		driver.requestNewPath("/ver-hook2/p");

		assertEquals(1, driver.responses);
		assertEquals(1, driver.enables, "a never-versioned bucket goes to the old protected enable hook");
		assertEquals(0, versioningPuts(), requests::toString);
		Thread.sleep(200);
		assertEquals(List.of(), notices.forBucket("ver-hook2"), notices.forBucket("ver-hook2")::toString);
	}

	@Test
	void anOverrideOfTheEnableHookThatCallsSuperEnables() throws Exception {
		final var driver = hookDriver(false, false);

		driver.requestNewPath("/ver-hook3/p");

		assertEquals(1, driver.enables);
		assertEquals(1, versioningPuts(), requests::toString);
		notices.awaitOnly("ver-hook3", Level.INFO, "versioning enabled by this run");
	}

	/** A driver extension that overrides the two protected hooks, as one written for 5.17.0 can. */
	@SuppressWarnings("rawtypes")
	private static final class HookDriver extends S3StorageDriver {

		private final boolean decideInResponseHook;
		private final boolean suppressEnable;
		volatile int responses;
		volatile int enables;

		@SuppressWarnings("unchecked")
		HookDriver(final Config storageConfig, final boolean decideInResponseHook, final boolean suppressEnable)
						throws Exception {
			super("first-use", new SeedDataInput(1, ITEM_SIZE, 1, true), storageConfig, false, 16);
			this.decideInResponseHook = decideInResponseHook;
			this.suppressEnable = suppressEnable;
		}

		@Override
		protected void handleCheckBucketVersioningResponse(
						final io.netty.handler.codec.http.FullHttpResponse response, final String nodeAddr, final String uri) {
			responses++;
			if (!decideInResponseHook) {
				super.handleCheckBucketVersioningResponse(response, nodeAddr, uri);
			}
		}

		@Override
		protected void enableBucketVersioning(final String nodeAddr, final String uri) {
			enables++;
			if (!suppressEnable) {
				super.enableBucketVersioning(nodeAddr, uri);
			}
		}
	}

	private HookDriver hookDriver(final boolean decideInResponseHook, final boolean suppressEnable) throws Exception {
		getVersioningBody = NEVER;
		final Config config = S3StorageDriverTest.baseConfig(true, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", 5_000);
		final var endpoint = listener();
		config.val("storage-net-node-port", Integer.parseInt(endpoint.substring(endpoint.indexOf(':') + 1)));
		final var driver = new HookDriver(config.configVal("storage"), decideInResponseHook, suppressEnable);
		resources.add(driver);
		driver.start();
		return driver;
	}

	private long versioningPuts() {
		return requests.stream().filter(r -> "PUT".equals(r.method()) && "versioning".equals(r.query())).count();
	}

	/** Answers a bucket's {@code ?versioning} request; false for an object-level one. */
	private boolean respondToVersioning(final HttpExchange exchange, final String method) throws IOException {
		if (exchange.getRequestURI().getRawPath().indexOf('/', 1) >= 0) {
			return false;
		}
		if ("GET".equals(method)) {
			if (!sleep(getVersioningDelay)) {
				return true;
			}
			final byte[] body = (getVersioningStatus / 100 == 2
							? getVersioningBody
							: "<Error><Code>AccessDenied</Code></Error>").getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(getVersioningStatus, body.length == 0 ? -1 : body.length);
			exchange.getResponseBody().write(body);
			return true;
		}
		if (!sleep(putVersioningDelayMillis)) {
			return true;
		}
		exchange.sendResponseHeaders(putVersioningStatus, -1);
		return true;
	}

	private static boolean sleep(final long millis) {
		if (millis <= 0) {
			return true;
		}
		try {
			Thread.sleep(millis);
			return true;
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Test
	void anInterruptEndsAPendingCheck() throws Exception {
		headDelayMillis = 30_000;
		final var driver = driver(List.of(listener()), "default", 60_000, false);
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final var thread = Thread.ofPlatform().start(() -> {
			try {
				driver.requestNewPath("/bench-0/p");
			} catch (final Throwable e) {
				failure.set(e);
			}
		});
		assertTrue(headArrived.await(10, TimeUnit.SECONDS), "the check reached the listener");

		thread.interrupt();
		thread.join(5_000);

		assertFalse(thread.isAlive(), "the interrupt ended the wait");
		assertInstanceOf(InterruptedException.class, failure.get());
		final int requestsAtInterrupt = requests.size();
		assertTimeoutPreemptively(java.time.Duration.ofSeconds(10), driver::close, "the driver closes without waiting for the check");
		assertEquals(requestsAtInterrupt, requests.size(), "nothing is sent after the interrupt: " + requests);
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private List<Operation.Status> putTwice(final S3StorageDriver driver, final String path) throws Exception {
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput((com.github.akurilov.commons.io.Output) results);
		final var outcomes = new ArrayList<Operation.Status>();
		for (int i = 0; i < 2; i++) {
			final Operation<DataItem> op = new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("k" + i, i, ITEM_SIZE), null, path, CREDENTIAL, null, 0);
			assertTrue(driver.put(op));
			outcomes.add(results.await().status());
		}
		return outcomes;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private S3StorageDriver driver(final List<String> endpoints, final String mode, final int timeoutMillis,
					final boolean versioning) throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(versioning, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", timeoutMillis);
		if ("round-robin".equals(mode)) {
			config.val("storage-net-node-addrs", endpoints);
			config.val("storage-net-endpoint-selection", "round-robin");
			config.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		} else {
			final var endpoint = endpoints.get(0);
			config.val("storage-net-node-port", Integer.parseInt(endpoint.substring(endpoint.indexOf(':') + 1)));
		}
		return start(config);
	}

	@SuppressWarnings("rawtypes")
	private S3StorageDriver driver(final Config config, final String endpoint, final int timeoutMillis) throws Exception {
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", timeoutMillis);
		config.val("storage-net-node-port", Integer.parseInt(endpoint.substring(endpoint.indexOf(':') + 1)));
		return start(config);
	}

	@SuppressWarnings("rawtypes")
	private S3StorageDriver start(final Config config) throws Exception {
		final var driver = (S3StorageDriver) new S3StorageDriverExtension().create(
						"first-use", new SeedDataInput(1, ITEM_SIZE, 1, true), config.configVal("storage"), false, 16);
		resources.add(driver);
		driver.start();
		return driver;
	}

	private String listener() throws IOException {
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();
		servers.add(server);
		return "127.0.0.1:" + server.getAddress().getPort();
	}

	private static int closedPort() throws IOException {
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			final var uri = exchange.getRequestURI();
			final var method = exchange.getRequestMethod();
			requests.add(new Captured(method, uri.getRawPath(), uri.getRawQuery()));
			final var headers = exchange.getRequestHeaders();
			final var dates = headers.get("Date");
			seen.add(new Seen(method, uri.getRawPath(), uri.getRawQuery(), headers.getFirst("Authorization"),
							headers.getFirst("x-probe"), dates == null ? 0 : dates.size()));
			if ("versioning".equals(uri.getRawQuery()) && respondToVersioning(exchange, method)) {
				return;
			}
			if ("HEAD".equals(method)) {
				headArrived.countDown();
				if (headDelayMillis > 0) {
					try {
						Thread.sleep(headDelayMillis);
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					}
				}
				exchange.sendResponseHeaders(headStatus, -1);
				return;
			}
			if ("PUT".equals(method)) {
				exchange.getResponseHeaders().set("ETag", "\"etag\"");
			}
			final byte[] body = "GET".equals(method) ? "<VersioningConfiguration/>".getBytes(StandardCharsets.UTF_8) : new byte[0];
			exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
			if (body.length > 0) {
				exchange.getResponseBody().write(body);
			}
		}
	}
}
