package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.driver;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.operation;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.StandaloneDeletePreparable;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.script.ScriptEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;

/** Real-driver and real-SDK canary backed by a deterministic loopback S3 endpoint. */
final class S3AwsDeleteRequestIntegrationTest {

	private static final long RESULT_TIMEOUT_SECONDS = 5;
	private static final String DELETE_RESULT_NAMESPACE = " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"";

	private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private HttpServer server;
	private volatile String multiDeleteResponse;
	private volatile Function<CapturedRequest, Integer> headResponseStatus = ignored -> 200;

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(executor);
		server.start();
	}

	@AfterEach
	void stopServer() {
		if (server != null) {
			server.stop(0);
		}
		executor.shutdownNow();
	}

	@Test
	void currentKeyDeleteRunsThroughRealAwsSdkDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(driver, operation(target("current-key", null)));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			assertCompletedTransportTiming(result);
			final CapturedRequest request = onlyRequest("DELETE");
			assertEquals("/bucket/current-key", request.rawPath());
			assertNull(request.rawQuery());
		}
	}

	@Test
	void realFactoryConstructionAndCloseDoNotCreateStandaloneDeleteResources() throws Exception {
		final var standaloneResourceCreations = new AtomicInteger();
		final var factory = new S3AwsStorageDriverFactory<IntegrityManifestDataItem, DeleteRequestOperation>(
						ignored -> {
							standaloneResourceCreations.incrementAndGet();
							throw new AssertionError("standalone DELETE resources must remain unused");
						});
		for (final String workload : List.of("read", "write", "list", "mixed")) {
			final Config storageConfig = scenarioConfig(101L).configVal("storage");
			try (final var ignored = factory.create(
							workload + "-resource-canary",
							DataInput.instance(
											null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
							storageConfig,
							false,
							16)) {
				ignored.start();
				assertEquals(0, standaloneResourceCreations.get());
			}
		}

		assertEquals(0, standaloneResourceCreations.get());
	}

	@Test
	void exactVersionDeleteRunsThroughRealAwsSdkDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("exact-key", "v+1/=")));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			assertCompletedTransportTiming(result);
			final CapturedRequest request = onlyRequest("DELETE");
			assertEquals("/bucket/exact-key", request.rawPath());
			assertEquals("versionId=v%2B1%2F%3D", request.rawQuery());
		}
	}

	@Test
	void realFactoryPreparesOneTimingResourceSetBeforeCurrentAndExactDelete() throws Exception {
		final var standaloneResourceCreations = new AtomicInteger();
		final var factory = new S3AwsStorageDriverFactory<IntegrityManifestDataItem, DeleteRequestOperation>(
						configuration -> {
							standaloneResourceCreations.incrementAndGet();
							return S3AwsStandaloneDeleteResources.create(configuration);
						});
		final Config storageConfig = scenarioConfig(102L).configVal("storage");

		try (final var driver = factory.create(
						"factory-delete-resource-canary",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storageConfig,
						false,
						16)) {
			assertEquals(0, standaloneResourceCreations.get());
			assertTrue(driver instanceof StandaloneDeletePreparable);
			driver.start();
			((StandaloneDeletePreparable) driver).prepareStandaloneDelete();
			assertEquals(1, standaloneResourceCreations.get());

			final DeleteRequestOperation current = execute(
							driver, operation(target("factory-current", null)));
			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, current.deleteResult().outcome());
			assertCompletedTransportTiming(current);
			assertEquals(1, standaloneResourceCreations.get());
			final CapturedRequest currentRequest = onlyRequest("DELETE");
			assertEquals("/bucket/factory-current", currentRequest.rawPath());
			assertNull(currentRequest.rawQuery());

			final DeleteRequestOperation exact = execute(
							driver, operation(target("factory-exact", "version+1/=")));
			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, exact.deleteResult().outcome());
			assertCompletedTransportTiming(exact);
			assertEquals(1, standaloneResourceCreations.get());
			final CapturedRequest exactRequest = requests.stream()
							.filter(request -> "/bucket/factory-exact".equals(request.rawPath()))
							.findFirst()
							.orElseThrow();
			assertEquals("versionId=version%2B1%2F%3D", exactRequest.rawQuery());
		}
	}

	@Test
	void concurrentFirstDeleteUseCreatesAndClosesOneResourceSet() throws Exception {
		final S3AsyncClient standaloneClient = mock(S3AsyncClient.class);
		final SdkAsyncHttpClient standaloneHttpClient = mock(SdkAsyncHttpClient.class);
		when(standaloneClient.deleteObject(any(DeleteObjectRequest.class)))
						.thenReturn(java.util.concurrent.CompletableFuture.completedFuture(
										DeleteObjectResponse.builder().build()));
		final var standaloneResourceCreations = new AtomicInteger();
		final var factory = new S3AwsStorageDriverFactory<IntegrityManifestDataItem, DeleteRequestOperation>(
						ignored -> {
							standaloneResourceCreations.incrementAndGet();
							return new S3AwsStandaloneDeleteResources(
											standaloneClient, standaloneHttpClient);
						});
		final Config storageConfig = scenarioConfig(103L).configVal("storage");
		final var driver = factory.create(
						"concurrent-delete-resource-canary",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 32, false),
						storageConfig,
						false,
						32);
		try {
			final int requestCount = 32;
			final var start = new java.util.concurrent.CountDownLatch(1);
			final var results = new ArrayList<DeleteRequestOperation>();
			try (final var callers = Executors.newVirtualThreadPerTaskExecutor()) {
				final var futures = new ArrayList<java.util.concurrent.Future<DeleteRequestOperation>>();
				for (int i = 0; i < requestCount; i++) {
					final DeleteRequestOperation request = operation(target("key-" + i, null));
					futures.add(callers.submit(() -> {
						assertTrue(start.await(3, TimeUnit.SECONDS));
						driver.execute(request).get(3, TimeUnit.SECONDS);
						return request;
					}));
				}
				start.countDown();
				for (final var future : futures) {
					results.add(future.get(3, TimeUnit.SECONDS));
				}
			}

			assertEquals(1, standaloneResourceCreations.get());
			assertEquals(requestCount, results.size());
			assertTrue(results.stream().allMatch(
							result -> result.deleteResult().outcome() == DeleteRequestOutcome.FULL_SUCCESS));
			verify(standaloneClient, times(requestCount))
							.deleteObject(any(DeleteObjectRequest.class));
		} finally {
			driver.close();
		}

		verify(standaloneClient).close();
		verify(standaloneHttpClient).close();
	}

	@Test
	void verificationHeadClassifiesCurrentExactAbsentAndUnresolvedThroughRealAwsSdk()
					throws Exception {
		headResponseStatus = request -> request.rawPath().contains("absent") ? 404
						: request.rawPath().contains("unresolved") ? 503 : 200;
		try (final var driver = newDriver()) {
			driver.start();
			assertEquals(DeleteVerificationProbe.Presence.PRESENT,
							driver.presence(target("current-key", null)));
			assertEquals(DeleteVerificationProbe.Presence.ABSENT,
							driver.presence(target("absent", "v+1/=")));
			assertEquals(DeleteVerificationProbe.Presence.UNRESOLVED,
							driver.presence(target("unresolved", null)));
		}

		final List<CapturedRequest> heads = requests.stream()
						.filter(request -> "HEAD".equals(request.method())).toList();
		assertTrue(heads.size() >= 3, "the SDK may retry unresolved service responses internally");
		assertNull(heads.get(0).rawQuery());
		assertTrue(heads.stream().anyMatch(
						request -> "versionId=v%2B1%2F%3D".equals(request.rawQuery())));
	}

	@Test
	void nativeTimingConnectionReturnsToSingleSlotPoolForTheNextRequest() throws Exception {
		try (final var driver = newDriver()) {
			final ResultOutput output = new ResultOutput();
			driver.operationResultOutput(output);
			driver.start();

			assertTrue(driver.put(operation(target("first-key", null))));
			final DeleteRequestOperation first = output.await();
			assertNotNull(first);
			assertCompletedTransportTiming(first);

			assertTrue(driver.put(operation(target("second-key", null))));
			final DeleteRequestOperation second = output.await();
			assertNotNull(second, "the one-slot CRT pool did not release its first connection");
			assertCompletedTransportTiming(second);
			assertEquals(2, requests.stream()
							.filter(request -> "DELETE".equals(request.method()))
							.count());
		}
	}

	@Test
	void batchDeleteRunsAsOneRealNonQuietSdkRequest() throws Exception {
		multiDeleteResponse = "<DeleteResult" + DELETE_RESULT_NAMESPACE + ">"
						+ "<Deleted><Key>one</Key></Deleted>"
						+ "<Deleted><Key>two</Key><VersionId>version-2</VersionId></Deleted>"
						+ "</DeleteResult>";
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("one", null), target("two", "version-2")));
			awaitCompletionAccounting(driver);

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyRequest("POST");
			assertEquals("delete", request.rawQuery());
			assertTrue(request.body().contains("<Key>one</Key>"));
			assertTrue(request.body().contains("<VersionId>version-2</VersionId>"));
			assertTrue(request.body().contains("<Quiet>false</Quiet>"));
			assertEquals(1, driver.scheduledOpCount());
			assertEquals(1, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount());
			assertCompletedTransportTiming(result);
		}
	}

	private static void assertCompletedTransportTiming(final DeleteRequestOperation result) {
		assertTrue(result.transportRequestLatency() > 0,
						"AWS CRT send-start/receive-start metrics did not populate request latency");
		assertEquals(result.respTimeStart(), result.responseFirstByteTime());
		assertTrue(result.respTimeDone() >= result.responseFirstByteTime());
		assertTrue(result.duration() > 0);
		assertTrue(result.duration() >= result.transportRequestLatency());
	}

	private static void awaitCompletionAccounting(
					final S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while ((driver.completedOpCount() != 1 || driver.activeOpCount() != 0)
						&& System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
	}

	@Test
	void partialSdkResponsePreservesFailureAfterDriverTimingFinalization() throws Exception {
		multiDeleteResponse = "<DeleteResult" + DELETE_RESULT_NAMESPACE + ">"
						+ "<Deleted><Key>accepted</Key></Deleted>"
						+ "<Error><Key>denied</Key><VersionId>version-3</VersionId>"
						+ "<Code>AccessDenied</Code><Message>denied</Message></Error>"
						+ "</DeleteResult>";
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("accepted", null), target("denied", "version-3")));

			assertEquals(DeleteRequestOutcome.PARTIAL, result.deleteResult().outcome());
			assertEquals(1, result.deleteResult().acceptedObjectCount());
			assertEquals(1, result.deleteResult().failedObjectCount());
			assertEquals(com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC, result.status());
			assertCompletedTransportTiming(result);
		}
	}

	@Test
	@Timeout(30)
	void strictPreValidationPublishesUnattemptedAwsEngineEvidence(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 777;
		final Path manifest = publishManifest(tempDir, runId);
		final String stepId = tempDir.getFileName() + "-aws-pre-validation";
		final String scenario = contractScenario(
						manifest, stepId,
						"\"preValidation\": true, \"postVerification\": true, ");
		headResponseStatus = ignored -> 404;

		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		assertTrue(extensions.stream().anyMatch(extension -> "Load".equals(extension.id())));
		assertTrue(extensions.stream().anyMatch(extension -> "s3-aws".equals(extension.id())));
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			metrics.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
			ScenarioUtil.configure(engine, extensions, config, metrics);
			final RuntimeException failure = assertThrows(
							RuntimeException.class,
							() -> new RunImpl("AWS strict pre-validation canary", scenario, engine, runId).run());
			assertNotNull(IntegrityTerminalException.find(failure));

			final var terminal = terminalStep(metrics, stepId);
			assertEquals(OpType.DELETE, terminal.opType);
			assertNotNull(terminal.deleteMetrics);
			assertEquals(2, terminal.deleteMetrics.objectSelected());
			assertEquals(0, terminal.deleteMetrics.objectAttempted());
			assertEquals(2, terminal.deleteMetrics.objectUnattempted());
			assertEquals(1, terminal.deleteMetrics.currentKeyCount());
			assertEquals(1, terminal.deleteMetrics.exactVersionCount());
			assertEquals(2, terminal.deleteMetrics.verification().preValidationFailures());
			assertTrue(terminal.deleteMetrics.verification().preValidationComplete());
			assertFalse(terminal.deleteMetrics.verification().postVerificationComplete());
			assertTrue(terminal.deleteMetrics.verification().postVerificationSkipped());
			assertEquals(-1, terminal.deleteMetrics.postVerificationNanos());
		}

		LogUtil.flushAll();
		final List<String> objects = Files.readAllLines(artifact(Loggers.DELETE_OBJECTS, stepId));
		final List<String> verification = Files.readAllLines(
						artifact(Loggers.DELETE_VERIFICATION, stepId));
		assertEquals(3, objects.size());
		assertTrue(objects.subList(1, objects.size()).stream()
						.allMatch(row -> row.contains(",unattempted,none,")));
		assertEquals(3, verification.size());
		assertTrue(verification.subList(1, verification.size()).stream()
						.allMatch(row -> row.contains(",unattempted,true,absent,true,unattempted,")));
		assertEquals(2, requests.stream().filter(request -> "HEAD".equals(request.method())).count());
		assertEquals(0, requests.stream()
						.filter(request -> "DELETE".equals(request.method()) || "POST".equals(request.method()))
						.count());
	}

	@Test
	@Timeout(30)
	void partialFailureJoinsPostVerificationAcrossAwsMetricsAndArtifacts(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 778;
		final Path manifest = publishManifest(tempDir, runId);
		final String stepId = tempDir.getFileName() + "-aws-post-verification";
		final String scenario = contractScenario(
						manifest, stepId,
						"\"preValidation\": false, \"postVerification\": true, ");
		multiDeleteResponse = "<DeleteResult" + DELETE_RESULT_NAMESPACE + ">"
						+ "<Deleted><Key>alpha</Key></Deleted>"
						+ "<Error><Key>denied</Key><VersionId>version-3</VersionId>"
						+ "<Code>AccessDenied</Code><Message>denied</Message></Error>"
						+ "</DeleteResult>";
		headResponseStatus = request -> request.rawPath().endsWith("/alpha") ? 404 : 200;

		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			metrics.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
			ScenarioUtil.configure(engine, extensions, config, metrics);
			final RuntimeException failure = assertThrows(
							RuntimeException.class,
							() -> new RunImpl("AWS post-verification join canary", scenario, engine, runId).run());
			final var integrityFailure = IntegrityTerminalException.find(failure);
			assertNotNull(integrityFailure);
			assertTrue(integrityFailure.getMessage().contains("failed-object budget"));

			final var delete = terminalStep(metrics, stepId).deleteMetrics;
			assertNotNull(delete);
			assertEquals(2, delete.objectAttempted());
			assertEquals(1, delete.objectAccepted());
			assertEquals(1, delete.objectFailed());
			assertEquals(0, delete.objectUnattempted());
			assertEquals(1, delete.currentKeyCount());
			assertEquals(1, delete.exactVersionCount());
			assertEquals(1, delete.verification().acceptedAbsent());
			assertEquals(1, delete.verification().failedPresent());
			assertEquals(1, delete.verification().residualCount());
		}

		LogUtil.flushAll();
		final List<String> objects = Files.readAllLines(artifact(Loggers.DELETE_OBJECTS, stepId));
		final List<String> verification = Files.readAllLines(
						artifact(Loggers.DELETE_VERIFICATION, stepId));
		final List<String> residual = Files.readAllLines(artifact(Loggers.DELETE_RESIDUAL, stepId));
		assertTrue(objects.stream().anyMatch(row -> row.contains(",accepted,none,")));
		assertTrue(objects.stream().anyMatch(row -> row.contains(",failed,operational,")));
		assertTrue(verification.stream()
						.anyMatch(row -> row.contains(",accepted,false,disabled,true,absent,")));
		assertTrue(verification.stream()
						.anyMatch(row -> row.contains(",failed,false,disabled,true,present,")));
		assertEquals(2, residual.size());
		assertTrue(residual.get(1).contains("denied"));
		assertEquals(1, requests.stream()
						.filter(request -> "POST".equals(request.method()) && "delete".equals(request.rawQuery()))
						.count());
	}

	private Config scenarioConfig(final long runId) throws Exception {
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final List<Map<String, Object>> schemas = Extension.load(classLoader).stream()
						.map(Extension::schemaProvider)
						.filter(Objects::nonNull)
						.map(provider -> {
							try {
								return provider.schema();
							} catch (final Exception failure) {
								throw new IllegalStateException(failure);
							}
						})
						.collect(Collectors.toCollection(ArrayList::new));
		SchemaProvider.resolve(Constants.APP_NAME, classLoader).stream()
						.findFirst()
						.ifPresent(schemas::add);
		final Config config = new BundledDefaultsProvider().config("-", TreeUtil.reduceForest(schemas));
		config.val("run-id", runId);
		config.val("storage-driver-type", "s3-aws");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-auth-uid", CREDENTIAL.getUid());
		config.val("storage-auth-secret", CREDENTIAL.getSecret());
		config.val("storage-region", "us-east-1");
		config.val("storage-net-node-addrs", List.of("127.0.0.1"));
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-ssl-enabled", false);
		config.val("output-color", false);
		return config;
	}

	private static Path publishManifest(final Path tempDir, final long runId) throws IOException {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\n"
										+ "bucket,alpha,9,\n"
										+ "bucket,denied,7,version-3\n",
						StandardCharsets.UTF_8);
		IntegrityManifestCompletion.create(
						manifest,
						runId,
						IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"spt-cli-items-stager-v1",
						2,
						2,
						2)
						.publish(manifest);
		return manifest;
	}

	private static String contractScenario(
					final Path manifest, final String stepId, final String verificationFlags) {
		final String manifestPath = manifest.toString().replace("\\", "\\\\").replace("\"", "\\\"");
		return """
						var setupStartedNanos = com.dell.spt.base.load.step.DurationTime.monotonicEpochNanos();
						var deleteSelection = com.dell.spt.base.item.op.deletion.StandaloneDeleteSelection.fromManifest("%s");
						DeleteLoad.config({
						  "storage": {"driver": {"type": "s3-aws", "limit": {"concurrency": 1}},
						    "integrity": {"mode": "metadata", "algorithm": "sha256",
						      "input": {"provenance": "cli_stager", "expectedProducerId": "spt-cli-items-stager-v1"}}},
						  "item": {"type": "data", "input": {"file": "%s"}},
						  "load": {"batch": {"size": 2}, "op": {"type": "delete",
						    "delete": {"standalone": true, %s"verificationTimeoutMillis": 25,
						      "batchSize": 2, "duration": false, "selectionOrder": "canonical",
						      "selected": deleteSelection.selected(),
						      "selectedCurrentKey": deleteSelection.selectedCurrentKey(),
						      "selectedExactVersion": deleteSelection.selectedExactVersion(),
						      "selectedBuckets": deleteSelection.selectedBuckets(),
						      "workflowStartedEpochNanos": setupStartedNanos},
						    "failureBudget": {"mode": "fixed", "maxFailedObjects": 100000,
						      "maxFailurePercent": 0, "graceSeconds": 30},
						    "recycle": {"mode": false}, "retry": false, "wait": {"finish": true}},
						    "step": {"id": "%s"}},
						  "output": {"metrics": {"summary": {"persist": true}}}
						}).run();
						""".formatted(manifestPath, manifestPath, verificationFlags, stepId);
	}

	private static com.dell.spt.base.metrics.TerminalStepEntry terminalStep(
					final MetricsManagerImpl metrics, final String stepId) {
		return metrics.getTerminalSteps().stream()
						.filter(entry -> !entry.distributed)
						.filter(entry -> stepId.equals(entry.stepId))
						.findFirst()
						.orElseThrow();
	}

	private static Path artifact(final org.apache.logging.log4j.Logger logger, final String stepId)
					throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId));
	}

	private S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> newDriver()
					throws Exception {
		final TimingClient timingClient = newTimingClient();
		return driver(
						newClient(), newClient(), timingClient.s3Client(), timingClient.httpClient());
	}

	private S3AsyncClient newClient() {
		final URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
		final var credentials = AwsBasicCredentials.create(CREDENTIAL.getUid(), CREDENTIAL.getSecret());
		return S3AsyncClient.builder()
						.credentialsProvider(StaticCredentialsProvider.create(credentials))
						.region(Region.US_EAST_1)
						.endpointOverride(endpoint)
						.forcePathStyle(true)
						.httpClientBuilder(AwsCrtAsyncHttpClient.builder().maxConcurrency(4))
						.overrideConfiguration(ClientOverrideConfiguration.builder()
										.apiCallAttemptTimeout(Duration.ofSeconds(2))
										.apiCallTimeout(Duration.ofSeconds(3))
										.build())
						.build();
	}

	private TimingClient newTimingClient() {
		final URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
		final var credentials = AwsBasicCredentials.create(CREDENTIAL.getUid(), CREDENTIAL.getSecret());
		final var httpClient = new DeleteTimingAsyncHttpClient(
						CrtDeleteTimingAsyncHttpClient.builder()
										.operationAttribute(DeleteTimingAsyncHttpClient.DELETE_OPERATION)
										.maxConcurrency(1)
										.build());
		final var s3Client = S3AsyncClient.builder()
						.credentialsProvider(StaticCredentialsProvider.create(credentials))
						.region(Region.US_EAST_1)
						.endpointOverride(endpoint)
						.forcePathStyle(true)
						.httpClient(httpClient)
						.overrideConfiguration(ClientOverrideConfiguration.builder()
										.apiCallAttemptTimeout(Duration.ofSeconds(2))
										.apiCallTimeout(Duration.ofSeconds(3))
										.build())
						.build();
		return new TimingClient(s3Client, httpClient);
	}

	private static DeleteRequestOperation execute(
					final S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver,
					final DeleteRequestOperation operation) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(operation));
		final DeleteRequestOperation result = output.await();
		assertNotNull(result, "real AWS SDK DELETE did not complete before the canary timeout");
		return result;
	}

	private CapturedRequest onlyRequest(final String method) {
		final List<CapturedRequest> matches = requests.stream()
						.filter(request -> method.equals(request.method()))
						.toList();
		assertEquals(1, matches.size());
		return matches.get(0);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
		requests.add(new CapturedRequest(
						exchange.getRequestMethod(),
						exchange.getRequestURI().getRawPath(),
						exchange.getRequestURI().getRawQuery(),
						body));
		if ("HEAD".equals(exchange.getRequestMethod())) {
			exchange.sendResponseHeaders(headResponseStatus.apply(requests.get(requests.size() - 1)), -1);
		} else if ("DELETE".equals(exchange.getRequestMethod())) {
			exchange.sendResponseHeaders(204, -1);
		} else if ("POST".equals(exchange.getRequestMethod())
						&& "delete".equals(exchange.getRequestURI().getRawQuery())) {
			final byte[] responseBody = multiDeleteResponse.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(200, responseBody.length);
			exchange.getResponseBody().write(responseBody);
		} else {
			exchange.sendResponseHeaders(500, -1);
		}
		exchange.close();
	}

	private record CapturedRequest(String method, String rawPath, String rawQuery, String body) {}

	private record TimingClient(
					S3AsyncClient s3Client, DeleteTimingAsyncHttpClient httpClient) {}

	private static final class ResultOutput implements Output<DeleteRequestOperation> {

		private final LinkedBlockingQueue<DeleteRequestOperation> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final DeleteRequestOperation value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<DeleteRequestOperation> values, final int from, final int to) {
			int count = 0;
			for (int i = from; i < to; i++) {
				if (!results.offer(values.get(i))) {
					break;
				}
				count++;
			}
			return count;
		}

		@Override
		public int put(final List<DeleteRequestOperation> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<DeleteRequestOperation> getInput() {
			return null;
		}

		@Override
		public void close() {
			results.clear();
		}

		private DeleteRequestOperation await() throws InterruptedException {
			return results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		}
	}
}
