package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.S3DeleteRequestTestFixture.operation;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3DeleteRequestTestFixture.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestAssembler;
import com.dell.spt.base.item.op.deletion.DeleteFailureClassification;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteTargetOutcome;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.dell.spt.base.item.op.deletion.SeededDeleteCleanupFinalizer;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.script.ScriptEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Real-driver canary backed only by a deterministic loopback S3 protocol endpoint. */
final class S3DeleteRequestIntegrationTest {

	private static final long RESULT_TIMEOUT_SECONDS = 5;

	private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private HttpServer server;
	private volatile String multiDeleteResponse;
	private volatile Function<String, String> multiDeleteResponseFactory;
	private volatile String listResponse;
	private volatile Function<CapturedRequest, String> listResponseFactory;
	private volatile Function<CapturedRequest, Integer> headResponseStatus = ignored -> 200;
	private volatile Function<CapturedRequest, Integer> deleteResponseStatus = ignored -> 204;
	private volatile int multiDeleteStatus = 200;
	private final AtomicInteger putResponseCount = new AtomicInteger();

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(
						new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
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
	void currentKeyDeleteRunsThroughRealNettyDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(driver, operation(target("current key", null)));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyDeleteRequest();
			assertEquals("DELETE", request.method());
			assertEquals("/bucket/current%20key", request.rawPath());
			assertNull(request.rawQuery());
			assertTrue(request.authorization().startsWith(S3Api.AUTH_V4_PREFIX));
		}
	}

	@Test
	void exactVersionDeleteRunsThroughRealNettyDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("exact/key", "v+1/=")));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyDeleteRequest();
			assertEquals("/bucket/exact/key", request.rawPath());
			assertEquals("versionId=v%2B1%2F%3D", request.rawQuery());
		}
	}

	@Test
	void verificationHeadClassifiesCurrentExactAbsentAndUnresolvedThroughRealNettyPath()
					throws Exception {
		headResponseStatus = request -> request.rawPath().contains("absent") ? 404
						: request.rawPath().contains("unresolved") ? 503 : 200;
		try (final var driver = newDriver()) {
			driver.start();
			assertEquals(DeleteVerificationProbe.Presence.PRESENT,
							driver.presence(target("current key", null)));
			assertEquals(DeleteVerificationProbe.Presence.ABSENT,
							driver.presence(target("absent", "v+1/=")));
			assertEquals(DeleteVerificationProbe.Presence.UNRESOLVED,
							driver.presence(target("unresolved", null)));
		}

		final List<CapturedRequest> heads = requests.stream()
						.filter(request -> "HEAD".equals(request.method())).toList();
		assertEquals(3, heads.size());
		assertEquals("/bucket/current%20key", heads.get(0).rawPath());
		assertNull(heads.get(0).rawQuery());
		assertEquals("versionId=v%2B1%2F%3D", heads.get(1).rawQuery());
		assertTrue(heads.stream().allMatch(
						request -> request.authorization().startsWith(S3Api.AUTH_V4_PREFIX)));
	}

	@Test
	void batchDeleteOwnsOneRequestPermitAndTimingSample() throws Exception {
		multiDeleteResponse = "<DeleteResult>"
						+ "<Deleted><Key>one</Key></Deleted>"
						+ "<Deleted><Key>two</Key><VersionId>version-2</VersionId></Deleted>"
						+ "</DeleteResult>";
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("one", null), target("two", "version-2")));
			awaitCompletionAccounting(driver);

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			assertEquals(1, requests.stream().filter(request -> "POST".equals(request.method())).count());
			assertEquals(1, driver.scheduledOpCount());
			assertEquals(1, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount());
			assertTrue(result.reqTimeStart() > 0);
			assertTrue(result.requestFirstByteTime() >= result.reqTimeStart());
			assertTrue(result.requestFirstByteTime() <= result.reqTimeDone());
			assertTrue(result.reqTimeDone() >= result.reqTimeStart());
			assertEquals(result.respTimeStart(), result.responseFirstByteTime());
			assertTrue(result.responseFirstByteTime() >= result.reqTimeDone());
			assertTrue(result.duration() > 0);
		}
	}

	@Test
	void canonicalManifestFlowsThroughAssemblerAndRealNettyDelete(@TempDir final Path tempDir)
					throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\n"
										+ "bucket,alpha,9,\n"
										+ "bucket,\"comma,key\",7,version-comma\n",
						StandardCharsets.UTF_8);
		multiDeleteResponse = "<DeleteResult>"
						+ "<Deleted><Key>alpha</Key></Deleted>"
						+ "<Deleted><Key>comma,key</Key><VersionId>version-comma</VersionId></Deleted>"
						+ "</DeleteResult>";

		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(0);
		builder.opType(OpType.DELETE).credentialInput(new ConstantValueInputImpl<>(
						S3DeleteRequestTestFixture.CREDENTIAL));
		final var assembler = new DeleteRequestAssembler(builder, 2);
		final var items = new ArrayList<IntegrityManifestDataItem>();
		final var operations = new ArrayList<DeleteRequestOperation>();
		try (final var input = new IntegrityManifestItemInput(manifest);
						final var driver = newDriver()) {
			assertEquals(2, input.get(items, 2));
			assertEquals(1, assembler.assemble(items, operations).emittedOperationCount());
			assertEquals(1, operations.size());

			final DeleteRequestOperation result = execute(driver, operations.get(0));
			awaitCompletionAccounting(driver);

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			assertEquals(2, result.deleteResult().acceptedObjectCount());
			assertEquals(0, result.deleteResult().failedObjectCount());
			assertEquals("alpha", result.deleteRequest().targets().get(0).key());
			assertEquals("comma,key", result.deleteRequest().targets().get(1).key());
			assertEquals("version-comma", result.deleteRequest().targets().get(1).versionId());
			assertEquals(1, driver.scheduledOpCount());
			assertEquals(1, driver.completedOpCount());
			assertEquals(1, requests.stream().filter(request -> "POST".equals(request.method())).count());
		}
	}

	@Test
	void cliContractRunsThroughConfiguredEngineStepAndRealNettyDelete(
					@TempDir final Path tempDir) throws Exception {
		final Path manifest = copyContractResource(tempDir, "verify-input.csv");
		copyContractResource(tempDir, "verify-input.complete.json");
		final String deleteStepId = tempDir.getFileName() + "-delete";
		final String scenario = contractScenarioText()
						.replace("/spt-input/items/verify-input.csv", manifest.toString())
						.replace("mt-001-20260822.120000.000-delete", deleteStepId);
		multiDeleteResponse = "<DeleteResult>"
						+ "<Deleted><Key>alpha</Key></Deleted>"
						+ "<Deleted><Key>comma,key</Key><VersionId>version-comma</VersionId></Deleted>"
						+ "</DeleteResult>";

		final Config driverConfig = S3StorageDriverTest.baseConfig(
						false, 4, false, null, "127.0.0.1");
		final Config defaults = new BundledDefaultsProvider().config("-", driverConfig.schema());
		final Config config = ConfigUtil.merge("-", List.of(defaults, driverConfig));
		config.val("run-id", 777L);
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		config.val("output-color", false);

		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		assertTrue(extensions.stream().anyMatch(extension -> "Load".equals(extension.id())));
		assertTrue(extensions.stream().anyMatch(extension -> "s3".equals(extension.id())));
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			metrics.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("CLI DELETE contract canary", scenario, engine, 777L).run();

			final var terminal = metrics.getTerminalSteps().stream()
							.filter(entry -> !entry.distributed)
							.filter(entry -> deleteStepId.equals(entry.stepId))
							.findFirst()
							.orElseThrow();
			assertEquals(OpType.DELETE, terminal.opType);
			assertEquals(1, terminal.successCount);
			assertEquals(0, terminal.failedCount);
			assertEquals(777, terminal.runId);
		}

		final List<CapturedRequest> deletes = requests.stream()
						.filter(request -> "POST".equals(request.method()) && "delete".equals(request.rawQuery()))
						.toList();
		assertEquals(1, deletes.size(), "configured standalone batch must own one terminal request");
		final String body = deletes.get(0).body();
		assertEquals(2, body.split("<Object>", -1).length - 1);
		assertTrue(body.contains("<Key>alpha</Key>"));
		assertTrue(body.contains("<Key>comma,key</Key>"));
		assertTrue(body.contains("<VersionId>version-comma</VersionId>"));
	}

	@Test
	void strictPreValidationFailureStopsBeforeTimedDeleteOnTheRealDriver(
					@TempDir final Path tempDir) throws Exception {
		final Path manifest = copyContractResource(tempDir, "verify-input.csv");
		copyContractResource(tempDir, "verify-input.complete.json");
		final String stepId = tempDir.getFileName() + "-pre-validation";
		final String scenario = contractScenarioText()
						.replace("/spt-input/items/verify-input.csv", manifest.toString())
						.replace("mt-001-20260822.120000.000-delete", stepId)
						.replace("\"standalone\": true,", "\"standalone\": true, \"preValidation\": true, \"verificationTimeoutMillis\": 25,");
		headResponseStatus = ignored -> 404;

		final Config config = scenarioConfig(777L);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, Extension.load(classLoader), config, metrics);
			final RuntimeException failure = assertThrows(
							RuntimeException.class,
							() -> new RunImpl("strict pre-validation canary", scenario, engine, 777L).run());
			assertNotNull(IntegrityTerminalException.find(failure));
		}

		assertEquals(2, requests.stream().filter(request -> "HEAD".equals(request.method())).count());
		assertEquals(0, requests.stream().filter(request -> "DELETE".equals(request.method()) || "POST".equals(request.method())).count());
	}

	@Test
	void postVerificationCorrectnessFailureCannotUseOperationalBudgetRoom(
					@TempDir final Path tempDir) throws Exception {
		final Path manifest = copyContractResource(tempDir, "verify-input.csv");
		copyContractResource(tempDir, "verify-input.complete.json");
		final String stepId = tempDir.getFileName() + "-post-verification";
		final String scenario = contractScenarioText()
						.replace("/spt-input/items/verify-input.csv", manifest.toString())
						.replace("mt-001-20260822.120000.000-delete", stepId)
						.replace("\"standalone\": true,", "\"standalone\": true, \"postVerification\": true, \"verificationTimeoutMillis\": 15,");
		headResponseStatus = ignored -> 200;

		final Config config = scenarioConfig(777L);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, Extension.load(classLoader), config, metrics);
			final RuntimeException failure = assertThrows(
							RuntimeException.class,
							() -> new RunImpl("post-verification correctness canary", scenario, engine, 777L).run());
			final var terminal = IntegrityTerminalException.find(failure);
			assertNotNull(terminal);
			assertTrue(terminal.getMessage().contains("outside operational budget room"));
		}

		assertEquals(1, requests.stream().filter(request -> "POST".equals(request.method()) && "delete".equals(request.rawQuery())).count());
		assertTrue(requests.stream().filter(request -> "HEAD".equals(request.method())).count() >= 2);
	}

	@Test
	void seededScenarioFreezesReturnedVersionsBeforeTimedCurrentAndExactDelete(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 900;
		final String scenarioId = tempDir.getFileName().toString();
		final String seedStep = scenarioId + "-seed";
		final String deleteStep = scenarioId + "-delete";
		final Path manifest = tempDir.resolve("written.csv");
		final String manifestPath = manifest.toString().replace("\\", "\\\\").replace("\"", "\\\"");
		final String scenario = """
						CreateLoad.config({
						  "storage": {
						    "driver": {"type": "s3", "limit": {"concurrency": 1}},
						    "integrity": {"mode": "metadata", "algorithm": "sha256",
						      "input": {"provenance": "none", "expectedProducerId": ""},
						      "output": {"requireExactCount": true}}
						  },
						  "item": {"type": "data", "data": {"size": "1KiB"},
						    "naming": {"prefix": "safe-root/spt-delete-900/"},
						    "output": {"path": "/bucket", "file": "%s"}},
						  "load": {"op": {"type": "create", "limit": {"count": 2}},
						    "step": {"id": "%s"}},
						  "output": {"metrics": {"summary": {"persist": true}}}
						}).run();
						DeleteLoad.config({
						  "storage": {
						    "driver": {"type": "s3", "limit": {"concurrency": 1}},
						    "integrity": {"mode": "metadata", "algorithm": "sha256",
						      "input": {"provenance": "engine_step", "expectedProducerId": "%s"}}
						  },
						  "item": {"type": "data", "input": {"file": "%s"}},
						  "load": {"batch": {"size": 2}, "op": {"type": "delete",
						    "delete": {"standalone": true, "batchSize": 2},
						    "recycle": {"mode": false}, "retry": false, "wait": {"finish": true}},
						    "step": {"id": "%s"}},
						  "output": {"metrics": {"summary": {"persist": true}}}
						}).run();
						""".formatted(manifestPath, seedStep, seedStep, manifestPath, deleteStep);

		final Config driverConfig = S3StorageDriverTest.baseConfig(
						false, 4, false, null, "127.0.0.1");
		final Config defaults = new BundledDefaultsProvider().config("-", driverConfig.schema());
		final Config config = ConfigUtil.merge("-", List.of(defaults, driverConfig));
		config.val("run-id", runId);
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		config.val("output-color", false);

		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			metrics.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("seeded DELETE scenario canary", scenario, engine, runId).run();

			final var seedTerminal = metrics.getTerminalSteps().stream()
							.filter(entry -> !entry.distributed && seedStep.equals(entry.stepId))
							.findFirst()
							.orElseThrow();
			final var deleteTerminal = metrics.getTerminalSteps().stream()
							.filter(entry -> !entry.distributed && deleteStep.equals(entry.stepId))
							.findFirst()
							.orElseThrow();
			assertEquals(OpType.CREATE, seedTerminal.opType);
			assertEquals(2, seedTerminal.successCount);
			assertEquals(OpType.DELETE, deleteTerminal.opType);
			assertEquals(1, deleteTerminal.successCount,
							"timed metrics must count one logical batch request, not seed operations");
		}

		final var frozen = new ArrayList<IntegrityManifestDataItem>();
		try (final var input = new IntegrityManifestItemInput(manifest)) {
			assertEquals(2, input.get(frozen, 2));
		}
		assertEquals(2, frozen.size());
		assertEquals(1, frozen.stream().filter(item -> item.versionId() == null).count());
		assertEquals(1, frozen.stream().filter(
						item -> "returned-version".equals(item.versionId())).count());
		assertTrue(frozen.stream().allMatch(
						item -> item.name().startsWith("safe-root/spt-delete-900/")));

		assertEquals(2, requests.stream().filter(request -> "PUT".equals(request.method())).count());
		final CapturedRequest delete = requests.stream()
						.filter(request -> "POST".equals(request.method()) && "delete".equals(request.rawQuery()))
						.findFirst()
						.orElseThrow();
		assertEquals(2, delete.body().split("<Object>", -1).length - 1);
		assertEquals(1, delete.body().split("<VersionId>returned-version</VersionId>", -1).length - 1);
	}

	@Test
	void seededResidualCleanupUsesProductionStepsArtifactsAndExactVersionDelete(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 902;
		final String scenarioId = tempDir.getFileName().toString();
		final String seedStep = scenarioId + "-seed";
		final String deleteStep = scenarioId + "-delete";
		final String cleanupStep = scenarioId + "-cleanup";
		final Path manifest = tempDir.resolve("written.csv");
		final Path residual = Path.of(
						FileManager.INSTANCE.logFileName(Loggers.DELETE_RESIDUAL.getName(), deleteStep))
						.toAbsolutePath();
		final String manifestPath = jsPath(manifest);
		final String residualPath = jsPath(residual);
		final String scenario = """
						var writtenFile = "%s";
						var residualFile = "%s";
						CreateLoad.config({
						  "storage": {
						    "driver": {"type": "s3", "limit": {"concurrency": 1}},
						    "integrity": {"mode": "metadata", "algorithm": "sha256",
						      "input": {"provenance": "none", "expectedProducerId": ""},
						      "output": {"requireExactCount": true}}
						  },
						  "item": {"type": "data", "data": {"size": "1KiB"},
						    "naming": {"prefix": "safe-root/spt~delete-902/"},
						    "output": {"path": "/bucket", "file": writtenFile}},
						  "load": {"op": {"type": "create", "limit": {"count": 3}},
						    "step": {"id": "%s"}},
						  "output": {"metrics": {"summary": {"persist": true}}}
						}).run();
						var deleteSelection = com.dell.spt.base.item.op.deletion.StandaloneDeleteSelection.fromManifest(writtenFile);
						var benchmarkFailure = null;
						try {
						  DeleteLoad.config({
						    "storage": {
						      "driver": {"type": "s3", "limit": {"concurrency": 1}},
						      "integrity": {"mode": "metadata", "algorithm": "sha256",
						        "input": {"provenance": "engine_step", "expectedProducerId": "%s"}}
						    },
						    "item": {"type": "data", "input": {"file": writtenFile}},
						    "load": {"batch": {"size": 2}, "op": {"type": "delete",
						      "delete": {"standalone": true, "batchSize": 2,
						        "selected": deleteSelection.selected(),
						        "selectedCurrentKey": deleteSelection.selectedCurrentKey(),
						        "selectedExactVersion": deleteSelection.selectedExactVersion(),
						        "selectedBuckets": deleteSelection.selectedBuckets()},
						      "failureBudget": {"mode": "fixed", "maxFailedObjects": 2,
						        "maxFailurePercent": 0.0, "graceSeconds": 0},
						      "recycle": {"mode": false}, "retry": false, "wait": {"finish": true}},
						      "step": {"id": "%s"}},
						    "output": {"metrics": {"summary": {"persist": true}}}
						  }).run();
						} catch (failure) {
						  benchmarkFailure = failure;
						}
						var cleanupFailure = null;
						var cleanupLoad = null;
						var cleanupStartedNanos = java.lang.System.nanoTime();
						try {
						  cleanupLoad = DeleteLoad.config({
						    "storage": {"driver": {"type": "s3", "limit": {"concurrency": 1}},
						      "object": {"tagging": {"enabled": true}}},
						    "item": {"type": "data", "input": {"file": residualFile}},
						    "load": {"op": {"type": "delete",
						      "limit": {"fail": {"count": 0, "rate": false}},
						      "retry": false, "wait": {"finish": true}},
						      "step": {"id": "%s"}},
						    "output": {"metrics": {"summary": {"persist": true}}}
						  });
						  cleanupLoad.run();
						} catch (failure) {
						  cleanupFailure = failure;
						}
						var cleanupOutcome = com.dell.spt.base.item.op.deletion.SeededDeleteCleanupFinalizer.finish(
						    "%s", java.lang.System.nanoTime() - cleanupStartedNanos,
						    benchmarkFailure, cleanupFailure, cleanupLoad, residualFile);
						""".formatted(
						manifestPath, residualPath, seedStep, seedStep, deleteStep,
						cleanupStep, cleanupStep);
		final var batchRejectedVersion = new AtomicBoolean();
		final var singleVersionAttempts = new AtomicInteger();
		multiDeleteResponseFactory = body -> {
			if (body.contains("<VersionId>returned-version</VersionId>")) {
				batchRejectedVersion.set(true);
			}
			return partialVersionedDeleteResponse(body);
		};
		deleteResponseStatus = request -> {
			if ("versionId=returned-version".equals(request.rawQuery())
							&& !batchRejectedVersion.get()
							&& singleVersionAttempts.getAndIncrement() == 0) {
				return 403;
			}
			return 204;
		};

		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			metrics.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("seeded residual cleanup canary", scenario, engine, runId).run();

			final var measured = awaitTerminalStep(metrics, deleteStep);
			assertNotNull(measured.deleteMetrics);
			assertEquals(2, measured.deleteMetrics.requestAttempted());
			assertEquals(3, measured.deleteMetrics.objectSelected());
			assertEquals(2, measured.deleteMetrics.objectAccepted());
			assertEquals(1, measured.deleteMetrics.objectFailed());
		}

		final var cleanupOutcome = (SeededDeleteCleanupFinalizer.Outcome) engine.get("cleanupOutcome");
		assertNotNull(cleanupOutcome);
		assertEquals(1, cleanupOutcome.selectedOperations());
		assertEquals(1, cleanupOutcome.succeededOperations());
		assertEquals(0, cleanupOutcome.failedOperations());
		assertNull(cleanupOutcome.failure());
		final var frozenResidual = new ArrayList<IntegrityManifestDataItem>();
		try (final var input = new IntegrityManifestItemInput(residual)) {
			assertEquals(1, input.get(frozenResidual, 2));
		}
		assertEquals("returned-version", frozenResidual.getFirst().versionId());
		final CapturedRequest cleanupDelete = requests.stream()
						.filter(request -> "DELETE".equals(request.method())
										&& "versionId=returned-version".equals(request.rawQuery()))
						.reduce((earlier, later) -> later)
						.orElseThrow();
		assertEquals("/bucket/" + frozenResidual.getFirst().name(), cleanupDelete.rawPath());
		assertTrue(cleanupDelete.rawPath().contains("spt~delete-902"));
		assertEquals(1, requests.stream()
						.filter(request -> "POST".equals(request.method()) && "delete".equals(request.rawQuery()))
						.count());
	}

	@Test
	void guardedExistingPrefixDiscoveryFreezesCappedCurrentKeysBeforeTimedDelete(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 901;
		final String scenarioId = tempDir.getFileName().toString();
		final String listStep = scenarioId + "-list";
		final String deleteStep = scenarioId + "-delete";
		final Path manifest = tempDir.resolve("verify-input.csv");
		listResponse = "<ListBucketResult>"
						+ "<Contents><Key>guarded/zulu</Key><Size>7</Size></Contents>"
						+ "<Contents><Key>guarded/alpha</Key><Size>9</Size></Contents>"
						+ "<Contents><Key>guarded/bravo</Key><Size>8</Size></Contents>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>";
		final String scenario = existingPrefixScenario(
						manifest, listStep, deleteStep, 2, false);

		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			metrics.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("guarded existing-prefix DELETE canary", scenario, engine, runId).run();

			final var listTerminal = metrics.getTerminalSteps().stream()
							.filter(entry -> !entry.distributed && listStep.equals(entry.stepId))
							.findFirst()
							.orElseThrow();
			final var deleteTerminal = metrics.getTerminalSteps().stream()
							.filter(entry -> !entry.distributed && deleteStep.equals(entry.stepId))
							.findFirst()
							.orElseThrow();
			assertEquals(OpType.LIST, listTerminal.opType);
			assertEquals(OpType.DELETE, deleteTerminal.opType);
			assertEquals(1, deleteTerminal.successCount,
							"timed metrics must contain one DELETE request, not discovery operations");
		}

		final var completion = IntegrityManifestCompletion.validate(
						manifest, runId, IntegrityInputProvenance.ENGINE_STEP, listStep);
		assertEquals(3, completion.sourceRecordCount());
		assertEquals(3, completion.uniqueRecordCount());
		assertEquals(2, completion.selectedRecordCount());
		assertFalse(completion.manifestSha256().isBlank());
		final var frozen = new ArrayList<IntegrityManifestDataItem>();
		try (final var input = new IntegrityManifestItemInput(manifest)) {
			assertEquals(2, input.get(frozen, 2));
		}
		assertEquals(List.of("guarded/alpha", "guarded/bravo"),
						frozen.stream().map(IntegrityManifestDataItem::name).toList());
		assertTrue(frozen.stream().allMatch(item -> item.versionId() == null));

		final int listIndex = requestIndex("GET", "list-type=2");
		final int deleteIndex = requestIndex("POST", "delete");
		assertTrue(listIndex >= 0 && deleteIndex > listIndex,
						"DELETE request must follow complete frozen discovery");
		final CapturedRequest list = requests.get(listIndex);
		assertEquals("/bucket", list.rawPath());
		assertEquals("list-type=2&prefix=guarded%2F&max-keys=1000", list.rawQuery());
		final String body = requests.get(deleteIndex).body();
		assertTrue(body.contains("<Key>guarded/alpha</Key>"));
		assertTrue(body.contains("<Key>guarded/bravo</Key>"));
		assertFalse(body.contains("guarded/zulu"));
		assertFalse(body.contains("<VersionId>"), "existing-prefix mode is current-key only");
	}

	@Test
	void guardedAllVersionConfigurationStopsBeforeListOrDelete(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 908;
		final String scenario = existingPrefixScenario(
						tempDir.resolve("verify-input.csv"),
						tempDir.getFileName() + "-list",
						tempDir.getFileName() + "-delete",
						2,
						true);
		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");

		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, extensions, config, metrics);
			final var failure = assertThrows(
							IntegrityTerminalException.class,
							() -> new RunImpl("hostile all-version guarded discovery", scenario, engine, runId).run());
			assertTrue(failure.getMessage().contains("invalid storage.integrity configuration"));
		}
		assertTrue(requests.isEmpty(), "effective configuration must fail before LIST or DELETE activity");
	}

	@Test
	void outOfPrefixDiscoveryResponseStopsBeforeAnyDeleteRequest(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 903;
		final Path manifest = tempDir.resolve("verify-input.csv");
		listResponse = "<ListBucketResult>"
						+ "<Contents><Key>guarded/inside</Key><Size>7</Size></Contents>"
						+ "<Contents><Key>outside/untrusted</Key><Size>8</Size></Contents>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>";
		final String scenario = existingPrefixScenario(
						manifest, tempDir.getFileName() + "-list", tempDir.getFileName() + "-delete", 0, false);
		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, extensions, config, metrics);
			assertThrows(
							IntegrityTerminalException.class,
							() -> new RunImpl("out-of-prefix guarded discovery", scenario, engine, runId).run());
		}
		final List<CapturedRequest> listRequests = requests.stream()
						.filter(request -> "GET".equals(request.method())
										&& request.rawQuery() != null
										&& request.rawQuery().contains("list-type=2"))
						.toList();
		assertEquals(1, listRequests.size());
		assertEquals("/bucket", listRequests.get(0).rawPath());
		assertEquals(
						"list-type=2&prefix=guarded%2F&max-keys=1000",
						listRequests.get(0).rawQuery());
		assertEquals(
						0,
						requests.stream()
										.filter(request -> "DELETE".equals(request.method())
														|| "POST".equals(request.method()))
										.count());
		assertFalse(Files.exists(manifest));
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	@Test
	void outOfPrefixDelimiterProbeStopsBeforeShardDiscoveryOrDelete(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 904;
		final Path manifest = tempDir.resolve("verify-input.csv");
		listResponse = "<ListBucketResult>"
						+ "<CommonPrefixes><Prefix>outside/a/</Prefix></CommonPrefixes>"
						+ "<CommonPrefixes><Prefix>outside/b/</Prefix></CommonPrefixes>"
						+ "<CommonPrefixes><Prefix>outside/c/</Prefix></CommonPrefixes>"
						+ "<CommonPrefixes><Prefix>outside/d/</Prefix></CommonPrefixes>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>";
		final String scenario = existingPrefixScenario(
						manifest, tempDir.getFileName() + "-list", tempDir.getFileName() + "-delete", 0, false)
						.replace("\"limit\": {\"concurrency\": 1}", "\"limit\": {\"concurrency\": 4}");
		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, extensions, config, metrics);
			final var failure = assertThrows(
							IntegrityTerminalException.class,
							() -> new RunImpl("out-of-prefix delimiter probe", scenario, engine, runId).run());
			Throwable cause = failure;
			while (cause.getCause() != null) {
				cause = cause.getCause();
			}
			assertTrue(cause.getMessage().contains("outside requested prefix"));
		}
		final List<CapturedRequest> listRequests = requests.stream()
						.filter(request -> "GET".equals(request.method()))
						.toList();
		assertEquals(1, listRequests.size());
		assertEquals("/bucket", listRequests.get(0).rawPath());
		assertEquals(
						"list-type=2&prefix=guarded%2F&delimiter=%2F&max-keys=1000",
						listRequests.get(0).rawQuery());
		assertEquals(
						0,
						requests.stream()
										.filter(request -> "DELETE".equals(request.method())
														|| "POST".equals(request.method()))
										.count());
		assertFalse(Files.exists(manifest));
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	@Test
	@Timeout(30)
	void integrityDiscoveryKeepsStartupPartitionAfterAdaptiveThreshold(
					@TempDir final Path tempDir) throws Exception {
		final long runId = 905;
		final Path manifest = tempDir.resolve("verify-input.csv");
		final AtomicInteger delimiterProbeCount = new AtomicInteger();
		final AtomicInteger listPageCount = new AtomicInteger();
		listResponseFactory = request -> {
			if (request.rawQuery() != null && request.rawQuery().contains("delimiter=%2F")) {
				if (delimiterProbeCount.incrementAndGet() == 1) {
					return "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
				}
				return "<ListBucketResult>"
								+ "<CommonPrefixes><Prefix>outside/a/</Prefix></CommonPrefixes>"
								+ "<CommonPrefixes><Prefix>outside/b/</Prefix></CommonPrefixes>"
								+ "<IsTruncated>false</IsTruncated></ListBucketResult>";
			}
			final int page = listPageCount.incrementAndGet();
			final String pageBody = "<ListBucketResult>"
							+ "<Contents><Key>guarded/alpha-" + page + "</Key><Size>9</Size></Contents>"
							+ "<Contents><Key>guarded/zulu-" + page + "</Key><Size>7</Size></Contents>";
			if (page <= 50) {
				return pageBody
								+ "<NextContinuationToken>page-" + page + "</NextContinuationToken>"
								+ "<IsTruncated>true</IsTruncated></ListBucketResult>";
			}
			return pageBody + "<IsTruncated>false</IsTruncated></ListBucketResult>";
		};
		final String scenario = existingPrefixScenario(
						manifest, tempDir.getFileName() + "-list", tempDir.getFileName() + "-delete", 2, false)
						.replace("\"limit\": {\"concurrency\": 1}", "\"limit\": {\"concurrency\": 4}")
						.replace(
										"\"include_versions\": false, \"max_keys\": 1000}",
										"\"include_versions\": false, \"max_keys\": 1000, "
														+ "\"sharding\": {\"mode\": \"static\", \"radix\": 1, "
														+ "\"delimiters\": \"/\"}}");
		final Config config = scenarioConfig(runId);
		config.val("load-op-limit-count", 1);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("immutable integrity LIST partition", scenario, engine, runId).run();
		}
		final List<CapturedRequest> listRequests = requests.stream()
						.filter(request -> "GET".equals(request.method()))
						.toList();
		assertEquals(52, listRequests.size());
		assertEquals(51, listPageCount.get());
		assertEquals(1, delimiterProbeCount.get());
		assertEquals(
						"list-type=2&prefix=guarded%2F&delimiter=%2F&max-keys=1000",
						listRequests.get(0).rawQuery());
		assertEquals("list-type=2&prefix=guarded%2F&max-keys=1000", listRequests.get(1).rawQuery());
		assertTrue(listRequests.get(51).rawQuery().contains("continuation-token=page-50"));
		final var completion = IntegrityManifestCompletion.validate(
						manifest,
						runId,
						IntegrityInputProvenance.ENGINE_STEP,
						tempDir.getFileName() + "-list");
		assertEquals(102, completion.sourceRecordCount());
		assertEquals(2, completion.selectedRecordCount());
		assertEquals(1, requests.stream().filter(request -> "POST".equals(request.method())).count());
		assertEquals(0, requests.stream().filter(request -> "DELETE".equals(request.method())).count());
	}

	@Test
	void emptyGuardedDiscoveryStopsBeforeAnyDeleteRequest(@TempDir final Path tempDir)
					throws Exception {
		final long runId = 902;
		final Path manifest = tempDir.resolve("verify-input.csv");
		listResponse = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
		final String scenario = existingPrefixScenario(
						manifest, tempDir.getFileName() + "-list", tempDir.getFileName() + "-delete", 0, false);
		final Config config = scenarioConfig(runId);
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final var extensions = Extension.load(classLoader);
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		assertNotNull(engine, "default JavaScript engine must be available");
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, extensions, config, metrics);
			final var failure = assertThrows(
							IntegrityTerminalException.class,
							() -> new RunImpl("empty guarded discovery", scenario, engine, runId).run());
			assertTrue(failure.getMessage().contains("zero object identities"));
		}
		assertEquals(0, requests.stream().filter(request -> "DELETE".equals(request.method()) || "POST".equals(request.method())).count());
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	private String existingPrefixScenario(
					final Path manifest,
					final String listStep,
					final String deleteStep,
					final long maxCount,
					final boolean includeVersions) {
		final String manifestPath = manifest.toString().replace("\\", "\\\\").replace("\"", "\\\"");
		return """
						var verifyInputFile = "%s";
						Load.config({
						  "storage": {
						    "driver": {"type": "s3", "limit": {"concurrency": 1}},
						    "integrity": {"mode": "metadata", "algorithm": "sha256",
						      "input": {"provenance": "none", "expectedProducerId": ""},
						      "selection": {"maxCount": %d, "requireNonEmpty": true}}
						  },
						  "item": {"type": "path", "input": {"file": "", "path": "/bucket"},
						    "naming": {"type": "random", "radix": 36, "prefix": "guarded/"},
						    "output": {"file": verifyInputFile}},
						  "load": {"batch": {"size": 1000}, "op": {"type": "list",
						    "list": {"delimiter": "", "fetch_metadata": true,
						      "include_versions": %s, "max_keys": 1000},
						    "limit": {"count": 0, "rate": 0}, "wait": {"finish": true}},
						    "step": {"id": "%s", "limit": {"size": 0, "time": 0}}},
						  "output": {"metrics": {"summary": {"persist": true}}}
						}).run();
						DeleteLoad.config({
						  "storage": {
						    "driver": {"type": "s3", "limit": {"concurrency": 1}},
						    "integrity": {"mode": "metadata", "algorithm": "sha256",
						      "input": {"provenance": "engine_step", "expectedProducerId": "%s"}}
						  },
						  "item": {"type": "data", "input": {"file": verifyInputFile}},
						  "load": {"batch": {"size": 2}, "op": {"type": "delete",
						    "delete": {"standalone": true, "batchSize": 2},
						    "recycle": {"mode": false}, "retry": false, "wait": {"finish": true}},
						    "step": {"id": "%s"}},
						  "output": {"metrics": {"summary": {"persist": true}}}
						}).run();
						""".formatted(
						manifestPath, maxCount, includeVersions, listStep, listStep, deleteStep);
	}

	private Config scenarioConfig(final long runId) throws IOException {
		final Config driverConfig = S3StorageDriverTest.baseConfig(
						false, 4, false, null, "127.0.0.1");
		final Config defaults = new BundledDefaultsProvider().config("-", driverConfig.schema());
		final Config config = ConfigUtil.merge("-", List.of(defaults, driverConfig));
		config.val("run-id", runId);
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		config.val("output-color", false);
		return config;
	}

	private int requestIndex(final String method, final String queryToken) {
		for (int index = 0; index < requests.size(); index++) {
			final CapturedRequest request = requests.get(index);
			if (method.equals(request.method())
							&& request.rawQuery() != null
							&& request.rawQuery().contains(queryToken)) {
				return index;
			}
		}
		return -1;
	}

	private static Path copyContractResource(final Path tempDir, final String name)
					throws IOException {
		final Path target = tempDir.resolve(name);
		Files.write(target, contractResourceBytes(name));
		return target;
	}

	private static String contractResourceText(final String name) throws IOException {
		return new String(contractResourceBytes(name), StandardCharsets.UTF_8);
	}

	private static String contractScenarioText() throws IOException {
		return new String(
						Base64.getDecoder().decode(contractResourceText("scenario.b64").strip()),
						StandardCharsets.UTF_8);
	}

	private static byte[] contractResourceBytes(final String name) throws IOException {
		try (final InputStream input = S3DeleteRequestIntegrationTest.class.getResourceAsStream(
						"/delete-cli-contract/" + name)) {
			assertNotNull(input, "missing DELETE CLI contract resource " + name);
			return input.readAllBytes();
		}
	}

	private static void awaitCompletionAccounting(
					final S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while ((driver.completedOpCount() != 1 || driver.activeOpCount() != 0)
						&& System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
	}

	@Test
	void partialBatchErrorRunsThroughRealNettyDriver() throws Exception {
		multiDeleteResponse = "<DeleteResult>"
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
			assertEquals(1, requests.stream().filter(request -> "POST".equals(request.method())).count());
		}
	}

	@Test
	void serviceFailureRunsThroughSharedConservativePolicy() throws Exception {
		multiDeleteStatus = 503;
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("one", null), target("two", "version-2")));

			assertEquals(DeleteRequestOutcome.FAILED, result.deleteResult().outcome());
			assertEquals(DeleteFailureClassification.OPERATIONAL,
							result.deleteResult().failureClassification());
			assertEquals(2, result.deleteResult().failedObjectCount());
		}
	}

	@Test
	void representativeItemFallthroughCompletesWithoutEmittingOrLeakingTransportOwnership()
					throws Exception {
		try (final var driver = newFallthroughDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("one", null), target("two", null)));
			awaitCompletionAccounting(driver);

			assertEquals(DeleteRequestOutcome.FAILED, result.deleteResult().outcome());
			assertEquals(DeleteFailureClassification.PROTOCOL,
							result.deleteResult().failureClassification());
			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, result.status());
			assertEquals(2, result.deleteResult().targetResults().size());
			assertTrue(result.deleteResult().targetResults().stream()
							.allMatch(targetResult -> targetResult.outcome() == DeleteTargetOutcome.FAILED
											&& targetResult.failureClassification() == DeleteFailureClassification.PROTOCOL));
			assertTrue(
							requests.stream().noneMatch(request -> "DELETE".equals(request.method())
											|| "POST".equals(request.method())),
							() -> "fallthrough emitted an object deletion request: " + requests);
			assertEquals(1, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount());
		}
	}

	private S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> newDriver()
					throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		final var driver = new S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation>(
						"delete-integration-test",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
						config.configVal("storage"),
						false,
						config.intVal("load-batch-size"));
		return driver;
	}

	private S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> newFallthroughDriver()
					throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		return new S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation>(
						"delete-fallthrough-integration-test",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
						config.configVal("storage"),
						false,
						config.intVal("load-batch-size")) {
			@Override
			protected HttpRequest httpRequest(
							final DeleteRequestOperation operation, final String nodeAddr)
							throws URISyntaxException {
				return ordinaryObjectRequest(operation, nodeAddr);
			}
		};
	}

	private static DeleteRequestOperation execute(
					final S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver,
					final DeleteRequestOperation operation) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(operation));
		final DeleteRequestOperation result = output.await();
		assertNotNull(result, "real Netty DELETE did not complete before the canary timeout");
		return result;
	}

	private CapturedRequest onlyDeleteRequest() {
		final List<CapturedRequest> deletes = requests.stream()
						.filter(request -> "DELETE".equals(request.method()))
						.toList();
		assertEquals(1, deletes.size());
		return deletes.get(0);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		final byte[] requestBody = exchange.getRequestBody().readAllBytes();
		final CapturedRequest request = new CapturedRequest(
						exchange.getRequestMethod(),
						exchange.getRequestURI().getRawPath(),
						exchange.getRequestURI().getRawQuery(),
						exchange.getRequestHeaders().getFirst("Authorization"),
						new String(requestBody, StandardCharsets.UTF_8));
		requests.add(request);
		if ("GET".equals(request.method())
						&& request.rawQuery() != null
						&& request.rawQuery().contains("list-type=2")) {
			final String responseXml = listResponseFactory != null
							? listResponseFactory.apply(request)
							: listResponse == null
											? "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
											: listResponse;
			final byte[] responseBody = responseXml.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(200, responseBody.length);
			exchange.getResponseBody().write(responseBody);
		} else if ("PUT".equals(request.method())) {
			if (putResponseCount.incrementAndGet() == 1) {
				exchange.getResponseHeaders().set("x-amz-version-id", "returned-version");
			}
			exchange.sendResponseHeaders(200, -1);
		} else if ("HEAD".equals(request.method())) {
			exchange.sendResponseHeaders(headResponseStatus.apply(request), -1);
		} else if ("DELETE".equals(request.method())) {
			exchange.sendResponseHeaders(deleteResponseStatus.apply(request), -1);
		} else if ("POST".equals(request.method()) && "delete".equals(request.rawQuery())) {
			if (multiDeleteStatus != 200) {
				exchange.sendResponseHeaders(multiDeleteStatus, -1);
				exchange.close();
				return;
			}
			final String responseXml = multiDeleteResponseFactory != null
							? multiDeleteResponseFactory.apply(request.body())
							: multiDeleteResponse == null
											? successfulDeleteResponse(request.body())
											: multiDeleteResponse;
			final byte[] responseBody = responseXml.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(200, responseBody.length);
			exchange.getResponseBody().write(responseBody);
		} else {
			exchange.sendResponseHeaders(500, -1);
		}
		exchange.close();
	}

	private static String successfulDeleteResponse(final String requestBody) {
		final Matcher objects = Pattern.compile(
						"<Object><Key>(.*?)</Key>(?:<VersionId>(.*?)</VersionId>)?</Object>",
						Pattern.DOTALL).matcher(requestBody);
		final StringBuilder response = new StringBuilder("<DeleteResult>");
		while (objects.find()) {
			response.append("<Deleted><Key>").append(objects.group(1)).append("</Key>");
			if (objects.group(2) != null) {
				response.append("<VersionId>").append(objects.group(2)).append("</VersionId>");
			}
			response.append("</Deleted>");
		}
		return response.append("</DeleteResult>").toString();
	}

	private static String partialVersionedDeleteResponse(final String requestBody) {
		final Matcher objects = Pattern.compile(
						"<Object><Key>(.*?)</Key>(?:<VersionId>(.*?)</VersionId>)?</Object>",
						Pattern.DOTALL).matcher(requestBody);
		final StringBuilder response = new StringBuilder("<DeleteResult>");
		while (objects.find()) {
			if (objects.group(2) == null) {
				response.append("<Deleted><Key>").append(objects.group(1)).append("</Key></Deleted>");
			} else {
				response.append("<Error><Key>").append(objects.group(1)).append("</Key><VersionId>")
								.append(objects.group(2))
								.append("</VersionId><Code>AccessDenied</Code><Message>denied</Message></Error>");
			}
		}
		return response.append("</DeleteResult>").toString();
	}

	private static String jsPath(final Path path) {
		return path.toString().replace("\\", "\\\\").replace("\"", "\\\"");
	}

	private static TerminalStepEntry awaitTerminalStep(
					final MetricsManagerImpl metrics, final String stepId) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		do {
			final var entry = metrics.getTerminalSteps().stream()
							.filter(candidate -> !candidate.distributed && stepId.equals(candidate.stepId))
							.findFirst();
			if (entry.isPresent()) {
				return entry.get();
			}
			TimeUnit.MILLISECONDS.sleep(10);
		} while (System.nanoTime() < deadline);
		throw new AssertionError("terminal metrics were not published for step " + stepId);
	}

	private record CapturedRequest(
					String method, String rawPath, String rawQuery, String authorization, String body) {}

	private static final class ResultOutput implements Output<DeleteRequestOperation> {

		private final LinkedBlockingQueue<DeleteRequestOperation> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final DeleteRequestOperation value) {
			return results.offer(value);
		}

		@Override
		public int put(
						final List<DeleteRequestOperation> values, final int from, final int to) {
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
