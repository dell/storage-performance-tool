package com.dell.spt.integration;

import static com.dell.spt.base.Constants.DIR_EXT;
import static com.dell.spt.base.Constants.MIB;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.AddCorsHeadersRule;
import com.dell.spt.base.control.ConfigServlet;
import com.dell.spt.base.control.FleetMetricsHandler;
import com.dell.spt.base.control.NodeMetricsHandler;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.control.logs.LogServlet;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.control.run.RunServlet;
import com.dell.spt.base.env.CoreResourcesToInstall;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.svc.Service;
import com.github.akurilov.confuse.Config;

import io.prometheus.client.exporter.MetricsServlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.script.ScriptEngine;
import javax.servlet.MultipartConfigElement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.dell.spt.testing.tags.IntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

@DisplayName("Application Smoke Tests")
@IntegrationTest
public class AppSmokeTest {
	final CoreResourcesToInstall coreResources = new CoreResourcesToInstall();
	final Path appHomePath = coreResources.appHomePath();
	final String initialStepId = "none-" + LogUtil.getDateTimeStamp();
	final MetricsManager metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	URLClassLoader extClsLoader = null;
	List<Extension> extensions = null;
	Config config = TestConfigBuilder.config();

	@BeforeEach
	public void setUp() {
		coreResources.install(appHomePath);
		final var extDir = Extension.resolveExtDir();
		final var extDirFile = extDir != null ? extDir : Paths.get(appHomePath.toString(), DIR_EXT).toFile();
		extClsLoader = Extension.extClassLoader(extDirFile);
		extensions = Extension.load(extClsLoader);
		installExtensions(extensions, appHomePath);
	}

	/* Description: Run a null scenario file which should invoke the default.js scenario and return no exception */
	@Test
	public void runBareScenarioTest() throws Exception {
		config.val("storage-driver-type", "s3");
		config.val("storage-net-node-port=9020");

		assertDoesNotThrow(() -> runScenario(config, extensions, extClsLoader, metricsMgr, appHomePath));
	}

	/* Initialize the API server and close, ensure no errors occur */
	@Test
	public void runAPITest() throws Exception {
		assertDoesNotThrow(() -> runNode(config, extClsLoader, extensions, metricsMgr, appHomePath));
	}

	/* Provide a scenario file path with an unknown extension to cover file-path branch without executing */
	@Test
	public void runScenarioWithUnknownExtensionFileTest() throws Exception {
		final var tmpFile = Files.createTempFile("scenario-", ".nope");
		Files.writeString(tmpFile, "// noop scenario content\n");
		config.val("run-scenario", tmpFile.toString());
		assertDoesNotThrow(() -> runScenario(config, extensions, extClsLoader, metricsMgr, appHomePath));
	}

	/* Utility Methods */
	private static void runScenario(
					final Config config,
					final List<Extension> extensions,
					final ClassLoader extClsLoader,
					final MetricsManager metricsMgr,
					final Path appHomePath) {
		Path scenarioPath = null;
		final var scenarioFile = config.stringVal("run-scenario");
		if (scenarioFile != null && !scenarioFile.isEmpty()) {
			scenarioPath = Paths.get(scenarioFile);
		}
		runScenarioFile(config, extensions, extClsLoader, metricsMgr, scenarioPath, appHomePath);
	}

	private static void runScenarioFile(
					final Config config,
					final List<Extension> extensions,
					final ClassLoader extClsLoader,
					final MetricsManager metricsMgr,
					final Path scenarioPath,
					final Path appHomePath) {
		final ScriptEngine scriptEngine;
		final String scenarioText;
		if (scenarioPath == null) {
			scriptEngine = ScenarioUtil.scriptEngineByDefault(extClsLoader);
			scenarioText = ScenarioUtil.defaultScenario(appHomePath);
		} else {
			scriptEngine = ScenarioUtil.scriptEngineByFilePath(scenarioPath, extClsLoader);
			final var strb = new StringBuilder();
			try (BufferedReader reader = Files.newBufferedReader(scenarioPath, StandardCharsets.UTF_8)) {
				String line;
				while ((line = reader.readLine()) != null) {
					strb.append(line).append(System.lineSeparator());
				}
			} catch (final IOException e) {
				LogUtil.exception(Level.FATAL, e, "Failed to read the scenario file \"{}\"", scenarioPath);
				try (java.util.stream.Stream<Path> entries = Files.list(scenarioPath.getParent())) {
					entries.forEach(System.out::println);
				} catch (final IOException ee) {
					LogUtil.trace(
									Loggers.ERR, Level.ERROR, ee, "Failed to list the scenarios parent directory");
				}
			}
			scenarioText = strb.toString();
		}
		if (scriptEngine == null) {
			Loggers.ERR.fatal("Failed to resolve the scenario engine for the file \"{}\"", scenarioPath);
		} else {
			Loggers.MSG.info(
							"Using the \"{}\" scenario engine", scriptEngine.getFactory().getEngineName());
			// expose the environment values
			System.getenv().forEach(scriptEngine::put);
			// expose the loaded configuration and the step types
			ScenarioUtil.configure(scriptEngine, extensions, config, metricsMgr);
			// go
			new RunImpl("", scenarioText, scriptEngine, config.longVal("run-id")).run();
		}
	}

	private static void runNode(
					final Config fullDefaultConfig,
					final ClassLoader extClsLoader,
					final List<Extension> extensions,
					final MetricsManager metricsMgr,
					final Path appHomePath)
					throws Exception {
		// init the API server
		final var port = fullDefaultConfig.intVal("run-port");
		final var server = new Server(port);
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
		final var addCorsHeaderHandler = new RewriteHandler();
		addCorsHeaderHandler.addRule(new AddCorsHeadersRule());
		server.insertHandler(addCorsHeaderHandler);
		context.addServlet(new ServletHolder(new ConfigServlet(fullDefaultConfig)), "/config/*");
		context.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
		context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		context.addServlet(new ServletHolder(new NodeMetricsHandler(metricsMgr, fullDefaultConfig)), "/metrics/json");
		context.addServlet(new ServletHolder(new FleetMetricsHandler(metricsMgr, fullDefaultConfig)), "/metrics/fleet/json");
		try {
			server.start();
			Loggers.MSG.info("Started to serve the remote API @ port # " + port);
			final var listenPort = fullDefaultConfig.intVal("load-step-node-port");
			try (final Service fileMgrSvc = new FileManagerServiceImpl(listenPort);
							final LoadStepManagerService scenarioStepSvc = new LoadStepManagerServiceImpl(listenPort, extensions, metricsMgr)) {
				fileMgrSvc.start();
				final var runServletHolder = new ServletHolder(
								new RunServlet(extClsLoader, extensions, metricsMgr, fullDefaultConfig, appHomePath, scenarioStepSvc, new ApiStatus()));
				runServletHolder
								.getRegistration()
								.setMultipartConfig(new MultipartConfigElement("", 16 * MIB, 16 * MIB, 16 * MIB));
				context.addServlet(runServletHolder, "/run/*");
				scenarioStepSvc.start();
				//scenarioStepSvc.await();
				// } catch (final InterruptedException e) {
				// 	throw e;
			} catch (final Throwable cause) {
				LogUtil.trace(Loggers.ERR, Level.FATAL, cause, "Run node failure");
			}
		} finally {
			server.stop();
		}
	}

	private static void installExtensions(final List<Extension> extensions, final Path appHomePath) {
		final var availExtMsg = new StringBuilder("Available/installed extensions:\n");
		extensions.forEach(
						ext -> {
							ext.install(appHomePath);
							final var extId = ext.id();
							final var extFqcn = ext.getClass().getCanonicalName();
							availExtMsg
											.append('\t')
											.append(extId)
											.append(' ')
											.append(StringUtils.repeat("-", extId.length() < 30 ? 30 - extId.length() : 1))
											.append("> ")
											.append(extFqcn)
											.append('\n');
						});
		Loggers.MSG.info(availExtMsg);
	}
}
