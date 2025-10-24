package com.dell.spt.base;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.DIR_EXT;
import static com.dell.spt.base.Constants.MIB;
import static com.dell.spt.base.Constants.PATH_DEFAULTS;
import static com.dell.spt.base.Constants.USER_HOME;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.config.CliArgUtil.ARG_PATH_SEP;
import static com.dell.spt.base.config.CliArgUtil.allCliArgs;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.AliasingUtil;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.CliArgUtil;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalArgumentNameException;
import com.dell.spt.base.control.AddCorsHeadersRule;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.control.StatusServlet;
import com.dell.spt.base.control.ConfigServlet;
import com.dell.spt.base.control.FleetMetricsHandler;
import com.dell.spt.base.control.NodeMetricsHandler;
import com.dell.spt.base.control.logs.LogServlet;
import com.dell.spt.base.control.ShutdownServlet;
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
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import com.github.akurilov.confuse.exceptions.InvalidValueTypeException;
import io.prometheus.client.exporter.MetricsServlet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.script.ScriptEngine;
import javax.servlet.MultipartConfigElement;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import com.dell.spt.base.control.HealthServlet;
import com.dell.spt.base.control.ReadinessGate;
import com.dell.spt.base.control.ReadinessServlet;
import org.eclipse.jetty.servlet.ServletHolder;

public final class Main {

	public static void main(final String... args) {

		// Check for version flag early
		for (String arg : args) {
			if ("--version".equals(arg) || "-v".equals(arg)) {
				handleVersionRequest();
				return;
			}
		}

		final var coreResources = new CoreResourcesToInstall();
		final var appHomePath = coreResources.appHomePath();
		final var initialStepId = "none-" + LogUtil.getDateTimeStamp();
		LogUtil.init(appHomePath.toString(), initialStepId);
		try {
			// install the core resources
			coreResources.install(appHomePath);
			// load the defaults
			final var defaultConfig = loadDefaultConfig(appHomePath);
			// extensions
			try (final var extClsLoader = Extension.extClassLoader(Paths.get(appHomePath.toString(), DIR_EXT).toFile())) {
				final var extensions = Extension.load(extClsLoader);
				// install the extensions
				installExtensions(extensions, appHomePath);
				final Config configWithArgs;
				try {
					// apply the extensions defaults
					final var fullDefaultConfig = collectDefaults(extensions, defaultConfig, appHomePath);
					// parse the CLI args and apply them to the config instance
					configWithArgs = applyArgsToConfig(args, fullDefaultConfig, initialStepId);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.exception(Level.ERROR, e, "Failed to load the defaults");
					throw e;
				}
				// init the metrics manager
				final MetricsManager metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
				// go on
				if (configWithArgs.boolVal("run-node")) {
					runNode(configWithArgs, extClsLoader, extensions, metricsMgr, appHomePath);
				} else {
					runScenario(configWithArgs, extensions, extClsLoader, metricsMgr, appHomePath);
				}
			}
		} catch (final InterruptedException e) {
			Loggers.MSG.debug("Interrupted", e);
		} catch (final Exception e) {
			LogUtil.trace(Loggers.ERR, Level.FATAL, e, "Unexpected failure");
		}
	}

	private static Config loadDefaultConfig(final Path appHomePath) throws Exception {
		final var mainConfigSchema = SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader()).stream()
						.findFirst()
						.orElseThrow(IllegalStateException::new);
		// load the defaults
		return ConfigUtil.loadConfig(
						Paths.get(appHomePath.toString(), PATH_DEFAULTS).toFile(), mainConfigSchema);
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

	private static Config collectDefaults(
					final List<Extension> extensions, final Config mainDefaults, final Path appHomePath)
					throws Exception {
		final List<Config> allDefaults = extensions.stream()
						.map(ext -> ext.defaults(appHomePath))
						.filter(Objects::nonNull)
						.collect(Collectors.toList());
		allDefaults.add(mainDefaults);
		return ConfigUtil.merge(mainDefaults.pathSep(), allDefaults);
	}

	private static Config applyArgsToConfig(
					final String[] args, final Config config, final String initialStepId) {
		try {
			argsWithAliases(args, config).forEach(config::val);
		} catch (final IllegalArgumentNameException e) {
			final var formattedAllCliArgs = allCliArgs(config.schema(), config.pathSep()).stream()
							.collect(Collectors.joining("\n", "\t", ""));
			Loggers.ERR.fatal(
							"Invalid argument: \"{}\"\nThe list of all possible args:\n{}",
							e.getMessage(),
							formattedAllCliArgs);
		} catch (final InvalidValuePathException e) {
			Loggers.ERR.fatal("Invalid configuration option: \"{}\"", e.path());
		} catch (final InvalidValueTypeException e) {
			Loggers.ERR.fatal(
							"Invalid configuration value type for the option \"{}\", expected: {}, " + "actual: {}",
							e.path(),
							e.expectedType(),
							e.actualType());
		}
		checkAndSetStepId(config, initialStepId);
		applyLogLevel(config);
		Arrays.stream(args).forEach(Loggers.CLI::info);
		return config;
	}

	private static void applyLogLevel(final Config config) {
		try {
			final var levelValue = readLogLevel(config);
			if (levelValue != null && !levelValue.isEmpty()) {
				final var level = Level.toLevel(levelValue.toUpperCase(Locale.ROOT), null);
				if (level == null) {
					Loggers.ERR.warn("Unsupported log level '{}', keeping existing configuration", levelValue);
					return;
				}
				final LoggerContext ctx = LoggerContext.getContext(false);
				final var configuration = ctx.getConfiguration();
				final LoggerConfig rootConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
				rootConfig.setLevel(level);
				final LoggerConfig msgConfig = configuration.getLoggerConfig(Loggers.MSG.getName());
				if (msgConfig != null) {
					msgConfig.setLevel(level);
				}
				ctx.updateLoggers();
				Loggers.MSG.info("Log level set to {}", level);
			}
		} catch (final Exception e) {
			Loggers.ERR.warn("Failed to apply log level", e);
		}
	}

	private static String readLogLevel(final Config config) {
		try {
			return config.stringVal("log-level");
		} catch (final Exception ignore) {
			try {
				return config.stringVal("log.level");
			} catch (final Exception ignored) {
				return null;
			}
		}
	}

	private static void checkAndSetStepId(final Config config, final String initialStepId) {
		if (null == config.val("load-step-id")) {
			config.val("load-step-id", initialStepId);
			config.val("load-step-idAutoGenerated", true);
		}
	}

	private static Map<String, String> argsWithAliases(final String[] args, final Config config) {
		final var parsedArgs = CliArgUtil.parseArgs(args);
		final List<Map<String, Object>> aliasingConfig = config.listVal("aliasing");
		return AliasingUtil.apply(parsedArgs, aliasingConfig);
	}

	private static boolean shouldExposeFleetMetrics(final Config config) {
		try {
			return config.boolVal("server-metrics-expose_fleet");
		} catch (final Exception ignore) {
			return true;
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
		// Status holder and endpoint
		final var apiStatus = new ApiStatus();
		apiStatus.setIdle();
		context.addServlet(new ServletHolder(new StatusServlet(apiStatus)), "/status");
		context.addServlet(new ServletHolder(new HealthServlet(metricsMgr, fullDefaultConfig)), "/health");
		final var readinessGate = new ReadinessGate();
		context.addServlet(new ServletHolder(new ReadinessServlet(readinessGate, metricsMgr, fullDefaultConfig)), "/ready");
		context.addServlet(new ServletHolder(new ConfigServlet(fullDefaultConfig)), "/config/*");
		context.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
		context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		context.addServlet(new ServletHolder(new NodeMetricsHandler(metricsMgr, fullDefaultConfig)), "/metrics/json");
		if (shouldExposeFleetMetrics(fullDefaultConfig)) {
			context.addServlet(new ServletHolder(new FleetMetricsHandler(metricsMgr, fullDefaultConfig)), "/metrics/fleet/json");
			context.addServlet(new ServletHolder(new FleetMetricsHandler(metricsMgr, fullDefaultConfig)), "/metrics/cluster/json");
		}
		try {
			final var listenPort = fullDefaultConfig.intVal("load-step-node-port");
			try (final Service fileMgrSvc = new FileManagerServiceImpl(listenPort);
							final LoadStepManagerService scenarioStepSvc = new LoadStepManagerServiceImpl(listenPort, extensions, metricsMgr)) {
				// Configure terminal metrics retention to match status linger
				try {
					metricsMgr.setTerminalRetentionMillis(fullDefaultConfig.intVal("api-linger-sec") * 1000L);
				} catch (final Exception e) {
					Loggers.MSG.warn("Unable to align terminal metrics retention with api-linger-sec; continuing with default retention", e);
				}
				// Register /run before starting the server to avoid a readiness race
				final var runServletHolder = new ServletHolder(
								new RunServlet(
												extClsLoader,
												extensions,
												metricsMgr,
												fullDefaultConfig,
												appHomePath,
												scenarioStepSvc,
												apiStatus));
				context.addServlet(runServletHolder, "/run");
				runServletHolder
								.getRegistration()
								.setMultipartConfig(new MultipartConfigElement("", 16 * MIB, 16 * MIB, 16 * MIB));

				// Register shutdown endpoint: gracefully closes services; server will stop afterwards
				context.addServlet(new ServletHolder(new ShutdownServlet(java.util.List.of(fileMgrSvc, scenarioStepSvc))), "/shutdown");

				server.start();
				Loggers.MSG.info("Started to serve the remote API @ port # " + port);

				// Start services only after the server is up
				fileMgrSvc.start();
				scenarioStepSvc.start();
				// Mark readiness after services start
				readinessGate.setReady(true);
				scenarioStepSvc.await();

				// Linger window after run completion/cleanup so clients can fetch final status/metrics
				final int lingerSec;
				try {
					lingerSec = fullDefaultConfig.intVal("api-linger-sec");
				} catch (final Exception ignore) {
					// Backward compatibility if config path is absent
					// Defaults.yaml will include this; if not present, skip linger
					//noinspection ConstantValue
					Loggers.MSG.debug("api-linger-sec not set; skipping API linger");
					return; // ensure finally still stops server
				}
				if (lingerSec > 0) {
					Loggers.MSG.info("API linger: keeping endpoints up for {} s", lingerSec);
					try {
						Thread.sleep(lingerSec * 1000L);
					} catch (final InterruptedException ie) {
						throw ie;
					}
				}
			} catch (final InterruptedException e) {
				throw e;
			} catch (final Throwable cause) {
				LogUtil.trace(Loggers.ERR, Level.FATAL, cause, "Run node failure");
			}
		} finally {
			server.stop();
		}
	}

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
			try {
				Files.lines(scenarioPath).forEach(line -> strb.append(line).append(System.lineSeparator()));
			} catch (final IOException e) {
				LogUtil.exception(Level.FATAL, e, "Failed to read the scenario file \"{}\"", scenarioPath);
				try {
					Files.list(scenarioPath.getParent()).forEach(System.out::println);
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

	private static void handleVersionRequest() {
		try {
			// Load the bundled defaults to get version
			final var schema = SchemaProvider.resolveAndReduce(APP_NAME, Thread.currentThread().getContextClassLoader());
			final var bundledDefaults = new BundledDefaultsProvider().config(ARG_PATH_SEP, schema);
			final var appVersion = bundledDefaults.stringVal("run-version");

			// Print version header
			final var msg = " " + APP_NAME + " v " + appVersion + " ";
			final var pad = StringUtils.repeat("#", (120 - msg.length()) / 2);
			System.out.println(pad + msg + pad);

			// Load and print extensions
			final var appHomePath = Paths.get(USER_HOME, "." + APP_NAME, appVersion);
			try (final var extClsLoader = Extension.extClassLoader(Paths.get(appHomePath.toString(), DIR_EXT).toFile())) {
				final var extensions = Extension.load(extClsLoader);
				if (!extensions.isEmpty()) {
					System.out.println("\nAvailable extensions:");
					extensions.forEach(ext -> {
						final var extId = ext.id();
						System.out.printf("\t%-30s %s%n", extId, ext.getClass().getCanonicalName());
					});
				} else {
					System.out.println("\nNo extensions loaded.");
				}
			}

			// Print additional info
			System.out.println("\nJava version: " + System.getProperty("java.version"));
			System.out.println("Java home: " + System.getProperty("java.home"));
			System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"));

		} catch (final Exception e) {
			System.err.println("Error getting version information: " + e.getMessage());
			System.exit(1);
		}
	}
}
