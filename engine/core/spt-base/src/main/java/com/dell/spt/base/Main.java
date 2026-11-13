package com.dell.spt.base;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.DIR_EXT;
import static com.dell.spt.base.Constants.MIB;
import static com.dell.spt.base.Constants.PATH_DEFAULTS;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.buildinfo.EngineBuildInfoPublisher;
import com.dell.spt.base.buildinfo.EngineBuildInfoProvider;
import com.dell.spt.base.buildinfo.EngineBuildInfoRenderer;
import com.dell.spt.base.config.AliasingUtil;
import com.dell.spt.base.config.CliArgUtil;
import com.dell.spt.base.config.CliArgumentException;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalArgumentNameException;
import com.dell.spt.base.control.AddCorsHeadersRule;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.control.NodeShutdownCoordinator;
import com.dell.spt.base.control.StatusServlet;
import com.dell.spt.base.control.VersionServlet;
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
import com.github.akurilov.commons.collection.TreeUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.prometheus.client.exporter.MetricsServlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

	static String resolveLogPath() {
		final String logDirEnv = System.getenv("SPT_LOG_DIR");
		final String logDirProp = System.getProperty("spt.log.dir");
		if (logDirEnv != null && !logDirEnv.isEmpty()) {
			return logDirEnv;
		} else if (logDirProp != null && !logDirProp.isEmpty()) {
			return logDirProp;
		} else {
			return System.getProperty("user.dir");
		}
	}

	/** The engine process's exit statuses, a contract for the scripts that run it. A run that ends normally exits 0. */
	static final int EXIT_SUCCESS = 0;
	/** A run, start or scenario failure: the node or the scenario could not start, or the scenario failed. */
	static final int EXIT_FAILURE = 1;
	/** An argument error, as {@code spt-nic-plan}'s usage error: nothing was started. */
	static final int EXIT_ARGUMENT_ERROR = 2;

	static final List<String> HELP_ARGS = List.of("--help", "-h");
	private static final String LOG4J_CONTEXT_FACTORY_PROPERTY = "log4j2.loggerContextFactory";
	private static final String LOG4J_SIMPLE_CONTEXT_FACTORY = "org.apache.logging.log4j.simple.SimpleLoggerContextFactory";

	public static void main(final String... args) {

		// Check for version flag early
		for (String arg : args) {
			if ("--version".equals(arg) || "-v".equals(arg)) {
				handleVersionRequest();
				return;
			}
		}
		for (final String arg : args) {
			if (HELP_ARGS.contains(arg)) {
				// An extension's static logger would load the log configuration, whose file appenders create
				// their directories under the working directory; the help request writes nothing.
				System.setProperty(LOG4J_CONTEXT_FACTORY_PROPERTY, LOG4J_SIMPLE_CONTEXT_FACTORY);
				handleHelpRequest();
				return;
			}
		}

		final int status = start(args);
		if (status != EXIT_SUCCESS) {
			// A run started directly has only its exit status to tell its caller that it failed. The log
			// shutdown hook flushes the asynchronous loggers, so the error is printed before the exit.
			System.exit(status);
		}
	}

	/** Starts the node or the scenario the arguments configure; returns the exit status. */
	private static int start(final String... args) {
		final var initialStepId = "none-" + LogUtil.getDateTimeStamp();
		// Build identity and its fallback warning may initialize logging.
		LogUtil.init(resolveLogPath(), initialStepId);
		final var coreResources = new CoreResourcesToInstall();
		final var appHomePath = coreResources.appHomePath();
		Loggers.MSG.info(EngineBuildInfoRenderer.startupLine(EngineBuildInfoProvider.global().snapshot()));
		try {
			// install the core resources
			coreResources.install(appHomePath);
			// load the defaults
			final var defaultConfig = loadDefaultConfig(appHomePath);
			// extensions: prefer ext/ next to the JAR, fall back to appHomePath/ext (IDE/dev)
			final var extDir = Extension.resolveExtDir();
			final var extDirFile = extDir != null ? extDir : Paths.get(appHomePath.toString(), DIR_EXT).toFile();
			try (final var extClsLoader = Extension.extClassLoader(extDirFile)) {
				final var extensions = Extension.load(extClsLoader);
				// install the extensions
				installExtensions(extensions, appHomePath);
				final Config fullDefaultConfig;
				try {
					// apply the extensions defaults
					fullDefaultConfig = collectDefaults(extensions, defaultConfig, appHomePath);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.exception(Level.ERROR, e, "Failed to load the defaults");
					throw e;
				}
				// parse the CLI args and apply them to the config instance; an argument error stops here
				final Config configWithArgs = applyArgsToConfig(args, fullDefaultConfig, initialStepId);

				applyVtParallelism(configWithArgs);
				Loggers.MSG.info(
								"Using {} for service tasks",
								ServiceTaskExecutor.TASK_EXECUTOR.getClass().getSimpleName());

				// init the metrics manager
				final MetricsManager metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.TASK_EXECUTOR);
				// go on
				if (configWithArgs.boolVal("run-node")) {
					runNode(configWithArgs, extClsLoader, extensions, metricsMgr, appHomePath);
				} else {
					EngineBuildInfoPublisher.global().publish(Path.of(resolveLogPath()), initialStepId);
					runScenario(configWithArgs, extensions, extClsLoader, metricsMgr, appHomePath);
				}
			}
		} catch (final InterruptedException e) {
			Loggers.MSG.debug("Interrupted", e);
		} catch (final CliArgumentException e) {
			Loggers.ERR.fatal("{}", e.getMessage());
			return EXIT_ARGUMENT_ERROR;
		} catch (final Exception e) {
			// A start failure, a scenario failure or a typed terminal failure of the run
			if (!(e instanceof ReportedFailure)) {
				LogUtil.trace(Loggers.ERR, Level.FATAL, e, "Unexpected failure");
			}
			return EXIT_FAILURE;
		}
		return EXIT_SUCCESS;
	}

	/** A start, run or scenario failure that is already logged; the engine exits with status 1. */
	static final class ReportedFailure extends RuntimeException {

		ReportedFailure(final String message, final Throwable cause) {
			super(message, cause);
		}
	}

	static void applyVtParallelism(final Config config) {
		try {
			final int vtParallelism = config.intVal("load-service-threads");
			if (vtParallelism > 0) {
				System.setProperty("jdk.virtualThreadScheduler.parallelism", String.valueOf(vtParallelism));
				Loggers.MSG.info("Set virtual thread scheduler parallelism to: {}", vtParallelism);
			}
		} catch (Exception e) {
			Loggers.ERR.warn("Failed to set virtual thread parallelism from config", e);
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

	/**
	 * The message for {@code arg}, the first of {@code args} without the {@code --} prefix. It never
	 * shows a value. After an argument with no {@code =}, it may be that argument's value given after a
	 * space, as in {@code --storage-auth-secret s3cr3t}, or {@code --storage-auth-secret pEau...=} with
	 * base64 padding, so it is named by the argument before it. Otherwise an argument with {@code =} is
	 * named by the part before it: the value of a mistyped {@code storage-auth-secret=...} is a
	 * credential.
	 */
	static String argumentWithoutPrefixMessage(final String[] args, final String arg) {
		final String form = ": an argument takes the form --<name>=<value>; --help lists the options";
		// Every argument before the first one without the prefix has it, so none of them equals arg.
		final int index = Arrays.asList(args).indexOf(arg);
		final String before = index > 0 ? args[index - 1] : null;
		if (before != null && !before.contains(CliArgUtil.ARG_VAL_SEP)) {
			return "Invalid argument after \"" + before + "\", not shown in case it is a value" + form;
		}
		if (arg.contains(CliArgUtil.ARG_VAL_SEP)) {
			return "Invalid argument: \"" + StringUtils.substringBefore(arg, CliArgUtil.ARG_VAL_SEP) + "\"" + form;
		}
		if (before != null) {
			return "Invalid argument after \"" + StringUtils.substringBefore(before, CliArgUtil.ARG_VAL_SEP)
							+ "\", not shown in case it is a value" + form;
		}
		return "Invalid first argument, not shown in case it is a value" + form;
	}

	/**
	 * Parses the command-line arguments and applies them to the configuration.
	 *
	 * <p>Option names are checked by the configuration itself, as it sets each value, so a map option
	 * such as {@code --storage-net-nodeInterfaces-<key>} takes any key. The values are applied in the
	 * order the parsed argument map gives, which is not the command-line order.
	 *
	 * @throws CliArgumentException for the first argument that cannot be applied. The configuration
	 *         may hold the arguments applied before it; the engine stops without using it.
	 */
	static Config applyArgsToConfig(
					final String[] args, final Config config, final String initialStepId) {
		final Map<String, String> parsedArgs;
		try {
			parsedArgs = CliArgUtil.parseArgs(args);
		} catch (final IllegalArgumentNameException e) {
			throw new CliArgumentException(argumentWithoutPrefixMessage(args, e.getMessage()), e);
		}
		final List<Map<String, Object>> aliasingConfig = config.listVal("aliasing");
		final Map<String, String> configArgs;
		try {
			configArgs = AliasingUtil.apply(parsedArgs, aliasingConfig);
		} catch (final IllegalArgumentException e) {
			// An alias with no target: an option the engine no longer takes
			throw new CliArgumentException(e.getMessage(), e);
		}
		for (final var arg : configArgs.entrySet()) {
			applyArg(config, arg.getKey(), arg.getValue(), givenName(arg.getKey(), parsedArgs, aliasingConfig));
		}
		EngineBuildInfoProvider.global().projectVersion(config, configArgs.containsKey("run-version"));
		checkAndSetStepId(config, initialStepId);
		applyLogLevel(config);
		Arrays.stream(args).forEach(Loggers.CLI::info);
		return config;
	}

	private static void applyArg(final Config config, final String path, final String value, final String givenName) {
		if (!ConfigUtil.isAcceptedBooleanValue(config.schema(), path, config.pathSep(), value)) {
			throw new CliArgumentException(
							"Invalid value for the option \"" + givenName + "\": \"" + value + "\" is not true or false", null);
		}
		try {
			config.val(path, value);
		} catch (final InvalidValuePathException e) {
			throw new CliArgumentException(
							"Invalid configuration option: \"" + givenName + "\"; --help lists the options", e);
		} catch (final InvalidValueTypeException e) {
			throw new CliArgumentException(
							"Invalid configuration value type for the option \"" + givenName + "\", expected: "
											+ e.expectedType() + ", actual: " + e.actualType(),
							e);
		} catch (final RuntimeException e) {
			// A value the option's type cannot parse, such as a number or a boolean
			throw new CliArgumentException(
							"Invalid value for the option \"" + givenName + "\": " + e.getMessage(), e);
		}
	}

	/** The option as the command line gave it, for messages: an alias's own name when an alias set the path. */
	private static String givenName(
					final String path, final Map<String, String> parsedArgs, final List<Map<String, Object>> aliasingConfig) {
		if (parsedArgs.containsKey(path)) {
			return path;
		}
		if (aliasingConfig != null) {
			for (final var entry : aliasingConfig) {
				if (entry.get(AliasingUtil.NAME) instanceof String name
								&& parsedArgs.containsKey(name)
								&& entry.get(AliasingUtil.TARGET) instanceof String target
								&& path.equals(StringUtils.substringBefore(target, AliasingUtil.ARG_VAL_SEP))) {
					return name;
				}
			}
		}
		return path;
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
		context.addServlet(
						new ServletHolder(new VersionServlet(EngineBuildInfoProvider.global().snapshot())), "/version");
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
				final var runServlet = new RunServlet(
								extClsLoader,
								extensions,
								metricsMgr,
								fullDefaultConfig,
								appHomePath,
								scenarioStepSvc,
								apiStatus);
				final var runServletHolder = new ServletHolder(runServlet);
				context.addServlet(runServletHolder, "/run");
				runServletHolder
								.getRegistration()
								.setMultipartConfig(new MultipartConfigElement("", 16 * MIB, 16 * MIB, 16 * MIB));

				// Stop and join active work before service closure begins API linger.
				final var shutdownCoordinator = new NodeShutdownCoordinator(
								runServlet, java.util.List.of(fileMgrSvc, scenarioStepSvc));
				context.addServlet(new ServletHolder(new ShutdownServlet(shutdownCoordinator)), "/shutdown");

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
				throw new ReportedFailure("Run node failure", cause);
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
		initializeRunId(config);
		Path scenarioPath = null;
		final var scenarioFile = config.stringVal("run-scenario");
		if (scenarioFile != null && !scenarioFile.isEmpty()) {
			scenarioPath = Paths.get(scenarioFile);
		}
		runScenarioFile(config, extensions, extClsLoader, metricsMgr, scenarioPath, appHomePath);
	}

	static void initializeRunId(final Config config) {
		if (config.longVal("run-id") == 0L) {
			config.val("run-id", System.currentTimeMillis());
		}
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
			scenarioText = readScenarioFile(scenarioPath);
		}
		if (scriptEngine == null) {
			Loggers.ERR.fatal("Failed to resolve the scenario engine for the file \"{}\"", scenarioPath);
			throw new ReportedFailure("No scenario engine", null);
		}
		Loggers.MSG.info(
						"Using the \"{}\" scenario engine", scriptEngine.getFactory().getEngineName());
		// expose the environment values
		System.getenv().forEach(scriptEngine::put);
		// expose the loaded configuration and the step types
		ScenarioUtil.configure(scriptEngine, extensions, config, metricsMgr);
		// go
		final var failure = new RunImpl("", scenarioText, scriptEngine, config.longVal("run-id")).runAndReturnFailure();
		if (failure != null) {
			throw new ReportedFailure("The scenario failed", failure);
		}
	}

	/**
	 * Reads a scenario file.
	 *
	 * @throws ReportedFailure when the file cannot be read, after logging it and listing the entries of
	 *         its directory
	 */
	static String readScenarioFile(final Path scenarioPath) {
		final var strb = new StringBuilder();
		try (BufferedReader reader = Files.newBufferedReader(scenarioPath, StandardCharsets.UTF_8)) {
			String line;
			while ((line = reader.readLine()) != null) {
				strb.append(line).append(System.lineSeparator());
			}
		} catch (final IOException e) {
			LogUtil.exception(Level.FATAL, e, "Failed to read the scenario file \"{}\"", scenarioPath);
			final Path parent = scenarioPath.getParent();
			if (parent != null) {
				try (Stream<Path> entries = Files.list(parent)) {
					entries.forEach(System.out::println);
				} catch (final IOException ee) {
					LogUtil.trace(
									Loggers.ERR, Level.ERROR, ee, "Failed to list the scenarios parent directory");
				}
			}
			throw new ReportedFailure("Failed to read the scenario file", e);
		}
		return strb.toString();
	}

	private static void handleHelpRequest() {
		try {
			System.out.print(helpText());
		} catch (final Exception e) {
			System.err.println("Error getting the list of options: " + e);
			System.exit(EXIT_FAILURE);
		}
	}

	/**
	 * The usage text for {@code --help}: the options of the base schema and of each extension's schema,
	 * and the aliases of the bundled defaults. It sets up no logging and installs nothing, so it reads
	 * the schemas and the defaults from the jars; the extensions load as for {@code --version}.
	 */
	static String helpText() throws Exception {
		final var ctxClsLoader = Thread.currentThread().getContextClassLoader();
		final List<Map<String, Object>> schemas = new ArrayList<>();
		schemas.add(SchemaProvider.resolve(APP_NAME, ctxClsLoader).stream()
						.findFirst()
						.orElseThrow(() -> new IllegalStateException("No configuration schema for \"" + APP_NAME + "\"")));
		final var extDir = Extension.resolveExtDir();
		if (extDir != null) {
			try (final var extClsLoader = Extension.extClassLoader(extDir)) {
				for (final var ext : Extension.load(extClsLoader)) {
					final var schemaProvider = ext.schemaProvider();
					if (schemaProvider != null) {
						schemas.add(schemaProvider.schema());
					}
				}
			}
		}
		final var schema = schemas.stream().reduce(TreeUtil::addBranches).orElseThrow();
		final Map<String, Object> defaults;
		try (InputStream in = Main.class.getResourceAsStream("/" + PATH_DEFAULTS)) {
			defaults = in == null
							? Map.of()
							: new YAMLMapper().readValue(in, new TypeReference<Map<String, Object>>() {});
		}
		@SuppressWarnings("unchecked")
		final var aliasingConfig = defaults.get("aliasing") instanceof List<?> aliasing
						? (List<Map<String, Object>>) aliasing
						: List.<Map<String, Object>> of();
		final var text = new StringBuilder()
						.append("Usage: java -jar spt.jar [--<option>=<value> ...]\n")
						.append("       java -jar spt.jar --version | --help | -h\n")
						.append("run.sh and the engine image take the same arguments.\n\n")
						.append("The engine runs the scenario that --run-scenario names, or the default scenario, and exits.\n")
						.append("With --run-node=true it serves the remote API as a worker node instead.\n")
						.append("A boolean option given with no value is true; a list takes comma-separated values.\n")
						.append("Exit status: 0 success; 1 a run, start or scenario failure; 2 an argument error.\n\n");
		if (extDir == null) {
			text.append("The extensions' options are not listed: no ext/ directory next to the jar.\n\n");
		}
		return text.append(CliArgUtil.usage(schema, aliasingConfig)).toString();
	}

	private static void handleVersionRequest() {
		try {
			final var buildInfo = EngineBuildInfoProvider.global().snapshot();

			// Print version header
			System.out.println(EngineBuildInfoRenderer.banner(buildInfo));
			EngineBuildInfoRenderer.versionDetails(buildInfo).forEach(System.out::println);

			// Load and print extensions from ext/ next to the JAR
			final var extDir = Extension.resolveExtDir();
			if (extDir != null) {
				try (final var extClsLoader = Extension.extClassLoader(extDir)) {
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
			} else {
				System.out.println("\nNo extensions loaded (ext/ directory not found next to JAR).");
			}

			// Print additional info
			System.out.println("\nJava version: " + System.getProperty("java.version"));
			System.out.println("Java home: " + System.getProperty("java.home"));
			System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"));

		} catch (final Exception e) {
			System.err.println("Error getting version information: " + e.getMessage());
			System.exit(EXIT_FAILURE);
		}
	}
}
