package com.dell.spt.base.control.run;

import static com.dell.spt.base.config.ConfigFormat.JSON;
import static com.dell.spt.base.config.ConfigFormat.YAML;
import static org.eclipse.jetty.http.MimeTypes.Type.APPLICATION_JSON;
import static org.eclipse.jetty.http.MimeTypes.Type.MULTIPART_FORM_DATA;
import static org.eclipse.jetty.http.MimeTypes.Type.TEXT_JSON;

import com.dell.spt.base.concurrent.SingleTaskExecutor;
import com.dell.spt.base.concurrent.SingleTaskExecutorImpl;
import com.dell.spt.base.buildinfo.EngineBuildInfoProvider;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InvalidBooleanValueException;
import com.dell.spt.base.config.InvalidTextValueException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import com.github.akurilov.confuse.exceptions.InvalidValueTypeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.script.ScriptEngine;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.github.akurilov.confuse.impl.BasicConfig;
import org.apache.logging.log4j.Level;
import org.eclipse.jetty.http.HttpHeader;

/** Servlet that orchestrates run submission, execution, and status updates. */
public class RunServlet extends HttpServlet {

	private static final String PART_KEY_DEFAULTS = "defaults";
	private static final String PART_KEY_SCENARIO = "scenario";

	private final ScriptEngine scriptEngine;
	private final List<Extension> extensions;
	private final MetricsManager metricsMgr;
	private final Config aggregatedConfigWithArgs;
	private final Path appHomePath;
	private final SingleTaskExecutor scenarioExecutor;
	private final LoadStepManagerService scenarioStepSvc;
	private final ApiStatus apiStatus;
	/** Makes the busy test and everything a start changes one step: a refused request changes nothing. */
	private final ReentrantLock runStartLock = new ReentrantLock();
	/** Outlives the executor slot, which a stop clears before the stopped run's cleanup returns. */
	private volatile StatusAwareRun lastAcceptedRun;

	public RunServlet(
					final ClassLoader clsLoader,
					final List<Extension> extensions,
					final MetricsManager metricsMgr,
					final Config aggregatedConfigWithArgs,
					final Path appHomePath,
					final LoadStepManagerService scenarioStepSvc,
					final ApiStatus apiStatus) {
		this(clsLoader, extensions, metricsMgr, aggregatedConfigWithArgs, appHomePath,
						scenarioStepSvc, apiStatus, new SingleTaskExecutorImpl());
	}

	RunServlet(
					final ClassLoader clsLoader,
					final List<Extension> extensions,
					final MetricsManager metricsMgr,
					final Config aggregatedConfigWithArgs,
					final Path appHomePath,
					final LoadStepManagerService scenarioStepSvc,
					final ApiStatus apiStatus,
					final SingleTaskExecutor scenarioExecutor) {
		final ClassLoader effectiveCl = (clsLoader != null) ? clsLoader : Thread.currentThread().getContextClassLoader();
		this.scriptEngine = ScenarioUtil.scriptEngineByDefault(effectiveCl);
		this.extensions = extensions;
		this.metricsMgr = metricsMgr;
		this.aggregatedConfigWithArgs = aggregatedConfigWithArgs;
		this.appHomePath = appHomePath;
		this.scenarioStepSvc = scenarioStepSvc;
		this.apiStatus = apiStatus;
		this.scenarioExecutor = scenarioExecutor;
	}

	@Override
	protected final void doPost(final HttpServletRequest req, final HttpServletResponse resp)
					throws IOException, ServletException {
		if (scenarioStepSvc.getStepService() != null) {
			resp.setStatus(HttpServletResponse.SC_CONFLICT);
			return;
		}
		final Part defaultsPart;
		final Part scenarioPart;
		final var contentTypeHeaderValue = req.getHeader(HttpHeader.CONTENT_TYPE.toString());
		if (contentTypeHeaderValue != null
						&& contentTypeHeaderValue.startsWith(MULTIPART_FORM_DATA.toString())) {
			defaultsPart = req.getPart(PART_KEY_DEFAULTS);
			scenarioPart = req.getPart(PART_KEY_SCENARIO);
		} else {
			defaultsPart = null;
			scenarioPart = null;
		}
		try {
			// Read outside the lock, so that a slow upload does not hold up other requests
			final var config = mergeIncomingWithLocalConfig(defaultsPart, resp, aggregatedConfigWithArgs);
			final var scenario = getIncomingScenarioOrDefault(scenarioPart, appHomePath);
			if (config.longVal("run-id") == 0) {
				config.val("run-id", System.currentTimeMillis());
			}
			startRunUnlessBusy(config, scenario, resp);
		} catch (final NoSuchMethodException | RuntimeException e) {
			LogUtil.exception(Level.WARN, e, "Failed to run a scenario with the request {}", req);
			setBadRequestResponse(resp, e);
		}
	}

	/**
	 * Starts the run, or answers 409 with nothing changed: the script engine is shared with the run in
	 * progress, whose later steps read its bindings.
	 */
	private void startRunUnlessBusy(final Config config, final String scenario, final HttpServletResponse resp)
					throws RemoteException {
		runStartLock.lock();
		try {
			if (activeTask() != null || scenarioStepSvc.getStepService() != null) {
				resp.setStatus(HttpServletResponse.SC_CONFLICT);
				return;
			}
			// expose the base configuration and the step types
			ScenarioUtil.configure(scriptEngine, extensions, config, metricsMgr);
			//
			final var run = (Run) new RunImpl(config.stringVal("run-comment"), scenario, scriptEngine, config.longVal("run-id"));
			final var stepId = config.stringVal("load-step-id");
			final var runId = config.longVal("run-id");
			// Update status before executing
			apiStatus.setRunning(stepId, runId);
			final var statusAwareRun = new StatusAwareRun(run, apiStatus);
			try {
				scenarioExecutor.execute(statusAwareRun);
				lastAcceptedRun = statusAwareRun;
				resp.setStatus(HttpServletResponse.SC_ACCEPTED);
				setRunTimestampHeader(statusAwareRun, resp);
			} catch (final RejectedExecutionException e) {
				// Only this method starts a task and it found the slot free, so this should not happen
				Loggers.ERR.warn("The scenario executor refused run {} on a node found idle", runId);
				resp.setStatus(HttpServletResponse.SC_CONFLICT);
			}
		} finally {
			runStartLock.unlock();
		}
	}

	/**
	 * The task in the executor slot or, once a stop has cleared the slot, the accepted run until its
	 * cleanup has returned and its terminal status is published.
	 */
	private Runnable activeTask() {
		final var slotTask = scenarioExecutor.task();
		if (slotTask != null) {
			return slotTask;
		}
		final var run = lastAcceptedRun;
		return run == null || run.isFinished() ? null : run;
	}

	/**
	 * Answers 400. A boolean value the engine refused is named in the body, as plain text, so the client
	 * that posted it can say which setting to correct; the message names a boolean setting and its value,
	 * never a secret. A defaults value refused where the setting takes text is named the same way; its
	 * message names the setting only. Any other failure keeps the empty body it had.
	 */
	static void setBadRequestResponse(final HttpServletResponse resp, final Throwable failure) throws IOException {
		resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		for (var cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof InvalidBooleanValueException || cause instanceof InvalidTextValueException) {
				resp.setContentType("text/plain; charset=UTF-8");
				resp.getWriter().print(cause.getMessage());
				return;
			}
		}
	}

	@Override
	protected final void doHead(final HttpServletRequest req, final HttpServletResponse resp) throws RemoteException {
		applyForActiveRunIfAny(resp, RunServlet::setRunExistsResponse);
	}

	@Override
	protected final void doGet(final HttpServletRequest req, final HttpServletResponse resp)
					throws IOException {
		extractRequestTimestampAndApply(
						req, resp, (run, runId) -> setRunMatchesResponse(run, resp, runId));
	}

	@Override
	protected final void doDelete(final HttpServletRequest req, final HttpServletResponse resp)
					throws IOException {
		extractRequestTimestampAndApply(
						req,
						resp,
						(run, runId) -> stopRunIfMatchesAndSetResponseInst(run, resp, runId));
	}

	static void setRunTimestampHeader(final Run task, final HttpServletResponse resp) {
		resp.setHeader(HttpHeader.ETAG.name(), String.valueOf(task.runId()));
	}

	void applyForActiveRunIfAny(
					final HttpServletResponse resp, final BiConsumer<Run, HttpServletResponse> action) throws RemoteException {
		final var activeTask = activeTask();
		final var activeService = scenarioStepSvc.getStepService();
		if (null == activeTask && activeService == null) {
			resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
		} else if (activeTask instanceof Run activeRun) {
			action.accept(activeRun, resp);
		} else if (activeService != null) {
			final var activeRun = new RunImpl("", "", null, activeService.runId());
			action.accept(activeRun, resp);
		} else {
			Loggers.ERR.warn("The scenario executor runs an alien task: {}", activeTask);
			resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
		}
	}

	void extractRequestTimestampAndApply(
					final HttpServletRequest req,
					final HttpServletResponse resp,
					final BiConsumer<Run, Long> runRespRunIdConsumer)
					throws IOException {
		final var rawIncomingRunId = Collections.list(req.getHeaders(HttpHeader.IF_MATCH.toString())).stream()
						.findAny()
						.orElse(null);

		if (rawIncomingRunId == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing header: " + HttpHeader.IF_MATCH);
		} else {
			try {
				final var incomingRunId = Long.parseLong(rawIncomingRunId);
				applyForActiveRunIfAny(
								resp, (run, resp_) -> runRespRunIdConsumer.accept(run, incomingRunId));
			} catch (final NumberFormatException e) {
				resp.sendError(
								HttpServletResponse.SC_BAD_REQUEST, "Invalid run ID format: " + rawIncomingRunId);
			}
		}
	}

	static void setRunExistsResponse(final Run run, final HttpServletResponse resp) {
		resp.setStatus(HttpServletResponse.SC_OK);
		setRunTimestampHeader(run, resp);
	}

	static void setRunMatchesResponse(
					final Run run, final HttpServletResponse resp, final long incomingRunId) {
		if (run.runId() == incomingRunId) {
			resp.setStatus(HttpServletResponse.SC_OK);
		} else {
			resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
		}
	}

	static void stopRunIfMatchesAndSetResponse(
					final Run run,
					final HttpServletResponse resp,
					final long runId,
					final SingleTaskExecutor scenarioExecutor) {
		if (run.runId() == runId) {
			if (run instanceof StatusAwareRun statusAwareRun) {
				statusAwareRun.requestStop(scenarioExecutor);
			} else {
				scenarioExecutor.stop(run);
			}
			if (null != scenarioExecutor.task()) {
				Loggers.ERR.warn("Run {} stop requested but task still active", runId);
			}
			resp.setStatus(HttpServletResponse.SC_OK);
		} else {
			resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
		}
	}

	private void stopRunIfMatchesAndSetResponseInst(
					final Run run,
					final HttpServletResponse resp,
					final long runId) {
		stopRunIfMatchesAndSetResponse(run, resp, runId, scenarioExecutor);
	}

	/**
	 * Requests cancellation of the active API run and waits for its cleanup and
	 * terminal-status publication. A false return means the bounded wait expired.
	 */
	public boolean stopActiveRunAndAwait(final long timeout, final TimeUnit timeUnit)
					throws InterruptedException {
		final var activeTask = scenarioExecutor.task();
		if (activeTask == null) {
			return true;
		}
		if (activeTask instanceof StatusAwareRun statusAwareRun) {
			return statusAwareRun.stopAndAwait(scenarioExecutor, timeout, timeUnit);
		}
		scenarioExecutor.stop(activeTask);
		Loggers.ERR.warn("Stopped an unrecognized API task without completion evidence: {}", activeTask);
		return false;
	}

	/**
	 * Wrapper around a Run that publishes terminal status only after the
	 * delegate's cleanup has returned: {@code STOPPED} after a requested stop, {@code FAILED} with
	 * a category and an engine-built message after a scenario failure, else {@code COMPLETED}.
	 */
	static final class StatusAwareRun implements Run {

		private enum ExecutionState {
			NEW, RUNNING, FINISHED
		}

		private final Run delegate;
		private final ApiStatus apiStatus;
		private final AtomicBoolean stopRequested = new AtomicBoolean();
		private final AtomicReference<ExecutionState> executionState = new AtomicReference<>(ExecutionState.NEW);
		private final CountDownLatch completion = new CountDownLatch(1);

		StatusAwareRun(final Run delegate, final ApiStatus apiStatus) {
			this.delegate = delegate;
			this.apiStatus = apiStatus;
		}

		@Override
		public long runId() throws IllegalStateException {
			return delegate.runId();
		}

		@Override
		public String comment() {
			return delegate.comment();
		}

		@Override
		public void run() {
			if (!executionState.compareAndSet(ExecutionState.NEW, ExecutionState.RUNNING)) {
				return;
			}
			Throwable failure = null;
			try {
				failure = runDelegate();
			} catch (final RunTerminalException e) {
				apiStatus.setFailed(e.stepId(), e.category(), e.getMessage());
				throw e;
			} catch (final Error e) {
				// A Java error from a call the scenario makes passes the script engine as it is
				failure = e;
				throw e;
			} finally {
				try {
					// A stop's interruption reaches the scenario as a failure, so a requested stop wins.
					if (stopRequested.get()) {
						apiStatus.setStopped();
					} else {
						if (failure != null) {
							apiStatus.setFailed(
											null, ScenarioFailureStatus.category(failure), ScenarioFailureStatus.message(failure));
						}
						apiStatus.completeIfNotStopped();
					}
				} finally {
					executionState.set(ExecutionState.FINISHED);
					completion.countDown();
				}
			}
		}

		/**
		 * Runs the delegate and returns the scenario's failure, which a scenario run logs and does not
		 * throw; a typed terminal failure is thrown.
		 */
		private Throwable runDelegate() {
			if (delegate instanceof RunImpl scenarioRun) {
				return scenarioRun.runAndReturnFailure();
			}
			delegate.run();
			return null;
		}

		/** True once the cleanup has returned and the terminal status is published. */
		boolean isFinished() {
			return completion.getCount() == 0;
		}

		boolean stopAndAwait(
						final SingleTaskExecutor scenarioExecutor,
						final long timeout,
						final TimeUnit timeUnit)
						throws InterruptedException {
			requestStop(scenarioExecutor);
			return completion.await(timeout, timeUnit);
		}

		private void requestStop(final SingleTaskExecutor scenarioExecutor) {
			stopRequested.set(true);
			scenarioExecutor.stop(this);
			if (executionState.compareAndSet(ExecutionState.NEW, ExecutionState.FINISHED)) {
				apiStatus.setStopped();
				completion.countDown();
			}
		}
	}

	static Config mergeIncomingWithLocalConfig(
					final Part defaultsPart,
					final HttpServletResponse resp,
					final Config aggregatedConfigWithArgs)
					throws IOException, NoSuchMethodException, InvalidValuePathException,
					InvalidValueTypeException {
		final Config configResult;
		boolean runVersionOverrideAttempted = false;
		if (defaultsPart == null) {
			// NOTE: If custom config hasn't been specified in POST request, set the default one
			configResult = new BasicConfig(aggregatedConfigWithArgs);
		} else {
			final var configIncoming = configFromPart(defaultsPart, resp, aggregatedConfigWithArgs.schema());
			runVersionOverrideAttempted = containsRunVersion(configIncoming);
			// the load step id was set manually if it is set to some non-null/non-empty value in the incoming config
			try {
				final var loadStepIdIncoming = configIncoming.stringVal("load-step-id");
				if (loadStepIdIncoming != null && !loadStepIdIncoming.isEmpty()) {
					configIncoming.val("load-step-idAutoGenerated", false);
				}
			} catch (final NoSuchElementException e) {
				Loggers.MSG.debug("Incoming config has no load-step-id; default auto-generation remains enabled");
			}
			configResult = ConfigUtil.merge(
							aggregatedConfigWithArgs.pathSep(),
							Arrays.asList(aggregatedConfigWithArgs, configIncoming));
		}
		EngineBuildInfoProvider.global().projectVersion(configResult, runVersionOverrideAttempted);
		return configResult;
	}

	private static boolean containsRunVersion(final Config config) {
		final Map<String, Object> root = config.mapVal("");
		if (!root.containsKey("run")) {
			return false;
		}
		return config.configVal("run").mapVal("").containsKey("version");
	}

	static Config configFromPart(
					final Part defaultsPart,
					final HttpServletResponse resp,
					final Map<String, Object> configSchema)
					throws IOException, NoSuchMethodException, InvalidValuePathException,
					InvalidValueTypeException {
		final String rawDefaultsData;
		try (final var br = new BufferedReader(
						new InputStreamReader(defaultsPart.getInputStream(), StandardCharsets.UTF_8))) {
			rawDefaultsData = br.lines().collect(Collectors.joining("\n"));
		}
		final var contentType = defaultsPart.getContentType();
		ConfigFormat format = YAML;
		if (contentType != null) {
			if (contentType.startsWith(APPLICATION_JSON.toString())) {
				format = JSON;
			} else if (contentType.startsWith(TEXT_JSON.toString())) {
				format = JSON;
			}
		}
		try {
			return ConfigUtil.loadConfig(rawDefaultsData, format, configSchema);
		} catch (final JsonParseException e) {
			if (YAML.equals(format)) {
				// was unable to detect format using content-type header (likely application/octet-stream),
				// fallback is YAML, but it may JSON actually
				return ConfigUtil.loadConfig(rawDefaultsData, JSON, configSchema);
			} else {
				throw e;
			}
		}
	}

	static String getIncomingScenarioOrDefault(final Part scenarioPart, final Path appHomePath)
					throws IOException {
		final String scenarioResult;
		if (scenarioPart == null) {
			scenarioResult = ScenarioUtil.defaultScenario(appHomePath);
		} else {
			try (final var br = new BufferedReader(
							new InputStreamReader(scenarioPart.getInputStream(), StandardCharsets.UTF_8))) {
				scenarioResult = br.lines().collect(Collectors.joining("\n"));
			}
		}
		return scenarioResult;
	}

	@Override
	public final void destroy() {
		scenarioExecutor.close();
		super.destroy();
	}
}
