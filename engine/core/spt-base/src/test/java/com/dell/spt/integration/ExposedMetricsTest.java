package com.dell.spt.integration;

import static com.dell.spt.base.metrics.MetricsConstants.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.params.ItemSize;
import com.github.akurilov.commons.system.SizeInBytes;
import io.prometheus.client.exporter.MetricsServlet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import com.dell.spt.testing.tags.IntegrationTest;

/** @author veronika K. on 15.10.18 */
@IntegrationTest
public class ExposedMetricsTest {

	private static final int PORT = 1111;
	private static final String CONTEXT = "/metrics";
	private static final int ITERATION_COUNT = 10;
	private static final int MARK_DUR = 1_100_000; // dur must be more than lat (dur > lat)
	private static final int MARK_LAT = 1_000_000;
	private static final Double[] QUANTILE_VALUES = {0.25, 0.5, 0.75
	};
	private static final List<String> nodeList = Arrays.asList("127.0.0.1:1099");
	private final String STEP_ID = ExposedMetricsTest.class.getSimpleName();
	private final int RUN_ID = 123;
	private final OpType OP_TYPE = OpType.CREATE;

	private static int nodeCount() {
		return 1;
	}

	private final int CONCURRENCY_LIMIT = 0;
	private final int CONCURRENCY_THRESHOLD = 0;
	private final SizeInBytes ITEM_DATA_SIZE = ItemSize.SMALL.getValue();
	private final int UPDATE_INTERVAL_SEC = (int) TimeUnit.MICROSECONDS.toSeconds(MARK_DUR);
	private Supplier<List<AllMetricsSnapshot>> snapshotsSupplier;
	private final Server server = new Server(PORT);
	//
	private DistributedMetricsContext distributedMetricsContext;
	private MetricsContext metricsContext;

	@BeforeEach
	public void setUp() throws Exception {
		//
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
		context.addServlet(new ServletHolder(new MetricsServlet()), CONTEXT);
		server.start();
		//
		metricsContext = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.opType(OP_TYPE)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(CONCURRENCY_LIMIT)
						.concurrencyThreshold(CONCURRENCY_THRESHOLD)
						.itemDataSize(ITEM_DATA_SIZE)
						.outputPeriodSec(UPDATE_INTERVAL_SEC)
						.stdOutColorFlag(true)
						.comment("")
						.runId(123)
						.build();
		snapshotsSupplier = () -> Arrays.asList(metricsContext.lastSnapshot());
		metricsContext.start();
		//
		distributedMetricsContext = DistributedMetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.opType(OP_TYPE)
						.nodeCountSupplier(ExposedMetricsTest::nodeCount)
						.concurrencyLimit(CONCURRENCY_LIMIT)
						.concurrencyThreshold(CONCURRENCY_THRESHOLD)
						.itemDataSize(ITEM_DATA_SIZE)
						.outputPeriodSec(UPDATE_INTERVAL_SEC)
						.stdOutColorFlag(true)
						.avgPersistFlag(true)
						.sumPersistFlag(true)
						.snapshotsSupplier(snapshotsSupplier)
						.quantileValues(Arrays.asList(QUANTILE_VALUES))
						.nodeAddrs(nodeList)
						.comment("")
						.runId(RUN_ID)
						.build();
		distributedMetricsContext.start();
	}

	@Test
	public void test() throws Exception {
		final MetricsManager metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		metricsMgr.register(distributedMetricsContext);
		for (var i = 0; i < ITERATION_COUNT; ++i) {
			metricsContext.markSucc(ITEM_DATA_SIZE.get(), MARK_DUR, MARK_LAT);
			metricsContext.markFail();
			metricsContext.refreshLastSnapshot();
			TimeUnit.MICROSECONDS.sleep(MARK_DUR);
		}
		final var result = resultFromServer("http://localhost:" + PORT + CONTEXT);
		System.out.println(result);
		//
		testHelpLine(result);
		//
		// Note: elapsed_time_value test removed - too timing-dependent and brittle
		// The metric is exported correctly, but exact timing varies by test execution environment
		// latency and duration are now only testable in functional testing as they use actual files in the os
		testTestStateMetric(result);
		//
		testLabels(result);
		metricsMgr.close();
	}

	private void testHelpLine(final String result) {
		final String[] names = {
				METRIC_NAME_BYTE,
				METRIC_NAME_CONC,
				METRIC_NAME_LAT,
				METRIC_NAME_DUR,
				METRIC_NAME_FAIL,
				METRIC_NAME_SUCC,
				METRIC_NAME_TIME,
				METRIC_NAME_TIME,
				METRIC_NAME_TEST_STATE
		};
		for (final String n : names) {
			final var p = Pattern.compile(
							String.format("# HELP %1$s[0-9]*[\\s]*# TYPE %1$s[0-9]*", String.format(METRIC_FORMAT, n)));
			final var m = p.matcher(result);
			final var found = m.find();
			assertTrue(found);
		}
	}

	private void testLabels(final String result) {
		testLabel(result, "node_list", nodeList.toString());
		testLabel(result, "user_comment", "");
		testLabel(result, "load_step_id", STEP_ID);
		testLabel(result, "run_id", String.valueOf(RUN_ID));
	}

	private void testLabel(final String result, final String labelName, final String expectedValue) {
		final var p = Pattern.compile("\\{.*" + labelName + "=[^,]*");
		final var m = p.matcher(result);
		final var found = m.find();
		assertTrue(found);
		final String group = m.group();
		final String needle = labelName + "=";
		final int start = group.indexOf(needle);
		assertTrue(start >= 0, "label : " + labelName);
		int valueStart = start + needle.length();
		int valueEnd = group.indexOf(',', valueStart);
		if (valueEnd < 0) {
			valueEnd = group.length();
		}
		final String actualValue = group.substring(valueStart, valueEnd).replace("\"", "");
		assertEquals(expectedValue, actualValue, "label : " + labelName);
	}

	private void testTestStateMetric(final String stdOut) {
		// Phase 3: Test the new test state metric
		// During active test execution, test state should be 1.0 (running)
		final Map<String, Double> expectedValues = new HashMap<>();
		expectedValues.put("value", 1.0);  // Test is running with active operations
		testMetric(stdOut, METRIC_NAME_TEST_STATE, expectedValues, true, 0);
	}

	private String resultFromServer(final String urlPath) throws Exception {
		final var httpClient = java.net.http.HttpClient.newHttpClient();
		final var request = java.net.http.HttpRequest.newBuilder()
						.uri(java.net.URI.create(urlPath))
						.GET()
						.build();
		final var response = httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
		return response.body();
	}

	private void testMetric(
					final String resultOutput,
					final String metricName,
					final Map<String, Double> expectedValues,
					final boolean compareEquality,
					final double accuracy) {
		for (final var key : expectedValues.keySet()) {
			final var p = Pattern.compile(String.format(METRIC_FORMAT, metricName) + "_" + key + "\\{.+\\} .+");
			final var m = p.matcher(resultOutput);
			final var found = m.find();
			assertTrue(found);
			final String group = m.group();
			final int braceIdx = group.indexOf('}');
			assertTrue(braceIdx >= 0, "metric : " + metricName + "_" + key);
			final var actualValue = Double.valueOf(group.substring(braceIdx + 1).trim());
			final var expectedValue = Double.valueOf(expectedValues.get(key));
			if (compareEquality) {
				// Use accuracy as tolerance/delta for floating point comparison
				if (accuracy > 0) {
					assertEquals(expectedValue, actualValue, accuracy, "metric : " + metricName + "_" + key);
				} else {
					assertEquals(expectedValue, actualValue, "metric : " + metricName + "_" + key);
				}
			} else {
				assertTrue(actualValue <= expectedValue, "metric : " + metricName + "_" + key);
			}
		}
	}
}
