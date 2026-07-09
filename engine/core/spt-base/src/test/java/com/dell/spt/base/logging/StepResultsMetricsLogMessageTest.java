package com.dell.spt.base.logging;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.Constants;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.type.ConcurrencyMeterImpl;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class StepResultsMetricsLogMessageTest extends StepResultsMetricsLogMessage {

	private static final OpType OP_TYPE = OpType.READ;
	private static final String STEP_ID = StepResultsMetricsLogMessageTest.class.getSimpleName();
	private static final int COUNT = 123456;
	private static final int DUR_MAX = 31416;
	private static final int LAT_MAX = 27183;
	private static final long[] DURATIONS = new long[COUNT];
	private static long durSum = 0;
	private static Map<Double, Long> latencies;
	private static Map<Double, Long> durations;
	static {
		for (int i = 0; i < COUNT; i++) {
			DURATIONS[i] = System.nanoTime() % DUR_MAX;
			durSum += DURATIONS[i];
		}
	}

	private static final long[] LATENCIES = new long[COUNT];
	static {
		for (int i = 0; i < COUNT; i++) {
			LATENCIES[i] = System.nanoTime() % LAT_MAX;
		}
	}

	private static final long[] CONCURRENCIES = new long[COUNT];

	static {
		for (int i = 0; i < COUNT; i++) {
			CONCURRENCIES[i] = 10;
		}
	}

	private static final DistributedAllMetricsSnapshot SNAPSHOT;

	static {
		final TimingMetricSnapshot dS = new TimingMeterImpl(MetricsConstants.METRIC_NAME_DUR).snapshot();
		final TimingMetricSnapshot lS = new TimingMeterImpl(MetricsConstants.METRIC_NAME_LAT).snapshot();
		final ConcurrencyMetricSnapshot cS = new ConcurrencyMeterImpl(MetricsConstants.METRIC_NAME_CONC).snapshot();
		final RateMetricSnapshot fS = new RateMetricSnapshotImpl(0, 0, MetricsConstants.METRIC_NAME_FAIL, 0, 0);
		final double countDividedByDur = (double) COUNT / durSum;
		final RateMetricSnapshot sS = new RateMetricSnapshotImpl(countDividedByDur, countDividedByDur, MetricsConstants.METRIC_NAME_SUCC, COUNT, 0);
		final RateMetricSnapshot bS = new RateMetricSnapshotImpl(
						countDividedByDur,
						countDividedByDur,
						MetricsConstants.METRIC_NAME_BYTE,
						Double.valueOf(COUNT * Constants.K).longValue(),
						0);
		SNAPSHOT = new DistributedAllMetricsSnapshotImpl(dS, lS, cS, fS, sS, bS, 2, 123456);
		// there is no way to unit-test TimingMetricQuantileResultsImpl as it requires creating several files with actual
		// metrics, so it's only covered by functional tests
		latencies = new LinkedHashMap<>();
		latencies.put(0.25, 25L);
		latencies.put(0.5, 50L);
		latencies.put(0.75, 75L);
		durations = new LinkedHashMap<>();
		durations.put(0.25, 26L);
		durations.put(0.5, 51L);
		durations.put(0.75, 76L);
	}

	public StepResultsMetricsLogMessageTest() {
		super(OP_TYPE, STEP_ID, 0, SNAPSHOT, latencies, durations);
	}

	@Test
	public final void testIsValidYaml() throws Exception {
		final StringBuilder buff = new StringBuilder();
		formatTo(buff);
		System.out.println(buff.toString());
		final YAMLFactory yamlFactory = new YAMLFactory();
		final ObjectMapper mapper = new ObjectMapper(yamlFactory);
		final JavaType parsedType = mapper.getTypeFactory().constructArrayType(Map.class);
		final Map<String, Object> parsed = ((Map<String, Object>[]) mapper.readValue(buff.toString(), parsedType))[0];
		assertEquals(STEP_ID, parsed.get("Load Step Id"));
		assertEquals(OP_TYPE.name(), parsed.get("Operation Type"));
		assertEquals(COUNT, ((Map<String, Object>) parsed.get("Operations Count")).get("Successful"));
		assertEquals(true, parsed.containsKey("Bandwidth [MiB/s]"));
		assertEquals(true, parsed.containsKey("Transfer Size"));
		assertEquals(false, parsed.containsKey("Bandwidth [MB/s]"));
	}
}
