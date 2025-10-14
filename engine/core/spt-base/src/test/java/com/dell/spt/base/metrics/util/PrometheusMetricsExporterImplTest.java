package com.dell.spt.base.metrics.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static com.dell.spt.base.metrics.MetricsConstants.METRIC_NAME_TEST_STATE;

import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import io.prometheus.client.Collector.MetricFamilySamples;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

/**
 * Test for PrometheusMetricsExporterImpl focusing on:
 * 1. Test state metric generation (Phase 3)
 * 2. Forced refresh behavior (Phase 1) 
 * 3. Zero-value detection and logging
 */
class PrometheusMetricsExporterImplTest {

	@Mock
	private DistributedMetricsContext mockMetricsContext;
	@Mock
	private DistributedAllMetricsSnapshot mockSnapshot;
	@Mock
	private RateMetricSnapshot mockSuccessSnapshot;
	@Mock
	private RateMetricSnapshot mockFailsSnapshot;
	@Mock
	private RateMetricSnapshot mockByteSnapshot;
	@Mock
	private TimingMetricSnapshot mockDurationSnapshot;
	@Mock
	private TimingMetricSnapshot mockLatencySnapshot;
	@Mock
	private ConcurrencyMetricSnapshot mockConcurrencySnapshot;

	private PrometheusMetricsExporterImpl exporter;

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
		exporter = new PrometheusMetricsExporterImpl(mockMetricsContext);
	}

	@Test
	void testCollectCallsRefreshLastSnapshot() {
		// Phase 1 fix: Verify that collect() always calls refreshLastSnapshot()
		when(mockMetricsContext.lastSnapshot()).thenReturn(null);
		
		List<MetricFamilySamples> result = exporter.collect();
		
		verify(mockMetricsContext, times(1)).refreshLastSnapshot();
		assertNotNull(result);
	}

	@Test
	void testTestStateMetric_NoSnapshot() {
		// Test state = 0 when no snapshot available
		when(mockMetricsContext.lastSnapshot()).thenReturn(null);
		
		List<MetricFamilySamples> result = exporter.collect();
		
		// Should contain test state metric with value 0.0
        MetricFamilySamples testStateMetric = findMetricByName(result, METRIC_NAME_TEST_STATE);
		assertNotNull(testStateMetric, "Test state metric should be present");
		assertEquals(1, testStateMetric.samples.size());
		assertEquals(0.0, testStateMetric.samples.get(0).value, "Test state should be 0 when no snapshot");
	}

	@Test
	void testTestStateMetric_TestRunning() {
		// Test state = 1 when test is running (has operations and concurrency > 0)
		setupMockSnapshot(100, 5, 2.5);
		when(mockMetricsContext.lastSnapshot()).thenReturn(mockSnapshot);

		List<MetricFamilySamples> result = exporter.collect();

		MetricFamilySamples testStateMetric = findMetricByName(result, METRIC_NAME_TEST_STATE);
		assertNotNull(testStateMetric, "Test state metric should be present");
		assertEquals(1.0, testStateMetric.samples.get(0).value, "Test state should be 1 when test is running");
	}

	@Test
	void testTestStateMetric_TestCompleted() {
		// Test state = 2 when test completed (has operations but concurrency = 0)
		setupMockSnapshot(100, 5, 0.0);
		when(mockMetricsContext.lastSnapshot()).thenReturn(mockSnapshot);

		List<MetricFamilySamples> result = exporter.collect();

		MetricFamilySamples testStateMetric = findMetricByName(result, METRIC_NAME_TEST_STATE);
		assertNotNull(testStateMetric, "Test state metric should be present");
		assertEquals(2.0, testStateMetric.samples.get(0).value, "Test state should be 2 when test is completed");
	}

	@Test
	void testTestStateMetric_TestStarting() {
		// Test state = 1 when test is starting (no operations yet but has snapshot)
		setupMockSnapshot(0, 0, 1.0);
		when(mockMetricsContext.lastSnapshot()).thenReturn(mockSnapshot);

		List<MetricFamilySamples> result = exporter.collect();

		MetricFamilySamples testStateMetric = findMetricByName(result, METRIC_NAME_TEST_STATE);
		assertNotNull(testStateMetric, "Test state metric should be present");
		assertEquals(1.0, testStateMetric.samples.get(0).value, "Test state should be 1 when test is starting");
	}

	@Test
	void testAllMetricsCollectedWhenSnapshotExists() {
		// Verify all expected metrics are collected when snapshot exists
		setupMockSnapshot(50, 2, 1.5);
		when(mockMetricsContext.lastSnapshot()).thenReturn(mockSnapshot);

		List<MetricFamilySamples> result = exporter.collect();

		// Should have metrics for: success, fails, bytes, duration, latency, concurrency, elapsed time, test state
		assertTrue(result.size() >= 8, "Should collect all metric types");
		assertNotNull(findMetricByName(result, METRIC_NAME_TEST_STATE), "Test state metric should be present");
	}

	private void setupMockSnapshot(long successCount, long failsCount, double concurrency) {
		// Setup success snapshot
		when(mockSuccessSnapshot.name()).thenReturn("spt_success");
		when(mockSuccessSnapshot.count()).thenReturn(successCount);
		when(mockSuccessSnapshot.mean()).thenReturn(10.0);
		when(mockSuccessSnapshot.last()).thenReturn(15.0);
		
		// Setup fails snapshot  
		when(mockFailsSnapshot.name()).thenReturn("spt_fails");
		when(mockFailsSnapshot.count()).thenReturn(failsCount);
		when(mockFailsSnapshot.mean()).thenReturn(1.0);
		when(mockFailsSnapshot.last()).thenReturn(0.5);
		
		// Setup byte snapshot
		when(mockByteSnapshot.name()).thenReturn("spt_bytes");
		when(mockByteSnapshot.count()).thenReturn(1024L);
		when(mockByteSnapshot.mean()).thenReturn(512.0);
		when(mockByteSnapshot.last()).thenReturn(256.0);
		
		// Setup timing snapshots (methods return correct types)
		when(mockDurationSnapshot.name()).thenReturn("spt_duration");
		when(mockDurationSnapshot.count()).thenReturn(successCount);
		when(mockDurationSnapshot.sum()).thenReturn(1000000L);
		when(mockDurationSnapshot.mean()).thenReturn(500000.0);  // double
		when(mockDurationSnapshot.min()).thenReturn(100000L);
		when(mockDurationSnapshot.max()).thenReturn(2000000L);
		
		when(mockLatencySnapshot.name()).thenReturn("spt_latency");
		when(mockLatencySnapshot.count()).thenReturn(successCount);
		when(mockLatencySnapshot.sum()).thenReturn(800000L);
		when(mockLatencySnapshot.mean()).thenReturn(400000.0);  // double
		when(mockLatencySnapshot.min()).thenReturn(50000L);
		when(mockLatencySnapshot.max()).thenReturn(1500000L);
		
		// Setup concurrency snapshot (last() returns long)
		when(mockConcurrencySnapshot.name()).thenReturn("spt_concurrency");
		when(mockConcurrencySnapshot.mean()).thenReturn(1.2);
		when(mockConcurrencySnapshot.last()).thenReturn((long) concurrency);
		
		// Setup main snapshot (elapsedTimeMillis() returns long)
		when(mockSnapshot.successSnapshot()).thenReturn(mockSuccessSnapshot);
		when(mockSnapshot.failsSnapshot()).thenReturn(mockFailsSnapshot);
		when(mockSnapshot.byteSnapshot()).thenReturn(mockByteSnapshot);
		when(mockSnapshot.durationSnapshot()).thenReturn(mockDurationSnapshot);
		when(mockSnapshot.latencySnapshot()).thenReturn(mockLatencySnapshot);
		when(mockSnapshot.concurrencySnapshot()).thenReturn(mockConcurrencySnapshot);
		when(mockSnapshot.elapsedTimeMillis()).thenReturn(5000L);  // long
	}

	private MetricFamilySamples findMetricByName(List<MetricFamilySamples> metrics, String nameSubstring) {
		return metrics.stream()
						.filter(mfs -> mfs.name.contains(nameSubstring))
						.findFirst()
						.orElse(null);
	}
}
