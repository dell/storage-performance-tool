package com.dell.spt.base.metrics.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.metrics.TimingMetricType;
import org.junit.jupiter.api.Test;

final class TimingMetricQuantileResultsImplTest {

	@Test
	void parseMetricValueHandlesSingleAndMultipleSpaces() {
		assertEquals(100L,
						TimingMetricQuantileResultsImpl.parseMetricValue("100  200  ", TimingMetricType.LATENCY));
		assertEquals(200L,
						TimingMetricQuantileResultsImpl.parseMetricValue(" 100\t200", TimingMetricType.DURATION));
	}

	@Test
	void parseMetricValueReturnsNullWhenColumnMissing() {
		assertNull(TimingMetricQuantileResultsImpl.parseMetricValue("42", TimingMetricType.DURATION));
	}

	@Test
	void parseMetricValueReturnsNullWhenTokenInvalid() {
		assertNull(TimingMetricQuantileResultsImpl.parseMetricValue("abc def", TimingMetricType.DURATION));
	}
}
