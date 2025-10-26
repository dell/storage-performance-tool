package com.dell.spt.base.env;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.junit.jupiter.api.Test;

class DateUtilTest {

	@Test
	void parseMetricsTableHonorsLegacyTwoDigitWindow() {
		assertAll(
						() -> assertEquals(Instant.parse("1968-01-01T00:00:00Z"), DateUtil.parseMetricsTable("680101000000")),
						() -> assertEquals(Instant.parse("1969-01-01T00:00:00Z"), DateUtil.parseMetricsTable("690101000000")),
						() -> assertEquals(Instant.parse("1970-01-01T00:00:00Z"), DateUtil.parseMetricsTable("700101000000")),
						() -> assertEquals(Instant.parse("1999-01-01T00:00:00Z"), DateUtil.parseMetricsTable("990101000000")),
						() -> assertEquals(Instant.parse("2000-01-01T00:00:00Z"), DateUtil.parseMetricsTable("000101000000")),
						() -> assertEquals(Instant.parse("2035-01-01T00:00:00Z"), DateUtil.parseMetricsTable("350101000000")));
	}

	@Test
	void formatMetricsTableMatchesLegacyOutput() {
		assertAll(
						() -> assertEquals("680101000000", DateUtil.formatMetricsTable(Instant.parse("1968-01-01T00:00:00Z"))),
						() -> assertEquals("690101000000", DateUtil.formatMetricsTable(Instant.parse("1969-01-01T00:00:00Z"))),
						() -> assertEquals("700101000000", DateUtil.formatMetricsTable(Instant.parse("1970-01-01T00:00:00Z"))),
						() -> assertEquals("990101000000", DateUtil.formatMetricsTable(Instant.parse("1999-01-01T00:00:00Z"))),
						() -> assertEquals("000101000000", DateUtil.formatMetricsTable(Instant.parse("2000-01-01T00:00:00Z"))),
						() -> assertEquals("350101000000", DateUtil.formatMetricsTable(Instant.parse("2035-01-01T00:00:00Z"))));
	}

	@Test
	void parseMetricsTableRejectsInvalidInput() {
		assertThrows(DateTimeParseException.class, () -> DateUtil.parseMetricsTable("bad-timestamp"));
	}
}
