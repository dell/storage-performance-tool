package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TimeUtilTest {
	@Nested
	class GetTimeValueTests {

		/**
		 * Parses raw time value into numeric value.
		 *
		 * @param raw        raw time value string
		 * @return          numeric value
		 */
		@ParameterizedTest
		@CsvSource({
				"0s,0",
				"1s,1",
				"10s,10",
				"5m,5",
				"2h,2",
				"3d,3",
				"7S,7",
				"8M,8",
				"9H,9",
				"10D,10",
				// no suffix
				"42,42",
				// compat pattern
				"15.minutes,15",
				"1.HOURS,1",
				"3.days,3"
		})
		void parsesNumericPortion(String raw, long expected) {
			assertEquals(expected, TimeUtil.getTimeValue(raw));
		}

		/**
		 * Verifies that an empty string input returns zero.
		 *
		 * @return         	a boolean indicating whether the test passed
		 */
		@Test
		void emptyStringReturnsZero() {
			assertEquals(0L, TimeUtil.getTimeValue(""));
		}

		/**
		 * Verifies that invalid time value patterns throw an IllegalArgumentException.
		 *
		 * @param raw	a raw time value string
		 * @return         	a boolean indicating whether the test passed
		 */
		@ParameterizedTest
		@ValueSource(strings = {
				"abc",       // no digits, no match
				"12.x",      // compat suffix not 4–7 letters
				"10.q",      // shorthand suffix not one of smhd
				" 5m",       // leading whitespace prevents regex match
				"5m "        // trailing whitespace prevents regex match
		})
		void invalidPatternsThrow(String raw) {
			assertThrows(IllegalArgumentException.class, () -> TimeUtil.getTimeValue(raw));
		}

		/**
		 * Verifies that a null input throws a NullPointerException.
		 *
		 * @return         	a boolean indicating whether the test passed
		 */
		@Test
		void nullInputNpe() {
			assertThrows(NullPointerException.class, () -> TimeUtil.getTimeValue(null));
		}
	}

	@Nested
	class GetTimeUnitTests {
		/**
		 * Verifies that the raw time value string can be parsed into the expected TimeUnit.
		 *
		 * @param raw        the raw time value string
		 * @param expected    the expected TimeUnit
		 * @return         	a boolean indicating whether the test passed
		 */
		@ParameterizedTest
		@CsvSource({
				"10s,SECONDS",
				"5m,MINUTES",
				"2h,HOURS",
				"3d,DAYS",
				"7S,SECONDS",
				"8M,MINUTES",
				"9H,HOURS",
				"10D,DAYS",
				// no suffix => default SECONDS
				"42,SECONDS",
				// compat pattern
				"15.minutes,MINUTES",
				"1.HOURS,HOURS",
				"3.days,DAYS",
				"60.SeConds,SECONDS"
		})
		void parsesTimeUnit(String raw, TimeUnit expected) {
			assertEquals(expected, TimeUtil.getTimeUnit(raw));
		}

		/**
		 * Verifies that invalid time value patterns throw an IllegalArgumentException.
		 *
		 * @param raw        a raw time value string
		 * @return         	a boolean indicating whether the test passed
		 */
		@ParameterizedTest
		@ValueSource(strings = {
				"abc",     // no digits, no match
				"12.x",    // compat suffix not 4–7 letters
				"10.q",    // shorthand suffix invalid
				" 5m",     // leading whitespace
				"5m "      // trailing whitespace
		})
		void invalidPatternsThrow(String raw) {
			assertThrows(IllegalArgumentException.class, () -> TimeUtil.getTimeUnit(raw));
		}

		/**
		 * Verifies that a null input throws a NullPointerException.
		 *
		 * @return         	a boolean indicating whether the test passed
		 */
		@Test
		void nullInputNpe() {
			assertThrows(NullPointerException.class, () -> TimeUtil.getTimeUnit(null));
		}
	}

	@Nested
	class GetTimeInSecondsTests {

		/**
		 * Verifies that a raw time value string can be converted into seconds.
		 *
		 * @param raw        a raw time value string
		 * @return         	a boolean indicating whether the test passed
		 */
		@ParameterizedTest
		@CsvSource({
				// shorthand
				"0s,0",
				"1s,1",
				"10s,10",
				"5m,300",
				"2h,7200",
				"3d,259200",
				// case-insensitive shorthand
				"7S,7",
				"1H,3600",
				// no suffix (defaults to seconds)
				"42,42",
				// compat pattern
				"15.minutes,900",
				"1.HOURS,3600",
				"3.days,259200",
				"60.SeConds,60"
		})
		void convertsToSeconds(String raw, long expectedSeconds) {
			assertEquals(expectedSeconds, TimeUtil.getTimeInSeconds(raw));
		}

		/**
		 * Verifies that an empty string input returns zero seconds.
		 *
		 * @return         	a boolean indicating whether the test passed
		 */
		@Test
		void emptyStringIsZero() {
			assertEquals(0L, TimeUtil.getTimeInSeconds(""));
		}

		/**
		 * Verifies that invalid time value patterns throw an IllegalArgumentException.
		 *
		 * @param  raw	a raw time value string
		 * @return         	a boolean indicating whether the test passed
		 */
		@ParameterizedTest
		@ValueSource(strings = {
				"abc",
				"12.x",
				"10.q",
				" 5m",
				"5m "
		})
		void invalidPatternsThrow(String raw) {
			assertThrows(IllegalArgumentException.class, () -> TimeUtil.getTimeInSeconds(raw));
		}

		/**
		 * Verifies that a null input throws a NullPointerException.
		 *
		 * @return         	a boolean indicating whether the test passed
		 */
		@Test
		void nullInputNpe() {
			assertThrows(NullPointerException.class, () -> TimeUtil.getTimeInSeconds(null));
		}
	}
}
