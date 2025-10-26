package com.dell.spt.base.env;

import com.dell.spt.base.Constants;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/** Created by andrey on 18.11.16. */
public interface DateUtil {

	TimeZone TZ_UTC = TimeZone.getTimeZone("UTC");
	ZoneOffset ZONE_OFFSET_UTC = ZoneOffset.UTC;

	// e.g. 2001-07-04T12:08:56.235-0700
	String PATTERN_ISO8601 = "yyyy-MM-dd'T'HH:mm:ss,SSS";
	DateTimeFormatter FMT_DATE_ISO8601 = DateTimeFormatter.ofPattern(PATTERN_ISO8601, Constants.LOCALE_DEFAULT)
					.withZone(ZONE_OFFSET_UTC);

	// e.g. 20210901T182320Z
	String PATTERN_AMAZON = "yyyyMMdd'T'HHmmss'Z'"; // close to ISO_INSTANT but not quite
	DateTimeFormatter FMT_DATE_AMAZON = DateTimeFormatter.ofPattern(PATTERN_AMAZON, Constants.LOCALE_DEFAULT)
					.withZone(ZONE_OFFSET_UTC);

	// e.g. Sun, 06 Nov 1994 08:49:37 GMT
	String PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";
	DateTimeFormatter FMT_DATE_RFC1123 = DateTimeFormatter.RFC_1123_DATE_TIME.withLocale(Constants.LOCALE_DEFAULT)
					.withZone(ZONE_OFFSET_UTC);

	String PATTERN_METRICS_TABLE = "yyMMddHHmmss";
	ThreadLocal<Map<String, DateFormat>> DATE_FORMATS = ThreadLocal.withInitial(HashMap::new);

	static String formatNowIso8601() {
		return formatIso8601(Instant.now());
	}

	static String formatIso8601(final Instant instant) {
		return FMT_DATE_ISO8601.format(instant);
	}

	static Instant parseIso8601(final String value) {
		return Instant.from(FMT_DATE_ISO8601.parse(value));
	}

	static String formatNowAmazonStyle() {
		return formatAmazon(Instant.now());
	}

	static String formatAmazon(final Instant instant) {
		return FMT_DATE_AMAZON.format(instant);
	}

	static Instant parseAmazon(final String value) {
		return Instant.from(FMT_DATE_AMAZON.parse(value));
	}

	static String formatNowRfc1123() {
		return formatRfc1123(Instant.now());
	}

	static String formatRfc1123(final Instant instant) {
		return FMT_DATE_RFC1123.format(instant);
	}

	static Instant parseRfc1123(final String value) throws DateTimeParseException {
		return Instant.from(FMT_DATE_RFC1123.parse(value));
	}

	static String formatMetricsTable(final Instant instant) {
		return dateFormat(PATTERN_METRICS_TABLE).format(Date.from(instant));
	}

	static Instant parseMetricsTable(final String value) {
		final var format = dateFormat(PATTERN_METRICS_TABLE);
		final var position = new ParsePosition(0);
		final var parsed = format.parse(value, position);
		if (parsed == null || position.getIndex() != value.length()) {
			final int errorIndex = position.getErrorIndex() >= 0 ? position.getErrorIndex() : position.getIndex();
			throw new DateTimeParseException("Unable to parse metrics table timestamp: " + value, value, Math.max(errorIndex, 0));
		}
		return parsed.toInstant();
	}

	@SuppressWarnings("JavaUtilDate") // Expression language historically exposes java.util.Date instances.
	static Date date(final long epochMillis) {
		return new Date(epochMillis);
	}

	static DateFormat dateFormat(final String pattern) {
		return DATE_FORMATS.get().computeIfAbsent(
						pattern,
						p -> {
							final var f = new SimpleDateFormat(p, Locale.ROOT);
							f.setTimeZone(TZ_UTC);
							return f;
						});
	}

}
