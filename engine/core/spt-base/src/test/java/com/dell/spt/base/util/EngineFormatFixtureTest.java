package com.dell.spt.base.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.base.config.TimeUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * Keeps the shared engine-format fixture equal to the engine's size and duration parsers.
 *
 * <p>The fixture lists values the engine accepts. The CLI's pre-launch checks
 * ({@code cli/internal/engineformat}) must accept every one of them, so a CLI grammar can never be
 * narrower than the engine's. Its {@code *.enumerated} sections hold every accepted string of up
 * to {@value #MAX_LENGTH} characters over a fixed alphabet and are regenerated here; its
 * {@code *.listed} sections hold documented forms and edge cases, each of which the engine must
 * accept; its {@code *.refused} sections hold values the engine and the CLI both refuse. A grammar
 * change in the engine or in java-commons fails this test.
 *
 * <p>To rewrite the enumerated sections after an intended grammar change, run this test with the
 * environment variable {@value #WRITE_ENV} set to {@code 1}, then review the diff.
 */
class EngineFormatFixtureTest {

	static final Path FIXTURE = Path.of("src", "test", "resources", "engine-format", "verdicts.txt");
	static final String WRITE_ENV = "SPT_WRITE_ENGINE_FORMAT_FIXTURE";
	static final int MAX_LENGTH = 5;
	static final String SIZE_ALPHABET = "01.kKiIbBe-, ";
	static final String DURATION_ALPHABET = "05.sSmhdayou ";

	static final String SIZE_ENUMERATED = "size.enumerated";
	static final String SIZE_LISTED = "size.listed";
	static final String DURATION_ENUMERATED = "duration.enumerated";
	static final String DURATION_LISTED = "duration.listed";
	static final String SIZE_REFUSED = "size.refused";
	static final String DURATION_REFUSED = "duration.refused";

	@Test
	void enumeratedSizesMatchTheEngine() throws IOException {
		assertEnumerated(SIZE_ENUMERATED, enumerate(SIZE_ALPHABET, EngineFormatFixtureTest::sizeAccepted));
	}

	@Test
	void enumeratedDurationsMatchTheEngine() throws IOException {
		assertEnumerated(
						DURATION_ENUMERATED, enumerate(DURATION_ALPHABET, EngineFormatFixtureTest::durationAccepted));
	}

	@Test
	void engineAcceptsEveryListedSize() throws IOException {
		assertVerdicts(SIZE_LISTED, EngineFormatFixtureTest::sizeAccepted);
	}

	@Test
	void engineAcceptsEveryListedDuration() throws IOException {
		assertVerdicts(DURATION_LISTED, EngineFormatFixtureTest::durationAccepted);
	}

	@Test
	void engineRefusesEveryRefusedSize() throws IOException {
		assertVerdicts(SIZE_REFUSED, value -> !sizeAccepted(value));
	}

	@Test
	void engineRefusesEveryRefusedDuration() throws IOException {
		assertVerdicts(DURATION_REFUSED, value -> !durationAccepted(value));
	}

	private static void assertEnumerated(final String section, final List<String> engine) throws IOException {
		if ("1".equals(System.getenv(WRITE_ENV))) {
			rewriteSection(section, engine);
		}
		final List<String> fixture = readSections().get(section);
		assertTrue(fixture != null, "fixture has no [" + section + "] section");
		if (!engine.equals(fixture)) {
			final List<String> missing = new ArrayList<>(engine);
			missing.removeAll(fixture);
			final List<String> extra = new ArrayList<>(fixture);
			extra.removeAll(engine);
			fail(String.format(
							"[%s] differs from the engine: %d accepted but not listed %s, %d listed but refused %s;"
											+ " if the grammar change is intended, rerun with %s=1",
							section, missing.size(), head(missing), extra.size(), head(extra), WRITE_ENV));
		}
	}

	/** Asserts the engine's verdict on every row of section is the one the section lists. */
	private static void assertVerdicts(final String section, final Predicate<String> verdict) throws IOException {
		final List<String> rows = readSections().get(section);
		assertTrue(rows != null && !rows.isEmpty(), "fixture has no [" + section + "] rows");
		final List<String> wrong = new ArrayList<>();
		for (final String row : rows) {
			if (!verdict.test(row)) {
				wrong.add(quote(row));
			}
		}
		assertEquals(List.of(), wrong, "[" + section + "] rows the engine gives the other verdict");
	}

	static boolean sizeAccepted(final String value) {
		try {
			BinarySizeFormat.parseSize(value);
			return true;
		} catch (final RuntimeException e) {
			return false;
		}
	}

	static boolean durationAccepted(final String value) {
		try {
			TimeUtil.getTimeInSeconds(value);
			return true;
		} catch (final RuntimeException e) {
			return false;
		}
	}

	/** Every string of up to {@link #MAX_LENGTH} characters over alphabet, in prefix order, that the engine accepts. */
	static List<String> enumerate(final String alphabet, final Predicate<String> engine) {
		final List<String> accepted = new ArrayList<>();
		enumerate(alphabet, new StringBuilder(), engine, accepted);
		return accepted;
	}

	private static void enumerate(
					final String alphabet, final StringBuilder prefix, final Predicate<String> engine, final List<String> out) {
		final String value = prefix.toString();
		if (engine.test(value)) {
			out.add(value);
		}
		if (prefix.length() == MAX_LENGTH) {
			return;
		}
		for (int i = 0; i < alphabet.length(); i++) {
			prefix.append(alphabet.charAt(i));
			enumerate(alphabet, prefix, engine, out);
			prefix.setLength(prefix.length() - 1);
		}
	}

	/** Reads the fixture: {@code [name]} starts a section, {@code #} starts a comment, each row is a quoted string. */
	static Map<String, List<String>> readSections() throws IOException {
		final Map<String, List<String>> sections = new LinkedHashMap<>();
		List<String> current = null;
		int number = 0;
		for (final String line : Files.readAllLines(FIXTURE, StandardCharsets.UTF_8)) {
			number++;
			if (line.isEmpty() || line.startsWith("#")) {
				continue;
			}
			if (line.startsWith("[") && line.endsWith("]")) {
				current = new ArrayList<>();
				sections.put(line.substring(1, line.length() - 1), current);
				continue;
			}
			if (current == null) {
				throw new IllegalStateException(FIXTURE + ":" + number + ": row before any section");
			}
			current.add(unquote(line, number));
		}
		return sections;
	}

	private static void rewriteSection(final String section, final List<String> rows) throws IOException {
		final List<String> out = new ArrayList<>();
		boolean skipping = false;
		boolean found = false;
		for (final String line : Files.readAllLines(FIXTURE, StandardCharsets.UTF_8)) {
			if (line.startsWith("[") && line.endsWith("]")) {
				if (skipping) {
					out.add("");
				}
				skipping = line.equals("[" + section + "]");
				if (skipping) {
					found = true;
					out.add(line);
					rows.forEach(row -> out.add(quote(row)));
					continue;
				}
			}
			if (!skipping) {
				out.add(line);
			}
		}
		if (!found) {
			throw new IllegalStateException(FIXTURE + " has no [" + section + "] section to rewrite");
		}
		Files.write(FIXTURE, out, StandardCharsets.UTF_8);
	}

	/** Quotes a row: {@code "} and {@code \} are escaped, and any character outside printable ASCII is {@code \}{@code uXXXX}. */
	static String quote(final String value) {
		final StringBuilder b = new StringBuilder(value.length() + 2).append('"');
		for (int i = 0; i < value.length(); i++) {
			final char c = value.charAt(i);
			if (c == '"' || c == '\\') {
				b.append('\\').append(c);
			} else if (c < 0x20 || c > 0x7e) {
				b.append(String.format("\\u%04X", (int) c));
			} else {
				b.append(c);
			}
		}
		return b.append('"').toString();
	}

	static String unquote(final String line, final int number) {
		if (line.length() < 2 || line.charAt(0) != '"' || line.charAt(line.length() - 1) != '"') {
			throw new IllegalStateException(FIXTURE + ":" + number + ": row is not a quoted string");
		}
		final StringBuilder b = new StringBuilder();
		for (int i = 1; i < line.length() - 1; i++) {
			final char c = line.charAt(i);
			if (c != '\\') {
				b.append(c);
				continue;
			}
			final char e = line.charAt(++i);
			if (e == 'u') {
				b.append((char) Integer.parseInt(line.substring(i + 1, i + 5), 16));
				i += 4;
			} else {
				b.append(e);
			}
		}
		return b.toString();
	}

	private static List<String> head(final List<String> values) {
		final List<String> quoted = new ArrayList<>();
		for (final String value : values.subList(0, Math.min(values.size(), 10))) {
			quoted.add(quote(value));
		}
		return quoted;
	}
}
