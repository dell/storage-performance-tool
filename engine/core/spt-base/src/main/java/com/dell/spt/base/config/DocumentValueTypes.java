package com.dell.spt.base.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Settles the values a YAML or JSON defaults document gives where a setting takes text. The document's
 * reader types an unquoted {@code 7} as a number and {@code true} as a boolean, which a text setting
 * would fail on with a cast when the driver or step reads it.
 *
 * <ul>
 *   <li>A number or boolean URI argument or request header becomes its text as written in the
 *       document, so {@code tid: 0012} sends {@code 0012}.
 *   <li>A number or boolean object tag becomes {@link String#valueOf}, the text the {@code s3-aws}
 *       driver already sends.
 *   <li>A list, map or missing value in one of those three maps is kept: a driver that does not read
 *       the map ignores it, and the Netty HTTP drivers refuse it where they read the map.
 *   <li>A number or boolean at one of the {@link #TEXT_SETTINGS_EVERY_RUN_READS} is refused, with a
 *       message that says to quote it. Each of them fails every run with a cast otherwise.
 * </ul>
 *
 * Values of any other type, and every other setting, are left as the reader gave them, so a text
 * setting whose reader ignores a number, or that a run reads only with some drivers or steps, behaves
 * as before.
 */
final class DocumentValueTypes {

	/** Reopens the document for its token text; called at most once per document. */
	@FunctionalInterface
	interface DocumentParser {
		JsonParser open() throws IOException;
	}

	static final List<String> URI_ARGS = List.of("storage", "net", "http", "uri", "args");

	static final List<String> HEADERS = List.of("storage", "net", "http", "headers");

	static final List<String> TAGS = List.of("storage", "object", "tagging", "tags");

	/**
	 * The text settings every run reads as a string from the posted configuration, before the scenario
	 * can set them, so a number or boolean there fails each run with a cast, whatever the driver, the
	 * step and the scenario: {@code RunServlet} reads the run comment and the step id, and
	 * {@code LoadStepBase} validates the integrity settings when the scenario's step types are bound.
	 * Every other text setting is left alone: a scenario can set it for each step, or a driver or step
	 * may not read it, so a number there can work.
	 */
	static final Set<List<String>> TEXT_SETTINGS_EVERY_RUN_READS = Set.of(
					List.of("load", "step", "id"),
					List.of("run", "comment"),
					List.of("storage", "integrity", "algorithm"),
					List.of("storage", "integrity", "input", "expectedProducerId"),
					List.of("storage", "integrity", "input", "provenance"),
					List.of("storage", "integrity", "mode"));

	private DocumentValueTypes() {}

	/**
	 * Settles the tree's values in place, as described above.
	 *
	 * @param schema the schema the tree is read with, or {@code null}, which skips the text-setting check
	 * @param configTree the tree the reader gave
	 * @param sep the path separator that refusals name settings with
	 * @param document reopens the document the tree was read from
	 * @throws InvalidTextValueException naming the setting, never the value
	 */
	static void settle(
					final Map<String, Object> schema,
					final Map<String, Object> configTree,
					final String sep,
					final DocumentParser document)
					throws IOException {
		if (configTree == null) {
			return;
		}
		Map<List<String>, String> asWritten = null;
		for (final var mapPath : List.of(URI_ARGS, HEADERS)) {
			final var entries = mapAt(configTree, mapPath);
			if (entries == null) {
				continue;
			}
			for (final var entry : entries.entrySet()) {
				final var value = entry.getValue();
				if (value instanceof Number || value instanceof Boolean) {
					if (asWritten == null) {
						asWritten = scalarTexts(document);
					}
					final var text = asWritten.get(child(mapPath, entry.getKey()));
					entry.setValue(text == null ? String.valueOf(value) : text);
				}
			}
		}
		final var tags = mapAt(configTree, TAGS);
		if (tags != null) {
			for (final var entry : tags.entrySet()) {
				final var value = entry.getValue();
				if (value instanceof Number || value instanceof Boolean) {
					entry.setValue(String.valueOf(value));
				}
			}
		}
		if (schema != null) {
			for (final var setting : TEXT_SETTINGS_EVERY_RUN_READS) {
				refuseTypedText(configTree, setting, sep);
			}
		}
	}

	private static void refuseTypedText(final Map<String, Object> configTree, final List<String> setting, final String sep) {
		final var branch = mapAt(configTree, setting.subList(0, setting.size() - 1));
		final var value = branch == null ? null : branch.get(setting.get(setting.size() - 1));
		if (value instanceof Number || value instanceof Boolean) {
			final var name = join(setting, sep);
			throw new InvalidTextValueException(name, name + ": the value is read as a "
							+ (value instanceof Number ? "number" : "boolean") + ", not text; quote it");
		}
	}

	/**
	 * The text as written of each number or boolean entry of the URI-argument and header maps, by path.
	 * Both Jackson readers keep a scalar's own text, such as {@code 0012}, {@code 1e3} or {@code True}.
	 * Only the document's first value is read, as the tree reader does.
	 */
	private static Map<List<String>, String> scalarTexts(final DocumentParser document) throws IOException {
		final Map<List<String>, String> texts = new HashMap<>();
		try (final JsonParser parser = document.open()) {
			for (var token = parser.nextToken(); token != null; token = parser.nextToken()) {
				if (token.isNumeric() || token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
					final var path = objectPath(parser);
					if (path != null && path.size() > 1) {
						final var parent = path.subList(0, path.size() - 1);
						if (parent.equals(URI_ARGS) || parent.equals(HEADERS)) {
							texts.put(path, parser.getText());
						}
					}
				}
				if (parser.getParsingContext().inRoot()) {
					break;
				}
			}
		}
		return texts;
	}

	/** The field names down to the current value, or {@code null} when it sits in an array. */
	private static List<String> objectPath(final JsonParser parser) {
		final var path = new ArrayList<String>();
		for (var context = parser.getParsingContext(); context != null && !context.inRoot(); context = context.getParent()) {
			if (!context.inObject()) {
				return null;
			}
			path.add(0, context.getCurrentName());
		}
		return path;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> mapAt(final Map<String, Object> tree, final List<String> path) {
		Object node = tree;
		for (final var key : path) {
			if (!(node instanceof Map<?, ?> branch)) {
				return null;
			}
			node = branch.get(key);
		}
		return node instanceof Map<?, ?> ? (Map<String, Object>) node : null;
	}

	private static List<String> child(final List<String> path, final Object key) {
		final var child = new ArrayList<String>(path.size() + 1);
		child.addAll(path);
		child.add(String.valueOf(key));
		return child;
	}

	private static String join(final List<String> path, final String sep) {
		return String.join(sep, path);
	}
}
