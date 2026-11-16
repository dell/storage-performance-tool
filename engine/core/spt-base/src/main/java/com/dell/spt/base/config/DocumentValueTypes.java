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
 *   <li>A list or map in one of those three maps, and a request header with no value, are refused. A
 *       URI argument or tag with no value is kept.
 *   <li>A number or boolean at another setting the schema types as a string is refused, with a message
 *       that says to quote it, except at the few text settings whose readers take a number.
 * </ul>
 *
 * Values of any other type, and every other setting, are left as the reader gave them.
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

	/** Text settings whose readers take a number as well ({@code RangeReadConfig}). */
	static final Set<List<String>> NUMBER_TAKING_TEXT_SETTINGS = Set.of(
					List.of("load", "op", "read", "range", "size"),
					List.of("load", "op", "read", "range", "offset"),
					List.of("load", "op", "read", "range", "align"));

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
				final var path = child(mapPath, entry.getKey());
				refuseNested(path, value, sep);
				if (value == null && mapPath.equals(HEADERS)) {
					throw new InvalidTextValueException(join(path, sep), join(path, sep) + ": a request header needs a value");
				}
				if (value instanceof Number || value instanceof Boolean) {
					if (asWritten == null) {
						asWritten = scalarTexts(document);
					}
					final var text = asWritten.get(path);
					entry.setValue(text == null ? String.valueOf(value) : text);
				}
			}
		}
		final var tags = mapAt(configTree, TAGS);
		if (tags != null) {
			for (final var entry : tags.entrySet()) {
				final var value = entry.getValue();
				refuseNested(child(TAGS, entry.getKey()), value, sep);
				if (value instanceof Number || value instanceof Boolean) {
					entry.setValue(String.valueOf(value));
				}
			}
		}
		if (schema != null) {
			refuseTypedText(schema, configTree, new ArrayList<>(), sep);
		}
	}

	private static void refuseNested(final List<String> path, final Object value, final String sep) {
		if (value instanceof List<?> || value instanceof Map<?, ?>) {
			final var name = join(path, sep);
			throw new InvalidTextValueException(name, name + ": the value is "
							+ (value instanceof List<?> ? "a list" : "a map") + ", not text");
		}
	}

	private static void refuseTypedText(
					final Map<?, ?> schema, final Map<?, ?> configTree, final List<String> prefix, final String sep) {
		for (final var node : configTree.entrySet()) {
			final var type = schema.get(node.getKey());
			final var value = node.getValue();
			final var path = child(prefix, node.getKey());
			if (type instanceof Map<?, ?> branchSchema && value instanceof Map<?, ?> branch) {
				refuseTypedText(branchSchema, branch, path, sep);
			} else if (type == String.class
							&& (value instanceof Number || value instanceof Boolean)
							&& !NUMBER_TAKING_TEXT_SETTINGS.contains(path)) {
				final var name = join(path, sep);
				throw new InvalidTextValueException(name, name + ": the value is read as a "
								+ (value instanceof Number ? "number" : "boolean") + ", not text; quote it");
			}
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
