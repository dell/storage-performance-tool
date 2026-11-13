package com.dell.spt.base.config;

import static java.lang.Boolean.TRUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

public interface CliArgUtil {

	String ARG_PREFIX = "--";
	String ARG_PATH_SEP = "-";
	String ARG_VAL_SEP = "=";

	static Map<String, String> parseArgs(final String... args) {
		// https://stackoverflow.com/questions/40039649/why-does-collectors-tomap-report-value-instead-of-key-on-duplicate-key-error
		final var eloquentCollector = Collector.<String[], Map<String, String>> of(
						HashMap::new,
						(map, argValPair) -> map.put(argValPair[0], argValPair[1]),
						(m1, m2) -> {
							m2.forEach(m1::put);
							return m1;
						});
		return Arrays.stream(args)
						.peek(CliArgUtil::checkArgPrefix)
						.map(arg -> arg.substring(ARG_PREFIX.length()))
						// split args to key/value pairs by the '=' symbol
						.map(arg -> arg.split(ARG_VAL_SEP, 2))
						.map(CliArgUtil::handleBooleanShortcuts)
						.collect(eloquentCollector);
	}

	private static void checkArgPrefix(final String arg) {
		if (!arg.startsWith(ARG_PREFIX)) {
			throw new IllegalArgumentNameException(arg);
		}
	}

	// handle the shortcuts for boolean options (--smth-enabled -> --smth-enabled=true)
	private static String[] handleBooleanShortcuts(final String[] argValPair) {
		return argValPair.length == 2 ? argValPair : new String[]{argValPair[0], TRUE.toString()
		};
	}

	/**
	 * The options the schema defines, one tab-indented line each and sorted by name, then the aliases
	 * of the aliasing table in its order. An option shows the type of its value; a map-typed option
	 * takes a free key, as {@code --storage-net-nodeInterfaces-<key>=<value>}.
	 */
	static String usage(final Map<String, Object> schema, final List<Map<String, Object>> aliasingConfig) {
		final var options = new TreeMap<String, String>();
		usageLines(schema, ARG_PREFIX, options);
		final var text = new StringBuilder("Options:\n");
		options.values().forEach(line -> text.append('\t').append(line).append('\n'));
		if (aliasingConfig != null && !aliasingConfig.isEmpty()) {
			text.append("\nAliases:\n");
			for (final var entry : aliasingConfig) {
				if (entry.get(AliasingUtil.NAME) instanceof String name) {
					text.append('\t').append(ARG_PREFIX).append(name).append("  ").append(aliasMeaning(entry)).append('\n');
				}
			}
		}
		return text.toString();
	}

	private static void usageLines(
					final Map<?, ?> schema, final String prefix, final Map<String, String> options) {
		for (final var schemaEntry : schema.entrySet()) {
			final var name = prefix + schemaEntry.getKey();
			final var schemaVal = schemaEntry.getValue();
			if (schemaVal instanceof Map<?, ?> branch) {
				usageLines(branch, name + ARG_PATH_SEP, options);
			} else if (schemaVal instanceof Class<?> type && Map.class.isAssignableFrom(type)) {
				options.put(name, name + ARG_PATH_SEP + "<key>" + ARG_VAL_SEP + "<value>");
			} else {
				options.put(name, name + ARG_VAL_SEP + '<' + typeName(schemaVal) + '>');
			}
		}
	}

	private static String typeName(final Object schemaVal) {
		if (schemaVal == boolean.class || schemaVal == Boolean.class) {
			return "true|false";
		} else if (schemaVal == int.class || schemaVal == Integer.class) {
			return "int";
		} else if (schemaVal == long.class || schemaVal == Long.class) {
			return "long";
		} else if (schemaVal == double.class || schemaVal == Double.class) {
			return "double";
		} else if (schemaVal == String.class) {
			return "string";
		} else if (schemaVal instanceof Class<?> type && List.class.isAssignableFrom(type)) {
			return "list";
		} else if (schemaVal instanceof Class<?> type && type != Object.class) {
			return type.getSimpleName();
		}
		return "value";
	}

	private static String aliasMeaning(final Map<String, Object> aliasingEntry) {
		final var deprecated = Boolean.TRUE.equals(aliasingEntry.get(AliasingUtil.DEPRECATED));
		if (aliasingEntry.get(AliasingUtil.TARGET) instanceof String target) {
			return "same as " + ARG_PREFIX + target + (deprecated ? " (deprecated)" : "");
		} else if (aliasingEntry.get(AliasingUtil.REASON) instanceof String reason) {
			return "not supported: " + reason;
		}
		return "no longer supported";
	}

	@SuppressWarnings("CollectionWithoutInitialCapacity")
	static List<String> allCliArgs(final Map<String, Object> schema, final String sep) {
		final List<String> allArgs = new ArrayList<>();
		schema.entrySet().stream()
						.map(schemaEntry -> argsFromSchemaEntry(ARG_PREFIX, sep, schemaEntry))
						.forEach(allArgs::addAll);
		return allArgs;
	}

	@SuppressWarnings({"CollectionWithoutInitialCapacity", "unchecked"
	})
	static List<String> argsFromSchemaEntry(
					final String prefix, final String sep, final Map.Entry<String, Object> schemaEntry) {
		final List<String> args = new ArrayList<>();
		final String schemaKey = schemaEntry.getKey();
		final Object schemaVal = schemaEntry.getValue();
		if (schemaVal instanceof Map) {
			((Map<String, Object>) schemaVal)
							.entrySet().stream()
							.map(e -> argsFromSchemaEntry(prefix + schemaKey + sep, sep, e))
							.forEach(args::addAll);
		} else {
			args.add(prefix + schemaKey + ARG_VAL_SEP + '<' + schemaVal + '>');
		}
		return args;
	}
}
