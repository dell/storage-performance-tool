package com.dell.spt.base.config;

import static java.lang.Boolean.TRUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
