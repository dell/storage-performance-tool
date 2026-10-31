package com.dell.spt.base.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.openjdk.nashorn.api.scripting.JSObject;

import static com.dell.spt.base.config.ConfigFormat.JSON;
import static com.dell.spt.base.config.ConfigFormat.YAML;

public interface ConfigUtil {

	String MIXED_LOAD_STEP_TYPE = "MixedLoad";

	/** Written in place of credential values in logged configuration; the CLI masks with the same value. */
	String MASKED_VALUE = "***";

	/** Credential leaves under {@code storage.auth} that logged configuration must not reveal. */
	List<String> MASKED_AUTH_KEYS = List.of("uid", "secret", "token");

	static ObjectMapper readConfigMapper(final ConfigFormat format, final Map<String, Object> schema)
					throws NoSuchMethodException {
		final ObjectMapper mapper;
		switch (format) {
		case JSON:
			mapper = new ObjectMapper();
			break;
		case YAML:
			mapper = new YAMLMapper();
			break;
		default:
			throw new AssertionError();
		}
		return mapper
						.enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
						.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
						.enable(SerializationFeature.INDENT_OUTPUT);
	}

	static ObjectWriter writerWithPrettyPrinter(final ObjectMapper om) {
		final var indenter = (DefaultPrettyPrinter.Indenter) new DefaultIndenter("  ", DefaultIndenter.SYS_LF);
		final var printer = new DefaultPrettyPrinter();
		printer.indentObjectsWith(indenter);
		printer.indentArraysWith(indenter);
		om.enable(SerializationFeature.INDENT_OUTPUT);
		return om.writer(printer);
	}

	static ObjectWriter configWriter(final ConfigFormat format) {
		final ObjectMapper mapper;
		switch (format) {
		case JSON:
			mapper = new ObjectMapper();
			break;
		case YAML:
			mapper = new YAMLMapper();
			break;
		default:
			throw new AssertionError();
		}
		return writerWithPrettyPrinter(mapper);
	}

	static Map<String, Object> configTree(final Config config) {
		final var configCopy = (Config) new BasicConfig(config);
		final var configTree = configCopy.mapVal(null);
		for (final var e : configTree.entrySet()) {
			final var val = e.getValue();
			if (val instanceof Config) {
				e.setValue(configTree((Config) val));
			}
		}
		return configTree;
	}

	static String toString(final Config config, final ConfigFormat format) {
		return toString(config, format, null);
	}

	static String toString(final Config config, final ConfigFormat format, final String stepTypeName) {
		final var configTree = configTree(config);
		trimLoadOpNoise(configTree, stepTypeName);
		return writeTree(configTree, format);
	}

	/** Serializes a step configuration for logs and result artifacts, with credential values masked. */
	static String toMaskedString(final Config config, final ConfigFormat format, final String stepTypeName) {
		final var configTree = configTree(config);
		trimLoadOpNoise(configTree, stepTypeName);
		maskCredentials(configTree);
		return writeTree(configTree, format);
	}

	private static String writeTree(final Map<String, Object> configTree, final ConfigFormat format) {
		try {
			return configWriter(format).writeValueAsString(configTree);
		} catch (final JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Replaces each set {@code storage.auth} credential with {@link #MASKED_VALUE}. The storage and auth
	 * maps are replaced with copies, so no map shared with the source configuration is modified.
	 */
	static void maskCredentials(final Map<String, Object> configTree) {
		if (!(configTree.get("storage") instanceof Map<?, ?> storage)
						|| !(storage.get("auth") instanceof Map<?, ?> auth)) {
			return;
		}
		final var maskedAuth = new LinkedHashMap<Object, Object>(auth);
		for (final var key : MASKED_AUTH_KEYS) {
			final var value = maskedAuth.get(key);
			if (value != null && !value.toString().isEmpty()) {
				maskedAuth.put(key, MASKED_VALUE);
			}
		}
		final var maskedStorage = new LinkedHashMap<Object, Object>(storage);
		maskedStorage.put("auth", maskedAuth);
		configTree.put("storage", maskedStorage);
	}

	@SuppressWarnings("unchecked")
	static void trimLoadOpNoise(final Map<String, Object> configTree, final String stepTypeName) {
		if (configTree == null || stepTypeName == null || stepTypeName.isEmpty()) {
			return;
		}
		final Object loadRaw = configTree.get("load");
		if (!(loadRaw instanceof Map)) {
			return;
		}
		final var load = (Map<String, Object>) loadRaw;
		final Object opRaw = load.get("op");
		if (!(opRaw instanceof Map)) {
			return;
		}
		final var op = (Map<String, Object>) opRaw;
		if (MIXED_LOAD_STEP_TYPE.equals(stepTypeName)) {
			final Object weightRaw = op.get("weight");
			if (weightRaw != null && !(weightRaw instanceof Map)) {
				op.remove("weight");
			}
		} else {
			op.remove("weights");
			final Object weightRaw = op.get("weight");
			if (isLegacyMixedWeightMap(weightRaw)) {
				op.remove("weight");
			}
		}
	}

	static boolean isLegacyMixedWeightMap(final Object weightRaw) {
		if (!(weightRaw instanceof Map<?, ?>)) {
			return false;
		}
		final var weightMap = (Map<?, ?>) weightRaw;
		return weightMap.containsKey("get")
						|| weightMap.containsKey("put")
						|| weightMap.containsKey("delete")
						|| weightMap.containsKey("stat");
	}

	static Config loadConfig(final File file, final Map<String, Object> schema)
					throws NoSuchMethodException, IOException {
		final ConfigFormat format;
		if (file.getName().endsWith(".json")) {
			format = JSON;
		} else {
			format = YAML;
		}
		final Map<String, Object> configTree = readConfigMapper(format, schema)
						.readValue(file, new TypeReference<Map<String, Object>>() {
							{}
						});
		return new BasicConfig("-", schema, configTree);
	}

	static Config loadConfig(final String content, final ConfigFormat format, final Map<String, Object> schema)
					throws NoSuchMethodException, IOException {
		final Map<String, Object> configTree = readConfigMapper(format, schema)
						.readValue(content, new TypeReference<Map<String, Object>>() {
							{}
						});
		return new BasicConfig("-", schema, configTree);
	}

	static Config merge(final String pathSep, final List<Config> configs) {
		final var schema = configs.stream()
						.map(Config::schema)
						.reduce(TreeUtil::addBranches)
						.orElseGet(Collections::emptyMap);
		final var configTree = configs.stream()
						.map(Config::deepToMap)
						.reduce(TreeUtil::addBranches)
						.orElseGet(Collections::emptyMap);
		return new BasicConfig(pathSep, schema, configTree);
	}

	static void flatten(
					final Map<String, Object> configMap,
					final Map<String, String> argValPairs,
					final String sep,
					final String prefix) {
		flatten(configMap, argValPairs, null, sep, prefix);
	}

	/**
	 * Flattens a scenario's configuration map into paths and string values. A list value becomes the
	 * comma-separated string of its elements and, when {@code listVals} is not null, is also recorded
	 * there as the list of those strings, for {@link #applyListValues}.
	 */
	static void flatten(
					final Map<String, Object> configMap,
					final Map<String, String> argValPairs,
					final Map<String, List<String>> listVals,
					final String sep,
					final String prefix) {
		for (final var k : configMap.keySet()) {
			final var v = scriptArrayAsList(configMap.get(k));
			final var path = prefix == null ? k : (prefix + sep + k);
			if (v instanceof Map) {
				flatten((Map<String, Object>) v, argValPairs, listVals, sep, path);
			} else if (v instanceof List) {
				final var elements = ((List<?>) v).stream()
								.map(e -> e == null ? ":" : e.toString())
								.toList();
				argValPairs.put(path, String.join(",", elements));
				if (listVals != null) {
					listVals.put(path, elements);
				}
			} else {
				argValPairs.put(path, v == null ? null : v.toString());
			}
		}
	}

	/**
	 * Sets each list value that {@link #flatten} recorded once more, as a list, on the settings the
	 * schema types as lists, so such a setting gets exactly the elements it was given. The
	 * comma-separated string alone reads an empty list back as one empty element, splits an element
	 * that holds a comma, and drops trailing empty elements. A value that aliasing renamed or replaced
	 * keeps its string.
	 *
	 * @param flatArgs the flattened values before aliasing
	 * @param appliedArgs the values after aliasing, already set on the configuration
	 */
	static void applyListValues(
					final Config config,
					final Map<String, List<String>> listVals,
					final Map<String, String> flatArgs,
					final Map<String, String> appliedArgs) {
		for (final var listVal : listVals.entrySet()) {
			final var path = listVal.getKey();
			if (appliedArgs.containsKey(path)
							&& Objects.equals(flatArgs.get(path), appliedArgs.get(path))
							&& isListSetting(config.schema(), path, config.pathSep())) {
				config.val(path, new ArrayList<>(listVal.getValue()));
			}
		}
	}

	/** Returns whether the schema types the setting at the path as a list. */
	static boolean isListSetting(final Map<String, Object> schema, final String path, final String sep) {
		Object node = schema;
		for (final var part : path.split(Pattern.quote(sep), -1)) {
			if (!(node instanceof Map<?, ?> branch)) {
				return false;
			}
			node = branch.get(part);
		}
		return node instanceof Class<?> type && List.class.isAssignableFrom(type);
	}

	/**
	 * A scenario's JavaScript array reaches Java as a Nashorn mirror, which is a {@link Map} keyed by
	 * its indexes; it stands for the list of its elements.
	 */
	private static Object scriptArrayAsList(final Object value) {
		if (value instanceof JSObject array && array.isArray()) {
			final var length = ((Number) array.getMember("length")).intValue();
			final var elements = new ArrayList<Object>(length);
			for (var i = 0; i < length; i++) {
				elements.add(array.getSlot(i));
			}
			return elements;
		}
		return value;
	}
}
