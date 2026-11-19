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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.openjdk.nashorn.api.scripting.JSObject;

import static com.dell.spt.base.config.ConfigFormat.JSON;
import static com.dell.spt.base.config.ConfigFormat.YAML;

public interface ConfigUtil {

	String MIXED_LOAD_STEP_TYPE = "MixedLoad";

	/** Written in place of credential values in logged configuration; the CLI masks with the same value. */
	String MASKED_VALUE = "***";

	/** Credential leaves under {@code storage.auth} that logged configuration must not reveal. */
	List<String> MASKED_AUTH_KEYS = List.of("uid", "secret", "token");

	/**
	 * Request headers under {@code storage.net.http.headers} whose values carry credentials, so logged
	 * and served configuration must not reveal them. Header names match regardless of case.
	 */
	List<String> MASKED_HEADER_NAMES = List.of("Authorization", "Proxy-Authorization", "Cookie", "x-amz-security-token", "X-Auth-Token");

	/** The command-line path of the {@code storage.auth} credentials, before the leaf name. */
	String AUTH_ARG_PATH_PREFIX = "storage-auth-";

	/** The command-line path of a request header, before the header name. */
	String HEADER_ARG_PATH_PREFIX = "storage-net-http-headers-";

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
	 * Replaces each set {@code storage.auth} credential, and the value of each request header under
	 * {@code storage.net.http.headers} named in {@link #MASKED_HEADER_NAMES}, with {@link #MASKED_VALUE}.
	 * A null or empty value stays as it is. The maps on the way to a masked value are replaced with
	 * copies, so no map shared with the source configuration is modified.
	 */
	static void maskCredentials(final Map<String, Object> configTree) {
		if (!(configTree.get("storage") instanceof Map<?, ?> storage)) {
			return;
		}
		final var maskedStorage = new LinkedHashMap<Object, Object>(storage);
		var masked = false;
		if (storage.get("auth") instanceof Map<?, ?> auth) {
			final var maskedAuth = new LinkedHashMap<Object, Object>(auth);
			for (final var key : MASKED_AUTH_KEYS) {
				maskIfSet(maskedAuth, key);
			}
			maskedStorage.put("auth", maskedAuth);
			masked = true;
		}
		if (storage.get("net") instanceof Map<?, ?> net
						&& net.get("http") instanceof Map<?, ?> http
						&& http.get("headers") instanceof Map<?, ?> headers) {
			final var maskedHeaders = new LinkedHashMap<Object, Object>(headers);
			for (final var name : headers.keySet()) {
				if (name instanceof String headerName && isMaskedHeaderName(headerName)) {
					maskIfSet(maskedHeaders, name);
				}
			}
			final var maskedHttp = new LinkedHashMap<Object, Object>(http);
			maskedHttp.put("headers", maskedHeaders);
			final var maskedNet = new LinkedHashMap<Object, Object>(net);
			maskedNet.put("http", maskedHttp);
			maskedStorage.put("net", maskedNet);
			masked = true;
		}
		if (masked) {
			configTree.put("storage", maskedStorage);
		}
	}

	private static void maskIfSet(final Map<Object, Object> values, final Object key) {
		final var value = values.get(key);
		if (value != null && !value.toString().isEmpty()) {
			values.put(key, MASKED_VALUE);
		}
	}

	/** Whether a request header's value carries a credential: its name is in {@link #MASKED_HEADER_NAMES}, in any case. */
	static boolean isMaskedHeaderName(final String headerName) {
		for (final var masked : MASKED_HEADER_NAMES) {
			if (masked.equalsIgnoreCase(headerName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Whether the value at a command-line configuration path, such as {@code storage-auth-secret} or
	 * {@code storage-net-http-headers-Authorization}, is one that {@link #maskCredentials} masks.
	 */
	static boolean isMaskedArgPath(final String path) {
		if (path.startsWith(AUTH_ARG_PATH_PREFIX)) {
			return MASKED_AUTH_KEYS.contains(path.substring(AUTH_ARG_PATH_PREFIX.length()));
		}
		return path.startsWith(HEADER_ARG_PATH_PREFIX)
						&& isMaskedHeaderName(path.substring(HEADER_ARG_PATH_PREFIX.length()));
	}

	/**
	 * A command-line argument as the engine logs it: {@code <name as given>=***} when its value is set
	 * and the name, after the aliasing, is a path {@link #isMaskedArgPath} masks; otherwise the argument
	 * as given. The name is resolved as {@link AliasingUtil#apply} resolves it: the first aliasing entry
	 * with that exact name decides, and its target is cut at the first {@code =}. An argument without
	 * a {@code =} or with an empty value is logged as given, as {@link #maskCredentials} leaves an empty
	 * value.
	 */
	static String maskedArg(final String arg, final List<Map<String, Object>> aliasingConfig) {
		final int valueAt = arg.indexOf(CliArgUtil.ARG_VAL_SEP);
		if (valueAt < 0 || valueAt == arg.length() - 1) {
			return arg;
		}
		final var givenName = arg.substring(0, valueAt);
		final var name = givenName.startsWith(CliArgUtil.ARG_PREFIX)
						? givenName.substring(CliArgUtil.ARG_PREFIX.length())
						: givenName;
		var path = name;
		if (aliasingConfig != null) {
			for (final var entry : aliasingConfig) {
				if (name.equals(entry.get(AliasingUtil.NAME))) {
					if (entry.get(AliasingUtil.TARGET) instanceof String target) {
						final int targetValueAt = target.indexOf(AliasingUtil.ARG_VAL_SEP);
						path = targetValueAt < 0 ? target : target.substring(0, targetValueAt);
					}
					break;
				}
			}
		}
		return isMaskedArgPath(path) ? givenName + CliArgUtil.ARG_VAL_SEP + MASKED_VALUE : arg;
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
		final var mapper = readConfigMapper(format, schema);
		final Map<String, Object> configTree = mapper.readValue(file, new TypeReference<Map<String, Object>>() {
			{}
		});
		validateBooleanValues(schema, configTree, "-");
		DocumentValueTypes.settle(schema, configTree, "-", () -> mapper.getFactory().createParser(file));
		return new BasicConfig("-", schema, configTree);
	}

	static Config loadConfig(final String content, final ConfigFormat format, final Map<String, Object> schema)
					throws NoSuchMethodException, IOException {
		final var mapper = readConfigMapper(format, schema);
		final Map<String, Object> configTree = mapper.readValue(content, new TypeReference<Map<String, Object>>() {
			{}
		});
		validateBooleanValues(schema, configTree, "-");
		DocumentValueTypes.settle(schema, configTree, "-", () -> mapper.getFactory().createParser(content));
		return new BasicConfig("-", schema, configTree);
	}

	/**
	 * Merges the configurations into a new one that shares no nested map or list with them, apart
	 * from the lists {@link #copyList} keeps by reference. {@code addBranches} adds to the first
	 * tree's maps in place, and {@code deepToMap} keeps a map-typed value, such as the HTTP headers,
	 * by reference, so merging the original trees would change the first configuration.
	 */
	static Config merge(final String pathSep, final List<Config> configs) {
		final var schema = configs.stream()
						.map(Config::schema)
						.map(ConfigUtil::copyMap)
						.reduce(TreeUtil::addBranches)
						.orElseGet(Collections::emptyMap);
		final var configTree = configs.stream()
						.map(Config::deepToMap)
						.map(ConfigUtil::copyMap)
						.reduce(TreeUtil::addBranches)
						.orElseGet(Collections::emptyMap);
		return new BasicConfig(pathSep, schema, configTree);
	}

	/**
	 * Copies the configuration so that it shares no nested map or list with the original, apart from
	 * the lists {@link #copyList} keeps by reference. {@code new BasicConfig(config)} copies only the
	 * branches and keeps a map- or list-typed value by reference, so setting a header on such a copy
	 * also sets it on the original.
	 */
	static Config copyOf(final Config config) {
		return new BasicConfig(config.pathSep(), config.schema(), copyMap(Config.deepToMap(config)));
	}

	/**
	 * Copies a map and what it nests. A {@link HashMap}, {@link LinkedHashMap} or {@link TreeMap} is
	 * copied into a map of its own class, because a schema may name that class as a setting's type.
	 * Any other map becomes a {@link LinkedHashMap}, which keeps its order: headers go out in the
	 * order written. Every map is copied, because a configuration branch is a map too.
	 */
	@SuppressWarnings("unchecked")
	private static <K> Map<K, Object> copyMap(final Map<K, ?> map) {
		final Map<K, Object> copy;
		if (map.getClass() == HashMap.class) {
			copy = new HashMap<>();
		} else if (map.getClass() == TreeMap.class) {
			copy = new TreeMap<>(((TreeMap<K, ?>) map).comparator());
		} else {
			copy = new LinkedHashMap<>();
		}
		map.forEach((key, value) -> copy.put(key, copyValue(value)));
		return copy;
	}

	private static Object copyValue(final Object value) {
		if (value instanceof Map<?, ?> map) {
			return copyMap(map);
		}
		if (value instanceof List<?> list) {
			return copyList(list);
		}
		return value;
	}

	/**
	 * Copies a list whose copy the JDK defines, with its elements copied as {@link #copyValue} copies
	 * them, and keeps the class, because a schema may name the list's class as the setting's type and
	 * the configuration refuses any other class. The rule goes by the exact class, before anything
	 * reads the list.
	 * <ul>
	 *   <li>An {@link ArrayList}, {@link LinkedList}, {@link Vector} or {@link CopyOnWriteArrayList}, of
	 *       exactly that class, becomes a new list of that class.
	 *   <li>An {@link Arrays#asList} list, such as a list set from text, becomes one of the same class
	 *       and fixed size over a new array: a {@code String[]} when every element is a string or null,
	 *       as in a list set from text, and an {@code Object[]} otherwise. The source's array class is
	 *       not read, so the copy may refuse other values than the source: an {@code Object[]} list of
	 *       only strings becomes a {@code String[]} one, whose {@code set} refuses a non-string, and an
	 *       array of any other class, such as an {@code Integer[]}, becomes an {@code Object[]}, which
	 *       takes any value. A list set from text is the only such list SPT itself makes.
	 *   <li>A read-only list, from {@link List#of}, {@link List#copyOf}, {@link Stream#toList},
	 *       {@link Collections#unmodifiableList} or {@link Collections#singletonList}, is kept when no
	 *       element copies to a new value, since nothing can write it: null queries, serialization and
	 *       the shared empty lists behave as before. Otherwise a {@link List#of} list is rebuilt with
	 *       {@link List#copyOf}, which refuses nulls as it does, a {@link Stream#toList} list, which
	 *       allows nulls, with that method, a singleton with that method, and an unmodifiable view over
	 *       a new {@link ArrayList}, or a {@link LinkedList} when the source is not {@link RandomAccess}.
	 *       Such a view then allows nulls and serializes as its new backing list does.
	 * </ul>
	 * Every other list is kept by reference, with its elements, as a configuration did before: this
	 * copy calls no constructor or method of it. That covers subclasses, an extension's own list
	 * classes, sub-lists, and {@link Collections#synchronizedList} and {@link Collections#checkedList}
	 * views. A map or list held in one is shared with the original.
	 */
	@SuppressWarnings("JdkObsolete") // the copy keeps the class the configuration was given
	private static Object copyList(final List<?> list) {
		final Class<?> listClass = list.getClass();
		if (listClass == ArrayList.class) {
			return new ArrayList<>(copyElements(list));
		}
		if (listClass == LinkedList.class) {
			return new LinkedList<>(copyElements(list));
		}
		if (listClass == Vector.class) {
			return new Vector<>(copyElements(list));
		}
		if (listClass == CopyOnWriteArrayList.class) {
			return new CopyOnWriteArrayList<>(copyElements(list));
		}
		if (listClass == JdkListClasses.ARRAYS_AS_LIST) {
			return copyArraysAsList(list);
		}
		if (listClass == JdkListClasses.LIST_OF_ONE_OR_TWO || listClass == JdkListClasses.LIST_OF_N) {
			final List<Object> elements = copyElements(list);
			if (sameElements(list, elements)) {
				return list;
			}
			final List<Object> rebuilt = refusesNullQuery(list) ? List.copyOf(elements) : elements.stream().toList();
			return rebuilt.getClass() == listClass ? rebuilt : list;
		}
		if (listClass == JdkListClasses.UNMODIFIABLE_RANDOM_ACCESS || listClass == JdkListClasses.UNMODIFIABLE_SEQUENTIAL) {
			final List<Object> elements = copyElements(list);
			if (sameElements(list, elements)) {
				return list;
			}
			return Collections.unmodifiableList(listClass == JdkListClasses.UNMODIFIABLE_RANDOM_ACCESS ? elements : new LinkedList<>(elements));
		}
		if (listClass == JdkListClasses.SINGLETON) {
			final Object element = list.get(0);
			final Object copy = copyValue(element);
			return copy == element ? list : Collections.singletonList(copy);
		}
		return list;
	}

	/** The list's elements, each copied as {@link #copyValue} copies it, in a new {@link ArrayList}. */
	private static List<Object> copyElements(final List<?> list) {
		final List<Object> copy = new ArrayList<>(list.size());
		list.forEach(element -> copy.add(copyValue(element)));
		return copy;
	}

	/** Whether each copy is the element itself, so the list holds nothing a write could reach. */
	private static boolean sameElements(final List<?> list, final List<Object> copies) {
		final var copy = copies.iterator();
		for (final Object element : list) {
			if (copy.next() != element) {
				return false;
			}
		}
		return true;
	}

	/**
	 * An {@link Arrays#asList} list of the copied elements over a {@code String[]} when every element
	 * is a string or null, and an {@code Object[]} otherwise. Only the elements' classes are read.
	 */
	private static Object copyArraysAsList(final List<?> list) {
		final List<Object> elements = copyElements(list);
		if (elements.stream().allMatch(element -> element == null || element instanceof String)) {
			return Arrays.asList(elements.toArray(new String[0]));
		}
		return Arrays.asList(elements.toArray());
	}

	/** Whether the read-only list throws when asked whether it holds null, as a {@link List#of} list does. */
	private static boolean refusesNullQuery(final List<?> list) {
		try {
			var unused = list.contains(null);
			return false;
		} catch (final NullPointerException e) {
			return true;
		}
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
	 * there as the list of those strings, for {@link #validateListValues}; a later value at the same
	 * path replaces it.
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
				if (listVals != null) {
					listVals.remove(path);
				}
			}
		}
	}

	/**
	 * Refuses a list value that its comma-separated string cannot carry, on the settings the schema
	 * types as lists: an element that is empty or holds a comma. The configuration reads the string
	 * back by splitting it at commas, so such an element would otherwise be split or dropped without a
	 * word. An empty list passes; it reads back as one empty element. Each value is checked at the
	 * setting it reaches: an aliasing entry that renames its path checks it at the target, and one
	 * that replaces its value leaves nothing to check.
	 *
	 * @throws IllegalArgumentException naming the setting and the element
	 */
	static void validateListValues(
					final Config config,
					final Map<String, List<String>> listVals,
					final List<Map<String, Object>> aliasingConfig) {
		// The first entry for a name decides, as AliasingUtil.apply applies it: a target path renames
		// the value, and a replacement value or a missing target (which fails the scenario) leaves
		// nothing to check.
		final Map<String, String> renamed = new HashMap<>();
		if (aliasingConfig != null) {
			for (final var entry : aliasingConfig) {
				if (entry.get(AliasingUtil.NAME) instanceof String name && !renamed.containsKey(name)) {
					final var parts = entry.get(AliasingUtil.TARGET) instanceof String target
									? target.split(AliasingUtil.ARG_VAL_SEP, 2)
									: new String[0];
					renamed.put(name, parts.length == 1 ? parts[0] : null);
				}
			}
		}
		for (final var listVal : listVals.entrySet()) {
			final var path = renamed.containsKey(listVal.getKey()) ? renamed.get(listVal.getKey()) : listVal.getKey();
			if (path == null || !isListSetting(config.schema(), path, config.pathSep())) {
				continue;
			}
			final var elements = listVal.getValue();
			for (int i = 0; i < elements.size(); i++) {
				final var element = elements.get(i);
				if (element.isEmpty() || element.contains(",")) {
					throw new IllegalArgumentException(path + ": element " + (i + 1) + " \"" + element
									+ "\" is empty or holds a comma, which a list in a scenario's configuration cannot carry");
				}
			}
		}
	}

	/** Returns whether the schema types the setting at the path as a list. */
	static boolean isListSetting(final Map<String, Object> schema, final String path, final String sep) {
		return settingType(schema, path, sep) instanceof Class<?> type && List.class.isAssignableFrom(type);
	}

	/**
	 * Returns the longest leading part of the path that the schema defines, joined with the separator,
	 * or {@code null} when not even its first part is defined. The parts returned are the schema's own
	 * keys, so they never carry text the path's author made up.
	 */
	static String definedPrefix(final Map<String, Object> schema, final String path, final String sep) {
		if (schema == null || path == null || sep == null || sep.isEmpty()) {
			return null;
		}
		Object node = schema;
		final var prefix = new StringBuilder();
		for (final var part : path.split(Pattern.quote(sep), -1)) {
			if (!(node instanceof Map<?, ?> branch) || !branch.containsKey(part)) {
				break;
			}
			if (prefix.length() > 0) {
				prefix.append(sep);
			}
			prefix.append(part);
			node = branch.get(part);
		}
		return prefix.length() == 0 ? null : prefix.toString();
	}

	/** Returns whether the schema types the setting at the path as a boolean. */
	static boolean isBooleanSetting(final Map<String, Object> schema, final String path, final String sep) {
		return isBooleanType(settingType(schema, path, sep));
	}

	/**
	 * Returns whether a value given as a string can set the setting at the path: any value for a
	 * setting the schema does not type as a boolean, and {@code true} or {@code false}, in any case, for
	 * one it does. The configuration reads any other string as false, so {@code yes}, {@code 1} or an
	 * empty value would otherwise turn the setting off without a word. A null value passes.
	 */
	static boolean isAcceptedBooleanValue(
					final Map<String, Object> schema, final String path, final String sep, final String value) {
		return value == null || isTrueOrFalse(value) || !isBooleanSetting(schema, path, sep);
	}

	/**
	 * Refuses a scenario's value for a boolean setting other than {@code true} or {@code false}; see
	 * {@link #isAcceptedBooleanValue}. The values are those of {@link #flatten}, after the aliasing.
	 *
	 * @throws InvalidBooleanValueException naming the setting and the value
	 */
	static void validateBooleanValues(final Config config, final Map<String, String> argValPairs) {
		for (final var argValPair : argValPairs.entrySet()) {
			final var path = argValPair.getKey();
			final var value = argValPair.getValue();
			if (!isAcceptedBooleanValue(config.schema(), path, config.pathSep(), value)) {
				throw notTrueOrFalse(path, value);
			}
		}
	}

	/**
	 * Refuses a string other than {@code true} or {@code false} at a boolean setting of a configuration
	 * tree read from a defaults file or a run request. Such a file states a boolean as YAML or JSON
	 * {@code true} or {@code false}; a quoted {@code "true"} or {@code "false"} passes, as on the
	 * command line, and any other string would read as false. A value of another type is left to the
	 * configuration, which refuses it.
	 *
	 * @throws InvalidBooleanValueException naming the setting and the value
	 */
	static void validateBooleanValues(
					final Map<String, Object> schema, final Map<String, Object> configTree, final String sep) {
		if (schema != null && configTree != null) {
			validateBooleanValues(schema, configTree, sep, null);
		}
	}

	private static void validateBooleanValues(
					final Map<?, ?> schema, final Map<?, ?> configTree, final String sep, final String prefix) {
		for (final var node : configTree.entrySet()) {
			final var key = String.valueOf(node.getKey());
			final var path = prefix == null ? key : prefix + sep + key;
			final var type = schema.get(node.getKey());
			final var value = node.getValue();
			if (type instanceof Map<?, ?> branchSchema && value instanceof Map<?, ?> branch) {
				validateBooleanValues(branchSchema, branch, sep, path);
			} else if (isBooleanType(type) && value instanceof String text && !isTrueOrFalse(text)) {
				throw notTrueOrFalse(path, text);
			}
		}
	}

	private static Object settingType(final Map<String, Object> schema, final String path, final String sep) {
		Object node = schema;
		for (final var part : path.split(Pattern.quote(sep), -1)) {
			if (!(node instanceof Map<?, ?> branch)) {
				return null;
			}
			node = branch.get(part);
		}
		return node;
	}

	private static boolean isBooleanType(final Object type) {
		return type == boolean.class || type == Boolean.class;
	}

	private static boolean isTrueOrFalse(final String value) {
		return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
	}

	private static InvalidBooleanValueException notTrueOrFalse(final String path, final String value) {
		return new InvalidBooleanValueException(path, path + ": \"" + value + "\" is not true or false");
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
