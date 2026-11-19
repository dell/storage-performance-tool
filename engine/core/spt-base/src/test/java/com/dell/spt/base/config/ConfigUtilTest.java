package com.dell.spt.base.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.Serial;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConfigUtilTest {

	@Test
	public void testMergeSharesNoMapWithItsInputs() {
		final Map<String, Object> schema = new HashMap<>(Map.of("headers", Map.class));
		final Config first = new BasicConfig(
						"-", schema, new HashMap<>(Map.of("headers", new HashMap<>(Map.of("X-First", "1")))));
		final Config second = new BasicConfig(
						"-", schema, new HashMap<>(Map.of("headers", new HashMap<>(Map.of("X-Second", "2")))));

		final Config merged = ConfigUtil.merge("-", List.of(first, second));

		assertEquals(Map.of("X-First", "1", "X-Second", "2"), merged.mapVal("headers"));
		assertEquals(Map.of("X-First", "1"), first.mapVal("headers"));
		assertEquals(Map.of("X-Second", "2"), second.mapVal("headers"));
		merged.mapVal("headers").put("X-Later", "3");
		assertEquals(Map.of("X-First", "1"), first.mapVal("headers"));
		assertEquals(Map.of("X-Second", "2"), second.mapVal("headers"));
	}

	@Test
	public void testMergeKeepsTheOrderOfAMapValue() {
		final Map<String, Object> schema = new HashMap<>(Map.of("headers", Map.class));
		final Map<String, Object> written = new LinkedHashMap<>();
		for (final String name : List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie")) {
			written.put(name, "1");
		}
		final Map<String, Object> added = new LinkedHashMap<>();
		added.put("X-Whiskey", "2");
		added.put("X-Delta", "2");
		final Config first = new BasicConfig("-", schema, new HashMap<>(Map.of("headers", written)));
		final Config second = new BasicConfig("-", schema, new HashMap<>(Map.of("headers", added)));

		final Config merged = ConfigUtil.merge("-", List.of(first, second));

		assertEquals(
						List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie", "X-Whiskey", "X-Delta"),
						List.copyOf(merged.mapVal("headers").keySet()));
	}

	@Test
	public void testMergeSharesNoListWithItsInputs() {
		final Map<String, Object> schema = new HashMap<>(Map.of("names", List.class));
		final List<Object> names = new ArrayList<>(List.of("a"));
		final Config first = new BasicConfig("-", schema, new HashMap<>(Map.of("names", names)));

		final Config merged = ConfigUtil.merge("-", List.of(first));

		assertEquals(names, merged.listVal("names"));
		assertNotSame(names, merged.listVal("names"));
	}

	@Test
	public void testCopyOfSharesNoMapOrListAndKeepsEveryValue() {
		final Map<String, Object> schema = new HashMap<>(Map.of(
						"branch", new HashMap<>(Map.of("headers", Map.class, "names", List.class, "count", int.class)),
						"title", String.class));
		final Map<String, Object> nestedInMap = new HashMap<>(Map.of("inner", "1"));
		final Map<String, Object> headers = new HashMap<>(Map.of("X-Base", "1", "nested", nestedInMap));
		final Map<String, Object> nestedInList = new HashMap<>(Map.of("inner", "2"));
		final List<Object> innerList = new ArrayList<>(List.of("x"));
		final List<Object> names = new ArrayList<>(List.of("a", nestedInList, innerList));
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of(
						"branch", new HashMap<>(Map.of("headers", headers, "names", names, "count", 7)),
						"title", "base")));

		final Config copy = ConfigUtil.copyOf(source);

		assertEquals(Config.deepToMap(source), Config.deepToMap(copy));
		assertEquals(7, copy.intVal("branch-count"));
		assertEquals("base", copy.stringVal("title"));
		assertEquals("-", copy.pathSep());
		final Map<String, Object> copiedHeaders = copy.mapVal("branch-headers");
		final List<Object> copiedNames = copy.listVal("branch-names");
		assertNotSame(headers, copiedHeaders);
		assertNotSame(nestedInMap, copiedHeaders.get("nested"));
		assertNotSame(names, copiedNames);
		assertNotSame(nestedInList, copiedNames.get(1));
		assertNotSame(innerList, copiedNames.get(2));
	}

	@Test
	public void testCopyOfTakesAWriteThatTheOriginalDoesNotSee() {
		final Config source = TestConfigBuilder.configWithHttpHeaders(Map.of("X-Base", "1"));

		final Config copy = ConfigUtil.copyOf(source);
		copy.val("storage-net-http-headers-X", "run");

		assertEquals(Map.of("X-Base", "1", "X", "run"), copy.mapVal("storage-net-http-headers"));
		assertEquals(Map.of("X-Base", "1"), source.mapVal("storage-net-http-headers"));
	}

	@Test
	public void testCopyOfKeepsTheOrderOfAMapValue() {
		final Map<String, Object> schema = new HashMap<>(Map.of("headers", Map.class));
		final Map<String, Object> written = new LinkedHashMap<>();
		final List<String> names = List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie");
		names.forEach(name -> written.put(name, "1"));
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of("headers", written)));

		assertEquals(names, List.copyOf(ConfigUtil.copyOf(source).mapVal("headers").keySet()));
	}

	@Test
	@SuppressWarnings("JdkObsolete") // an extension's schema may name LinkedList
	public void testMergeAndCopyOfKeepTheClassOfAListOrMapThatTheSchemaNames() {
		// An extension's defaults may name a concrete class: the configuration refuses any other one.
		final Map<String, Object> schema = new HashMap<>(Map.of("names", LinkedList.class, "sorted", TreeMap.class));
		final List<Object> names = new LinkedList<>(List.of("a", "b"));
		final Map<String, Object> sorted = new TreeMap<>(Comparator.reverseOrder());
		sorted.put("a", "1");
		sorted.put("c", "3");
		sorted.put("b", "2");
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of("names", names, "sorted", sorted)));

		final Config merged = assertDoesNotThrow(() -> ConfigUtil.merge("-", List.of(source)));
		final Config copy = assertDoesNotThrow(() -> ConfigUtil.copyOf(source));

		for (final Config config : List.of(merged, copy)) {
			final List<Object> copiedNames = config.listVal("names");
			final Map<String, Object> copiedSorted = config.mapVal("sorted");
			assertEquals(LinkedList.class, copiedNames.getClass());
			assertEquals(names, copiedNames);
			assertNotSame(names, copiedNames);
			assertEquals(TreeMap.class, copiedSorted.getClass());
			assertNotSame(sorted, copiedSorted);
			copiedSorted.put("d", "4");
			assertEquals(List.of("d", "c", "b", "a"), List.copyOf(copiedSorted.keySet()));
		}
		assertEquals(List.of("c", "b", "a"), List.copyOf(sorted.keySet()));
	}

	@Test
	public void testCopyOfKeepsTheOrderOfAnyOtherMap() {
		final Map<String, Object> schema = new HashMap<>(Map.of("headers", Map.class));
		final Map<String, Object> written = new LinkedHashMap<>();
		final List<String> headerNames = List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie");
		headerNames.forEach(name -> written.put(name, "1"));
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of(
						"headers", Collections.unmodifiableMap(written))));

		final Config copy = ConfigUtil.copyOf(source);

		assertEquals(headerNames, List.copyOf(copy.mapVal("headers").keySet()));
		copy.mapVal("headers").put("X-Later", "2");
		assertEquals(headerNames, List.copyOf(written.keySet()));
	}

	/**
	 * A backing list that its owner writes: it appends to itself whenever it is read whole, as another
	 * thread appending during startup would, and counts every call made to it.
	 */
	static final class SelfAppendingList extends AbstractList<Object> {

		final AtomicInteger calls = new AtomicInteger();
		private final List<Object> elements;

		SelfAppendingList(final List<Object> elements) {
			this.elements = new ArrayList<>(elements);
		}

		private void wholeRead() {
			calls.incrementAndGet();
			elements.add("appended");
		}

		@Override
		public Object get(final int index) {
			calls.incrementAndGet();
			return elements.get(index);
		}

		@Override
		public int size() {
			calls.incrementAndGet();
			return elements.size();
		}

		@Override
		public Iterator<Object> iterator() {
			wholeRead();
			return elements.iterator();
		}

		@Override
		public void forEach(final Consumer<? super Object> action) {
			wholeRead();
			elements.forEach(action);
		}

		@Override
		public Object[] toArray() {
			wholeRead();
			return elements.toArray();
		}

		@Override
		public boolean contains(final Object element) {
			calls.incrementAndGet();
			return elements.contains(element);
		}
	}

	@Test
	public void testMergeAndCopyOfKeepAnUnmodifiableViewWithoutReadingItsBackingList() {
		// only text, as in Sol's fixture: a second read of the list runs past the copies
		for (final List<Object> elements : List.<List<Object>> of(List.of("a", "b"), List.of("a", element()))) {
			final SelfAppendingList backing = new SelfAppendingList(elements);
			final List<Object> view = Collections.unmodifiableList(backing);
			for (final boolean viaMerge : List.of(true, false)) {
				final Object copied = assertDoesNotThrow(() -> copyNames(view, viaMerge));

				assertSame(view, copied);
			}
			assertEquals(0, backing.calls.get());
		}
	}

	/**
	 * Lists the copy rebuilds, each holding a mutable map where the list takes one. The schema names
	 * either {@code List} or the list's exact class, as an extension's defaults may, for example with
	 * {@code Arrays.asList("p", "q").getClass()}.
	 */
	@SuppressWarnings("JdkObsolete") // an extension's defaults may hold these classes
	static Stream<Arguments> listsTheCopyRebuilds() {
		final List<Supplier<List<Object>>> lists = List.of(
						() -> new ArrayList<>(List.of("p", element())),
						() -> new LinkedList<>(List.of("p", element())),
						() -> Arrays.asList("p", "q"),
						() -> Arrays.asList("p", element()),
						() -> List.of(element()),
						() -> List.of("p", element()),
						() -> List.of("p", "q", element()),
						() -> List.copyOf(List.of("p", "q", "r", element())),
						() -> Stream.<Object> of("p", element()).toList(),
						() -> Stream.<Object> of("p", null, element()).toList(),
						() -> Collections.singletonList(element()),
						() -> new Vector<>(List.of("p", "q", element())),
						() -> new CopyOnWriteArrayList<>(List.of("p", element())));
		return lists.stream().flatMap(list -> Stream.of(
						Arguments.of(list.get().getClass().getName() + " as List", list, false),
						Arguments.of(list.get().getClass().getName() + " as its class", list, true)));
	}

	private static Map<String, Object> element() {
		return new LinkedHashMap<>(Map.of("value", "first"));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("listsTheCopyRebuilds")
	@SuppressWarnings("unchecked")
	public void testMergeAndCopyOfRebuildAListOfItsOwnClassThatSharesNothing(
					final String name, final Supplier<List<Object>> lists, final boolean schemaNamesTheClass) {
		for (final boolean viaMerge : List.of(true, false)) {
			final List<Object> source = lists.get();
			final List<Object> expected = new ArrayList<>(source);
			final Map<String, Object> schema = new HashMap<>(Map.of("names", schemaNamesTheClass ? source.getClass() : List.class));
			final Config config = new BasicConfig("-", schema, new HashMap<>(Map.of("names", source)));

			final List<Object> copied = assertDoesNotThrow(
							() -> viaMerge ? ConfigUtil.merge("-", List.of(config)) : ConfigUtil.copyOf(config))
							.listVal("names");

			assertEquals(source.getClass(), copied.getClass());
			assertNotSame(source, copied);
			assertEquals(source, copied);
			for (int i = 0; i < source.size(); i++) {
				if (source.get(i) instanceof Map<?, ?> element) {
					assertNotSame(element, copied.get(i));
					((Map<String, Object>) copied.get(i)).put("value", "changed");
					assertEquals("first", element.get("value"));
				}
			}
			final boolean copySets = changes(() -> copied.set(0, "changed"));
			assertEquals(expected, source);
			assertEquals(changes(() -> source.set(0, "changed")), copySets);
			assertEquals(changes(() -> source.add("added")), changes(() -> copied.add("added")));
		}
	}

	/** Runs the change and returns false if the list refuses it. */
	private static boolean changes(final Runnable change) {
		try {
			change.run();
			return true;
		} catch (final UnsupportedOperationException e) {
			return false;
		}
	}

	/** Lists of a class the copy does not know: a subclass, a view, or an extension's own class. */
	@SuppressWarnings("JdkObsolete") // an extension's defaults may hold these classes
	static Stream<Arguments> listsTheCopyKeeps() {
		final List<Supplier<List<Object>>> lists = List.of(
						() -> List.<Object> of("p", "q", element()).subList(0, 3),
						() -> new ArrayList<Object>(List.of("p", element())).subList(0, 2),
						() -> Collections.synchronizedList(new ArrayList<>(List.of("p", element()))),
						() -> Collections.checkedList(new ArrayList<>(List.of("p", element())), Object.class),
						() -> Collections.unmodifiableList(new ArrayList<>(List.of("p", element()))),
						() -> Collections.unmodifiableList(new LinkedList<>(List.of("p", element()))),
						() -> new ArrayList<Object>(List.of("p", element())) {},
						() -> new LinkedList<Object>(List.of("p", element())) {},
						() -> Collections.emptyList());
		return lists.stream().map(list -> Arguments.of(list.get().getClass().getName(), list));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("listsTheCopyKeeps")
	public void testMergeAndCopyOfKeepAListOfAnyOtherClassWithItsElements(final String name, final Supplier<List<Object>> lists) {
		final List<Object> source = lists.get();
		final Config config = new BasicConfig(
						"-", new HashMap<>(Map.of("names", List.class)), new HashMap<>(Map.of("names", source)));

		assertSame(source, ConfigUtil.copyOf(config).listVal("names"));
		assertSame(source, ConfigUtil.merge("-", List.of(config)).listVal("names"));
	}

	/** A list whose no-argument constructor seeds an entry, as an extension's list class may. */
	@SuppressWarnings("JdkObsolete") // Sol's fixture extends LinkedList
	public static final class SeededList extends LinkedList<Object> {

		static final AtomicInteger CONSTRUCTED = new AtomicInteger();
		static final AtomicInteger ADDED = new AtomicInteger();

		public SeededList() {
			CONSTRUCTED.incrementAndGet();
			super.add("implicit");
		}

		@Override
		public boolean add(final Object element) {
			ADDED.incrementAndGet();
			return super.add(element);
		}

		@Override
		public boolean addAll(final Collection<?> elements) {
			ADDED.incrementAndGet();
			return super.addAll(elements);
		}
	}

	/** A list that holds at most a fixed number of elements, as an extension's list class may. */
	public static final class CapacityList extends AbstractList<Object> {

		static final AtomicInteger CONSTRUCTED = new AtomicInteger();
		static final AtomicInteger ADDED = new AtomicInteger();
		private final List<Object> elements = new ArrayList<>();
		private final int capacity;

		public CapacityList() {
			this(1);
		}

		CapacityList(final int capacity) {
			CONSTRUCTED.incrementAndGet();
			this.capacity = capacity;
		}

		@Override
		public Object get(final int index) {
			return elements.get(index);
		}

		@Override
		public int size() {
			return elements.size();
		}

		@Override
		public void add(final int index, final Object element) {
			ADDED.incrementAndGet();
			if (elements.size() == capacity) {
				throw new IllegalStateException("full at " + capacity);
			}
			elements.add(index, element);
		}

		@Override
		public boolean addAll(final Collection<?> added) {
			ADDED.incrementAndGet();
			return super.addAll(added);
		}
	}

	@Test
	public void testMergeAndCopyOfKeepAnExtensionListThatSeedsAnEntry() {
		final SeededList source = new SeededList();
		source.add(element());
		SeededList.CONSTRUCTED.set(0);
		SeededList.ADDED.set(0);

		assertKeptUntouched(source);
		assertEquals(0, SeededList.CONSTRUCTED.get());
		assertEquals(0, SeededList.ADDED.get());
		assertEquals(List.of("implicit", element()), source);
	}

	@Test
	public void testMergeAndCopyOfKeepAnExtensionListWithACapacity() {
		final CapacityList source = new CapacityList(2);
		source.add("p");
		source.add(element());
		CapacityList.CONSTRUCTED.set(0);
		CapacityList.ADDED.set(0);

		assertKeptUntouched(source);
		assertEquals(0, CapacityList.CONSTRUCTED.get());
		assertEquals(0, CapacityList.ADDED.get());
		assertEquals(List.of("p", element()), source);
	}

	/** Asserts that merge and copyOf both keep the list itself, under a schema of List and of its class. */
	private static void assertKeptUntouched(final List<Object> source) {
		for (final Class<?> type : List.of(List.class, source.getClass())) {
			final Config config = new BasicConfig(
							"-", new HashMap<>(Map.of("names", type)), new HashMap<>(Map.of("names", source)));

			final Object copied = assertDoesNotThrow(() -> ConfigUtil.copyOf(config).listVal("names"));
			final Object merged = assertDoesNotThrow(() -> ConfigUtil.merge("-", List.of(config)).listVal("names"));

			assertSame(source, copied);
			assertSame(copied, merged);
		}
	}

	@Test
	@SuppressWarnings("JdkObsolete") // the copy keeps the classes the configuration was given
	public void testMergeAndCopyOfCopyEachJdkMutableListIntoItsClass() {
		final List<Supplier<List<Object>>> lists = List.of(
						() -> new ArrayList<>(List.of("p", element())),
						() -> new LinkedList<>(List.of("p", element())),
						() -> new Vector<>(List.of("p", element())),
						() -> new CopyOnWriteArrayList<>(List.of("p", element())));
		for (final Supplier<List<Object>> list : lists) {
			for (final boolean viaMerge : List.of(true, false)) {
				final List<Object> source = list.get();
				final Config config = new BasicConfig(
								"-", new HashMap<>(Map.of("names", source.getClass())), new HashMap<>(Map.of("names", source)));

				final List<Object> copied = (viaMerge ? ConfigUtil.merge("-", List.of(config)) : ConfigUtil.copyOf(config))
								.listVal("names");

				assertSame(source.getClass(), copied.getClass());
				assertNotSame(source, copied);
				assertEquals(List.of("p", element()), copied);
				copied.add("added");
				@SuppressWarnings("unchecked")
				final var element = (Map<String, Object>) copied.get(1);
				element.put("value", "changed");
				assertEquals(List.of("p", element()), source);
			}
		}
	}

	@Test
	public void testMergeAndCopyOfBackAFixedSizeListOfTextWithAStringArray() {
		final List<String> typed = Arrays.asList("p", "q");
		final Config fromText = new BasicConfig("-", new HashMap<>(Map.of("names", List.class)), new HashMap<>());
		fromText.val("names", "p,q"); // a list set from text, as an engine argument is
		final List<Object> textList = fromText.listVal("names");
		assertThrows(ArrayStoreException.class, () -> textList.set(0, 42));
		// an Object[] of only strings: the copy is stricter than the source, as the Javadoc records
		final List<Object> untypedText = Arrays.asList("p", "q");
		for (final List<?> source : List.of(typed, textList, untypedText)) {
			for (final boolean viaMerge : List.of(true, false)) {
				final List<Object> copied = copyNames(source, viaMerge);

				assertSame(source.getClass(), copied.getClass());
				assertNotSame(source, copied);
				assertEquals(List.of("p", "q"), copied);
				assertThrows(ArrayStoreException.class, () -> copied.set(0, 42));
				assertThrows(UnsupportedOperationException.class, () -> copied.add("r"));
				copied.set(0, "changed");
				assertEquals(List.of("p", "q"), source);
			}
		}
		final List<Object> copiedWithNull = copyNames(Arrays.asList("p", null), false);
		assertEquals(Arrays.asList("p", null), copiedWithNull);
		assertThrows(ArrayStoreException.class, () -> copiedWithNull.set(1, 42));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMergeAndCopyOfBackAnyOtherFixedSizeListWithAnObjectArray() {
		final ConcurrentHashMap<String, Object> concurrent = new ConcurrentHashMap<>(Map.of("value", "first"));
		final ConcurrentHashMap<?, ?>[] maps = new ConcurrentHashMap<?, ?>[1];
		maps[0] = concurrent;
		final List<List<?>> sources = List.of(
						Arrays.asList("p", element()),
						Arrays.asList(1, 2), // an Integer[]: the copy is looser than the source
						Arrays.asList(maps)); // a ConcurrentHashMap[], which cannot hold its map's LinkedHashMap copy
		assertThrows(ArrayStoreException.class, () -> ((List<Object>) sources.get(1)).set(0, "p"));
		for (final List<?> source : sources) {
			for (final boolean viaMerge : List.of(true, false)) {
				final List<Object> copied = copyNames(source, viaMerge);

				assertSame(source.getClass(), copied.getClass());
				assertNotSame(source, copied);
				assertEquals(source, copied);
				for (int i = 0; i < source.size(); i++) {
					if (source.get(i) instanceof Map<?, ?> element) {
						assertNotSame(element, copied.get(i));
						((Map<String, Object>) copied.get(i)).put("value", "changed");
						assertEquals("first", element.get("value"));
					}
				}
				copied.set(0, new Object()); // an Object[] takes any value
			}
		}
		assertEquals(Map.of("value", "first"), concurrent);
	}

	/** An element whose serialization hook throws; the copy must never call it. */
	static final class ThrowingHook implements Serializable {
		private static final long serialVersionUID = 1L;

		@Serial
		private Object writeReplace() {
			throw new IllegalStateException("writeReplace called");
		}
	}

	/** An element whose serialization hook counts its calls; the copy must never call it. */
	static final class CountingHook implements Serializable {
		private static final long serialVersionUID = 1L;
		static final AtomicInteger CALLS = new AtomicInteger();

		@Serial
		private Object writeReplace() {
			CALLS.incrementAndGet();
			return this;
		}
	}

	@Test
	public void testMergeAndCopyOfAFixedSizeListCallNoThrowingSerializationHook() {
		assertCopiedWithoutHooks(Arrays.asList("p", new ThrowingHook()));
	}

	@Test
	public void testMergeAndCopyOfAFixedSizeListCallNoCountingSerializationHook() {
		CountingHook.CALLS.set(0);
		assertCopiedWithoutHooks(Arrays.asList(new CountingHook(), "q"));
		assertEquals(0, CountingHook.CALLS.get());
	}

	private static void assertCopiedWithoutHooks(final List<Object> source) {
		for (final boolean viaMerge : List.of(true, false)) {
			final List<Object> copied = assertDoesNotThrow(() -> copyNames(source, viaMerge));

			assertSame(source.getClass(), copied.getClass());
			assertNotSame(source, copied);
			assertEquals(source, copied);
		}
	}

	/** The list setting {@code names} of a configuration holding the source, after a merge or a copy. */
	private static List<Object> copyNames(final List<?> source, final boolean viaMerge) {
		final Config config = new BasicConfig(
						"-", new HashMap<>(Map.of("names", List.class)), new HashMap<>(Map.of("names", source)));
		return (viaMerge ? ConfigUtil.merge("-", List.of(config)) : ConfigUtil.copyOf(config)).listVal("names");
	}

	@Test
	public void testMergeAndCopyOfKeepAReadOnlyListOfUnchangeableValues() {
		final List<List<?>> lists = List.of(
						List.of("p", "q"),
						List.of("p", "q", "r"),
						List.of(),
						Stream.of("p", null).toList(),
						Collections.singletonList("p"),
						List.of(List.of("p"), 1));
		for (final List<?> source : lists) {
			final Config config = new BasicConfig(
							"-", new HashMap<>(Map.of("names", List.class)), new HashMap<>(Map.of("names", source)));

			assertSame(source, ConfigUtil.copyOf(config).listVal("names"));
			assertSame(source, ConfigUtil.merge("-", List.of(config)).listVal("names"));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCopyOfAListOfAMutableMapIsADetachedListThatStillRefusesNull() {
		final Map<String, Object> element = element();
		final List<Object> source = List.of(element);
		final Config config = new BasicConfig(
						"-", new HashMap<>(Map.of("names", List.class)), new HashMap<>(Map.of("names", source)));

		final List<Object> copied = ConfigUtil.copyOf(config).listVal("names");

		assertSame(source.getClass(), copied.getClass());
		assertNotSame(source, copied);
		assertEquals(source, copied);
		assertThrows(NullPointerException.class, () -> copied.contains(null));
		((Map<String, Object>) copied.get(0)).put("value", "changed");
		assertEquals("first", element.get("value"));
	}

	@Test
	public void testFlatten() throws Exception {

		final Map<String, Object> srcMap = new HashMap<>();
		final Map<String, Object> aMap = new HashMap<>();
		aMap.put("aa", null);
		aMap.put("bb", 123);
		srcMap.put("a", aMap);
		final Map<String, Object> bMap = new HashMap<>();
		bMap.put("aa", "yohoho");
		bMap.put("bb", true);
		srcMap.put("b", bMap);

		final String sep = "-";
		final Map<String, String> dstMap = new HashMap<>();
		ConfigUtil.flatten(srcMap, dstMap, sep, null);

		assertNull(dstMap.get("a-aa"));
		assertEquals("123", dstMap.get("a-bb"));
		assertEquals("yohoho", dstMap.get("b-aa"));
		assertEquals("true", dstMap.get("b-bb"));
	}

	@Test
	public void testTrimLoadOpNoiseMixedRemovesScalarWeightOnly() {
		final Map<String, Object> configTree = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		final Map<String, Object> op = new HashMap<>();
		final Map<String, Object> weights = new HashMap<>();
		weights.put("get", 50);
		weights.put("put", 50);
		weights.put("delete", 0);
		weights.put("stat", 0);
		op.put("weight", 1);
		op.put("weights", weights);
		load.put("op", op);
		configTree.put("load", load);

		ConfigUtil.trimLoadOpNoise(configTree, ConfigUtil.MIXED_LOAD_STEP_TYPE);

		assertNull(op.get("weight"));
		assertNotNull(op.get("weights"));
	}

	@Test
	public void testTrimLoadOpNoiseNonMixedRemovesWeightsAndLegacyWeightMap() {
		final Map<String, Object> configTree = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		final Map<String, Object> op = new HashMap<>();
		final Map<String, Object> legacyWeightMap = new HashMap<>();
		legacyWeightMap.put("get", 45);
		legacyWeightMap.put("put", 15);
		legacyWeightMap.put("delete", 10);
		legacyWeightMap.put("stat", 30);
		final Map<String, Object> weights = new HashMap<>();
		weights.put("get", 45);
		weights.put("put", 15);
		weights.put("delete", 10);
		weights.put("stat", 30);
		op.put("weight", legacyWeightMap);
		op.put("weights", weights);
		load.put("op", op);
		configTree.put("load", load);

		ConfigUtil.trimLoadOpNoise(configTree, "Load");

		assertNull(op.get("weight"));
		assertNull(op.get("weights"));
	}

	@Test
	public void testTrimLoadOpNoiseNonMixedKeepsScalarWeight() {
		final Map<String, Object> configTree = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		final Map<String, Object> op = new HashMap<>();
		final Map<String, Object> weights = new HashMap<>();
		weights.put("get", 45);
		weights.put("put", 15);
		weights.put("delete", 10);
		weights.put("stat", 30);
		op.put("weight", 1);
		op.put("weights", weights);
		load.put("op", op);
		configTree.put("load", load);

		ConfigUtil.trimLoadOpNoise(configTree, "Load");

		assertEquals(1, op.get("weight"));
		assertNull(op.get("weights"));
	}

	@Test
	public void testTrimLoadOpNoiseGuardReturns() {
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(null, "Load"));
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(new HashMap<>(), null));
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(new HashMap<>(), ""));

		final Map<String, Object> configTreeWithNonMapLoad = new HashMap<>();
		configTreeWithNonMapLoad.put("load", 1);
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(configTreeWithNonMapLoad, "Load"));

		final Map<String, Object> configTreeWithoutOp = new HashMap<>();
		configTreeWithoutOp.put("load", new HashMap<>());
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(configTreeWithoutOp, "Load"));

		final Map<String, Object> configTreeWithNonMapOp = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		load.put("op", 1);
		configTreeWithNonMapOp.put("load", load);
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(configTreeWithNonMapOp, "Load"));
	}

	@Test
	public void maskedStringHidesCredentialsWithoutChangingLiveConfig() throws Exception {
		final var config = TestConfigBuilder.config();
		config.val("storage-auth-uid", "uid-value-1");
		config.val("storage-auth-secret", "secret-value-1");
		config.val("storage-auth-token", "token-value-1");

		final var masked = ConfigUtil.toMaskedString(config, ConfigFormat.YAML, "Load");

		for (final var value : List.of("uid-value-1", "secret-value-1", "token-value-1")) {
			assertFalse(masked.contains(value), value);
		}
		final var auth = authOf(parse(masked));
		for (final var key : ConfigUtil.MASKED_AUTH_KEYS) {
			assertEquals(ConfigUtil.MASKED_VALUE, auth.get(key), key);
		}
		assertEquals("secret-value-1", config.stringVal("storage-auth-secret"));
		assertTrue(ConfigUtil.toString(config, ConfigFormat.YAML, "Load").contains("secret-value-1"));
	}

	@Test
	public void maskedStringChangesOnlyCredentialValues() throws Exception {
		for (final var stepType : Arrays.asList(null, "Load", ConfigUtil.MIXED_LOAD_STEP_TYPE)) {
			final var config = TestConfigBuilder.config();
			config.val("storage-auth-uid", "uid-value-2");
			config.val("storage-auth-secret", "secret-value-2");
			final var expected = parse(ConfigUtil.toString(config, ConfigFormat.YAML, stepType));
			authOf(expected).put("uid", ConfigUtil.MASKED_VALUE);
			authOf(expected).put("secret", ConfigUtil.MASKED_VALUE);

			assertEquals(expected, parse(ConfigUtil.toMaskedString(config, ConfigFormat.YAML, stepType)), stepType);
		}
	}

	@Test
	public void maskedStringKeepsUnsetAndEmptyCredentials() throws Exception {
		final var config = TestConfigBuilder.config();
		config.val("storage-auth-uid", "");
		config.val("storage-auth-secret", null);

		final var auth = authOf(parse(ConfigUtil.toMaskedString(config, ConfigFormat.YAML, null)));

		assertEquals("", auth.get("uid"));
		assertTrue(auth.containsKey("secret"));
		assertNull(auth.get("secret"));
		assertEquals("", auth.get("token"));
	}

	@Test
	public void maskCredentialsCopiesInsteadOfModifyingSharedMaps() {
		final Map<String, Object> auth = Map.of("uid", "u", "secret", "s", "file", "/creds.csv");
		final Map<String, Object> storage = Map.of("auth", auth, "driver", Map.of("type", "s3"));
		final Map<String, Object> configTree = new HashMap<>(Map.of("storage", storage));

		ConfigUtil.maskCredentials(configTree);

		assertEquals(
						Map.of("auth", Map.of("uid", "***", "secret", "***", "file", "/creds.csv"), "driver", Map.of("type", "s3")),
						configTree.get("storage"));
		assertEquals("s", auth.get("secret"));
	}

	@Test
	public void maskCredentialsIgnoresTreesWithoutAuth() {
		final Map<String, Object> noStorage = new HashMap<>(Map.of("load", Map.of()));
		final Map<String, Object> noAuth = new HashMap<>(Map.of("storage", Map.of("driver", Map.of())));
		final Map<String, Object> scalarAuth = new HashMap<>(Map.of("storage", Map.of("auth", "x")));

		ConfigUtil.maskCredentials(noStorage);
		ConfigUtil.maskCredentials(noAuth);
		ConfigUtil.maskCredentials(scalarAuth);

		assertEquals(Map.of("load", Map.of()), noStorage);
		assertEquals(Map.of("storage", Map.of("driver", Map.of())), noAuth);
		assertEquals(Map.of("storage", Map.of("auth", "x")), scalarAuth);
	}

	@Test
	public void maskedStringHidesListedHeadersInAnyCaseWithoutChangingLiveConfig() throws Exception {
		final var headers = new HashMap<String, Object>();
		headers.put("Authorization", "AWS fake-auth-header-1");
		headers.put("x-auth-token", "fake-swift-token-1");
		headers.put("COOKIE", "session=fake-cookie-1");
		headers.put("Proxy-Authorization", "Basic ZmFrZTpmYWtl");
		headers.put("X-Amz-Security-Token", "fake-session-token-1");
		headers.put("X-Custom", "custom-value-1");
		headers.put("Cookie2", "unlisted-cookie-1");
		headers.put("x-amz-date", "");
		final var config = TestConfigBuilder.configWithHttpHeaders(headers);

		final var masked = ConfigUtil.toMaskedString(config, ConfigFormat.YAML, "Load");

		for (final var value : List.of("fake-auth-header-1", "fake-swift-token-1", "fake-cookie-1", "ZmFrZTpmYWtl",
						"fake-session-token-1")) {
			assertFalse(masked.contains(value), value);
		}
		final var maskedHeaders = headersOf(parse(masked));
		for (final var name : List.of("Authorization", "x-auth-token", "COOKIE", "Proxy-Authorization",
						"X-Amz-Security-Token")) {
			assertEquals(ConfigUtil.MASKED_VALUE, maskedHeaders.get(name), name);
		}
		assertEquals("custom-value-1", maskedHeaders.get("X-Custom"));
		assertEquals("unlisted-cookie-1", maskedHeaders.get("Cookie2"));
		assertEquals("", maskedHeaders.get("x-amz-date"));
		assertEquals("AWS fake-auth-header-1", config.mapVal("storage-net-http-headers").get("Authorization"));
		assertEquals("fake-swift-token-1", config.mapVal("storage-net-http-headers").get("x-auth-token"));
	}

	@Test
	public void maskedStringChangesOnlyCredentialAndListedHeaderValues() throws Exception {
		final var headers = new HashMap<String, Object>();
		headers.put("authorization", "fake-auth-header-2");
		headers.put("X-Custom", "custom-value-2");
		final var config = TestConfigBuilder.configWithHttpHeaders(headers);
		config.val("storage-auth-secret", "secret-value-3");
		final var expected = parse(ConfigUtil.toString(config, ConfigFormat.YAML, "Load"));
		authOf(expected).put("secret", ConfigUtil.MASKED_VALUE);
		headersOf(expected).put("authorization", ConfigUtil.MASKED_VALUE);

		assertEquals(expected, parse(ConfigUtil.toMaskedString(config, ConfigFormat.YAML, "Load")));
	}

	@Test
	public void maskCredentialsKeepsUnsetAndEmptyHeadersAndCopiesSharedMaps() {
		final var headers = new HashMap<String, Object>();
		headers.put("Authorization", null);
		headers.put("Cookie", "");
		headers.put("X-Auth-Token", "fake-swift-token-2");
		final Map<String, Object> http = Map.of("headers", headers, "uri", Map.of("args", Map.of()));
		final Map<String, Object> net = Map.of("http", http, "node", Map.of("port", 9020));
		final Map<String, Object> storage = Map.of("net", net, "driver", Map.of("type", "s3"));
		final Map<String, Object> configTree = new HashMap<>(Map.of("storage", storage));

		ConfigUtil.maskCredentials(configTree);

		final var maskedHeaders = headersOf(configTree);
		assertTrue(maskedHeaders.containsKey("Authorization"));
		assertNull(maskedHeaders.get("Authorization"));
		assertEquals("", maskedHeaders.get("Cookie"));
		assertEquals(ConfigUtil.MASKED_VALUE, maskedHeaders.get("X-Auth-Token"));
		assertEquals("fake-swift-token-2", headers.get("X-Auth-Token"));
		final var maskedStorage = (Map<?, ?>) configTree.get("storage");
		assertEquals(Map.of("type", "s3"), maskedStorage.get("driver"));
		assertEquals(Map.of("port", 9020), ((Map<?, ?>) maskedStorage.get("net")).get("node"));
		assertFalse(maskedStorage.containsKey("auth"));
	}

	@Test
	public void maskedArgPathsAreTheAuthCredentialsAndListedHeaders() {
		for (final var path : List.of("storage-auth-uid", "storage-auth-secret", "storage-auth-token",
						"storage-net-http-headers-Authorization", "storage-net-http-headers-proxy-authorization",
						"storage-net-http-headers-cookie", "storage-net-http-headers-X-Amz-Security-Token",
						"storage-net-http-headers-x-auth-token")) {
			assertTrue(ConfigUtil.isMaskedArgPath(path), path);
		}
		for (final var path : List.of("storage-auth-file", "storage-auth-uid-x", "storage-auth", "storage-driver-type",
						"storage-net-http-headers-X-Custom", "storage-net-http-headers-", "storage-net-http-uri-args-token",
						"item-output-path")) {
			assertFalse(ConfigUtil.isMaskedArgPath(path), path);
		}
	}

	@Test
	public void maskedArgFollowsTheFirstAliasingEntryAndKeepsTheGivenName() {
		final List<Map<String, Object>> aliasing = List.of(
						Map.of(AliasingUtil.NAME, "atmos-subtenant", AliasingUtil.TARGET, "storage-auth-token"),
						Map.of(AliasingUtil.NAME, "fixed-secret", AliasingUtil.TARGET, "storage-auth-secret=x"),
						Map.of(AliasingUtil.NAME, "s3-output-bucket", AliasingUtil.TARGET, "item-output-path"),
						Map.of(AliasingUtil.NAME, "s3-output-bucket", AliasingUtil.TARGET, "storage-auth-secret"),
						Map.of(AliasingUtil.NAME, "storage-auth-file", AliasingUtil.TARGET, "storage-auth-secret"));

		assertEquals("--atmos-subtenant=***", ConfigUtil.maskedArg("--atmos-subtenant=fake-token-3", aliasing));
		assertEquals("--fixed-secret=***", ConfigUtil.maskedArg("--fixed-secret=fake-secret-3", aliasing));
		assertEquals("--s3-output-bucket=/b", ConfigUtil.maskedArg("--s3-output-bucket=/b", aliasing));
		assertEquals("--storage-auth-file=***", ConfigUtil.maskedArg("--storage-auth-file=/creds.csv", aliasing));
		assertEquals("--storage-auth-uid=***", ConfigUtil.maskedArg("--storage-auth-uid=fake-uid-3", null));
		assertEquals("--storage-auth-secret=***", ConfigUtil.maskedArg("--storage-auth-secret==a=b=", List.of()));
		assertEquals("--storage-auth-secret=", ConfigUtil.maskedArg("--storage-auth-secret=", aliasing));
		assertEquals("--storage-auth-secret", ConfigUtil.maskedArg("--storage-auth-secret", aliasing));
		assertEquals("--storage-auth-file=/c", ConfigUtil.maskedArg("--storage-auth-file=/c", List.of()));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> headersOf(final Map<String, Object> configTree) {
		final var storage = (Map<String, Object>) configTree.get("storage");
		final var net = (Map<String, Object>) storage.get("net");
		return (Map<String, Object>) ((Map<String, Object>) net.get("http")).get("headers");
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parse(final String yaml) throws Exception {
		return new YAMLMapper().readValue(yaml, Map.class);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> authOf(final Map<String, Object> configTree) {
		return (Map<String, Object>) ((Map<String, Object>) configTree.get("storage")).get("auth");
	}
}
