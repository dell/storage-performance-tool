package com.dell.spt.base.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/** The JDK's own list classes, which have no public name, that a configuration copy rebuilds. */
final class JdkListClasses {

	static final Class<?> ARRAYS_AS_LIST = Arrays.asList().getClass();
	/** {@link List#of} and {@link List#copyOf} with one or two elements. */
	static final Class<?> LIST_OF_ONE_OR_TWO = List.of(1).getClass();
	/** {@link List#of} and {@link List#copyOf} with none or more than two, and every {@code Stream.toList}. */
	static final Class<?> LIST_OF_N = List.of().getClass();
	static final Class<?> UNMODIFIABLE_RANDOM_ACCESS = Collections.unmodifiableList(new ArrayList<>()).getClass();
	@SuppressWarnings("JdkObsolete") // the class of a view over a list that is not RandomAccess
	static final Class<?> UNMODIFIABLE_SEQUENTIAL = Collections.unmodifiableList(new LinkedList<>()).getClass();
	static final Class<?> SINGLETON = Collections.singletonList(null).getClass();

	private JdkListClasses() {}
}
