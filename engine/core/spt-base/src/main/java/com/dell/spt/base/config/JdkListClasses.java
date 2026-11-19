package com.dell.spt.base.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** The JDK's own list classes, which have no public name, that a configuration copy copies. */
final class JdkListClasses {

	static final Class<?> ARRAYS_AS_LIST = Arrays.asList().getClass();
	/** {@link List#of} and {@link List#copyOf} with one or two elements. */
	static final Class<?> LIST_OF_ONE_OR_TWO = List.of(1).getClass();
	/** {@link List#of} and {@link List#copyOf} with none or more than two, and every {@code Stream.toList}. */
	static final Class<?> LIST_OF_N = List.of().getClass();
	static final Class<?> SINGLETON = Collections.singletonList(null).getClass();

	private JdkListClasses() {}
}
