package com.dell.spt.base.config;

import com.github.akurilov.commons.io.Input;

/**
 * A marker interface notifying that the given input returns the same value for every invocation.
 *
 * @param <T> value type emitted by the input
 */
public interface ConstantValueInput<T> extends Input<T> {}
