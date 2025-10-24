package com.dell.spt.base.config.el;

import com.github.akurilov.commons.io.collection.CompositeStringInput;

import java.lang.reflect.Method;

public interface CompositeExpressionInputBuilder {

	CompositeExpressionInputBuilder expression(final String expr);

	CompositeExpressionInputBuilder function(final String prefix, final String name, final Method method);

	CompositeExpressionInputBuilder value(final String name, final Object value, final Class<?> type);

	CompositeStringInput build();

	static CompositeExpressionInputBuilder newInstance() {
		return new CompositeExpressionInputBuilderImpl();
	}
}
