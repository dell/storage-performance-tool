package com.dell.spt.base.config.el;

import com.dell.spt.base.env.DateUtil;
import com.github.akurilov.commons.io.el.ExpressionInput;
import com.github.akurilov.commons.math.MathUtil;
import com.github.akurilov.commons.math.Random;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public interface Language {

	String FUNC_NAME_SEPARATOR = ":";

	Map<String, Method> FUNCTIONS = buildFunctions();

	static String format(final String pattern, final Object... args) {
		return CompositeExpressionInputBuilderImpl.FORMATTER.format(pattern, args).toString();
	}

	static long xor(final long x1, final long x2) {
		return x1 ^ x2;
	}

	final class Value {
		final Object value;
		final Class<?> type;

		Value(final Object value, final Class<?> type) {
			this.value = value;
			this.type = type;
		}
	}

	Map<String, Value> VALUES = buildValues();

	static ExpressionInput.Builder withLanguage(final ExpressionInput.Builder builder) {
		for (final var func : Language.FUNCTIONS.entrySet()) {
			final var methodKey = func.getKey();
			final int separatorIndex = methodKey.indexOf(FUNC_NAME_SEPARATOR);
			if (separatorIndex <= 0 || separatorIndex == methodKey.length() - 1) {
				throw new IllegalStateException("Invalid function name: " + methodKey);
			}
			final var prefix = methodKey.substring(0, separatorIndex);
			final var methodName = methodKey.substring(separatorIndex + 1);
			builder.function(prefix, methodName, func.getValue());
		}
		for (final var val : Language.VALUES.entrySet()) {
			final var name = val.getKey();
			final var v = val.getValue();
			builder.value(name, v.value, v.type);
		}
		return builder;
	}

	private static Map<String, Method> buildFunctions() {
		final Map<String, Method> functions = new LinkedHashMap<>();
		try {
			functions.put("date:formatNowAmazonStyle", DateUtil.class.getMethod("formatNowAmazonStyle"));
			functions.put("date:formatNowIso8601", DateUtil.class.getMethod("formatNowIso8601"));
			functions.put("date:formatNowRfc1123", DateUtil.class.getMethod("formatNowRfc1123"));
			functions.put("date:format", DateUtil.class.getMethod("dateFormat", String.class));
			functions.put("date:from", DateUtil.class.getMethod("date", long.class));
			functions.put("env:get", System.class.getMethod("getenv", String.class));
			functions.put("int64:toString", Long.class.getMethod("toString", long.class, int.class));
			functions.put("int64:toUnsignedString", Long.class.getMethod("toUnsignedString", long.class, int.class));
			functions.put("int64:reverse", Long.class.getMethod("reverse", long.class));
			functions.put("int64:reverseBytes", Long.class.getMethod("reverseBytes", long.class));
			functions.put("int64:rotateLeft", Long.class.getMethod("rotateLeft", long.class, int.class));
			functions.put("int64:rotateRight", Long.class.getMethod("rotateRight", long.class, int.class));
			functions.put("int64:xor", Language.class.getMethod("xor", long.class, long.class));
			functions.put("int64:xorShift", MathUtil.class.getMethod("xorShift", long.class));
			functions.put("math:absInt32", Math.class.getMethod("abs", int.class));
			functions.put("math:absInt64", Math.class.getMethod("abs", long.class));
			functions.put("math:absFloat32", Math.class.getMethod("abs", float.class));
			functions.put("math:absFloat64", Math.class.getMethod("abs", double.class));
			functions.put("math:acos", Math.class.getMethod("acos", double.class));
			functions.put("math:asin", Math.class.getMethod("asin", double.class));
			functions.put("math:atan", Math.class.getMethod("atan", double.class));
			functions.put("math:ceil", Math.class.getMethod("ceil", double.class));
			functions.put("math:cos", Math.class.getMethod("cos", double.class));
			functions.put("math:exp", Math.class.getMethod("exp", double.class));
			functions.put("math:floor", Math.class.getMethod("floor", double.class));
			functions.put("math:log", Math.class.getMethod("log", double.class));
			functions.put("math:log10", Math.class.getMethod("log10", double.class));
			functions.put("math:maxInt32", Math.class.getMethod("max", int.class, int.class));
			functions.put("math:maxInt64", Math.class.getMethod("max", long.class, long.class));
			functions.put("math:maxFloat32", Math.class.getMethod("max", float.class, float.class));
			functions.put("math:maxFloat64", Math.class.getMethod("max", double.class, double.class));
			functions.put("math:minInt32", Math.class.getMethod("min", int.class, int.class));
			functions.put("math:minInt64", Math.class.getMethod("min", long.class, long.class));
			functions.put("math:minFloat32", Math.class.getMethod("min", float.class, float.class));
			functions.put("math:minFloat64", Math.class.getMethod("min", double.class, double.class));
			functions.put("math:pow", Math.class.getMethod("pow", double.class, double.class));
			functions.put("math:sin", Math.class.getMethod("sin", double.class));
			functions.put("math:sqrt", Math.class.getMethod("sqrt", double.class));
			functions.put("math:tan", Math.class.getMethod("tan", double.class));
			functions.put("path:random", RandomPath.class.getMethod("get", int.class, int.class));
			functions.put("string:format", Language.class.getMethod("format", String.class, Object[].class));
			functions.put("string:join", String.class.getMethod("join", CharSequence.class, CharSequence[].class));
			functions.put("time:millisSinceEpoch", System.class.getMethod("currentTimeMillis"));
			functions.put("time:nanos", System.class.getMethod("nanoTime"));
		} catch (final NoSuchMethodException e) {
			throw new AssertionError(e);
		}
		return Collections.unmodifiableMap(functions);
	}

	private static Map<String, Value> buildValues() {
		final Map<String, Value> values = new LinkedHashMap<>();
		values.put("e", new Value(Math.E, double.class));
		values.put("lineSep", new Value(System.lineSeparator(), String.class));
		values.put("pathSep", new Value(File.pathSeparator, String.class));
		values.put("pi", new Value(Math.PI, double.class));
		values.put("rnd", new Value(new Random(), Random.class));
		return Collections.unmodifiableMap(values);
	}
}
