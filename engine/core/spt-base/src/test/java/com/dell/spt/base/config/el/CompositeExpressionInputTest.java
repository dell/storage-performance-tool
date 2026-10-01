package com.dell.spt.base.config.el;

import static com.dell.spt.base.config.el.CompositeExpressionInputBuilderImpl.INITIAL_VALUE_PATTERN;
import static java.lang.System.currentTimeMillis;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import com.github.akurilov.commons.io.el.ExpressionInputImpl;

import com.dell.spt.base.env.DateUtil;
import com.github.akurilov.commons.io.collection.CompositeStringInput;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class CompositeExpressionInputTest {

	@Test
	public void testRandomItemId() throws Exception {
		final var radix = 36;
		final var length = 10;
		final var init = "%{math:absInt64(int64:xor(int64:reverse(time:millisSinceEpoch()), int64:reverseBytes(time:nanos())))}";
		final var expr = "${math:absInt64(int64:xorShift(this.last()) % math:pow(radix, length))}";
		final CompositeStringInput offsetInput = CompositeExpressionInputBuilder.newInstance()
						.expression(expr + init)
						.value("radix", radix, int.class)
						.value("length", length, int.class)
						.build();
		final var itemNameInput = CompositeExpressionInputBuilder.newInstance()
						.expression("${int64:toString(offsetInput.get(), radix)}")
						.value("offsetInput", offsetInput, CompositeStringInput.class)
						.value("radix", radix, int.class)
						.build();
		final var id = itemNameInput.get();
		assertTrue(length >= id.length());
	}

	@Test
	public void testSelfReferenceInCompositeExpression()
					throws Exception {
		final var data = "Foo${this.expr()}Bar#{this.expr()}";
		try (final var in = CompositeExpressionInputBuilder.newInstance().expression(data).build()) {
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			String result;
			do {
				result = in.get();
				if (data.equals(result)) {
					break;
				}
				Thread.sleep(1);
			} while (System.nanoTime() < deadline);
			assertEquals(data, result, "asynchronous segment should become ready");
			for (int i = 0; i < 10_000; i++) {
				assertEquals(data, in.get(), "mixed segments must remain stable during concurrent evaluation");
			}
		}
	}

	@Test
	public void segmentsAndRepeatedBuildsOwnSeparateEvaluationContexts() throws Exception {
		final var builder = CompositeExpressionInputBuilder.newInstance()
						.expression("${this.expr()}|${this.expr()}");
		try (final var first = builder.build(); final var second = builder.build()) {
			// ELContext contains mutable method-resolution state. Assert isolation directly,
			// so this regression does not depend on winning a thread scheduling race.
			final var segmentsField = CompositeStringInput.class.getDeclaredField("segments");
			segmentsField.setAccessible(true);
			final var contextField = ExpressionInputImpl.class.getDeclaredField("ctx");
			contextField.setAccessible(true);
			final var contexts = new java.util.ArrayList<Object>();
			for (final var input : new CompositeStringInput[]{first, second
			}) {
				for (final var segment : (Object[]) segmentsField.get(input)) {
					if (segment instanceof ExpressionInputImpl) {
						final var context = contextField.get(segment);
						for (final var previous : contexts) {
							assertNotSame(previous, context, "independently evaluated segments must not share ELContext");
						}
						contexts.add(context);
					}
				}
				assertEquals("${this.expr()}|${this.expr()}", input.get());
			}
			assertEquals(4, contexts.size());
		}
	}

	@Test
	public void isolatedSegmentsRetainBindingsOverridesAndInitialValues() throws Exception {
		final var builder = CompositeExpressionInputBuilder.newInstance()
						.value("increment", 2, int.class)
						.function("custom", "abs", Math.class.getMethod("abs", int.class))
						.expression("${this.last() + increment}%{0}|${custom:abs(-increment)}");
		try (final var first = builder.build()) {
			assertEquals("2|2", first.get());
			builder.value("increment", 3, int.class);
			try (final var second = builder.build()) {
				assertEquals("3|3", second.get());
				assertEquals("4|2", first.get(), "reusing a builder must preserve existing input bindings");
			}
		}
	}

	@Test
	public void testVararg() throws Exception {
		final var inputBuilder = CompositeExpressionInputBuilder.newInstance();
		var in = inputBuilder
						.expression("${string:join('_', 'a')}")
						.build();
		assertEquals("a", in.get());
		in.close();
		in = inputBuilder.expression("${string:join('_', 'a', 'b')}").build();
		assertEquals("a_b", in.get());
		in.close();
	}

	@Test
	public void testPaths() throws Exception {
		final var exprPathInput = CompositeExpressionInputBuilder.newInstance()
						.expression("/${path:random(16, 2)}")
						.build();
		String p;
		String[] pp;
		for (var i = 0; i < 100; i++) {
			p = exprPathInput.get();
			assertTrue(p.startsWith("/"));
			assertTrue(p.endsWith("/"));
			pp = p.split("/", 4);
			assertTrue(pp.length > 2 && pp.length < 5);
			for (var ppp : pp) {
				if (!ppp.isEmpty()) {
					assertTrue(16 > Integer.parseInt(ppp, 16));
				}
			}
		}
	}

	@Test
	public void testInitialPattern() throws Exception {
		final var withInitVal = "prefix_${this.last() + 1}%{-1}suffix";
		var m = INITIAL_VALUE_PATTERN.matcher(withInitVal);
		assertTrue(m.find());
		assertEquals("%{-1}", m.group(1));
		final var noInitVal = "prefix_${this.last() + 1}suffix";
		m = INITIAL_VALUE_PATTERN.matcher(noInitVal);
		assertFalse(m.find());
	}

	@Test
	public void testRandomDateInRangeCustomFormat()
					throws Exception {
		final var dateInput = CompositeExpressionInputBuilder.newInstance()
						.expression("${date:format(\"" + DateUtil.PATTERN_METRICS_TABLE + "\").format(date:from(rnd.nextLong(time:millisSinceEpoch())))}")
						.build();
		final var rndDateStr = dateInput.get();
		final var rndDate = DateUtil.parseMetricsTable(rndDateStr);
		assertTrue(rndDate.isAfter(Instant.EPOCH));
		assertTrue(rndDate.isBefore(Instant.ofEpochMilli(currentTimeMillis())));
	}

	@Test
	public void testMessageFormat()
					throws Exception {
		final var msgInput = CompositeExpressionInputBuilder.newInstance()
						.expression("${string:format(\"At %tT %1$tZ on %1$tY %1$tb %1$te, there was %s on planet %d.\", date:from(0), \"a disturbance in the Force\", 7)}")
						.build();
		assertEquals("At 00:00:00 UTC on 1970 Jan 1, there was a disturbance in the Force on planet 7.", msgInput.get());
	}

	@Test
	public void testConstantValueExpression()
					throws Exception {
		final var t0 = System.currentTimeMillis();
		TimeUnit.SECONDS.sleep(1);
		final var in = CompositeExpressionInputBuilder.newInstance()
						.expression("%{time:millisSinceEpoch()}")
						.build();
		final var t1 = Long.parseLong(in.get());
		TimeUnit.SECONDS.sleep(1);
		final var t2 = System.currentTimeMillis();
		assertTrue(t0 < t1);
		assertTrue(t1 < t2);
		TimeUnit.SECONDS.sleep(1); // wait, maybe the value will change...
		final var t3 = Long.parseLong(in.get());
		assertEquals(t1, t3); // no, it has not been changed
		in.close();
	}
}
