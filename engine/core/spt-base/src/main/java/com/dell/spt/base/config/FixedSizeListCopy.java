package com.dell.spt.base.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Makes an empty {@link Arrays#asList} list of the source's size whose backing array has the exact
 * class of the source's array, so a copy refuses the same values the source refuses: a list set from
 * text is backed by a {@code String[]}, and setting a number in it throws {@link ArrayStoreException}.
 *
 * <p>The list is written to a serialization stream and read back. The stream keeps the list and its
 * backing array, and writes {@code null} in place of every element, so no element is serialized. An
 * array is written with its class descriptor, and reading resolves each descriptor to the very
 * {@link Class} object that was written, so the new array has the source array's class, whatever its
 * class loader. No reflection into {@code java.base} is needed.
 */
final class FixedSizeListCopy {

	private FixedSizeListCopy() {}

	/**
	 * An {@link Arrays#asList} list of the source's size and array class, holding only nulls, or
	 * empty when the stream cannot rebuild it.
	 */
	static Optional<List<Object>> emptyLike(final List<?> source) {
		final Map<String, Class<?>> written = new HashMap<>();
		final var bytes = new ByteArrayOutputStream();
		try (var out = new ElementlessOutput(bytes, source, written)) {
			out.writeObject(source);
		} catch (final IOException e) {
			return Optional.empty();
		}
		try (var in = new WrittenClassInput(new ByteArrayInputStream(bytes.toByteArray()), written)) {
			if (in.readObject() instanceof List<?> list && list.getClass() == source.getClass() && list.size() == source.size()) {
				@SuppressWarnings("unchecked")
				final var copy = (List<Object>) list;
				return Optional.of(copy);
			}
			return Optional.empty();
		} catch (final IOException | ClassNotFoundException e) {
			return Optional.empty();
		}
	}

	/** Writes the list and its backing array, and null for every other object, and records each class it writes. */
	private static final class ElementlessOutput extends ObjectOutputStream {

		private final Object source;
		private final Map<String, Class<?>> written;
		private boolean arrayWritten;

		ElementlessOutput(final ByteArrayOutputStream out, final Object source, final Map<String, Class<?>> written)
						throws IOException {
			super(out);
			this.source = source;
			this.written = written;
			enableReplaceObject(true);
		}

		@Override
		protected void annotateClass(final Class<?> cl) {
			written.put(cl.getName(), cl);
		}

		@Override
		protected Object replaceObject(final Object obj) {
			if (obj == source) {
				return obj;
			}
			// the list's only field is its array, so the first array after the list is that one
			if (!arrayWritten && obj.getClass().isArray()) {
				arrayWritten = true;
				return obj;
			}
			return null;
		}
	}

	/** Resolves each class descriptor to the class that was written. */
	private static final class WrittenClassInput extends ObjectInputStream {

		private final Map<String, Class<?>> written;

		WrittenClassInput(final ByteArrayInputStream in, final Map<String, Class<?>> written) throws IOException {
			super(in);
			this.written = written;
		}

		@Override
		protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
			final var cl = written.get(desc.getName());
			if (cl == null) {
				throw new ClassNotFoundException(desc.getName());
			}
			return cl;
		}
	}
}
