package com.dell.spt.base.integrity;

import java.io.IOException;

/** Runs narrow cleanup actions with try-with-resources suppression semantics. */
public final class FailurePreservingCleanup {

	@FunctionalInterface
	public interface IOAction {
		void run() throws IOException;
	}

	@FunctionalInterface
	public interface IOSupplier<T> {
		T get() throws IOException;
	}

	private interface IOScope extends AutoCloseable {
		@Override
		void close() throws IOException;
	}

	private static final class FailureScope implements IOScope {
		private final IOAction cleanup;
		private boolean completed;

		private FailureScope(final IOAction cleanup) {
			this.cleanup = cleanup;
		}

		@Override
		public void close() throws IOException {
			if (!completed) {
				cleanup.run();
			}
		}
	}

	private FailurePreservingCleanup() {}

	public static <T> T always(final IOSupplier<T> operation, final IOAction cleanup)
					throws IOException {
		try (IOScope ignored = cleanup::run) {
			return operation.get();
		}
	}

	public static <T> T onFailure(final IOSupplier<T> operation, final IOAction cleanup)
					throws IOException {
		final FailureScope scope = new FailureScope(cleanup);
		try (scope) {
			final T result = operation.get();
			scope.completed = true;
			return result;
		}
	}
}
