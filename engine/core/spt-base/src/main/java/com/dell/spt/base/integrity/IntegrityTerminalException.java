package com.dell.spt.base.integrity;

import com.dell.spt.base.control.run.RunTerminalException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;

/** Typed terminal cause for metadata-mode lifecycle failures. */
public final class IntegrityTerminalException extends RunTerminalException {

	public enum Category {
		CONFIGURATION, INPUT, EXECUTION, AGGREGATION, PUBLICATION, CLEANUP;

		public String wireName() {
			return name().toLowerCase(Locale.ROOT);
		}
	}

	public IntegrityTerminalException(
					final Category category, final String message, final Throwable cause) {
		this(category, null, message, cause);
	}

	public IntegrityTerminalException(final Category category, final String message) {
		this(category, null, message, null);
	}

	public IntegrityTerminalException(
					final Category category,
					final String stepId,
					final String message,
					final Throwable cause) {
		super(category, stepId, message, cause);
	}

	@Override
	public IntegrityTerminalException withStepId(final String value) {
		if (stepId() != null || value == null || value.isBlank()) {
			return this;
		}
		return copySuppressed(new IntegrityTerminalException(category(), value, getMessage(), this));
	}

	/** Finds a typed terminal cause through script-engine/runtime wrappers. */
	public static IntegrityTerminalException find(final Throwable outer) {
		final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		var current = outer;
		while (current != null && visited.add(current)) {
			if (current instanceof IntegrityTerminalException) {
				return (IntegrityTerminalException) current;
			}
			current = current.getCause();
		}
		return null;
	}
}
