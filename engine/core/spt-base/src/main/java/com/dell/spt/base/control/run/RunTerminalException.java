package com.dell.spt.base.control.run;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Typed cause that ends a scenario run with a structured FAILED status.
 *
 * <p>The message is public: it reaches {@code /status}, the CLI and the run metadata, so it must
 * carry no credentials, authorization material, query strings, response bodies or internal
 * endpoints. The cause is technical detail for the engine's own logs.
 */
public class RunTerminalException extends RuntimeException {

	private final IntegrityTerminalException.Category category;
	private final String stepId;

	public RunTerminalException(
					final IntegrityTerminalException.Category category, final String message, final Throwable cause) {
		this(category, null, message, cause);
	}

	public RunTerminalException(
					final IntegrityTerminalException.Category category,
					final String stepId,
					final String message,
					final Throwable cause) {
		super(message, cause);
		this.category = category;
		this.stepId = stepId;
	}

	public IntegrityTerminalException.Category category() {
		return category;
	}

	public String stepId() {
		return stepId;
	}

	/** Returns a copy attributed to the given step, or this instance when it already has one. */
	public RunTerminalException withStepId(final String value) {
		if (stepId != null || value == null || value.isBlank()) {
			return this;
		}
		return copySuppressed(new RunTerminalException(category, value, getMessage(), this));
	}

	protected final <T extends RunTerminalException> T copySuppressed(final T result) {
		for (final var suppressed : getSuppressed()) {
			result.addSuppressed(suppressed);
		}
		return result;
	}

	/** Finds a typed terminal cause, integrity or generic, through script-engine/runtime wrappers. */
	public static RunTerminalException find(final Throwable outer) {
		final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		var current = outer;
		while (current != null && visited.add(current)) {
			if (current instanceof RunTerminalException) {
				return (RunTerminalException) current;
			}
			current = current.getCause();
		}
		return null;
	}
}
