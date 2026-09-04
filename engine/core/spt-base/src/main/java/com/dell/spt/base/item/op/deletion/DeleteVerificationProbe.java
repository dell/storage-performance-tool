package com.dell.spt.base.item.op.deletion;

/** Storage-driver capability for checking one current-key or exact-version DELETE identity. */
@FunctionalInterface
public interface DeleteVerificationProbe {

	/** A target is either observable, absent, or not conclusively classified. */
	enum Presence {
		PRESENT((byte) 0), ABSENT((byte) 1), UNRESOLVED((byte) 2);

		private final byte code;

		Presence(final byte code) {
			this.code = code;
		}

		/** Stable single-byte encoding used by the verification ledger. */
		byte code() {
			return code;
		}
	}

	/** Checks the exact identity represented by {@code target}. */
	Presence presence(DeleteTarget target);
}
