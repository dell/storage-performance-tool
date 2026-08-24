package com.dell.spt.base.item.op.deletion;

/** Storage-driver capability for checking one current-key or exact-version DELETE identity. */
@FunctionalInterface
public interface DeleteVerificationProbe {

	/** A target is either observable, absent, or not conclusively classified. */
	enum Presence {
		PRESENT, ABSENT, UNRESOLVED
	}

	/** Checks the exact identity represented by {@code target}. */
	Presence presence(DeleteTarget target);
}
