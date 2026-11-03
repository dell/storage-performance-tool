package com.dell.spt.base.storage.driver;

/**
 * Lets a driver's start name what it is doing while it runs on a step's preparation thread, for the
 * message a preparation timeout gives. On any other thread a report does nothing.
 */
public final class AdmissionPhase {

	private static final ThreadLocal<AdmissionPreparable.Preparation> CURRENT = new ThreadLocal<>();

	private AdmissionPhase() {}

	/** Binds the calling thread to a preparation, or to none. */
	public static void current(final AdmissionPreparable.Preparation preparation) {
		if (preparation == null) {
			CURRENT.remove();
		} else {
			CURRENT.set(preparation);
		}
	}

	public static void report(final String phase) {
		final var preparation = CURRENT.get();
		if (preparation != null) {
			preparation.phase(phase);
		}
	}
}
