package com.dell.spt.base.concurrent;

import java.util.function.Predicate;

/** The daemon registry, for tests outside the base module. */
public final class DaemonRegistryView {

	private DaemonRegistryView() {}

	public static boolean isRegistered(final Predicate<? super Daemon> matcher) {
		return DaemonBase.isRegistered(matcher);
	}
}
