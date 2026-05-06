package com.dell.spt.storage.driver.coop.aws.s3.config;

/**
 * Object size classification for smart CRT configuration.
 * Each profile has optimized CRT parameters for that size range.
 */
public enum ObjectSizeProfile {
	SMALL("small", 0, 64 * 1024), MEDIUM("medium", 64 * 1024, 8 * 1024 * 1024), LARGE("large", 8 * 1024 * 1024, 100 * 1024 * 1024), VERY_LARGE("very_large", 100 * 1024 * 1024, Long.MAX_VALUE);

	private final String name;
	private final long minSize;
	private final long maxSize;

	ObjectSizeProfile(final String name, final long minSize, final long maxSize) {
		this.name = name;
		this.minSize = minSize;
		this.maxSize = maxSize;
	}

	public String getName() {
		return name;
	}

	public long getMinSize() {
		return minSize;
	}

	public long getMaxSize() {
		return maxSize;
	}

	/**
	 * Determine if a given object size falls into this profile.
	 */
	public boolean matches(final long size) {
		return size >= minSize && size < maxSize;
	}

	/**
	 * Get the profile for a given object size.
	 */
	public static ObjectSizeProfile fromSize(final long size) {
		for (ObjectSizeProfile profile : values()) {
			if (profile.matches(size)) {
				return profile;
			}
		}
		return MEDIUM; // Default fallback
	}
}
