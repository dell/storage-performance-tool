package com.dell.spt.base.item;

/**
 * The one rule for whether an item name already holds its storage path.
 *
 * <p>A name holds a path only when it starts with the path in the same form and the match ends at
 * a path boundary. A name with a leading slash is a full path, so it compares against the path with
 * its leading slash. A relative name is a key, so it holds a path only if that path is also written
 * without a leading slash: with the path {@code /abc}, the generated name {@code abc/k} is the key
 * {@code abc/k} in bucket {@code abc}, and {@code /abc/k} is the key {@code k}.
 *
 * <p>Called once or twice per operation on the request and result paths, so nothing here allocates
 * unless it returns a new string.
 */
public final class ItemNamePaths {

	private static final char SEPARATOR = '/';
	private static final int DIFFERENT_FORM = -1;

	private ItemNamePaths() {}

	/** Returns whether the name already holds the path, under the rule in the class comment. */
	public static boolean holdsPath(final String name, final String path) {
		if (path.isEmpty()) {
			return false;
		}
		final int offset = pathOffset(name, path);
		return offset != DIFFERENT_FORM && matchesAt(name, path, offset);
	}

	/**
	 * Returns whether the name starts with the path exactly as written, ending at a path boundary.
	 * For paths whose leading slash carries meaning, such as filesystem paths, where a relative path
	 * and an absolute one are different places.
	 */
	public static boolean startsWithPath(final String name, final String path) {
		return !path.isEmpty() && matchesAt(name, path, 0);
	}

	/** Returns what follows the path in a name for which {@link #holdsPath} is true. */
	public static String relativeToPath(final String name, final String path) {
		final int end = pathOffset(name, path) + path.length();
		return endsWithSeparator(path) ? name.substring(end) : name.substring(end + 1);
	}

	/** Returns the name placed under the path, with exactly one separator between them. */
	public static String join(final String path, final String name) {
		final boolean nameRooted = isRooted(name);
		if (endsWithSeparator(path)) {
			return nameRooted ? path + name.substring(1) : path + name;
		}
		return nameRooted ? path + name : path + SEPARATOR + name;
	}

	private static int pathOffset(final String name, final String path) {
		final boolean nameRooted = isRooted(name);
		if (nameRooted == isRooted(path)) {
			return 0;
		}
		return nameRooted ? 1 : DIFFERENT_FORM;
	}

	private static boolean matchesAt(final String name, final String path, final int offset) {
		if (!name.startsWith(path, offset)) {
			return false;
		}
		final int end = offset + path.length();
		return endsWithSeparator(path) || (end < name.length() && name.charAt(end) == SEPARATOR);
	}

	private static boolean isRooted(final String value) {
		return !value.isEmpty() && value.charAt(0) == SEPARATOR;
	}

	private static boolean endsWithSeparator(final String path) {
		return !path.isEmpty() && path.charAt(path.length() - 1) == SEPARATOR;
	}
}
