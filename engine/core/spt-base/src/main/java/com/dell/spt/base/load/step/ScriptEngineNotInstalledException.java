package com.dell.spt.base.load.step;

/**
 * No installed script engine claims a scenario file whose language needs one in {@code ext/}. The
 * message names only the language and the file's extension, never the file's path or text.
 */
public final class ScriptEngineNotInstalledException extends IllegalStateException {

	private static final long serialVersionUID = 1L;

	/**
	 * @param lang the language's name, such as {@code Groovy}
	 * @param ext the scenario file's extension, without the dot
	 */
	public ScriptEngineNotInstalledException(final String lang, final String ext) {
		super("The " + lang + " script engine is not installed, so the \"." + ext
						+ "\" scenario cannot run: put a JSR-223 " + lang + " engine jar in the ext/ directory next to"
						+ " spt.jar (doc/usage/input/scenarios/README.md, section 3.2). JavaScript scenarios need"
						+ " no engine jar.");
	}
}
