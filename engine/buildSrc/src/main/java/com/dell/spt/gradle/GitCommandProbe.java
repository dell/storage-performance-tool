package com.dell.spt.gradle;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Best-effort local Git discovery for development builds. */
public final class GitCommandProbe implements EngineBuildMetadata.GitProbe {

	private final File repositoryRoot;

	public GitCommandProbe(final File repositoryRoot) {
		this.repositoryRoot = repositoryRoot;
	}

	@Override
	public String revision() {
		final var result = execute("rev-parse", "HEAD");
		return result == null || result.isBlank() ? null : result.trim();
	}

	@Override
	public Boolean dirty() {
		final var result = execute("status", "--porcelain");
		return result == null ? null : !result.isBlank();
	}

	private String execute(final String... arguments) {
		final var command = new String[arguments.length + 3];
		command[0] = "git";
		command[1] = "-C";
		command[2] = repositoryRoot.getAbsolutePath();
		System.arraycopy(arguments, 0, command, 3, arguments.length);
		try {
			final var process = new ProcessBuilder(command).redirectErrorStream(true).start();
			final var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			return process.waitFor() == 0 ? output : null;
		} catch (final IOException e) {
			return null;
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		}
	}
}
