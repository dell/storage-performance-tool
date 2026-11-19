package com.dell.spt.base.env;

import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Adler32;

/**
 * Installs bundled resources into the app home. A file whose installed copy matches the bundled content is
 * left alone; any other is replaced atomically, through a temporary file in the same directory, so engines
 * sharing a home do not read a half-written file where the file system moves files atomically. A file that still differs or is missing after the attempt
 * stops the start with a {@link ResourceInstallException}.
 */
public abstract class InstallableJarResources implements Installable {

	/** Post-install checks after a non-atomic replace, when a concurrent engine may still be writing the file. */
	static final int NON_ATOMIC_RECHECKS = 5;

	private static final long NON_ATOMIC_RECHECK_DELAY_MILLIS = 100;

	/** The environment variable that sets the app home; see {@link CoreResourcesToInstall}. */
	private static final String ENV_SPT_HOME = "SPT_HOME";

	/** The checksum of a destination that is missing or cannot be read; an Adler32 value is never negative. */
	private static final long NO_CHECKSUM = -1;

	@Override
	public void install(final Path dstPath) {
		try {
			Files.createDirectories(dstPath);
		} catch (final IOException e) {
			// The first file that cannot be installed reports it, with the cause.
			Loggers.MSG.debug("Failed to create the directory \"{}\": {}", dstPath, e.toString());
		}
		resourceFilesToInstall().forEach(resFile -> installResourcesFile(dstPath, resFile));
		Loggers.MSG.debug("Installer finished: \"{}\"", getClass().getCanonicalName());
	}

	private void installResourcesFile(final Path appHomePath, final String srcFilePath) {

		final var dstPath = Paths.get(appHomePath.toString(), srcFilePath);

		final byte[] content;
		try (final var in = resourceStream(srcFilePath)) {
			if (in == null) {
				Loggers.ERR.warn("Resource file \"{}\" not found, skipping", srcFilePath);
				return;
			}
			content = in.readAllBytes();
		} catch (final IOException e) {
			throw new ResourceInstallException(
							"Failed to read the bundled resource \"" + srcFilePath + "\" (" + describe(e) + ")", e);
		}
		final var srcChecksum = checksum(content);

		final var existed = Files.exists(dstPath);
		final var dstChecksum = installedChecksum(dstPath);
		if (srcChecksum == dstChecksum) {
			Loggers.MSG.debug(
							"The destination file \"{}\" has the same checksum ({}) as source, skipping",
							dstPath,
							Long.toHexString(srcChecksum));
			return;
		}

		IOException failure = null;
		var atomic = true;
		Path tmpPath = null;
		try {
			Files.createDirectories(dstPath.getParent());
			tmpPath = dstPath.resolveSibling(
							"." + dstPath.getFileName() + "." + ProcessHandle.current().pid() + "."
											+ Long.toHexString(ThreadLocalRandom.current().nextLong()) + ".tmp");
			// Created with the default permissions, as the installed file was before.
			Files.write(tmpPath, content, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
			try {
				moveAtomically(tmpPath, dstPath);
			} catch (final AtomicMoveNotSupportedException e) {
				atomic = false;
				Loggers.MSG.debug("Atomic move unsupported for \"{}\", replacing it in place", dstPath);
				replaceNonAtomically(tmpPath, dstPath);
			}
		} catch (final IOException e) {
			failure = e;
		} finally {
			deleteQuietly(tmpPath);
		}

		var installedChecksum = installedChecksum(dstPath);
		for (var i = 0; !atomic && installedChecksum != srcChecksum && i < NON_ATOMIC_RECHECKS; i++) {
			pauseBeforeRecheck();
			installedChecksum = installedChecksum(dstPath);
		}
		if (installedChecksum != srcChecksum) {
			throw new ResourceInstallException(refusal(appHomePath, dstPath, existed, failure), failure);
		}
		if (existed) {
			Loggers.MSG.warn(
							"The destination file \"{}\" had a different checksum ({}) than source ({}), replaced",
							dstPath,
							Long.toHexString(dstChecksum),
							Long.toHexString(srcChecksum));
		} else {
			Loggers.MSG.debug("The file {} installed ({})", dstPath, content.length);
		}
	}

	private String refusal(
					final Path appHomePath, final Path dstPath, final boolean existed, final IOException failure) {
		final var cause = failure == null
						? "it differs after the install, so another engine may have replaced it"
						: describe(failure);
		final var state = existed
						? "holds \"" + dstPath + "\" from another build, and the engine cannot replace it"
						: "lacks \"" + dstPath + "\", and the engine cannot create it";
		final var dir = nearestDirectory(dstPath.getParent());
		final var advice = isPermissionFailure(failure)
						? "Make \"" + dir + "\" writable by the engine's user"
						: "Check \"" + dstPath + "\" and the directory \"" + dir + "\"";
		final var homeSet = homeSetByEnvironment();
		return "The app home \"" + appHomePath + "\"" + (homeSet ? " (SPT_HOME) " : " ") + state + " (" + cause
						+ "). " + advice + (homeSet ? ", or unset SPT_HOME." : ".");
	}

	/** Whether the failure is one that write permission on the directory would remove. */
	private static boolean isPermissionFailure(final IOException failure) {
		if (failure instanceof AccessDeniedException) {
			return true;
		}
		return failure instanceof FileSystemException fsFailure
						&& fsFailure.getReason() != null
						&& fsFailure.getReason().contains("Read-only file system");
	}

	/** Whether {@code SPT_HOME} chose the app home, so that unsetting it is advice the user can take. A test seam. */
	boolean homeSetByEnvironment() {
		return System.getenv(ENV_SPT_HOME) != null;
	}

	/** The directory, or its nearest existing ancestor, that must be writable to install a file into it. */
	private static Path nearestDirectory(final Path dir) {
		var existing = dir;
		while (existing.getParent() != null && !Files.isDirectory(existing)) {
			existing = existing.getParent();
		}
		return existing;
	}

	private static String describe(final Exception e) {
		return e.getClass().getSimpleName() + ": " + e.getMessage();
	}

	private static long checksum(final byte[] content) {
		final var checksumCalc = new Adler32();
		checksumCalc.update(content);
		return checksumCalc.getValue();
	}

	private static long installedChecksum(final Path dstPath) {
		if (!Files.isRegularFile(dstPath)) {
			return NO_CHECKSUM;
		}
		final var checksumCalc = new Adler32();
		final var buff = new byte[0x2000];
		try (final var in = Files.newInputStream(dstPath, StandardOpenOption.READ)) {
			int n;
			while (-1 < (n = in.read(buff))) {
				checksumCalc.update(buff, 0, n);
			}
		} catch (final IOException e) {
			Loggers.MSG.debug("Failed to read the destination file \"{}\": {}", dstPath, e.toString());
			return NO_CHECKSUM;
		}
		return checksumCalc.getValue();
	}

	private static void deleteQuietly(final Path path) {
		if (path != null) {
			try {
				Files.deleteIfExists(path);
			} catch (final IOException e) {
				Loggers.MSG.debug("Failed to remove the temporary file \"{}\": {}", path, e.toString());
			}
		}
	}

	/** Moves the complete temporary file over the destination in one step. A test seam. */
	void moveAtomically(final Path tmpPath, final Path dstPath) throws IOException {
		Files.move(tmpPath, dstPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
	}

	/** Replaces the destination where an atomic move is unsupported. A test seam. */
	void replaceNonAtomically(final Path tmpPath, final Path dstPath) throws IOException {
		Files.move(tmpPath, dstPath, StandardCopyOption.REPLACE_EXISTING);
	}

	/** Waits before a post-install check that follows a non-atomic replace. A test seam. */
	void pauseBeforeRecheck() {
		try {
			Thread.sleep(NON_ATOMIC_RECHECK_DELAY_MILLIS);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	protected InputStream resourceStream(final String resPath) {
		return getClass().getResourceAsStream("/" + resPath);
	}

	protected abstract List<String> resourceFilesToInstall();
}
