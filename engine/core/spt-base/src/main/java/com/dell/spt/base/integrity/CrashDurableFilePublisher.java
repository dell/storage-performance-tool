package com.dell.spt.base.integrity;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Publishes integrity evidence with file and directory synchronization.
 *
 * <p>The durability guarantee requires a filesystem/provider that honors {@link FileChannel#force}
 * for regular files and directories and supports hard links within one directory. Publication
 * fails closed when any of those operations is unsupported. The final name is created as a hard
 * link so publication never replaces an existing artifact. Callers must not assume this contract
 * on network or userspace filesystems whose server-side persistence semantics do not honor those
 * operations.
 */
public final class CrashDurableFilePublisher {

	interface Operations {

		void syncFile(Path path) throws IOException;

		void createLinkNoReplace(Path source, Path target) throws IOException;

		void delete(Path path) throws IOException;

		void syncDirectory(Path path) throws IOException;
	}

	private static final Operations NIO_OPERATIONS = new Operations() {

		@Override
		public void syncFile(final Path path) throws IOException {
			try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
				channel.force(true);
			} catch (final UnsupportedOperationException e) {
				throw new IOException("file synchronization is not supported for " + path, e);
			}
		}

		@Override
		public void createLinkNoReplace(final Path source, final Path target) throws IOException {
			try {
				Files.createLink(target, source);
			} catch (final FileAlreadyExistsException e) {
				throw e;
			} catch (final UnsupportedOperationException e) {
				throw new IOException(
								"atomic no-replace hard-link publication is not supported for " + target, e);
			}
		}

		@Override
		public void delete(final Path path) throws IOException {
			Files.delete(path);
		}

		@Override
		public void syncDirectory(final Path path) throws IOException {
			try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
				channel.force(true);
			} catch (final UnsupportedOperationException e) {
				throw new IOException("directory synchronization is not supported for " + path, e);
			}
		}
	};

	private CrashDurableFilePublisher() {}

	/**
	 * Synchronizes an existing file and its containing directory.
	 *
	 * @param path the complete file to make durable
	 * @throws IOException if the filesystem cannot provide the required synchronization
	 */
	public static void syncExisting(final Path path) throws IOException {
		final Path normalized = path.toAbsolutePath().normalize();
		final Path parent = requireParent(normalized);
		NIO_OPERATIONS.syncFile(normalized);
		NIO_OPERATIONS.syncDirectory(parent);
	}

	/**
	 * Synchronizes a complete staging file, atomically creates its final name without replacement,
	 * removes the staging name, and synchronizes the containing directory. The source and target
	 * must be in the same directory on a filesystem that supports hard links.
	 *
	 * @param staging the complete staging file
	 * @param target the final published name
	 * @throws IOException if any durability operation fails
	 */
	public static void publish(final Path staging, final Path target) throws IOException {
		publish(staging, target, NIO_OPERATIONS);
	}

	static void publish(final Path staging, final Path target, final Operations operations)
					throws IOException {
		Objects.requireNonNull(operations, "operations");
		final Path normalizedStaging = staging.toAbsolutePath().normalize();
		final Path normalizedTarget = target.toAbsolutePath().normalize();
		final Path stagingParent = requireParent(normalizedStaging);
		final Path targetParent = requireParent(normalizedTarget);
		if (!stagingParent.equals(targetParent)) {
			throw new IOException(
							"crash-durable publication requires source and target in one directory: "
											+ staging + " -> " + target);
		}
		operations.syncFile(normalizedStaging);
		operations.createLinkNoReplace(normalizedStaging, normalizedTarget);
		try {
			operations.delete(normalizedStaging);
		} catch (final IOException e) {
			throw new IOException(
							"published " + normalizedTarget
											+ " but failed to remove its staging name; durable state is indeterminate",
							e);
		}
		try {
			operations.syncDirectory(targetParent);
		} catch (final IOException e) {
			throw new IOException(
							"published " + normalizedTarget
											+ " but failed to synchronize its directory; durable state is indeterminate",
							e);
		}
	}

	private static Path requireParent(final Path path) throws IOException {
		final Path parent = path.getParent();
		if (parent == null) {
			throw new IOException("publication path has no containing directory: " + path);
		}
		return parent;
	}
}
