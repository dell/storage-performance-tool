package com.dell.spt.base.env;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The installer leaves a current file alone, replaces any other atomically, and refuses to start on one it cannot install. */
class InstallableJarResourcesTest {

	private static final String DEFAULTS = "config/defaults.yaml";
	private static final String SCENARIO = "example/scenario/js/default.js";
	private static final byte[] BUNDLED = "bundled: content\n".repeat(64).getBytes(StandardCharsets.UTF_8);
	private static final byte[] STALE = "stale: content\n".getBytes(StandardCharsets.UTF_8);

	@TempDir
	Path appHome;

	private final List<Path> madeReadOnly = new ArrayList<>();

	/** Resources with fixed content; a path mapped to null is a resource the jar lacks. */
	private static class FixedResources extends InstallableJarResources {

		final Map<String, byte[]> resources = new HashMap<>();

		FixedResources with(final String path, final byte[] content) {
			resources.put(path, content);
			return this;
		}

		@Override
		protected InputStream resourceStream(final String resPath) {
			final var content = resources.get(resPath);
			return content == null ? null : new ByteArrayInputStream(content);
		}

		@Override
		protected List<String> resourceFilesToInstall() {
			return List.copyOf(resources.keySet());
		}
	}

	@AfterEach
	void restoreWritable() throws IOException {
		for (final var dir : madeReadOnly) {
			Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
		}
	}

	private static void assumePosixAndNotRoot() {
		assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
		// POSIX permissions do not stop root
		assumeFalse("root".equals(System.getProperty("user.name")));
	}

	private void makeReadOnly(final Path dir) throws IOException {
		Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-x------"));
		madeReadOnly.add(dir);
	}

	private Path writeInstalled(final String path, final byte[] content) throws IOException {
		final var file = appHome.resolve(path);
		Files.createDirectories(file.getParent());
		Files.write(file, content);
		return file;
	}

	private static List<String> leftoverTempFiles(final Path dir) throws IOException {
		try (Stream<Path> files = Files.list(dir)) {
			return files.map(f -> f.getFileName().toString()).filter(n -> n.endsWith(".tmp")).toList();
		}
	}

	@Test
	void aResourceTheJarLacksLeavesTheInstalledFileInPlace() throws IOException {
		final var installed = writeInstalled(DEFAULTS, STALE);
		final var resources = new FixedResources().with(DEFAULTS, null);

		assertDoesNotThrow(() -> resources.install(appHome));

		assertArrayEquals(STALE, Files.readAllBytes(installed));
	}

	@Test
	void aMissingFileIsInstalledWithItsDirectories() throws IOException {
		new FixedResources().with(DEFAULTS, BUNDLED).with(SCENARIO, BUNDLED).install(appHome);

		assertArrayEquals(BUNDLED, Files.readAllBytes(appHome.resolve(DEFAULTS)));
		assertArrayEquals(BUNDLED, Files.readAllBytes(appHome.resolve(SCENARIO)));
		assertEquals(List.of(), leftoverTempFiles(appHome.resolve("config")));
	}

	@Test
	void aStaleFileInAWritableHomeIsReplaced() throws IOException {
		final var installed = writeInstalled(DEFAULTS, STALE);

		new FixedResources().with(DEFAULTS, BUNDLED).install(appHome);

		assertArrayEquals(BUNDLED, Files.readAllBytes(installed));
		assertEquals(List.of(), leftoverTempFiles(installed.getParent()));
	}

	@Test
	void aCurrentFileIsNotRewrittenEvenInAReadOnlyHome() throws IOException {
		assumePosixAndNotRoot();
		final var installed = writeInstalled(DEFAULTS, BUNDLED);
		final var mtime = FileTime.fromMillis(1_000_000_000_000L);
		Files.setLastModifiedTime(installed, mtime);
		makeReadOnly(installed.getParent());
		makeReadOnly(appHome);

		assertDoesNotThrow(() -> new FixedResources().with(DEFAULTS, BUNDLED).install(appHome));

		assertEquals(mtime, Files.getLastModifiedTime(installed));
	}

	@Test
	void aStaleFileInAReadOnlyDirectoryStopsTheStart() throws IOException {
		assumePosixAndNotRoot();
		final var installed = writeInstalled(DEFAULTS, STALE);
		makeReadOnly(installed.getParent());

		final var failure = assertThrows(
						ResourceInstallException.class,
						() -> new FixedResources().with(DEFAULTS, BUNDLED).install(appHome));

		final var message = failure.getMessage();
		assertTrue(message.contains("\"" + appHome + "\" (SPT_HOME)"), message);
		assertTrue(message.contains("holds \"" + installed + "\" from another build"), message);
		assertTrue(message.contains("AccessDeniedException"), message);
		assertTrue(message.contains("Make \"" + installed.getParent() + "\" writable by the engine's user, or unset SPT_HOME."), message);
		assertFalse(message.contains("\n"), message);
		assertInstanceOf(AccessDeniedException.class, failure.getCause());
		assertArrayEquals(STALE, Files.readAllBytes(installed));
	}

	@Test
	void anEmptyReadOnlyHomeNamesTheFirstFileItCannotCreate() throws IOException {
		assumePosixAndNotRoot();
		makeReadOnly(appHome);

		final var failure = assertThrows(
						ResourceInstallException.class,
						() -> new FixedResources().with(DEFAULTS, BUNDLED).install(appHome));

		assertTrue(failure.getMessage().contains("lacks \"" + appHome.resolve(DEFAULTS) + "\""), failure.getMessage());
		assertTrue(failure.getMessage().contains("AccessDeniedException"), failure.getMessage());
	}

	@Test
	void aFailedReplaceStopsTheStartAndRemovesItsTemporaryFile() throws IOException {
		// The same refusal without POSIX permissions, which do not stop root
		final var installed = writeInstalled(DEFAULTS, STALE);
		final var resources = new FixedResources() {

			@Override
			void moveAtomically(final Path tmpPath, final Path dstPath) throws IOException {
				throw new AccessDeniedException(dstPath.toString());
			}
		}.with(DEFAULTS, BUNDLED);

		final var failure = assertThrows(ResourceInstallException.class, () -> resources.install(appHome));

		assertTrue(failure.getMessage().contains("(AccessDeniedException: " + installed + ")"), failure.getMessage());
		assertArrayEquals(STALE, Files.readAllBytes(installed));
		assertEquals(List.of(), leftoverTempFiles(installed.getParent()));
	}

	@Test
	void anExceptionDoesNotFailAnInstallThatLeftTheBundledContent() throws IOException {
		final var installed = writeInstalled(DEFAULTS, STALE);
		final var resources = new FixedResources() {

			@Override
			void moveAtomically(final Path tmpPath, final Path dstPath) throws IOException {
				super.moveAtomically(tmpPath, dstPath);
				throw new IOException("reported after the move");
			}
		}.with(DEFAULTS, BUNDLED);

		assertDoesNotThrow(() -> resources.install(appHome));
		assertArrayEquals(BUNDLED, Files.readAllBytes(installed));
	}

	/**
	 * A file system without atomic moves, where a concurrent engine's non-atomic replace is seen half-written
	 * at the first check and complete after the given number of pauses.
	 */
	private static final class NonAtomicResources extends FixedResources {

		private final Path installed;
		private final int completeAfterPauses;
		int pauses;

		NonAtomicResources(final Path installed, final int completeAfterPauses) {
			this.installed = installed;
			this.completeAfterPauses = completeAfterPauses;
			with(DEFAULTS, BUNDLED);
		}

		@Override
		void moveAtomically(final Path tmpPath, final Path dstPath) throws IOException {
			throw new AtomicMoveNotSupportedException(tmpPath.toString(), dstPath.toString(), "test");
		}

		@Override
		void replaceNonAtomically(final Path tmpPath, final Path dstPath) throws IOException {
			Files.write(dstPath, Arrays.copyOf(BUNDLED, BUNDLED.length / 2));
		}

		@Override
		void pauseBeforeRecheck() {
			pauses++;
			if (pauses == completeAfterPauses) {
				try {
					Files.write(installed, BUNDLED);
				} catch (final IOException e) {
					throw new UncheckedIOException(e);
				}
			}
		}
	}

	@Test
	void aNonAtomicReplaceCompletedByALaterCheckDoesNotFail() throws IOException {
		final var installed = writeInstalled(DEFAULTS, STALE);
		final var resources = new NonAtomicResources(installed, 2);

		assertDoesNotThrow(() -> resources.install(appHome));

		assertEquals(2, resources.pauses);
		assertArrayEquals(BUNDLED, Files.readAllBytes(installed));
		assertEquals(List.of(), leftoverTempFiles(installed.getParent()));
	}

	@Test
	void aNonAtomicReplaceThatNeverCompletesFailsAfterTheRechecks() throws IOException {
		final var installed = writeInstalled(DEFAULTS, STALE);
		final var resources = new NonAtomicResources(installed, Integer.MAX_VALUE);

		final var failure = assertThrows(ResourceInstallException.class, () -> resources.install(appHome));

		assertEquals(InstallableJarResources.NON_ATOMIC_RECHECKS, resources.pauses);
		assertTrue(failure.getMessage().contains("holds \"" + installed + "\""), failure.getMessage());
	}

	@Test
	void installsRacingOnOneHomeAllSucceedWithTheBundledContent() throws Exception {
		for (final var stale : List.of(false, true)) {
			final var home = Files.createTempDirectory(appHome, stale ? "stale" : "fresh");
			if (stale) {
				final var file = home.resolve(DEFAULTS);
				Files.createDirectories(file.getParent());
				Files.write(file, STALE);
			}
			final var installers = 8;
			final var barrier = new CyclicBarrier(installers);
			final ExecutorService executor = Executors.newFixedThreadPool(installers);
			try {
				final List<Future<Void>> results = new ArrayList<>();
				for (var i = 0; i < installers; i++) {
					final Callable<Void> install = () -> {
						barrier.await(10, TimeUnit.SECONDS);
						new FixedResources().with(DEFAULTS, BUNDLED).with(SCENARIO, BUNDLED).install(home);
						return null;
					};
					results.add(executor.submit(install));
				}
				for (final var result : results) {
					result.get(30, TimeUnit.SECONDS);
				}
			} finally {
				executor.shutdownNow();
			}
			assertArrayEquals(BUNDLED, Files.readAllBytes(home.resolve(DEFAULTS)));
			assertArrayEquals(BUNDLED, Files.readAllBytes(home.resolve(SCENARIO)));
			assertEquals(List.of(), leftoverTempFiles(home.resolve("config")));
		}
	}
}
