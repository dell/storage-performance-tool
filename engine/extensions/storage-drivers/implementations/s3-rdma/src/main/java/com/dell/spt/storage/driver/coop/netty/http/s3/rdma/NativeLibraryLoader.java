package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/**
 * Utility class to load native libraries bundled in the JAR.
 *
 * The native library is expected to be packaged at:
 * {@code /native/{os}-{arch}/lib{name}.so} (Linux)
 * {@code /native/{os}-{arch}/{name}.dll} (Windows)
 * {@code /native/{os}-{arch}/lib{name}.dylib} (macOS)
 *
 * The library is extracted to a temporary directory and loaded from there.
 */
public final class NativeLibraryLoader {

	private static final String NATIVE_RESOURCE_PREFIX = "/native/";

	private NativeLibraryLoader() {}

	/**
	 * Load a native library by name.
	 *
	 * @param libraryName the library name without prefix/suffix (e.g., "spt_rdma_native")
	 * @throws UnsatisfiedLinkError if the library cannot be found or loaded
	 */
	public static void load(final String libraryName) {
		final String osArch = getOsArch();
		final String libFileName = getLibraryFileName(libraryName);
		final String resourcePath = NATIVE_RESOURCE_PREFIX + osArch + "/" + libFileName;

		Loggers.MSG.debug("Loading native library: {} from {}", libraryName, resourcePath);

		// Try to load from JAR resources first
		try (final InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
			if (is != null) {
				loadFromStream(is, libraryName, libFileName);
				return;
			}
		} catch (final IOException e) {
			Loggers.MSG.debug("Failed to load native library from JAR: {}", e.getMessage());
		}

		// Fall back to system library path
		Loggers.MSG.debug("Native library not found in JAR, trying system path: {}", libraryName);
		try {
			System.loadLibrary(libraryName);
			Loggers.MSG.info("Loaded native library from system path: {}", libraryName);
		} catch (final UnsatisfiedLinkError e) {
			// Try loading from rdma-object-client dist directory as last resort
			final String distPath = System.getProperty("user.home") +
							"/repos/rdma-object-client/dist/lib/" + libFileName;
			final File distFile = new File(distPath);
			if (distFile.exists()) {
				Loggers.MSG.debug("Trying to load from rdma-object-client dist: {}", distPath);
				System.load(distFile.getAbsolutePath());
				Loggers.MSG.info("Loaded native library from dist: {}", distPath);
			} else {
				throw new UnsatisfiedLinkError(
								"Native library '" + libraryName + "' not found in JAR (" +
												resourcePath + "), system path, or dist directory");
			}
		}
	}

	/**
	 * Load a native library from an input stream by extracting to a temp file.
	 */
	private static void loadFromStream(final InputStream is, final String libraryName,
					final String libFileName) throws IOException {
		// Create temp directory for native libs
		final Path tempDir = Files.createTempDirectory("spt-native-");
		tempDir.toFile().deleteOnExit();

		final Path tempLib = tempDir.resolve(libFileName);
		tempLib.toFile().deleteOnExit();

		// Extract library to temp file
		Files.copy(is, tempLib, StandardCopyOption.REPLACE_EXISTING);

		// Load the library
		System.load(tempLib.toAbsolutePath().toString());
		Loggers.MSG.info("Loaded native library from JAR: {}", libraryName);
	}

	/**
	 * Get the OS-architecture string (e.g., "linux-amd64", "linux-aarch64").
	 */
	private static String getOsArch() {
		final String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
		final String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

		final String osName;
		if (os.contains("linux")) {
			osName = "linux";
		} else if (os.contains("mac") || os.contains("darwin")) {
			osName = "darwin";
		} else if (os.contains("windows")) {
			osName = "windows";
		} else {
			osName = os.replaceAll("\\s+", "");
		}

		final String archName;
		if (arch.equals("amd64") || arch.equals("x86_64")) {
			archName = "amd64";
		} else if (arch.equals("aarch64") || arch.equals("arm64")) {
			archName = "aarch64";
		} else {
			archName = arch;
		}

		return osName + "-" + archName;
	}

	/**
	 * Get the platform-specific library filename.
	 */
	private static String getLibraryFileName(final String libraryName) {
		final String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);

		if (os.contains("windows")) {
			return libraryName + ".dll";
		} else if (os.contains("mac") || os.contains("darwin")) {
			return "lib" + libraryName + ".dylib";
		} else {
			// Linux and others
			return "lib" + libraryName + ".so";
		}
	}
}
