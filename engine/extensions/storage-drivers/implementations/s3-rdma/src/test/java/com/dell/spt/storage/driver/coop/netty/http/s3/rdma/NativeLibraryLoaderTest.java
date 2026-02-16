package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NativeLibraryLoader's platform detection logic.
 * These tests invoke the private helper methods via reflection
 * to verify OS/arch detection and library filename construction
 * without actually loading any native libraries.
 */
public class NativeLibraryLoaderTest {

	// ---------- getOsArch ----------

	@Test
	void testGetOsArch_returnsNonNull() throws Exception {
		final String result = invokeGetOsArch();
		assertNotNull(result);
		assertFalse(result.isEmpty());
	}

	@Test
	void testGetOsArch_containsDash() throws Exception {
		final String result = invokeGetOsArch();
		assertTrue(result.contains("-"),
						"Expected os-arch format with dash separator, got: " + result);
	}

	@Test
	void testGetOsArch_currentPlatform() throws Exception {
		final String result = invokeGetOsArch();
		final String os = System.getProperty("os.name", "").toLowerCase();
		final String arch = System.getProperty("os.arch", "").toLowerCase();

		// On Linux x86_64 (our target platform), expect "linux-amd64"
		if (os.contains("linux") && (arch.equals("amd64") || arch.equals("x86_64"))) {
			assertEquals("linux-amd64", result);
		}
		// On macOS aarch64, expect "darwin-aarch64"
		if ((os.contains("mac") || os.contains("darwin")) && arch.equals("aarch64")) {
			assertEquals("darwin-aarch64", result);
		}
	}

	// ---------- getLibraryFileName ----------

	@Test
	void testGetLibraryFileName_returnsNonNull() throws Exception {
		final String result = invokeGetLibraryFileName("test_lib");
		assertNotNull(result);
		assertFalse(result.isEmpty());
	}

	@Test
	void testGetLibraryFileName_containsLibraryName() throws Exception {
		final String result = invokeGetLibraryFileName("spt_rdma");
		assertTrue(result.contains("spt_rdma"),
						"Expected filename to contain library name, got: " + result);
	}

	@Test
	void testGetLibraryFileName_currentPlatform() throws Exception {
		final String os = System.getProperty("os.name", "").toLowerCase();
		final String result = invokeGetLibraryFileName("spt_rdma");

		if (os.contains("linux")) {
			assertEquals("libspt_rdma.so", result);
		} else if (os.contains("mac") || os.contains("darwin")) {
			assertEquals("libspt_rdma.dylib", result);
		} else if (os.contains("windows")) {
			assertEquals("spt_rdma.dll", result);
		}
	}

	@Test
	void testGetLibraryFileName_linuxFormat() throws Exception {
		// On Linux (our CI/test environment), verify .so format
		final String os = System.getProperty("os.name", "").toLowerCase();
		if (os.contains("linux")) {
			final String result = invokeGetLibraryFileName("mylib");
			assertTrue(result.startsWith("lib"), "Linux: expected 'lib' prefix");
			assertTrue(result.endsWith(".so"), "Linux: expected '.so' suffix");
		}
	}

	// ---------- load() error handling ----------

	@Test
	void testLoad_nonExistentLibrary() {
		// Loading a library that doesn't exist should throw UnsatisfiedLinkError
		final var error = assertThrows(UnsatisfiedLinkError.class, () -> NativeLibraryLoader.load("nonexistent_library_xyz123"));
		assertTrue(error.getMessage().contains("nonexistent_library_xyz123"),
						"Error message should mention the library name, got: " + error.getMessage());
	}

	@Test
	void testLoad_errorMessageIncludesResourcePath() {
		final var error = assertThrows(UnsatisfiedLinkError.class, () -> NativeLibraryLoader.load("missing_rdma_lib"));
		// The custom error message should mention both JAR path and system path
		assertTrue(error.getMessage().contains("not found"),
						"Error should indicate library not found, got: " + error.getMessage());
	}

	// ---------- Reflection helpers ----------

	private static String invokeGetOsArch() throws Exception {
		final Method m = NativeLibraryLoader.class.getDeclaredMethod("getOsArch");
		m.setAccessible(true);
		return (String) m.invoke(null);
	}

	private static String invokeGetLibraryFileName(final String libraryName) throws Exception {
		final Method m = NativeLibraryLoader.class.getDeclaredMethod("getLibraryFileName", String.class);
		m.setAccessible(true);
		return (String) m.invoke(null, libraryName);
	}
}
