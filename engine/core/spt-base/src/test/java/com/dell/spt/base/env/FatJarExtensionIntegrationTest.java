package com.dell.spt.base.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for loading extensions as fat JARs with the ParentLastURLClassLoader.
 * These tests require the actual extension JARs to be built.
 */
public class FatJarExtensionIntegrationTest {

	private static final String BUILD_DIR = System.getProperty("user.dir");
	private static final String S3_FAT_JAR_PATH = "extensions/storage-drivers/implementations/s3/build/libs/s3-4.2.23-all.jar";

	@TempDir
	Path tempDir;

	/**
	 * Dynamically finds the S3 fat JAR path. This method checks for the file existence
	 * each time it's called, avoiding static caching issues with @BeforeAll.
	 */
	private static Path findS3FatJar() {
		// The working directory when running tests is the subproject directory (core/spt-base)
		// so we need to go up to the project root
		Path projectRoot = Paths.get(BUILD_DIR).getParent().getParent();

		// Try different possible locations based on where tests might run from
		Path[] possiblePaths = {
				projectRoot.resolve(S3_FAT_JAR_PATH),
				Paths.get(BUILD_DIR).resolve("../../" + S3_FAT_JAR_PATH),
				Paths.get("../../" + S3_FAT_JAR_PATH),
				Paths.get(S3_FAT_JAR_PATH)
		};

		for (Path path : possiblePaths) {
			if (Files.exists(path)) {
				System.out.println("Found S3 fat JAR at: " + path.toAbsolutePath());
				return path.toAbsolutePath();
			}
		}

		System.out.println("S3 fat JAR not found. Tried paths:");
		for (Path path : possiblePaths) {
			System.out.println("  - " + path.toAbsolutePath());
		}
		return null;
	}

	@Test
	void testLoadFatJarExtension() throws Exception {
		Path s3FatJar = findS3FatJar();
		assumeTrue(s3FatJar != null && Files.exists(s3FatJar), "S3 fat JAR not found. Run './gradlew :extensions:storage-drivers:implementations:s3:shadowJar' first");

		// Create extension directory with the fat JAR
		Path extDir = tempDir.resolve("ext");
		Files.createDirectory(extDir);
		Files.copy(s3FatJar, extDir.resolve("s3-all.jar"));

		// Load extensions using the Extension.extClassLoader method
		URLClassLoader extClassLoader = Extension.extClassLoader(extDir.toFile());

		assertNotNull(extClassLoader);
		assertTrue(extClassLoader instanceof ParentLastURLClassLoader,
						"Should use ParentLastURLClassLoader");

		// Load extensions through ServiceLoader
		List<Extension> extensions = Extension.load(extClassLoader);

		// Should find the S3 extension
		assertTrue(extensions.size() > 0, "Should find at least one extension");

		Optional<Extension> s3Extension = extensions.stream()
						.filter(ext -> "s3".equals(ext.id()))
						.findFirst();

		assertTrue(s3Extension.isPresent(), "Should find S3 extension");
	}

	@Test
	void testDependencyIsolation() throws Exception {
		Path s3FatJar = findS3FatJar();
		assumeTrue(s3FatJar != null && Files.exists(s3FatJar), "S3 fat JAR not found");

		// Create extension directory
		Path extDir = tempDir.resolve("ext");
		Files.createDirectory(extDir);
		Files.copy(s3FatJar, extDir.resolve("s3-all.jar"));

		// Create a parent-last classloader for the extension
		URLClassLoader extClassLoader = new ParentLastURLClassLoader(
						new URL[]{extDir.resolve("s3-all.jar").toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		// Load a Netty class from the extension (should come from fat JAR)
		Class<?> nettyClass = extClassLoader.loadClass("io.netty.channel.Channel");
		assertNotNull(nettyClass);

		// The class should be loaded by our extension classloader, not system
		assertEquals(extClassLoader, nettyClass.getClassLoader(),
						"Netty class should be loaded from extension JAR");

		// Load a spt-base class (should come from parent)
		Class<?> sptClass = extClassLoader.loadClass("com.dell.spt.base.item.Item");
		assertNotNull(sptClass);

		// This should come from parent classloader
		assertNotEquals(extClassLoader, sptClass.getClassLoader(),
						"Spt base class should be loaded from parent");
	}

	@Test
	void testMultipleExtensionsWithDifferentVersions() throws Exception {
		// This test simulates loading two extensions with different versions of the same dependency
		// For now, we'll create mock JARs to test the concept

		// Create two mock extensions with different "versions" of a test class
		Path ext1Jar = createMockExtensionJar("ext1", "version1");
		Path ext2Jar = createMockExtensionJar("ext2", "version2");

		// Create separate classloaders for each
		ParentLastURLClassLoader loader1 = new ParentLastURLClassLoader(
						new URL[]{ext1Jar.toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		ParentLastURLClassLoader loader2 = new ParentLastURLClassLoader(
						new URL[]{ext2Jar.toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		// Load the "same" class from both
		Class<?> class1 = loader1.loadClass("com.test.VersionedDependency");
		Class<?> class2 = loader2.loadClass("com.test.VersionedDependency");

		// They should be different classes (different classloaders)
		assertNotSame(class1, class2, "Classes from different extensions should be isolated");

		// Verify versions
		Object instance1 = class1.getDeclaredConstructor().newInstance();
		Object instance2 = class2.getDeclaredConstructor().newInstance();

		String version1 = (String) class1.getMethod("getVersion").invoke(instance1);
		String version2 = (String) class2.getMethod("getVersion").invoke(instance2);

		assertEquals("version1", version1);
		assertEquals("version2", version2);

		// Clean up
		loader1.close();
		loader2.close();
	}

	@Test
	void testServiceLoaderWithFatJars() throws Exception {
		Path s3FatJar = findS3FatJar();
		assumeTrue(s3FatJar != null && Files.exists(s3FatJar), "S3 fat JAR not found");

		// Create extension directory
		Path extDir = tempDir.resolve("ext");
		Files.createDirectory(extDir);
		Files.copy(s3FatJar, extDir.resolve("s3-all.jar"));

		// Load using Extension's standard mechanism
		URLClassLoader extClassLoader = Extension.extClassLoader(extDir.toFile());
		List<Extension> extensions = Extension.load(extClassLoader);

		// Verify ServiceLoader found the extension
		assertFalse(extensions.isEmpty(), "ServiceLoader should find extensions in fat JARs");

		// Verify the extension can provide its configuration
		Optional<Extension> s3Ext = extensions.stream()
						.filter(ext -> "s3".equals(ext.id()))
						.findFirst();

		assertTrue(s3Ext.isPresent());
		Extension s3Extension = s3Ext.get();

		// Test that the extension can load its resources
		assertNotNull(s3Extension.schemaProvider(), "Should be able to get schema provider");
		assertDoesNotThrow(() -> s3Extension.defaults(Paths.get(".")),
						"Should be able to get defaults");
	}

	@Test
	void testExtensionLoadingWithMissingDependencies() throws Exception {
		// Create a thin JAR (without dependencies) to test error handling
		Path thinJar = createThinExtensionJar();

		Path extDir = tempDir.resolve("ext");
		Files.createDirectory(extDir);
		Files.copy(thinJar, extDir.resolve("thin-ext.jar"));

		ParentLastURLClassLoader loader = new ParentLastURLClassLoader(
						new URL[]{extDir.resolve("thin-ext.jar").toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		// Loading the extension class should work
		Class<?> extClass = loader.loadClass("com.test.ThinExtension");
		assertNotNull(extClass);

		// Try to load a class that definitely doesn't exist anywhere
		// This tests that the classloader properly throws ClassNotFoundException
		assertThrows(ClassNotFoundException.class, () -> {
			loader.loadClass("com.definitely.does.not.exist.FakeClass");
		}, "Should not find non-existent class");

		// Also verify we can't load a class that's only in the thin JAR's imports
		// but not actually present (simulating a missing dependency scenario)
		assertThrows(ClassNotFoundException.class, () -> {
			loader.loadClass("com.test.MissingDependency");
		}, "Should not find missing dependency class");

		loader.close();
	}

	// Helper methods

	private Path createMockExtensionJar(String name, String version) throws Exception {
		// Create a temporary JAR with a versioned test class
		// This is implemented similarly to ParentLastURLClassLoaderTest
		Path jarPath = tempDir.resolve(name + ".jar");

		TestJarBuilder builder = new TestJarBuilder(jarPath);
		builder.addClass("com.test.VersionedDependency",
						"package com.test; " +
										"public class VersionedDependency { " +
										"  public String getVersion() { return \"" + version + "\"; } " +
										"}");
		builder.addClass("com.test." + name + ".Extension",
						"package com.test." + name + "; " +
										"public class Extension { " +
										"  public String getName() { return \"" + name + "\"; } " +
										"}");
		builder.build();

		return jarPath;
	}

	private Path createThinExtensionJar() throws Exception {
		Path jarPath = tempDir.resolve("thin-extension.jar");

		TestJarBuilder builder = new TestJarBuilder(jarPath);
		builder.addClass("com.test.ThinExtension",
						"package com.test; " +
										"public class ThinExtension { " +
										"  public String getName() { return \"thin\"; } " +
										"}");
		builder.build();

		return jarPath;
	}

}
