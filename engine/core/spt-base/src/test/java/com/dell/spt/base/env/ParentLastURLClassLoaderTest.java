package com.dell.spt.base.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.jar.*;
import javax.tools.*;

import static org.junit.jupiter.api.Assertions.*;

public class ParentLastURLClassLoaderTest {

	@TempDir
	Path tempDir;

	private Path parentJar;
	private Path childJar;
	private URLClassLoader parentClassLoader;
	private ParentLastURLClassLoader childClassLoader;

	@BeforeEach
	void setUp() throws Exception {
		// Create test JARs with different versions of the same class
		parentJar = createParentJar();
		childJar = createChildJar();

		// Set up classloaders
		parentClassLoader = new URLClassLoader(
						new URL[]{parentJar.toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		childClassLoader = new ParentLastURLClassLoader(
						new URL[]{childJar.toUri().toURL()
						},
						parentClassLoader);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (childClassLoader != null) {
			childClassLoader.close();
		}
		if (parentClassLoader != null) {
			parentClassLoader.close();
		}
	}

	@Test
	void testChildClassLoadedFirst() throws Exception {
		// Load a class that exists in both parent and child
		Class<?> clazz = childClassLoader.loadClass("com.test.VersionedClass");

		// Verify we got the child version
		Object instance = clazz.getDeclaredConstructor().newInstance();
		String version = (String) clazz.getMethod("getVersion").invoke(instance);
		assertEquals("child-version", version, "Should load child version of class");
	}

	@Test
	void testSptBasePackageDelegatedToParent() throws Exception {
		// Create a mock spt base class in parent
		Path sptJar = createJarWithClass(
						"com.dell.spt.base.TestApi",
						"package com.dell.spt.base; public class TestApi { public String getVersion() { return \"parent-api\"; } }");

		URLClassLoader parentWithSpt = new URLClassLoader(
						new URL[]{sptJar.toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		ParentLastURLClassLoader testLoader = new ParentLastURLClassLoader(
						new URL[]{childJar.toUri().toURL()
						},
						parentWithSpt);

		try {
			Class<?> clazz = testLoader.loadClass("com.dell.spt.base.TestApi");
			Object instance = clazz.getDeclaredConstructor().newInstance();
			String version = (String) clazz.getMethod("getVersion").invoke(instance);
			assertEquals("parent-api", version, "Spt base classes should always come from parent");
		} finally {
			testLoader.close();
			parentWithSpt.close();
		}
	}

	@Test
	void testSystemClassesDelegatedToParent() throws Exception {
		// System classes should always come from parent
		Class<?> stringClass = childClassLoader.loadClass("java.lang.String");
		assertSame(String.class, stringClass, "System classes should come from system classloader");

		Class<?> listClass = childClassLoader.loadClass("java.util.List");
		assertSame(List.class, listClass, "System classes should come from system classloader");
	}

	@Test
	void testFallbackToParentWhenNotInChild() throws Exception {
		// Create a class that only exists in parent
		Path parentOnlyJar = createJarWithClass(
						"com.test.ParentOnlyClass",
						"package com.test; public class ParentOnlyClass { public String identify() { return \"parent-only\"; } }");

		URLClassLoader parentWithExtra = new URLClassLoader(
						new URL[]{parentOnlyJar.toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		ParentLastURLClassLoader testLoader = new ParentLastURLClassLoader(
						new URL[]{childJar.toUri().toURL()
						},
						parentWithExtra);

		try {
			Class<?> clazz = testLoader.loadClass("com.test.ParentOnlyClass");
			assertNotNull(clazz, "Should find class in parent when not in child");
			Object instance = clazz.getDeclaredConstructor().newInstance();
			String result = (String) clazz.getMethod("identify").invoke(instance);
			assertEquals("parent-only", result);
		} finally {
			testLoader.close();
			parentWithExtra.close();
		}
	}

	@Test
	void testClassNotFoundThrowsException() {
		assertThrows(ClassNotFoundException.class, () -> {
			childClassLoader.loadClass("com.test.NonExistentClass");
		}, "Should throw ClassNotFoundException for non-existent class");
	}

	@Test
	void testResourceLoadingChildFirst() throws Exception {
		// Both JARs contain test.properties with different content
		URL resource = childClassLoader.getResource("test.properties");
		assertNotNull(resource, "Should find resource");

		try (InputStream is = resource.openStream();
						Scanner scanner = new Scanner(is, StandardCharsets.UTF_8)) {
			String content = scanner.nextLine();
			assertEquals("source=child", content, "Should load resource from child first");
		}
	}

	@Test
	void testGetResourcesReturnsBoth() throws Exception {
		// Both JARs contain test.properties
		Enumeration<URL> resources = childClassLoader.getResources("test.properties");

		List<String> sources = new ArrayList<>();
		while (resources.hasMoreElements()) {
			URL url = resources.nextElement();
			try (InputStream is = url.openStream();
							Scanner scanner = new Scanner(is, StandardCharsets.UTF_8)) {
				sources.add(scanner.nextLine());
			}
		}

		assertEquals(2, sources.size(), "Should find resources in both child and parent");
		assertTrue(sources.contains("source=child"), "Should include child resource");
		assertTrue(sources.contains("source=parent"), "Should include parent resource");
		// Child should come first
		assertEquals("source=child", sources.get(0), "Child resource should come first");
	}

	@Test
	void testResourceAsStream() throws Exception {
		InputStream stream = childClassLoader.getResourceAsStream("test.properties");
		assertNotNull(stream, "Should get resource as stream");

		try (Scanner scanner = new Scanner(stream, StandardCharsets.UTF_8)) {
			String content = scanner.nextLine();
			assertEquals("source=child", content, "Should load child resource");
		}
	}

	@Test
	void testServiceLoaderCompatibility() throws Exception {
		// Create JARs with META-INF/services
		Path serviceJar = createJarWithService();

		ParentLastURLClassLoader serviceLoader = new ParentLastURLClassLoader(
						new URL[]{serviceJar.toUri().toURL()
						},
						ClassLoader.getSystemClassLoader());

		try {
			// Load the service interface
			Class<?> serviceInterface = serviceLoader.loadClass("com.test.TestService");

			// Use ServiceLoader to find implementations
			ServiceLoader<?> loader = ServiceLoader.load(serviceInterface, serviceLoader);

			List<Object> implementations = new ArrayList<>();
			for (Object impl : loader) {
				implementations.add(impl);
			}

			assertEquals(1, implementations.size(), "Should find service implementation");

			// Verify it's the right implementation
			Object impl = implementations.get(0);
			String name = (String) impl.getClass().getMethod("getName").invoke(impl);
			assertEquals("TestServiceImpl", name, "Should load correct service implementation");
		} finally {
			serviceLoader.close();
		}
	}

	// Helper methods to create test JARs

	private Path createParentJar() throws Exception {
		return createJarWithContents(tempDir.resolve("parent.jar"), Map.of(
						"com/test/VersionedClass.class", compileClass(
										"com.test.VersionedClass",
										"package com.test; public class VersionedClass { public String getVersion() { return \"parent-version\"; } }"),
						"test.properties", "source=parent".getBytes(StandardCharsets.UTF_8)));
	}

	private Path createChildJar() throws Exception {
		return createJarWithContents(tempDir.resolve("child.jar"), Map.of(
						"com/test/VersionedClass.class", compileClass(
										"com.test.VersionedClass",
										"package com.test; public class VersionedClass { public String getVersion() { return \"child-version\"; } }"),
						"test.properties", "source=child".getBytes(StandardCharsets.UTF_8)));
	}

	private Path createJarWithClass(String className, String sourceCode) throws Exception {
		Path jarPath = tempDir.resolve(className.replace('.', '_') + ".jar");
		String classPath = className.replace('.', '/') + ".class";

		return createJarWithContents(jarPath, Map.of(
						classPath, compileClass(className, sourceCode)));
	}

	private Path createJarWithService() throws Exception {
		// First compile both classes together to ensure proper dependencies
		String interfaceCode = "package com.test;\npublic interface TestService { String getName(); }";
		String implCode = "package com.test;\npublic class TestServiceImpl implements TestService { public String getName() { return \"TestServiceImpl\"; } }";

		// Create source files
		Path srcDir = tempDir.resolve("src");
		Files.createDirectories(srcDir.resolve("com/test"));
		Files.writeString(srcDir.resolve("com/test/TestService.java"), interfaceCode);
		Files.writeString(srcDir.resolve("com/test/TestServiceImpl.java"), implCode);

		// Compile both together
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null)) {
			Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromPaths(
							Arrays.asList(srcDir.resolve("com/test/TestService.java"), srcDir.resolve("com/test/TestServiceImpl.java")));

			List<String> options = Arrays.asList("-d", tempDir.toString());
			JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, options, null, compilationUnits);

			if (!task.call()) {
				throw new RuntimeException("Compilation failed");
			}
		}

		// Read compiled classes
		byte[] interfaceClass = Files.readAllBytes(tempDir.resolve("com/test/TestService.class"));
		byte[] implClass = Files.readAllBytes(tempDir.resolve("com/test/TestServiceImpl.class"));

		return createJarWithContents(tempDir.resolve("service.jar"), Map.of(
						"com/test/TestService.class", interfaceClass,
						"com/test/TestServiceImpl.class", implClass,
						"META-INF/services/com.test.TestService", "com.test.TestServiceImpl".getBytes(StandardCharsets.UTF_8)));
	}

	private Path createJarWithContents(Path jarPath, Map<String, byte[]> contents) throws Exception {
		try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jarPath))) {
			for (Map.Entry<String, byte[]> entry : contents.entrySet()) {
				jos.putNextEntry(new JarEntry(entry.getKey()));
				jos.write(entry.getValue());
				jos.closeEntry();
			}
		}
		return jarPath;
	}

	private byte[] compileClass(String className, String sourceCode) throws Exception {
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

		try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null)) {
			// Create source file
			JavaFileObject sourceFile = new javax.tools.SimpleJavaFileObject(
							java.net.URI.create("string:///" + className.replace('.', '/') + ".java"),
							JavaFileObject.Kind.SOURCE) {
				@Override
				public CharSequence getCharContent(boolean ignoreEncodingErrors) {
					return sourceCode;
				}
			};

			// Set up compilation
			StringWriter errors = new StringWriter();
			List<String> options = Arrays.asList("-d", tempDir.toString());
			JavaCompiler.CompilationTask task = compiler.getTask(
							errors,
							fileManager,
							null,
							options,
							null,
							Arrays.asList(sourceFile));

			// Compile
			if (!task.call()) {
				throw new RuntimeException("Compilation failed: " + errors.toString());
			}

			// Read compiled class
			Path classFile = tempDir.resolve(className.replace('.', '/') + ".class");
			return Files.readAllBytes(classFile);
		}
	}

}
