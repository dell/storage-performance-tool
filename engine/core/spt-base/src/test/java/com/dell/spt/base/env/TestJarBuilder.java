package com.dell.spt.base.env;

import javax.tools.*;
import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.jar.*;

/**
 * Helper class to build test JAR files programmatically for integration testing.
 */
class TestJarBuilder {
	private final Path jarPath;
	private final Path tempDir;
	private final Map<String, String> classes = new HashMap<>();
	private final Map<String, byte[]> resources = new HashMap<>();

	TestJarBuilder(Path jarPath) throws IOException {
		this.jarPath = jarPath;
		this.tempDir = Files.createTempDirectory("test-jar-builder");
	}

	TestJarBuilder addClass(String className, String sourceCode) {
		classes.put(className, sourceCode);
		return this;
	}

	TestJarBuilder addResource(String path, String content) {
		resources.put(path, content.getBytes());
		return this;
	}

	TestJarBuilder addResource(String path, byte[] content) {
		resources.put(path, content);
		return this;
	}

	TestJarBuilder addServiceProvider(String serviceInterface, String... implementations) {
		String content = String.join("\n", implementations);
		return addResource("META-INF/services/" + serviceInterface, content);
	}

	void build() throws IOException {
		// Compile all classes
		Map<String, byte[]> compiledClasses = compileClasses();

		// Create JAR
		try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jarPath))) {
			// Add compiled classes
			for (Map.Entry<String, byte[]> entry : compiledClasses.entrySet()) {
				String classPath = entry.getKey().replace('.', '/') + ".class";
				jos.putNextEntry(new JarEntry(classPath));
				jos.write(entry.getValue());
				jos.closeEntry();
			}

			// Add resources
			for (Map.Entry<String, byte[]> entry : resources.entrySet()) {
				jos.putNextEntry(new JarEntry(entry.getKey()));
				jos.write(entry.getValue());
				jos.closeEntry();
			}
		}

		// Clean up temp directory
		cleanupTempDir();
	}

	private Map<String, byte[]> compileClasses() throws IOException {
		if (classes.isEmpty()) {
			return Collections.emptyMap();
		}

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		if (compiler == null) {
			throw new RuntimeException("No Java compiler available. Make sure you're running with a JDK, not just a JRE.");
		}

		Map<String, byte[]> compiledClasses = new HashMap<>();

		try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null)) {
			// Create source files
			List<JavaFileObject> sourceFiles = new ArrayList<>();
			for (Map.Entry<String, String> entry : classes.entrySet()) {
				String className = entry.getKey();
				String sourceCode = entry.getValue();
				sourceFiles.add(new SimpleJavaSourceFile(className, sourceCode));
			}

			// Set up compilation
			List<String> options = Arrays.asList("-d", tempDir.toString());
			JavaCompiler.CompilationTask task = compiler.getTask(
							null, fileManager, null, options, null, sourceFiles);

			// Compile
			if (!task.call()) {
				throw new RuntimeException("Compilation failed");
			}

			// Read compiled classes
			for (String className : classes.keySet()) {
				Path classFile = tempDir.resolve(className.replace('.', '/') + ".class");
				compiledClasses.put(className, Files.readAllBytes(classFile));
			}
		}

		return compiledClasses;
	}

	private void cleanupTempDir() throws IOException {
		Files.walk(tempDir)
						.sorted(Comparator.reverseOrder())
						.map(Path::toFile)
						.forEach(File::delete);
	}

	/**
	 * Simple JavaFileObject implementation for in-memory source files.
	 */
	private static class SimpleJavaSourceFile extends SimpleJavaFileObject {
		private final String content;

		SimpleJavaSourceFile(String className, String content) {
			super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension),
							Kind.SOURCE);
			this.content = content;
		}

		@Override
		public CharSequence getCharContent(boolean ignoreEncodingErrors) {
			return content;
		}
	}
}
