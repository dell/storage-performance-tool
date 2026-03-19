package com.dell.spt.base.env;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.jar.JarFile;
import java.util.logging.Logger;

/**
 * Extension is an @see {@link Installable} with the configuration and the
 * configuration schema.
 */
public interface Extension extends Installable {

	Logger LOG = Logger.getLogger(Extension.class.getSimpleName());

	static List<Extension> load(final ClassLoader extClsLoader) {
		final List<Extension> extensions = new ArrayList<>();
		for (final Extension extension : ServiceLoader.load(Extension.class, extClsLoader)) {
			extensions.add(extension);
		}
		return extensions;
	}

	static URLClassLoader extClassLoader(final File dirExt) {
		final URLClassLoader extClsLoader;
		if (!dirExt.exists() || !dirExt.isDirectory()) {
			LOG.warning("No \"" + dirExt.getAbsolutePath() + "\" directory, loaded no extensions");
			extClsLoader = new ParentLastURLClassLoader(new URL[]{}, Extension.class.getClassLoader());
		} else {
			final File[] extFiles = dirExt.listFiles();
			if (extFiles == null) {
				LOG.warning(
								"Failed to load the contents of the \""
												+ dirExt.getAbsolutePath()
												+ "\" directory, loaded no extensions");
				extClsLoader = new ParentLastURLClassLoader(new URL[]{}, Extension.class.getClassLoader());
			} else {
				final URL[] extFileUrls = Arrays.stream(extFiles)
								.filter(Extension::isJarFile)
								.map(Extension::fileToUrl)
								.filter(Objects::nonNull)
								.toArray(URL[]::new);
				extClsLoader = new ParentLastURLClassLoader(extFileUrls, Extension.class.getClassLoader());
			}
		}
		return extClsLoader;
	}

	/**
	 * Resolve the extension directory by looking for {@code ext/} next to the JAR file.
	 * Returns {@code null} if the JAR location cannot be determined or no {@code ext/}
	 * directory exists there (e.g. running from an IDE).
	 */
	static File resolveExtDir() {
		try {
			final var codeSource = Extension.class.getProtectionDomain().getCodeSource();
			if (codeSource != null) {
				final var location = codeSource.getLocation();
				final var jarPath = Path.of(location.toURI());
				final var extDir = jarPath.getParent().resolve("ext").toFile();
				if (extDir.isDirectory()) {
					return extDir;
				}
			}
		} catch (final URISyntaxException | SecurityException e) {
			LOG.warning("Failed to resolve ext directory from JAR location: " + e);
		}
		return null;
	}

	static boolean isJarFile(final File f) {
		try {
			new JarFile(f);
		} catch (final Exception e) {
			LOG.warning("Failed to load the file \"" + f + "\", expected a valid JAR/ZIP file");
			return false;
		}
		return true;
	}

	static URL fileToUrl(final File f) {
		try {
			return f.toURI().toURL();
		} catch (final MalformedURLException e) {
			LOG.severe(e.toString());
		}
		return null;
	}

	String id();

	Config defaults(final Path appHomePath);

	SchemaProvider schemaProvider();
}
