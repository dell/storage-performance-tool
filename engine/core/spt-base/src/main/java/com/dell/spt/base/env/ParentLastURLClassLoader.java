package com.dell.spt.base.env;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * A URLClassLoader that loads classes and resources from its own URLs first,
 * before delegating to the parent classloader. This provides proper isolation
 * for extensions with their own dependencies.
 * 
 * Only classes from com.dell.spt.base.* package are always delegated to
 * the parent to ensure core API compatibility.
 */
public class ParentLastURLClassLoader extends URLClassLoader {

	private static final String SPT_BASE_PACKAGE = "com.dell.spt.base.";

	public ParentLastURLClassLoader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
	}

	@Override
	protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		// First, check if the class has already been loaded
		Class<?> loadedClass = findLoadedClass(name);
		if (loadedClass != null) {
			return loadedClass;
		}

		// Always delegate spt-base classes to parent for API compatibility
		if (name.startsWith(SPT_BASE_PACKAGE)) {
			return super.loadClass(name, resolve);
		}

		// Also delegate API classes to parent to avoid linkage errors
		if (name.startsWith("org.apache.logging.log4j.") ||
						name.startsWith("com.github.akurilov.confuse.") ||
						name.startsWith("com.github.akurilov.commons.")) {
			return super.loadClass(name, resolve);
		}

		// For system classes, always use parent classloader
		if (name.startsWith("java.") || name.startsWith("javax.") ||
						name.startsWith("sun.") || name.startsWith("jdk.")) {
			return super.loadClass(name, resolve);
		}

		// Try to load the class from this classloader's URLs first
		try {
			loadedClass = findClass(name);
			if (resolve) {
				resolveClass(loadedClass);
			}
			return loadedClass;
		} catch (ClassNotFoundException e) {
			// If not found in our URLs, delegate to parent
			return super.loadClass(name, resolve);
		}
	}

	@Override
	public URL getResource(String name) {
		// First try to find the resource in this classloader
		URL url = findResource(name);
		if (url != null) {
			return url;
		}

		// If not found, delegate to parent
		ClassLoader parent = getParent();
		if (parent != null) {
			return parent.getResource(name);
		}

		return null;
	}

	@Override
	public Enumeration<URL> getResources(String name) throws IOException {
		// Get resources from both this classloader and parent
		List<URL> allResources = new ArrayList<>();

		// First add resources from this classloader
		Enumeration<URL> thisResources = findResources(name);
		while (thisResources.hasMoreElements()) {
			allResources.add(thisResources.nextElement());
		}

		// Then add resources from parent
		ClassLoader parent = getParent();
		if (parent != null) {
			Enumeration<URL> parentResources = parent.getResources(name);
			while (parentResources.hasMoreElements()) {
				allResources.add(parentResources.nextElement());
			}
		}

		return Collections.enumeration(allResources);
	}

	@Override
	public InputStream getResourceAsStream(String name) {
		URL url = getResource(name);
		if (url != null) {
			try {
				return url.openStream();
			} catch (IOException e) {
				return null;
			}
		}
		return null;
	}
}
