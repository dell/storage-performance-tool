import com.dell.spt.base.env.ParentLastURLClassLoader;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Writes Parquet files with the packaged s3-tables extension, loaded the way the engine loads
 * extensions. Run by the bundle task {@code verifyS3TablesParquetWrite} as
 * {@code java -cp spt.jar S3TablesParquetWriteCanary.java <dist>/ext}.
 *
 * <p>Hadoop's {@code Configuration} reads {@code core-default.xml} through the thread's context
 * class loader, and parsing it needs Hadoop's shaded Guava. Engine threads keep the application
 * loader, which can't see that file, so the canary also writes with the extension loader as the
 * context loader: the case that needs the shaded Guava in the s3-tables JAR.
 */
public final class S3TablesParquetWriteCanary {

	private static final String WRITER = "com.dell.spt.storage.driver.coop.netty.http.s3.tables.ParquetRowGroupWriter";
	private static final String MAGIC = "PAR1";

	private S3TablesParquetWriteCanary() {}

	public static void main(final String[] args) throws Exception {
		final URL[] jars;
		try (Stream<Path> files = Files.list(Path.of(args[0]))) {
			jars = files.filter(p -> p.toString().endsWith(".jar")).sorted().map(p -> {
				try {
					return p.toUri().toURL();
				} catch (final java.net.MalformedURLException e) {
					throw new IllegalStateException(e);
				}
			}).toArray(URL[]::new);
		}
		try (ParentLastURLClassLoader loader = new ParentLastURLClassLoader(jars, S3TablesParquetWriteCanary.class.getClassLoader())) {
			final String withEngineLoader = write(loader, null);
			final String withExtensionLoader = write(loader, loader);
			if (withEngineLoader != null || withExtensionLoader != null) {
				System.err.println("With the engine's context loader: " + (withEngineLoader == null ? "ok" : withEngineLoader));
				System.err.println("With the extension loader as context loader: "
								+ (withExtensionLoader == null ? "ok" : withExtensionLoader));
				System.exit(1);
			}
			System.out.println("S3_TABLES_PARQUET_WRITE_OK");
		}
	}

	/** Writes one file on a new thread; returns null on success, otherwise what went wrong. */
	private static String write(final ClassLoader extensionLoader, final ClassLoader contextLoader)
					throws InterruptedException {
		final AtomicReference<String> problem = new AtomicReference<>();
		final Thread thread = new Thread(() -> {
			try {
				final Class<?> writerClass = Class.forName(WRITER, true, extensionLoader);
				final var constructor = writerClass.getDeclaredConstructor(long.class);
				constructor.setAccessible(true);
				final Object writer = constructor.newInstance(64L * 1024);
				final var write = writerClass.getDeclaredMethod("write", long.class);
				write.setAccessible(true);
				final byte[] file = (byte[]) write.invoke(writer, 0L);
				if (file.length < 2 * MAGIC.length()
								|| !MAGIC.equals(new String(file, 0, MAGIC.length(), StandardCharsets.US_ASCII))
								|| !MAGIC.equals(new String(file, file.length - MAGIC.length(), MAGIC.length(), StandardCharsets.US_ASCII))) {
					problem.set(file.length + " bytes that are not a Parquet file");
				}
			} catch (final InvocationTargetException e) {
				problem.set(String.valueOf(e.getCause()));
			} catch (final Throwable e) {
				// Anything else, a NullPointerException included, must fail the canary too.
				problem.set(String.valueOf(e));
			}
		});
		if (contextLoader != null) {
			thread.setContextClassLoader(contextLoader);
		}
		thread.start();
		thread.join();
		return problem.get();
	}
}
