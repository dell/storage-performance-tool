package com.dell.spt.base.load.step.file;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.rmi.Remote;
import java.util.List;

public interface FileManager extends Remote {

	FileManager INSTANCE = new FileManagerImpl();

	byte[] EMPTY = new byte[0];
	List<OpenOption> READ_OPTIONS = List.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
	List<OpenOption> WRITE_OPEN_OPTIONS = List.of(
					StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
	List<OpenOption> APPEND_OPEN_OPTIONS = List.of(
					StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
	Path TMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"), "spt");

	/**
	 * Determine the file name for the given logger and step id pair.
	 *
	 * @param loggerName the logger identifier
	 * @param testStepId the load step id
	 * @return the file name
	 */
	String logFileName(final String loggerName, final String testStepId) throws IOException;

	/**
	 * Generate the temporary file name.
	 *
	 * @return the temporary file name
	 */
	String newTmpFileName() throws IOException;

	/**
	 * Read the next batch of bytes from the remote file.
	 *
	 * @return the bytes that were read; an empty buffer signals EOF
	 * @throws EOFException if end of file is reached
	 */
	byte[] readFromFile(final String fileName, final long offset) throws IOException;

	/**
	 * Write some bytes to the remote file. Blocks until all the bytes are written.
	 *
	 * @param buff the bytes to write to the remote file
	 */
	void writeToFile(final String fileName, final byte[] buff) throws IOException;

	long fileSize(final String fileName) throws IOException;

	void truncateFile(final String fileName, final long size) throws IOException;

	void deleteFile(final String fileName) throws IOException;

	static OpenOption[] readOpenOptions() {
		return READ_OPTIONS.toArray(OpenOption[]::new);
	}

	static OpenOption[] writeOpenOptions() {
		return WRITE_OPEN_OPTIONS.toArray(OpenOption[]::new);
	}

	static OpenOption[] appendOpenOptions() {
		return APPEND_OPEN_OPTIONS.toArray(OpenOption[]::new);
	}
}
