package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

/**
 * Exception thrown by the RDMA native layer (JNI) when an RDMA operation fails.
 *
 * <p>Constructed from native code with the error message, errno value, and
 * the corresponding strerror() text.
 */
public class RdmaException extends RuntimeException {

	private final int errno;
	private final String errorName;

	public RdmaException(final String message, final int errno, final String errorName) {
		super(message + " (errno=" + errno + ": " + errorName + ")");
		this.errno = errno;
		this.errorName = errorName;
	}

	public int getErrno() {
		return errno;
	}

	public String getErrorName() {
		return errorName;
	}
}
