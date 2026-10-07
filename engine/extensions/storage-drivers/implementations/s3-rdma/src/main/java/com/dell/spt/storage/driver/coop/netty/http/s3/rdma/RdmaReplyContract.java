package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.item.op.OpType;

/**
 * Classifies the server's answer to an RDMA-proposed request.
 *
 * <p>A server that accepts the {@code x-amz-rdma-token} proposal reports the transfer outcome in
 * {@code x-amz-rdma-reply}: 200/204/206 mean the payload moved over RDMA, 501 means the server
 * declined RDMA. A missing reply header means the server ignored the token. A declined PUT
 * received no data, so it did not create the object even though the HTTP status is 2xx. A
 * declined GET carries the object in the HTTP body instead. A successful GET reports the bytes
 * written into client memory in {@code x-amz-rdma-bytes-transferred}, and every successful reply
 * has an empty HTTP body. A GET success without that header is a protocol error unless the legacy
 * {@code storage.rdma.allowMissingBytesHeader} option accepts the requested size instead.
 */
final class RdmaReplyContract {

	static final String REPLY_HEADER = "x-amz-rdma-reply";
	static final String BYTES_TRANSFERRED_HEADER = "x-amz-rdma-bytes-transferred";

	static final int REPLY_OK = 200;
	static final int REPLY_NO_CONTENT = 204;
	static final int REPLY_PARTIAL_CONTENT = 206;
	static final int REPLY_DECLINED = 501;

	/** Reply header was not present in the response. */
	static final int REPLY_ABSENT = -1;
	/** Reply header was present but not a decimal status code. */
	static final int REPLY_MALFORMED = -2;
	/** Header value or HTTP content length that was not present. */
	static final long VALUE_ABSENT = -1;
	/** Header value that was present but not a non-negative decimal integer. */
	static final long VALUE_MALFORMED = -2;

	enum Outcome {
		/** Payload moved over RDMA as requested. */
		TRANSFERRED,
		/** Server declined or ignored RDMA: PUT stored nothing, GET body arrived over HTTP. */
		DECLINED,
		/** Server accepted RDMA but the response violates the reply contract. */
		PROTOCOL_ERROR,
	}

	/**
	 * @param bytes        payload bytes moved over RDMA (0 unless {@link Outcome#TRANSFERRED})
	 * @param bytesAssumed GET success without a bytes-transferred header; {@code bytes} is the requested size
	 * @param reason       short diagnostic for non-transferred outcomes, else {@code null}
	 */
	record Result(Outcome outcome, long bytes, boolean bytesAssumed, String reason) {}

	private RdmaReplyContract() {}

	static int parseReply(final String value) {
		if (value == null) {
			return REPLY_ABSENT;
		}
		final long parsed = parseCount(value);
		return parsed < 0 || parsed > Integer.MAX_VALUE ? REPLY_MALFORMED : (int) parsed;
	}

	static long parseCount(final String value) {
		if (value == null) {
			return VALUE_ABSENT;
		}
		final String trimmed = value.trim();
		if (trimmed.isEmpty() || trimmed.length() > 19) {
			return VALUE_MALFORMED;
		}
		long result = 0;
		for (int i = 0; i < trimmed.length(); i++) {
			final char c = trimmed.charAt(i);
			if (c < '0' || c > '9') {
				return VALUE_MALFORMED;
			}
			result = result * 10 + (c - '0');
		}
		return result;
	}

	/**
	 * Classifies a response whose HTTP status was successful.
	 *
	 * @param opType                  CREATE (PUT/UploadPart) or READ (GET)
	 * @param reply                   parsed {@code x-amz-rdma-reply}
	 * @param bytesHeader             parsed {@code x-amz-rdma-bytes-transferred}
	 * @param bodyBytes               HTTP body size: the larger of Content-Length and the body bytes
	 *                                actually received, so chunked bodies are included
	 * @param requestedSize           bytes described by the request's token
	 * @param allowMissingBytesHeader legacy compatibility: a GET success without the bytes header
	 *                                counts the requested size instead of failing
	 */
	static Result classify(
					final OpType opType,
					final int reply,
					final long bytesHeader,
					final long bodyBytes,
					final long requestedSize,
					final boolean allowMissingBytesHeader) {
		if (reply == REPLY_ABSENT || reply == REPLY_DECLINED) {
			return new Result(Outcome.DECLINED, 0, false, reply == REPLY_ABSENT ? "reply header absent" : "reply 501");
		}
		if (reply != REPLY_OK && reply != REPLY_NO_CONTENT && reply != REPLY_PARTIAL_CONTENT) {
			return new Result(Outcome.PROTOCOL_ERROR, 0, false,
							reply == REPLY_MALFORMED ? "malformed reply header" : "unexpected reply " + reply);
		}
		if (bodyBytes > 0) {
			return new Result(Outcome.PROTOCOL_ERROR, 0, false,
							"reply " + reply + " with HTTP body of " + bodyBytes + " bytes");
		}
		if (opType == OpType.CREATE) {
			return new Result(Outcome.TRANSFERRED, requestedSize, false, null);
		}
		if (bytesHeader == VALUE_MALFORMED) {
			return new Result(Outcome.PROTOCOL_ERROR, 0, false, "malformed bytes-transferred header");
		}
		if (bytesHeader == VALUE_ABSENT) {
			// Without the header nothing proves how much was written into client memory.
			return allowMissingBytesHeader
							? new Result(Outcome.TRANSFERRED, requestedSize, true, null)
							: new Result(Outcome.PROTOCOL_ERROR, 0, false,
											"bytes-transferred header absent (storage.rdma.allowMissingBytesHeader=false)");
		}
		if (bytesHeader != requestedSize) {
			return new Result(Outcome.PROTOCOL_ERROR, 0, false,
							"bytes-transferred " + bytesHeader + " != requested " + requestedSize);
		}
		return new Result(Outcome.TRANSFERRED, bytesHeader, false, null);
	}
}
