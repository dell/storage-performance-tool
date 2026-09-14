package com.dell.spt.storage.driver.coop.netty.http;

import io.netty.handler.codec.http.HttpDecoderConfig;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpResponseDecoder;

/**
 * Range-only decoder: retain ordinary HTTP size limits and reject ambiguous framing before
 * Netty can normalize away its evidence. The GET-only range pipeline uses its validated
 * positive transport timeout and channelInactive settlement for missing responses.
 */
public final class RangeReadResponseDecoder extends HttpResponseDecoder {
	public RangeReadResponseDecoder(int maxChunkSize) {
		super(new HttpDecoderConfig().setMaxInitialLineLength(HttpStorageDriver.REQ_LINE_LEN)
						.setMaxHeaderSize(HttpStorageDriver.HEADERS_LEN).setMaxChunkSize(maxChunkSize).setAllowDuplicateContentLengths(false));
	}

	@Override
	protected void handleTransferEncodingChunkedWithContentLength(HttpMessage message) {
		throw new IllegalArgumentException("Range response contains both Transfer-Encoding and Content-Length");
	}
}
