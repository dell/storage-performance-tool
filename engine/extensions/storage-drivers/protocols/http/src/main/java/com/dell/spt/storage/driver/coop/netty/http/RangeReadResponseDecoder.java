package com.dell.spt.storage.driver.coop.netty.http;

import io.netty.handler.codec.http.HttpDecoderConfig;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpResponseDecoder;

/** Range-only decoder: reject ambiguous framing before Netty can normalize away its evidence. */
public final class RangeReadResponseDecoder extends HttpResponseDecoder {
	public RangeReadResponseDecoder(int maxChunkSize) {
		super(new HttpDecoderConfig().setMaxChunkSize(maxChunkSize).setAllowDuplicateContentLengths(false));
	}

	@Override
	protected void handleTransferEncodingChunkedWithContentLength(HttpMessage message) {
		throw new IllegalArgumentException("Range response contains both Transfer-Encoding and Content-Length");
	}
}
