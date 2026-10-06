package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaReplyContract.Outcome.DECLINED;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaReplyContract.Outcome.PROTOCOL_ERROR;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaReplyContract.Outcome.TRANSFERRED;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaReplyContract.REPLY_ABSENT;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaReplyContract.REPLY_MALFORMED;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaReplyContract.VALUE_ABSENT;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaReplyContract.VALUE_MALFORMED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

class RdmaReplyContractTest {

	private static final long SIZE = 4_194_304L;

	@Test
	void parsesReplyAndCountHeaders() {
		assertEquals(200, RdmaReplyContract.parseReply("200"));
		assertEquals(501, RdmaReplyContract.parseReply(" 501 "));
		assertEquals(REPLY_ABSENT, RdmaReplyContract.parseReply(null));
		assertEquals(REPLY_MALFORMED, RdmaReplyContract.parseReply("ok"));
		assertEquals(REPLY_MALFORMED, RdmaReplyContract.parseReply("-200"));
		assertEquals(SIZE, RdmaReplyContract.parseCount("4194304"));
		assertEquals(VALUE_ABSENT, RdmaReplyContract.parseCount(null));
		assertEquals(VALUE_MALFORMED, RdmaReplyContract.parseCount(""));
		assertEquals(VALUE_MALFORMED, RdmaReplyContract.parseCount("12a"));
		assertEquals(VALUE_MALFORMED, RdmaReplyContract.parseCount("99999999999999999999"));
	}

	@Test
	void putSuccessRequiresRdmaReplyAndEmptyBody() {
		final var result = RdmaReplyContract.classify(OpType.CREATE, 200, VALUE_ABSENT, 0, SIZE);
		assertEquals(TRANSFERRED, result.outcome());
		assertEquals(SIZE, result.bytes());
		assertFalse(result.bytesAssumed());
		assertEquals(TRANSFERRED, RdmaReplyContract.classify(OpType.CREATE, 204, VALUE_ABSENT, VALUE_ABSENT, SIZE).outcome());
	}

	@Test
	void putDeclineIsNotATransferEvenWithHttpSuccess() {
		// Observed from an ECS 4.5 server whose RDMA server could not start: HTTP 200, x-amz-rdma-reply: 501, RDMANotSupported body.
		final var declined = RdmaReplyContract.classify(OpType.CREATE, 501, VALUE_ABSENT, 134, SIZE);
		assertEquals(DECLINED, declined.outcome());
		assertEquals(0, declined.bytes());
		final var ignored = RdmaReplyContract.classify(OpType.CREATE, REPLY_ABSENT, VALUE_ABSENT, 0, SIZE);
		assertEquals(DECLINED, ignored.outcome());
		assertEquals(0, ignored.bytes());
	}

	@Test
	void getSuccessUsesBytesTransferred() {
		final var result = RdmaReplyContract.classify(OpType.READ, 200, SIZE, 0, SIZE);
		assertEquals(TRANSFERRED, result.outcome());
		assertEquals(SIZE, result.bytes());
		assertFalse(result.bytesAssumed());
	}

	@Test
	void getSuccessWithoutBytesHeaderAssumesRequestedSize() {
		final var result = RdmaReplyContract.classify(OpType.READ, 200, VALUE_ABSENT, 0, SIZE);
		assertEquals(TRANSFERRED, result.outcome());
		assertEquals(SIZE, result.bytes());
		assertTrue(result.bytesAssumed());
	}

	@Test
	void getDeclineDeliversBodyOverHttp() {
		assertEquals(DECLINED, RdmaReplyContract.classify(OpType.READ, 501, VALUE_ABSENT, SIZE, SIZE).outcome());
		assertEquals(DECLINED, RdmaReplyContract.classify(OpType.READ, REPLY_ABSENT, VALUE_ABSENT, SIZE, SIZE).outcome());
	}

	@Test
	void contractViolationsAreProtocolErrors() {
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, SIZE - 1, 0, SIZE).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, SIZE + 1, 0, SIZE).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, VALUE_MALFORMED, 0, SIZE).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, SIZE, SIZE, SIZE).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.CREATE, 200, VALUE_ABSENT, 10, SIZE).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.CREATE, 500, VALUE_ABSENT, 0, SIZE).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.CREATE, REPLY_MALFORMED, VALUE_ABSENT, 0, SIZE).outcome());
	}
}
