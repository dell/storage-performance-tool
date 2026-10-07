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
		final var result = RdmaReplyContract.classify(OpType.CREATE, 200, VALUE_ABSENT, 0, SIZE, false);
		assertEquals(TRANSFERRED, result.outcome());
		assertEquals(SIZE, result.bytes());
		assertFalse(result.bytesAssumed());
		assertEquals(TRANSFERRED, RdmaReplyContract.classify(OpType.CREATE, 204, VALUE_ABSENT, VALUE_ABSENT, SIZE, false).outcome());
	}

	@Test
	void putDeclineIsNotATransferEvenWithHttpSuccess() {
		// Observed from an ECS 4.5 server whose RDMA server could not start: HTTP 200, x-amz-rdma-reply: 501, RDMANotSupported body.
		final var declined = RdmaReplyContract.classify(OpType.CREATE, 501, VALUE_ABSENT, 134, SIZE, false);
		assertEquals(DECLINED, declined.outcome());
		assertEquals(0, declined.bytes());
		final var ignored = RdmaReplyContract.classify(OpType.CREATE, REPLY_ABSENT, VALUE_ABSENT, 0, SIZE, false);
		assertEquals(DECLINED, ignored.outcome());
		assertEquals(0, ignored.bytes());
	}

	@Test
	void getSuccessUsesBytesTransferred() {
		final var result = RdmaReplyContract.classify(OpType.READ, 200, SIZE, 0, SIZE, false);
		assertEquals(TRANSFERRED, result.outcome());
		assertEquals(SIZE, result.bytes());
		assertFalse(result.bytesAssumed());
	}

	@Test
	void getSuccessWithoutBytesHeaderIsProtocolErrorByDefault() {
		final var result = RdmaReplyContract.classify(OpType.READ, 200, VALUE_ABSENT, 0, SIZE, false);
		assertEquals(PROTOCOL_ERROR, result.outcome());
		assertEquals(0, result.bytes());
	}

	@Test
	void legacyOptInAcceptsMissingBytesHeaderAsRequestedSize() {
		final var result = RdmaReplyContract.classify(OpType.READ, 200, VALUE_ABSENT, 0, SIZE, true);
		assertEquals(TRANSFERRED, result.outcome());
		assertEquals(SIZE, result.bytes());
		assertTrue(result.bytesAssumed());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, VALUE_MALFORMED, 0, SIZE, true).outcome(),
						"the opt-in covers a missing header, not a malformed one");
	}

	@Test
	void anyHttpBodyOnAcceptedRdmaReplyIsProtocolError() {
		// Body size counted from the received bytes covers chunked responses without Content-Length.
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, SIZE, 1, SIZE, false).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.CREATE, 200, VALUE_ABSENT, 1, SIZE, false).outcome());
	}

	@Test
	void getDeclineDeliversBodyOverHttp() {
		assertEquals(DECLINED, RdmaReplyContract.classify(OpType.READ, 501, VALUE_ABSENT, SIZE, SIZE, false).outcome());
		assertEquals(DECLINED, RdmaReplyContract.classify(OpType.READ, REPLY_ABSENT, VALUE_ABSENT, SIZE, SIZE, false).outcome());
	}

	@Test
	void contractViolationsAreProtocolErrors() {
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, SIZE - 1, 0, SIZE, false).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, SIZE + 1, 0, SIZE, false).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, VALUE_MALFORMED, 0, SIZE, false).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.READ, 200, SIZE, SIZE, SIZE, false).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.CREATE, 200, VALUE_ABSENT, 10, SIZE, false).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.CREATE, 500, VALUE_ABSENT, 0, SIZE, false).outcome());
		assertEquals(PROTOCOL_ERROR, RdmaReplyContract.classify(OpType.CREATE, REPLY_MALFORMED, VALUE_ABSENT, 0, SIZE, false).outcome());
	}
}
