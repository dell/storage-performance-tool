package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DeleteVerificationCauseTest {
	private static final DeleteTarget TARGET = new DeleteTarget(
					new IntegrityManifestDataItem("bucket", "key", 1, null), 0);

	@Test
	void causesAreBuiltOnlyFromCategoriesStatusCodesAndClassNames() {
		assertEquals("HTTP 503", DeleteVerificationCause.httpStatus(503).toString());
		assertEquals("HTTP status out of range", DeleteVerificationCause.httpStatus(-1).toString());
		assertEquals(
						"no HEAD response (the connection closed or the response timed out)",
						DeleteVerificationCause.noResponse().toString());
		final var signed = new IllegalStateException("https://h.example.invalid/k?X-Amz-Signature=abc");
		assertEquals("probe error (IllegalStateException)", DeleteVerificationCause.probeError(signed).toString());
		assertEquals(
						"connection failure (ConnectException)",
						DeleteVerificationCause.connectionFailure(new java.net.ConnectException("10.0.0.1:9000"))
										.toString());
		assertEquals(
						"I/O error (UncheckedIOException)",
						DeleteVerificationCause.ioError(new UncheckedIOException(new java.io.IOException("x"))).toString());
		assertEquals(
						"probe error (unnamed)",
						DeleteVerificationCause.probeError(new RuntimeException("anonymous") {}).toString());
		assertEquals("probe error (unknown)", DeleteVerificationCause.probeError(null).toString());
	}

	@Test
	void defaultObserveCallsPresenceOnceAndCollectsOnlyThatCallsCause() {
		final AtomicInteger calls = new AtomicInteger();
		final DeleteVerificationProbe probe = target -> {
			if (calls.incrementAndGet() == 1) {
				DeleteVerificationCause.report(DeleteVerificationCause.httpStatus(500));
				return DeleteVerificationProbe.Presence.UNRESOLVED;
			}
			return DeleteVerificationProbe.Presence.UNRESOLVED;
		};

		assertEquals(
						DeleteVerificationProbe.Observation.unresolved(DeleteVerificationCause.httpStatus(500)),
						probe.observe(TARGET));
		assertEquals(1, calls.get());
		assertEquals(DeleteVerificationProbe.Observation.of(DeleteVerificationProbe.Presence.UNRESOLVED),
						probe.observe(TARGET), "a cause outlived its call");
		assertEquals(2, calls.get());
	}

	@Test
	void causeOfAClassifiedResultIsDroppedAndReportOutsideObserveIsIgnored() {
		DeleteVerificationCause.report(DeleteVerificationCause.httpStatus(503));
		final DeleteVerificationProbe probe = target -> {
			DeleteVerificationCause.report(DeleteVerificationCause.httpStatus(503));
			return DeleteVerificationProbe.Presence.PRESENT;
		};
		final var observation = probe.observe(TARGET);
		assertEquals(DeleteVerificationProbe.Presence.PRESENT, observation.presence());
		assertNull(observation.unresolvedCause());
	}

	@Test
	void nestedObserveRestoresTheOuterCapture() {
		final DeleteVerificationProbe inner = target -> {
			DeleteVerificationCause.report(DeleteVerificationCause.httpStatus(500));
			return DeleteVerificationProbe.Presence.UNRESOLVED;
		};
		final DeleteVerificationProbe outer = target -> {
			DeleteVerificationCause.report(DeleteVerificationCause.noResponse());
			inner.observe(target);
			return DeleteVerificationProbe.Presence.UNRESOLVED;
		};
		assertEquals(DeleteVerificationCause.noResponse(), outer.observe(TARGET).unresolvedCause());
	}
}
