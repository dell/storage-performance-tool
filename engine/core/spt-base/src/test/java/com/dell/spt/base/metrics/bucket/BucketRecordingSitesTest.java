package com.dell.spt.base.metrics.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Enumerates the sites that update the aggregate, so that a new site with no bucket update beside it
 * fails here. Bucket rows sum to the aggregate only if both receive every call.
 */
class BucketRecordingSitesTest {

	private static final Path SOURCE = Path.of(
					"src/main/java/com/dell/spt/base/load/step/local/context/LoadStepContextImpl.java");
	private static final Pattern AGGREGATE_CALL = Pattern.compile(
					"(?:metrics|metricsCtx|resolveMetrics\\([^;]*?\\))\\.mark(Succ|PartSucc|Fail|Corrupt)\\(");
	private static final Pattern BUCKET_CALL = Pattern.compile(
					"(?:bucketMeters|recorder\\.(?:meterSet|primaryMeterSet)\\([^)]*\\))\\.mark(Succ|PartSucc|Fail|Corrupt)\\(");

	@Test
	void everyAggregateUpdateHasABucketUpdate() throws Exception {
		final String source = Files.readString(SOURCE);
		final long aggregate = AGGREGATE_CALL.matcher(source).results().count();
		final long bucket = BUCKET_CALL.matcher(source).results().count();
		assertTrue(aggregate >= 9, "the pattern no longer finds the sites: " + aggregate);
		assertEquals(aggregate, bucket, "a site updates the aggregate without updating its bucket meters");
		for (final String kind : new String[]{"Succ", "PartSucc", "Fail", "Corrupt"
		}) {
			final long aggregateOfKind = AGGREGATE_CALL.matcher(source).results()
							.filter(match -> kind.equals(match.group(1))).count();
			final long bucketOfKind = BUCKET_CALL.matcher(source).results()
							.filter(match -> kind.equals(match.group(1))).count();
			assertEquals(aggregateOfKind, bucketOfKind, "mark" + kind);
		}
	}
}
