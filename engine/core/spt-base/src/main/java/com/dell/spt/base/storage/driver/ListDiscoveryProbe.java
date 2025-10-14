package com.dell.spt.base.storage.driver;

import java.io.IOException;
import java.util.List;

/**
 * Optional capability for storage drivers to probe delimiter-based segments (CommonPrefixes) without
 * performing a full counting LIST. Implemented by the S3 driver to support delimiter-first seeding.
 */
public interface ListDiscoveryProbe {

	record DiscoverResult(List<String> commonPrefixes, boolean hasContents, boolean truncated) {}

	DiscoverResult probeCommonPrefixes(
					String bucketPath, String prefix, String delimiter, int maxKeys) throws IOException;
}
