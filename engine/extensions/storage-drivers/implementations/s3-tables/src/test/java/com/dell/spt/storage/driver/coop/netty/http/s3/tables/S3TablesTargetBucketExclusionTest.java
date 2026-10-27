package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControlFactory;
import org.junit.jupiter.api.Test;

/** S3 Tables manages its own table buckets; sharing the S3 driver hierarchy does not opt it in. */
class S3TablesTargetBucketExclusionTest {

	@Test
	void s3TablesFactoryDoesNotProvisionTargetBuckets() {
		final Object factory = Extension.load(Thread.currentThread().getContextClassLoader()).stream()
						.filter(e -> e instanceof StorageDriverFactory<?, ?, ?> f && "s3-tables".equals(f.id()))
						.findFirst()
						.orElseThrow();
		assertFalse(factory instanceof TargetBucketControlFactory);
	}
}
