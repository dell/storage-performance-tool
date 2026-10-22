package com.dell.spt.base.storage.driver.bucket;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class AwsS3RegionTest {

	@ParameterizedTest
	@CsvSource({
			"s3.amazonaws.com, us-east-1",
			"S3.AmazonAWS.com:443, us-east-1",
			"https://s3.amazonaws.com/, us-east-1",
			"s3.eu-west-1.amazonaws.com, eu-west-1",
			"s3.us-gov-west-1.amazonaws.com:80, us-gov-west-1",
			"http://s3.ap-south-1.amazonaws.com:9000/path, ap-south-1",
			"s3.eu-central-1.amazonaws.com., eu-central-1"
	})
	void recognizesStandardAwsEndpoints(final String endpoint, final String region) {
		assertEquals(region, AwsS3Region.ofEndpoint(endpoint));
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"bucket.s3.us-east-1.amazonaws.com", "s3.dualstack.us-east-1.amazonaws.com",
			"s3-fips.us-east-1.amazonaws.com", "s3-external-1.amazonaws.com",
			"s3.cn-north-1.amazonaws.com.cn", "minio.example:9000", "10.0.0.5", "[::1]:9000", ""
	})
	void treatsEverythingElseAsGenericS3(final String endpoint) {
		assertNull(AwsS3Region.ofEndpoint(endpoint));
		assertArrayEquals(new byte[0], AwsS3Region.createBucketBody("eu-west-1", endpoint));
	}

	@Test
	void precedenceIsConfiguredThenEndpointThenDefault() {
		final List<String> warnings = new ArrayList<>();
		assertEquals("eu-west-3", AwsS3Region.effective("eu-west-3", "minio.example", warnings::add));
		assertEquals("eu-west-1", AwsS3Region.effective("", "s3.eu-west-1.amazonaws.com:443", warnings::add));
		assertEquals("us-east-1", AwsS3Region.effective(null, "minio.example", warnings::add));
		assertEquals("us-east-1", AwsS3Region.effective(null, null, warnings::add));
		assertEquals(List.of(), warnings);

		assertEquals("us-west-2", AwsS3Region.effective(" us-west-2 ", "s3.eu-west-1.amazonaws.com", warnings::add));
		assertEquals(1, warnings.size());
		assertTrue(warnings.get(0).contains("us-west-2") && warnings.get(0).contains("eu-west-1"));
	}

	@Test
	void locationConstraintOnlyForAwsEndpointsOutsideTheDefaultRegion() {
		assertArrayEquals(new byte[0], AwsS3Region.createBucketBody("us-east-1", "s3.amazonaws.com"));
		assertArrayEquals(new byte[0], AwsS3Region.createBucketBody("us-east-1", "s3.us-east-1.amazonaws.com"));
		assertEquals(
						"<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
										+ "<LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>",
						new String(AwsS3Region.createBucketBody("eu-west-1", "s3.eu-west-1.amazonaws.com:443"),
										StandardCharsets.UTF_8));
	}
}
