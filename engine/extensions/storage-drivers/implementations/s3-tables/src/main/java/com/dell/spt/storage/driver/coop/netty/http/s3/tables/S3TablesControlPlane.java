package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import com.dell.spt.base.logging.Loggers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Implements the S3 Tables control-plane REST API.
 *
 * All requests are signed with SigV4 service name "s3tables" via the parent driver.
 * Idempotency: HTTP 409 (ConflictException) on CreateTableBucket, CreateNamespace, and
 * CreateTable is treated as success — resource already exists, nothing to do.
 */
final class S3TablesControlPlane {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final int HTTP_CONFLICT = 409;

	private final String bucket;
	private final String namespace;
	private final String tableName;
	private final S3TablesStorageDriver<?, ?> driver;

	/** Set during provision; used to build control-plane URIs after provisioning. */
	private volatile String bucketArn;

	/** URL-encoded form of bucketArn, cached alongside it. */
	private volatile String bucketArnEncoded;

	/** True once fetchBucketArn() has been attempted at least once. */
	private volatile boolean arnFetched = false;

	S3TablesControlPlane(
					final String endpoint,
					final String bucket,
					final String namespace,
					final String tableName,
					final S3TablesStorageDriver<?, ?> driver) {
		this.bucket = bucket;
		this.namespace = namespace;
		this.tableName = tableName;
		this.driver = driver;
		this.bucketArn = null;
		this.bucketArnEncoded = null;
	}

	/**
	 * Idempotent provision: CreateTableBucket → CreateNamespace → CreateTable.
	 * HTTP 409 on any step is treated as success (already exists).
	 */
	void provision() throws Exception {
		Loggers.MSG.info("{}: provisioning s3tables bucket={} namespace={} table={}",
						driver.getStepId(), bucket, namespace, tableName);
		createTableBucket();
		createNamespace();
		createTable();
		Loggers.MSG.info("{}: provision complete", driver.getStepId());
	}

	private void createTableBucket() throws Exception {
		final ObjectNode body = MAPPER.createObjectNode();
		body.put("name", bucket);
		final byte[] bodyBytes = MAPPER.writeValueAsBytes(body);
		final FullHttpResponse resp = driver.executeControlPlaneRequest(HttpMethod.PUT, "/buckets", bodyBytes);
		if (resp == null) {
			throw new Exception("CreateTableBucket: no response (timeout)");
		}
		final int status = resp.status().code();
		if (status == HTTP_CONFLICT) {
			Loggers.MSG.debug("{}: CreateTableBucket: already exists (409), continuing", driver.getStepId());
			fetchBucketArn();
			return;
		}
		if (status < 200 || status >= 300) {
			final String bodyStr = resp.content().toString(StandardCharsets.UTF_8);
			throw new Exception("CreateTableBucket failed: HTTP " + status + " — " + bodyStr);
		}
		try {
			final var node = MAPPER.readTree(resp.content().toString(StandardCharsets.UTF_8));
			if (node.has("arn")) {
				setBucketArn(node.get("arn").asText());
				Loggers.MSG.debug("{}: CreateTableBucket: parsed ARN={}", driver.getStepId(), this.bucketArn);
			}
		} catch (final Exception e) {
			Loggers.MSG.debug("{}: could not parse bucket ARN from CreateTableBucket response: {}", driver.getStepId(), e.getMessage());
		}
		if (this.bucketArn == null) {
			fetchBucketArn();
		}
		Loggers.MSG.info("{}: CreateTableBucket: created bucket={} arn={}",
						driver.getStepId(), bucket, bucketArn);
	}

	private void setBucketArn(final String arn) {
		this.bucketArn = arn;
		try {
			this.bucketArnEncoded = URLEncoder.encode(arn, StandardCharsets.UTF_8.name());
		} catch (final Exception e) {
			this.bucketArnEncoded = arn;
		}
	}

	private void fetchBucketArn() throws Exception {
		arnFetched = true;
		final FullHttpResponse resp = driver.executeControlPlaneRequest(
						HttpMethod.GET, "/buckets", new byte[0]);
		if (resp == null) {
			throw new Exception("ListTableBuckets: no response (timeout)");
		}
		final int status = resp.status().code();
		if (status < 200 || status >= 300) {
			throw new Exception("ListTableBuckets failed: HTTP " + status);
		}
		try {
			final var node = MAPPER.readTree(resp.content().toString(StandardCharsets.UTF_8));
			final var arr = node.has("tableBuckets") ? node.get("tableBuckets") : node.get("buckets");
			if (arr != null) {
				for (final var b : arr) {
					if (bucket.equals(b.path("name").asText())) {
						setBucketArn(b.path("arn").asText());
						Loggers.MSG.debug("{}: resolved bucket ARN={}", driver.getStepId(), this.bucketArn);
						break;
					}
				}
			}
		} catch (final Exception e) {
			Loggers.MSG.warn("{}: could not parse ARN from ListTableBuckets: {}", driver.getStepId(), e.getMessage());
		}
	}

	private void createNamespace() throws Exception {
		final String arn = effectiveArn();
		final String uri = "/namespaces/" + arn;
		final ObjectNode body = MAPPER.createObjectNode();
		body.putArray("namespace").add(namespace);
		final byte[] bodyBytes = MAPPER.writeValueAsBytes(body);
		final FullHttpResponse resp = driver.executeControlPlaneRequest(HttpMethod.PUT, uri, bodyBytes);
		if (resp == null) {
			throw new Exception("CreateNamespace: no response (timeout)");
		}
		final int status = resp.status().code();
		if (status == HTTP_CONFLICT) {
			Loggers.MSG.debug("{}: CreateNamespace: already exists (409), continuing", driver.getStepId());
			return;
		}
		if (status < 200 || status >= 300) {
			final String bodyStr = resp.content().toString(StandardCharsets.UTF_8);
			throw new Exception("CreateNamespace failed: HTTP " + status + " — " + bodyStr);
		}
		Loggers.MSG.info("{}: CreateNamespace: created namespace={}", driver.getStepId(), namespace);
	}

	private void createTable() throws Exception {
		final String arn = effectiveArn();
		final String uri = "/tables/" + arn + "/" + namespace;
		final ObjectNode body = MAPPER.createObjectNode();
		body.put("name", tableName);
		body.put("format", "ICEBERG");
		final byte[] bodyBytes = MAPPER.writeValueAsBytes(body);
		final FullHttpResponse resp = driver.executeControlPlaneRequest(HttpMethod.PUT, uri, bodyBytes);
		if (resp == null) {
			throw new Exception("CreateTable: no response (timeout)");
		}
		final int status = resp.status().code();
		if (status == HTTP_CONFLICT) {
			Loggers.MSG.debug("{}: CreateTable: already exists (409), continuing", driver.getStepId());
			return;
		}
		if (status < 200 || status >= 300) {
			final String bodyStr = resp.content().toString(StandardCharsets.UTF_8);
			throw new Exception("CreateTable failed: HTTP " + status + " — " + bodyStr);
		}
		Loggers.MSG.info("{}: CreateTable: created table={}", driver.getStepId(), tableName);
	}

	/**
	 * GetTableMetadataLocation — returns current metadataLocation and versionToken.
	 * REST: GET /tables/{tableBucketARN}/{namespace}/{name}/metadata-location
	 */
	IcebergCommitter.MetadataLocationResult getTableMetadataLocation() throws Exception {
		final String arn = effectiveArn();
		final String uri = "/tables/" + arn + "/" + namespace + "/" + tableName + "/metadata-location";
		final FullHttpResponse resp = driver.executeControlPlaneRequest(
						HttpMethod.GET, uri, new byte[0]);
		if (resp == null) {
			throw new Exception("GetTableMetadataLocation: no response (timeout)");
		}
		final int status = resp.status().code();
		if (status < 200 || status >= 300) {
			throw new Exception("GetTableMetadataLocation failed: HTTP " + status);
		}
		final var node = MAPPER.readTree(resp.content().toString(StandardCharsets.UTF_8));
		final String metadataLocation = node.path("metadataLocation").asText();
		final String versionToken = node.path("versionToken").asText();
		return new IcebergCommitter.MetadataLocationResult(metadataLocation, versionToken);
	}

	/**
	 * UpdateTableMetadataLocation — Iceberg commit operation.
	 * REST: PUT /tables/{tableBucketARN}/{namespace}/{name}/metadata-location
	 *
	 * @param currentVersionToken the versionToken from the last GetTableMetadataLocation
	 * @param newMetadataLocation S3 URI of the new table metadata JSON
	 * @return HTTP status code (200 = success, 409 = conflict/retry)
	 */
	int updateTableMetadataLocation(
					final String currentVersionToken,
					final String newMetadataLocation) throws Exception {
		final String arn = effectiveArn();
		final String uri = "/tables/" + arn + "/" + namespace + "/" + tableName + "/metadata-location";
		final ObjectNode body = MAPPER.createObjectNode();
		body.put("metadataLocation", newMetadataLocation);
		body.put("versionToken", currentVersionToken);
		final byte[] bodyBytes = MAPPER.writeValueAsBytes(body);
		final FullHttpResponse resp = driver.executeControlPlaneRequest(
						HttpMethod.PUT, uri, bodyBytes);
		if (resp == null) {
			throw new Exception("UpdateTableMetadataLocation: no response (timeout)");
		}
		return resp.status().code();
	}

	/**
	 * PutTableMaintenanceConfiguration — triggers compaction on the table.
	 * REST: PUT /tables/{tableBucketARN}/{namespace}/{name}/maintenance/{type}
	 */
	void putTableMaintenanceConfiguration() throws Exception {
		final String arn = effectiveArn();
		final String uri = "/tables/" + arn + "/" + namespace + "/" + tableName + "/maintenance/icebergCompaction";
		final ObjectNode body = MAPPER.createObjectNode();
		final ObjectNode value = body.putObject("value");
		value.put("status", "enabled");
		final ObjectNode settings = value.putObject("settings");
		final ObjectNode icebergCompaction = settings.putObject("icebergCompaction");
		icebergCompaction.put("targetFileSizeMB",
						(int) Math.max(1, driver.targetFileSizeBytes / (1024 * 1024)));
		final byte[] bodyBytes = MAPPER.writeValueAsBytes(body);
		final FullHttpResponse resp = driver.executeControlPlaneRequest(HttpMethod.PUT, uri, bodyBytes);
		if (resp == null) {
			throw new Exception("PutTableMaintenanceConfiguration: no response (timeout)");
		}
		final int status = resp.status().code();
		if (status < 200 || status >= 300) {
			final String bodyStr = resp.content().toString(StandardCharsets.UTF_8);
			throw new Exception("PutTableMaintenanceConfiguration failed: HTTP " + status + " — " + bodyStr);
		}
		Loggers.MSG.info("{}: PutTableMaintenanceConfiguration: compaction triggered on table={}",
						driver.getStepId(), tableName);
	}

	/**
	 * Returns the URL-encoded bucket ARN if known, otherwise the bucket name.
	 * Attempts a lazy fetch via ListTableBuckets once if ARN has not been set yet.
	 */
	private String effectiveArn() {
		if (bucketArnEncoded != null) {
			return bucketArnEncoded;
		}
		if (!arnFetched) {
			try {
				fetchBucketArn();
			} catch (final Exception e) {
				Loggers.MSG.debug("{}: effectiveArn lazy-fetch failed, falling back to bucket name: {}", driver.getStepId(), e.getMessage());
			}
		}
		return bucketArnEncoded != null ? bucketArnEncoded : bucket;
	}

}
