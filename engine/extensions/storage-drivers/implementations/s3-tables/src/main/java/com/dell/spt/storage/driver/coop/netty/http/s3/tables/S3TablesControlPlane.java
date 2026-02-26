package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import com.dell.spt.base.logging.Loggers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;

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
		final String uri = "/buckets/" + bucket;
		final byte[] bodyBytes = MAPPER.writeValueAsBytes(MAPPER.createObjectNode());
		final FullHttpResponse resp = driver.executeControlPlaneRequest(HttpMethod.PUT, uri, bodyBytes);
		if (resp == null) {
			throw new Exception("CreateTableBucket: no response (timeout)");
		}
		final int status = resp.status().code();
		if (status == HTTP_CONFLICT) {
			Loggers.MSG.debug("{}: CreateTableBucket: already exists (409), continuing", driver.getStepId());
			return;
		}
		if (status < 200 || status >= 300) {
			final String bodyStr = resp.content().toString(StandardCharsets.UTF_8);
			throw new Exception("CreateTableBucket failed: HTTP " + status + " — " + bodyStr);
		}
		Loggers.MSG.info("{}: CreateTableBucket: created bucket={}", driver.getStepId(), bucket);
	}

	private void createNamespace() throws Exception {
		final String uri = "/buckets/" + bucket + "/namespaces";
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
		final String uri = "/buckets/" + bucket + "/tables";
		final ObjectNode body = MAPPER.createObjectNode();
		body.put("name", tableName);
		body.putArray("namespace").add(namespace);
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

}
