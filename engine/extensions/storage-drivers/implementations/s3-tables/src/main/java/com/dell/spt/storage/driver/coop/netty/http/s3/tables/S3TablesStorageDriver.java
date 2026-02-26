package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver;

import com.github.akurilov.confuse.Config;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

public class S3TablesStorageDriver<I extends Item, O extends Operation<I>>
				extends S3StorageDriver<I, O> {

	static final String OP_MODE_PROVISION = "provision";
	static final String OP_MODE_TABLE_WRITE = "tableWrite";
	static final String OP_MODE_TABLE_READ = "tableRead";
	static final String OP_MODE_TABLE_CATALOG = "tableCatalog";
	static final String OP_MODE_CATALOG_SEED = "catalogSeed";
	static final String OP_MODE_TABLE_COMPACTION_POLL = "tableCompactionPoll";

	protected final String opMode;
	protected final S3TablesControlPlane controlPlane;

	public S3TablesStorageDriver(
					final String stepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, dataInput, storageConfig, verifyFlag, batchSize, "s3tables");
		final Config s3tablesConfig = storageConfig.configVal("s3tables");
		this.opMode = s3tablesConfig.stringVal("opMode");
		final String controlPlaneEndpoint = s3tablesConfig.stringVal("controlPlaneEndpoint");
		final String effectiveEndpoint = controlPlaneEndpoint.isEmpty()
						? storageNodeAddrs[0]
						: controlPlaneEndpoint;
		this.controlPlane = new S3TablesControlPlane(
						effectiveEndpoint,
						s3tablesConfig.stringVal("bucket"),
						s3tablesConfig.stringVal("namespace"),
						s3tablesConfig.stringVal("tableName"),
						this);
	}

	@Override
	protected String requestNewPath(final String path) {
		if (OP_MODE_PROVISION.equals(opMode)) {
			try {
				controlPlane.provision();
			} catch (final Exception e) {
				Loggers.ERR.error("{}: provision failed: {}", stepId, e.getMessage());
				return null;
			}
		}
		return path;
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		switch (opMode) {
		case OP_MODE_PROVISION:
			op.status(Operation.Status.SUCC);
			return true;
		default:
			throw new IllegalStateException("Unsupported opMode for Phase 1: " + opMode);
		}
	}

	@Override
	protected int submit(final List<O> ops, final int from, final int to) throws IllegalStateException {
		int submitted = 0;
		for (int i = from; i < to; i++) {
			if (submit(ops.get(i))) {
				submitted++;
			} else {
				break;
			}
		}
		return submitted;
	}

	String getStepId() {
		return stepId;
	}

	FullHttpResponse executeControlPlaneRequest(
					final HttpMethod method,
					final String uri,
					final byte[] bodyBytes)
					throws Exception {
		final HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, storageNodeAddrs[0]);
		headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
		headers.set(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);
		applyDynamicHeaders(headers);
		applySharedHeaders(headers);
		applyAuthHeaders(headers, method, uri, credential);
		final FullHttpRequest req = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						method,
						uri,
						Unpooled.wrappedBuffer(bodyBytes),
						headers,
						EmptyHttpHeaders.INSTANCE);
		try {
			return executeHttpRequest(req);
		} catch (final ConnectException e) {
			throw new Exception("Connection failure calling " + method + " " + uri + ": " + e.getMessage(), e);
		}
	}

	@Override
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count)
					throws IOException {
		return java.util.Collections.emptyList();
	}
}
