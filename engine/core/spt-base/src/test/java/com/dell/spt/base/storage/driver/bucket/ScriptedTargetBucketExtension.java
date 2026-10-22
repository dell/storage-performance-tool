package com.dell.spt.base.storage.driver.bucket;

import com.dell.spt.base.Constants;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.ThreadContext;

/**
 * Deterministic target-bucket capability for tests: each bucket answers from a script, and every
 * request and close is recorded.
 */
@SuppressWarnings("rawtypes")
final class ScriptedTargetBucketExtension implements StorageDriverFactory, TargetBucketControlFactory {

	static final String TYPE = "scripted-bucket";

	final List<String> requests = new ArrayList<>();
	final Map<String, Deque<TargetBucketResponse>> checks = new HashMap<>();
	final Map<String, TargetBucketResponse> creates = new HashMap<>();
	final List<Config> openedWith = new ArrayList<>();
	final List<String> stepIdsSeen = new ArrayList<>();
	int opened;
	int closed;
	boolean interruptOnCheck;
	/** Leaves its own log context behind, as the Netty helper requests do. */
	boolean leakLogContext;

	ScriptedTargetBucketExtension check(final String bucket, final TargetBucketResponse... responses) {
		checks.computeIfAbsent(bucket, key -> new ArrayDeque<>()).addAll(List.of(responses));
		return this;
	}

	ScriptedTargetBucketExtension create(final String bucket, final TargetBucketResponse response) {
		creates.put(bucket, response);
		return this;
	}

	@Override
	public String id() {
		return TYPE;
	}

	@Override
	public SchemaProvider schemaProvider() {
		return null;
	}

	@Override
	public void install(final Path dstPath) {}

	@Override
	public Config defaults(final Path appHomePath) {
		return null;
	}

	@Override
	public StorageDriver create(
					final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize) {
		throw new UnsupportedOperationException("provisioning tests open only the control client");
	}

	@Override
	public TargetBucketControl openTargetBucketControl(final String stepId, final Config storageConfig) {
		opened++;
		openedWith.add(storageConfig);
		return new TargetBucketControl() {
			@Override
			public TargetBucketResponse check(final String bucket, final Credential credential)
							throws InterruptedException {
				requests.add("HEAD " + bucket + uid(credential));
				stepIdsSeen.add(ThreadContext.get(Constants.KEY_STEP_ID));
				if (leakLogContext) {
					ThreadContext.put(Constants.KEY_STEP_ID, "leaked-step");
					ThreadContext.put(Constants.KEY_CLASS_NAME, "LeakyDriver");
				}
				if (interruptOnCheck) {
					throw new InterruptedException("test interruption");
				}
				final var scripted = checks.get(bucket);
				return scripted == null || scripted.isEmpty() ? TargetBucketResponse.ok() : scripted.poll();
			}

			@Override
			public TargetBucketResponse create(final String bucket, final Credential credential) {
				requests.add("PUT " + bucket + uid(credential));
				return creates.getOrDefault(bucket, TargetBucketResponse.ok());
			}

			@Override
			public void close() {
				closed++;
			}
		};
	}

	private static String uid(final Credential credential) {
		return credential == null ? "" : " as " + credential.getUid();
	}

	/** A storage driver factory of the same type family that does not opt into provisioning. */
	static final class WithoutCapability implements StorageDriverFactory {

		@Override
		public String id() {
			return TYPE;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		public void install(final Path dstPath) {}

		@Override
		public Config defaults(final Path appHomePath) {
			return null;
		}

		@Override
		public StorageDriver create(
						final String stepId, final DataInput dataInput, final Config storageConfig,
						final boolean verifyFlag, final int batchSize) {
			throw new UnsupportedOperationException();
		}
	}
}
