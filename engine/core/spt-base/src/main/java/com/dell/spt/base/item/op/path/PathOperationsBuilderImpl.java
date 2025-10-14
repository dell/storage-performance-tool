package com.dell.spt.base.item.op.path;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.item.op.list.shard.ListShardingContext;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.util.List;

/** Created by kurila on 30.01.17. */
public class PathOperationsBuilderImpl<I extends PathItem, O extends PathOperation<I>>
				extends OperationsBuilderImpl<I, O> implements PathOperationsBuilder<I, O> {

	private ListOptions listOptionsTemplate = ListOptions.DEFAULT;
	private ListShardingContext shardingContext;

	public PathOperationsBuilderImpl(final int originIndex) {
		super(originIndex);
	}

	public PathOperationsBuilderImpl<I, O> listOptions(final ListOptions options) {
		this.listOptionsTemplate = options == null ? ListOptions.DEFAULT : options;
		return this;
	}

	public PathOperationsBuilderImpl<I, O> listShardingContext(final ListShardingContext context) {
		this.shardingContext = context;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public O buildOp(final I pathItem) throws IOException {
		final String outputPath = getNextOutputPath();
		return (O) newPathOperation(pathItem, outputPath);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void buildOps(final List<I> items, final List<O> buff) throws IOException {
		String outputPath;
		for (final I nextItem : items) {
			outputPath = getNextOutputPath();
			buff.add((O) newPathOperation(nextItem, outputPath));
		}
	}

	private PathOperationImpl<I> newPathOperation(final I item, final String outputPath) {
		if (OpType.LIST.equals(opType)) {
			final var originalName = item == null ? null : item.toString();
			final var credentialPath = (outputPath == null || outputPath.isEmpty()) ? inputPath : outputPath;
			final var listOp = new ListOperationImpl<>(originIndex, opType, item, getNextCredential(credentialPath));
			final var bucketPath = normalizePath(inputPath);
			if (bucketPath != null) {
				listOp.srcPath(bucketPath);
			}
			if (originalName != null && item != null) {
				item.name(relativizeKey(bucketPath, originalName));
			}
			ListShard shard = null;
			if (shardingContext != null && item != null) {
				shard = shardingContext.shardForPrefix(item.name());
				if (shard != null) {
					shardingContext.metricsRecorder().onLease(shard);
					item.name(shard.prefix());
					if (Loggers.MSG.isDebugEnabled()) {
						Loggers.MSG.debug(
										"LIST shard assigned prefix={} continuationToken={} startAfter={}",
										shard.prefix(),
										shard.continuationToken(),
										shard.startAfter());
					}
				}
			}
			final var optionsBuilder = listOptionsTemplate.toBuilder();
			if (shard != null && shard.startAfter() != null) {
				optionsBuilder.startAfter(shard.startAfter());
				if (Loggers.MSG.isDebugEnabled()) {
					Loggers.MSG.debug(
									"LIST shard start-after applied prefix={} startAfter={}",
									shard.prefix(),
									shard.startAfter());
				}
			}
			if (shard != null && shard.delimiter() != null) {
				optionsBuilder.delimiter(shard.delimiter());
				if (Loggers.MSG.isDebugEnabled()) {
					Loggers.MSG.debug(
									"LIST shard delimiter applied prefix={} delimiter={}",
									shard.prefix(), shard.delimiter());
				}
			}
			listOp.options(optionsBuilder.build());
			if (shard != null) {
				listOp.shard(shard);
			}
			return listOp;
		}
		return new PathOperationImpl<>(originIndex, opType, item, getNextCredential(outputPath));
	}

	private static String normalizePath(final String path) {
		if (path == null || path.isEmpty()) {
			return null;
		}
		return path.startsWith("/") ? path : "/" + path;
	}

	private static String relativizeKey(final String bucketPath, final String fullPath) {
		if (fullPath == null || fullPath.isEmpty()) {
			return fullPath;
		}
		final String normalizedFull = fullPath.startsWith("/") ? fullPath : "/" + fullPath;
		if (bucketPath != null && !bucketPath.isEmpty()) {
			final var normalizedBucket = bucketPath.startsWith("/") ? bucketPath : "/" + bucketPath;
			if (normalizedFull.startsWith(normalizedBucket)) {
				var relative = normalizedFull.substring(normalizedBucket.length());
				if (relative.startsWith("/")) {
					relative = relative.substring(1);
				}
				return relative;
			}
		}
		return normalizedFull.startsWith("/") ? normalizedFull.substring(1) : normalizedFull;
	}
}
