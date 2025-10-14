package com.dell.spt.base.item.op.list.shard;

import java.util.Objects;

/**
 * Immutable descriptor for a list slice tracked by the sharding logic. {@code prefix} represents
 * the concrete prefix string passed to the storage driver (for S3 it maps to the `prefix` query
 * parameter). {@code upperBound} is optional metadata that future phases may use to bound
 * lexicographic ranges. {@code continuationToken} and {@code startAfter} capture the last known
 * pagination state.
 */
public record ListShard(String prefix,String upperBound,String continuationToken,String startAfter,String lastKey,int splitDepth,Kind kind,String delimiter){

public ListShard{continuationToken=normalize(continuationToken);startAfter=normalize(startAfter);lastKey=normalize(lastKey);splitDepth=Math.max(0,splitDepth);kind=kind==null?Kind.ENUMERATE:kind;delimiter=normalize(delimiter);}

public ListShard(final String prefix,final String upperBound,final String continuationToken,final String startAfter){this(prefix,upperBound,continuationToken,startAfter,null,0,Kind.ENUMERATE,null);}

// Backward-compatible convenience for legacy callers
public ListShard(final String prefix,final String upperBound,final String continuationToken,final String startAfter,final String lastKey,final int splitDepth){this(prefix,upperBound,continuationToken,startAfter,lastKey,splitDepth,Kind.ENUMERATE,null);}

private static String normalize(final String value){return(value==null||value.isEmpty())?null:value;}

public ListShard withContinuationToken(final String token){return new ListShard(prefix,upperBound,token,startAfter,lastKey,splitDepth,kind,delimiter);}

public ListShard withStartAfter(final String value){return new ListShard(prefix,upperBound,continuationToken,value,lastKey,splitDepth,kind,delimiter);}

public ListShard withLastKey(final String value){return new ListShard(prefix,upperBound,continuationToken,startAfter,value,splitDepth,kind,delimiter);}

public ListShard withSplitDepth(final int depth){return new ListShard(prefix,upperBound,continuationToken,startAfter,lastKey,depth,kind,delimiter);}

public ListShard incrementDepth(){return withSplitDepth(splitDepth+1);}

public ListShard withBounds(final String newPrefix,final String newUpperBound){return new ListShard(newPrefix,newUpperBound,continuationToken,startAfter,lastKey,splitDepth,kind,delimiter);}

public ListShard withKind(final Kind newKind){return new ListShard(prefix,upperBound,continuationToken,startAfter,lastKey,splitDepth,newKind,delimiter);}

public ListShard withDelimiter(final String newDelimiter){return new ListShard(prefix,upperBound,continuationToken,startAfter,lastKey,splitDepth,kind,newDelimiter);}

public enum Kind {
	ENUMERATE, DISCOVER, FLAT_CHILDREN

	}

	@Override
	public String toString() {
		return "ListShard{"
						+ "prefix='"
						+ prefix
						+ '\''
						+ ", upperBound='"
						+ upperBound
						+ '\''
						+ ", continuationToken='"
						+ continuationToken
						+ '\''
						+ ", startAfter='"
						+ startAfter
						+ '\''
						+ ", lastKey='"
						+ lastKey
						+ '\''
						+ ", splitDepth="
						+ splitDepth
						+ '}';
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ListShard shard)) {
			return false;
		}
		return splitDepth == shard.splitDepth
						&& Objects.equals(prefix, shard.prefix)
						&& Objects.equals(upperBound, shard.upperBound)
						&& Objects.equals(continuationToken, shard.continuationToken)
						&& Objects.equals(startAfter, shard.startAfter)
						&& Objects.equals(lastKey, shard.lastKey);
	}

	@Override
	public int hashCode() {
		return Objects.hash(prefix, upperBound, continuationToken, startAfter, lastKey, splitDepth);
	}
}
