package com.dell.spt.base.storage.driver;

import java.util.Objects;

/**
 * Encapsulates parameters controlling LIST operations (delimiter, pagination, metadata hints).
 */
public final class ListOptions {

	public static final ListOptions DEFAULT = builder().build();

	private final String delimiter;
	private final boolean fetchMetadata;
	private final boolean includeVersions;
	private final int maxKeys;
	private final String continuationToken;
	private final String startAfter;
	private final String keyMarker;
	private final String versionIdMarker;

	private ListOptions(final Builder builder) {
		this.delimiter = builder.delimiter;
		this.fetchMetadata = builder.fetchMetadata;
		this.includeVersions = builder.includeVersions;
		this.maxKeys = builder.maxKeys;
		this.continuationToken = builder.continuationToken;
		this.startAfter = builder.startAfter;
		this.keyMarker = builder.keyMarker;
		this.versionIdMarker = builder.versionIdMarker;
	}

	public static Builder builder() {
		return new Builder();
	}

	public String delimiter() {
		return delimiter;
	}

	public boolean fetchMetadata() {
		return fetchMetadata;
	}

	public boolean includeVersions() {
		return includeVersions;
	}

	public int maxKeys() {
		return maxKeys;
	}

	public String continuationToken() {
		return continuationToken;
	}

	public String startAfter() {
		return startAfter;
	}

	public String keyMarker() {
		return keyMarker;
	}

	public String versionIdMarker() {
		return versionIdMarker;
	}

	public Builder toBuilder() {
		return builder()
						.delimiter(delimiter)
						.fetchMetadata(fetchMetadata)
						.includeVersions(includeVersions)
						.maxKeys(maxKeys)
						.continuationToken(continuationToken)
						.startAfter(startAfter)
						.keyMarker(keyMarker)
						.versionIdMarker(versionIdMarker);
	}

	@Override
	public String toString() {
		return "ListOptions{"
						+ "delimiter='"
						+ delimiter
						+ '\''
						+ ", fetchMetadata="
						+ fetchMetadata
						+ ", includeVersions="
						+ includeVersions
						+ ", maxKeys="
						+ maxKeys
						+ ", continuationToken='"
						+ continuationToken
						+ '\''
						+ ", startAfter='"
						+ startAfter
						+ '\''
						+ '}';
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ListOptions)) {
			return false;
		}
		final ListOptions that = (ListOptions) o;
		return fetchMetadata == that.fetchMetadata
						&& includeVersions == that.includeVersions
						&& maxKeys == that.maxKeys
						&& Objects.equals(delimiter, that.delimiter)
						&& Objects.equals(continuationToken, that.continuationToken)
						&& Objects.equals(startAfter, that.startAfter)
						&& Objects.equals(keyMarker, that.keyMarker)
						&& Objects.equals(versionIdMarker, that.versionIdMarker);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
						delimiter,
						fetchMetadata,
						includeVersions,
						maxKeys,
						continuationToken,
						startAfter,
						keyMarker,
						versionIdMarker);
	}

	public static final class Builder {

		private String delimiter;
		private boolean fetchMetadata;
		private boolean includeVersions;
		private int maxKeys;
		private String continuationToken;
		private String startAfter;
		private String keyMarker;
		private String versionIdMarker;

		private Builder() {}

		public Builder delimiter(final String value) {
			this.delimiter = value;
			return this;
		}

		public Builder fetchMetadata(final boolean value) {
			this.fetchMetadata = value;
			return this;
		}

		public Builder includeVersions(final boolean value) {
			this.includeVersions = value;
			return this;
		}

		public Builder maxKeys(final int value) {
			this.maxKeys = value;
			return this;
		}

		public Builder continuationToken(final String value) {
			this.continuationToken = value;
			return this;
		}

		public Builder startAfter(final String value) {
			this.startAfter = value;
			return this;
		}

		public Builder keyMarker(final String value) {
			this.keyMarker = value;
			return this;
		}

		public Builder versionIdMarker(final String value) {
			this.versionIdMarker = value;
			return this;
		}

		public ListOptions build() {
			return new ListOptions(this);
		}
	}
}
