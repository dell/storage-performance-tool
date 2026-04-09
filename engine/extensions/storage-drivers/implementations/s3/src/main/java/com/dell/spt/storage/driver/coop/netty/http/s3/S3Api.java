package com.dell.spt.storage.driver.coop.netty.http.s3;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
Created by kurila on 02.08.16.
*/
public interface S3Api {

	String PREFIX_KEY_X_AMZ = "x-amz-";

	String AUTH_PREFIX = "AWS ";

	String AUTH_V4_PREFIX = "AWS4-HMAC-SHA256 ";

	String KEY_X_AMZ_COPY_SOURCE = PREFIX_KEY_X_AMZ + "copy-source";

	String KEY_X_AMZ_SECURITY_TOKEN = PREFIX_KEY_X_AMZ + "security-token";

	String AMZ_DEFAULT_REGION = "us-east-1";
	String AMZ_DATE_HEADER = "x-amz-date";
	String AMZ_PAYLOAD_HEADER = "x-amz-content-sha256";
	String AMZ_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
	String AMZ_EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

	AsciiString HEADERS_CANONICAL[] = {
			HttpHeaderNames.CONTENT_MD5,
			HttpHeaderNames.CONTENT_TYPE,
			//HttpHeaderNames.RANGE,
			HttpHeaderNames.DATE
	};

	AsciiString HEADERS_CANONICAL_V4[] = {
			HttpHeaderNames.CONTENT_MD5,
			HttpHeaderNames.CONTENT_TYPE,
			//HttpHeaderNames.RANGE,
			HttpHeaderNames.DATE,
			HttpHeaderNames.HOST,
	};
	String URL_ARG_VERSIONING = "versioning";

	String SIGN_METHOD = "HmacSHA1";

	String SIGN_V4_METHOD = "HmacSHA256";

	byte[] VERSIONING_ENABLE_CONTENT = ("<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
					"<Status>Enabled</Status></VersioningConfiguration>").getBytes(StandardCharsets.US_ASCII);

	byte[] VERSIONING_DISABLE_CONTENT = ("<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
					"<Status>Suspended</Status></VersioningConfiguration>").getBytes(StandardCharsets.US_ASCII);

	String KEY_UPLOAD_ID = "uploadId";

	String KEY_MPU_ABORT = "mpuAbort";

	AttributeKey<String> KEY_ATTR_UPLOAD_ID = AttributeKey.newInstance(KEY_UPLOAD_ID);

	String COMPLETE_MPU_HEADER = "<CompleteMultipartUpload>\n";
	String COMPLETE_MPU_PART_NUM_START = "\t<Part>\n\t\t<PartNumber>";
	String COMPLETE_MPU_PART_NUM_END = "</PartNumber>\n\t\t<ETag>";
	String COMPLETE_MPU_PART_ETAG_END = "</ETag>";
	String COMPLETE_MPU_PART_CLOSE = "\n\t</Part>\n";
	String COMPLETE_MPU_FOOTER = "</CompleteMultipartUpload>";

	String TAGGING_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
					"<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\t<TagSet>\n";
	String TAGGING_ENTRY_START = "\t\t<Tag><Key>";
	String TAGGING_ENTRY_MIDDLE = "</Key><Value>";
	String TAGGING_ENTRY_END = "</Value></Tag>\n";
	String TAGGING_FOOTER = "\t</TagSet>\n</Tagging>\n";

	int MAX_KEYS_LIMIT = 1000;
	String QNAME_ITEM = "Contents";
	String QNAME_ITEM_ID = "Key";
	String QNAME_ITEM_SIZE = "Size";
	String QNAME_IS_TRUNCATED = "IsTruncated";
	String QNAME_NEXT_CONTINUATION_TOKEN = "NextContinuationToken";
	String QNAME_CONTINUATION_TOKEN = "ContinuationToken";
	String QNAME_COMMON_PREFIXES = "CommonPrefixes";
	String QNAME_PREFIX = "Prefix";
	String QNAME_VERSION_ENTRY = "Version";
	String QNAME_DELETE_MARKER = "DeleteMarker";
	String QNAME_VERSION_ID = "VersionId";
	String QNAME_NEXT_KEY_MARKER = "NextKeyMarker";
	String QNAME_NEXT_VERSION_ID_MARKER = "NextVersionIdMarker";

	String AMZ_CHECKSUM_PREFIX = "x-amz-checksum-";
	String AMZ_CHECKSUM_ALGORITHM_HEADER = "x-amz-checksum-algorithm";

	/** Key prefix used to store per-part checksum values in the MPU context map. */
	String KEY_PART_CHECKSUM_PREFIX = "checksum-";

	enum AMZChecksum {
		MD5, CRC32, CRC32C, SHA1, SHA256;

		/** Returns the XML element name used inside CompleteMultipartUpload, e.g. "ChecksumCRC32C". */
		String xmlElementName() {
			return switch (this) {
			case CRC32 -> "ChecksumCRC32";
			case CRC32C -> "ChecksumCRC32C";
			case SHA1 -> "ChecksumSHA1";
			case SHA256 -> "ChecksumSHA256";
			case MD5 -> "ChecksumMD5";
			};
		}
	}

	static String amzChecksumRegex() {
		return String.format("^(%s)$", Arrays.stream(AMZChecksum.values())
						.map(Enum::name)
						.map(String::toLowerCase)
						.collect(Collectors.joining("|")));
	}
}
