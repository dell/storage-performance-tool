package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListedObject;
import java.util.ArrayList;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** SAX handler to extract ListObjectsV2 / ListObjectVersions metrics. */
final class ListObjectsXmlHandler extends DefaultHandler {

	private final ListOperation<?> listOp;
	private final boolean includeVersions;
	private final boolean fetchMetadata;

	private final StringBuilder text = new StringBuilder();

	private boolean insideContents;
	private boolean insideVersion;
	private boolean insideDeleteMarker;

	private String currentKey;
	private long currentSize;
	private String lastKey;
	private String firstKey;

	private boolean truncated;
	private String nextContinuationToken;
	private String nextKeyMarker;
	private String nextVersionIdMarker;

	private int objectCount;
	private long bytesTotal;
	private final List<ListedObject> listedObjects = new ArrayList<>();

	ListObjectsXmlHandler(
					final ListOperation<?> listOp, final boolean includeVersions, final boolean fetchMetadata) {
		this.listOp = listOp;
		this.includeVersions = includeVersions;
		this.fetchMetadata = fetchMetadata;
	}

	@Override
	public void startDocument() throws SAXException {
		lastKey = null;
		firstKey = null;
		objectCount = 0;
		bytesTotal = 0;
		listedObjects.clear();
		truncated = false;
		nextContinuationToken = null;
		nextKeyMarker = null;
		nextVersionIdMarker = null;
	}

	@Override
	public void startElement(
					final String uri, final String localName, final String qName, final Attributes attributes)
					throws SAXException {
		text.setLength(0);
		if (S3Api.QNAME_ITEM.equals(qName)) {
			insideContents = true;
			currentKey = null;
			currentSize = 0;
		} else if (S3Api.QNAME_VERSION_ENTRY.equals(qName)) {
			insideVersion = true;
			currentKey = null;
			currentSize = 0;
		} else if (S3Api.QNAME_DELETE_MARKER.equals(qName)) {
			insideDeleteMarker = true;
			currentKey = null;
			currentSize = 0;
		}
	}

	@Override
	public void characters(final char[] ch, final int start, final int length) throws SAXException {
		text.append(ch, start, length);
	}

	private String consumeValue() {
		final var value = text.toString().trim();
		text.setLength(0);
		return value;
	}

	@Override
	public void endElement(final String uri, final String localName, final String qName) throws SAXException {
		switch (qName) {
		case S3Api.QNAME_ITEM_ID:
			currentKey = consumeValue();
			if (firstKey == null || firstKey.isEmpty()) {
				firstKey = currentKey;
			}
			break;
		case S3Api.QNAME_ITEM_SIZE:
			currentSize = parseLong(consumeValue());
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			truncated = parseBoolean(consumeValue());
			break;
		case S3Api.QNAME_NEXT_CONTINUATION_TOKEN:
			nextContinuationToken = consumeValue();
			break;
		case S3Api.QNAME_CONTINUATION_TOKEN:
			consumeValue(); // unused
			break;
		case S3Api.QNAME_NEXT_KEY_MARKER:
			nextKeyMarker = consumeValue();
			break;
		case S3Api.QNAME_NEXT_VERSION_ID_MARKER:
			nextVersionIdMarker = consumeValue();
			break;
		case S3Api.QNAME_PREFIX:
			consumeValue(); // ignore
			break;
		case S3Api.QNAME_COMMON_PREFIXES:
			consumeValue(); // ignore entire element
			break;
		case S3Api.QNAME_VERSION_ID:
			consumeValue(); // currently not exposed; could be used later
			break;
		case S3Api.QNAME_ITEM:
			finishObjectEntry();
			insideContents = false;
			break;
		case S3Api.QNAME_VERSION_ENTRY:
			finishObjectEntry();
			insideVersion = false;
			break;
		case S3Api.QNAME_DELETE_MARKER:
			finishObjectEntry();
			insideDeleteMarker = false;
			break;
		default:
			consumeValue();
			break;
		}
	}

	@Override
	public void endDocument() throws SAXException {
		listOp.objectsListed(objectCount);
		listOp.listedObjects(listedObjects);
		listOp.bytesListed(fetchMetadata ? bytesTotal : 0);
		listOp.truncated(truncated);
		if (firstKey != null) {
			listOp.pageFirstKey(firstKey);
		}
		if (!includeVersions) {
			listOp.continuationToken(nextContinuationToken);
		} else {
			listOp.continuationToken(nextKeyMarker);
		}
		listOp.options(
						listOp.options()
										.toBuilder()
										.continuationToken(nextContinuationToken)
										.keyMarker(nextKeyMarker)
										.versionIdMarker(nextVersionIdMarker)
										.build());
		if (lastKey != null) {
			listOp.startAfter(lastKey);
		}
		listOp.countBytesDone(listOp.bytesListed());
	}

	private void finishObjectEntry() {
		if (currentKey == null) {
			return;
		}
		objectCount++;
		lastKey = currentKey;
		if (!insideDeleteMarker) {
			listedObjects.add(new ListedObject(currentKey, Math.max(0, currentSize)));
		}
		if (fetchMetadata) {
			bytesTotal += Math.max(0, currentSize);
		}
	}

	private static boolean parseBoolean(final String s) {
		return "true".equalsIgnoreCase(s);
	}

	private static long parseLong(final String s) {
		try {
			return Long.parseLong(s);
		} catch (final NumberFormatException ignored) {
			return 0L;
		}
	}
}
