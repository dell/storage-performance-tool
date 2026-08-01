package com.dell.spt.storage.driver.coop.netty.http.s3;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** SAX handler to extract CommonPrefixes from ListObjectsV2 responses for delimiter probes. */
final class CommonPrefixesXmlHandler extends DefaultHandler {

	private final StringBuilder text = new StringBuilder();
	private final List<String> commonPrefixes = new ArrayList<>(256);
	private final Deque<String> elementStack = new ArrayDeque<>();
	private boolean documentElementSeen;
	private boolean rootClosed;
	private boolean insideCommonPrefixes;
	private boolean prefixSeen;
	private boolean hasContents;
	private boolean truncationSeen;
	private boolean truncated;
	private String activeScalar;

	@Override
	public void startDocument() {
		commonPrefixes.clear();
		elementStack.clear();
		documentElementSeen = false;
		rootClosed = false;
		insideCommonPrefixes = false;
		prefixSeen = false;
		hasContents = false;
		truncationSeen = false;
		truncated = false;
		activeScalar = null;
		text.setLength(0);
	}

	@Override
	public void startElement(
					final String uri, final String localName, final String qName,
					final Attributes attributes) throws SAXException {
		if (activeScalar != null) {
			throw new SAXException(
							"Nested element " + qName + " is invalid inside S3 LIST field "
											+ activeScalar);
		}
		text.setLength(0);
		if (elementStack.isEmpty()) {
			if (documentElementSeen) {
				throw new SAXException("S3 delimiter listing contains content after its root");
			}
			documentElementSeen = true;
			if (!S3Api.QNAME_LIST_BUCKET_RESULT.equals(qName)) {
				throw new SAXException("Expected S3 ListBucketResult root but found " + qName);
			}
			elementStack.push(qName);
			return;
		}
		if (rootClosed) {
			throw new SAXException("S3 delimiter listing contains content after its root");
		}
		final String parent = elementStack.peek();
		switch (qName) {
		case S3Api.QNAME_COMMON_PREFIXES:
			requireDirectChild(qName, parent, S3Api.QNAME_LIST_BUCKET_RESULT);
			if (insideCommonPrefixes) {
				throw new SAXException("Nested CommonPrefixes entry");
			}
			insideCommonPrefixes = true;
			prefixSeen = false;
			break;
		case S3Api.QNAME_PREFIX:
			if (!insideCommonPrefixes) {
				throw new SAXException("Prefix is invalid outside CommonPrefixes");
			}
			requireDirectChild(qName, parent, S3Api.QNAME_COMMON_PREFIXES);
			if (prefixSeen) {
				throw new SAXException("Duplicate Prefix in CommonPrefixes entry");
			}
			prefixSeen = true;
			activeScalar = qName;
			break;
		case S3Api.QNAME_ITEM:
			requireDirectChild(qName, parent, S3Api.QNAME_LIST_BUCKET_RESULT);
			hasContents = true;
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			requireDirectChild(qName, parent, S3Api.QNAME_LIST_BUCKET_RESULT);
			if (truncationSeen) {
				throw new SAXException("Duplicate IsTruncated in S3 delimiter listing");
			}
			truncationSeen = true;
			activeScalar = qName;
			break;
		case S3Api.QNAME_LIST_BUCKET_RESULT:
			throw new SAXException("Nested S3 ListBucketResult root");
		default:
			break;
		}
		elementStack.push(qName);
	}

	private static void requireDirectChild(
					final String child, final String actualParent, final String expectedParent)
					throws SAXException {
		if (!expectedParent.equals(actualParent)) {
			throw new SAXException(
							child + " must be a direct child of " + expectedParent
											+ ", found parent " + actualParent);
		}
	}

	@Override
	public void characters(final char[] ch, final int start, final int length) {
		if (activeScalar != null) {
			text.append(ch, start, length);
		}
	}

	@Override
	public void endElement(
					final String uri, final String localName, final String qName) throws SAXException {
		if (elementStack.isEmpty() || !qName.equals(elementStack.peek())) {
			throw new SAXException("Mismatched S3 delimiter listing element close: " + qName);
		}
		switch (qName) {
		case S3Api.QNAME_PREFIX:
			final String prefix = text.toString();
			if (prefix.isEmpty()) {
				throw new SAXException("CommonPrefixes entry contains an empty Prefix");
			}
			commonPrefixes.add(prefix);
			activeScalar = null;
			break;
		case S3Api.QNAME_COMMON_PREFIXES:
			if (!prefixSeen) {
				throw new SAXException("CommonPrefixes entry is missing Prefix");
			}
			insideCommonPrefixes = false;
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			final String value = text.toString().trim();
			if (!"true".equals(value) && !"false".equals(value)) {
				throw new SAXException("Invalid IsTruncated value: " + value);
			}
			truncated = Boolean.parseBoolean(value);
			activeScalar = null;
			break;
		case S3Api.QNAME_LIST_BUCKET_RESULT:
			if (elementStack.size() != 1) {
				throw new SAXException("S3 delimiter listing root closed at an invalid hierarchy position");
			}
			rootClosed = true;
			break;
		default:
			break;
		}
		elementStack.pop();
		text.setLength(0);
	}

	@Override
	public void endDocument() throws SAXException {
		if (!documentElementSeen || !rootClosed || !elementStack.isEmpty()) {
			throw new SAXException("S3 delimiter listing has no complete ListBucketResult root");
		}
		if (insideCommonPrefixes) {
			throw new SAXException("S3 delimiter listing ended inside CommonPrefixes");
		}
		if (!truncationSeen) {
			throw new SAXException("S3 delimiter listing is missing IsTruncated");
		}
	}

	List<String> commonPrefixes() {
		return commonPrefixes;
	}

	boolean hasContents() {
		return hasContents;
	}

	boolean truncated() {
		return truncated;
	}
}
