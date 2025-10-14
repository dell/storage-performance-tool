package com.dell.spt.storage.driver.coop.netty.http.s3;

import java.util.ArrayList;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** SAX handler to extract CommonPrefixes from ListObjectsV2 responses for delimiter probes. */
final class CommonPrefixesXmlHandler extends DefaultHandler {

	private final StringBuilder text = new StringBuilder();
	private final List<String> commonPrefixes = new ArrayList<>(256);
	private boolean insideCommonPrefixes;
	private boolean hasContents;
	private boolean truncated;

	@Override
	public void startDocument() throws SAXException {
		commonPrefixes.clear();
		insideCommonPrefixes = false;
		hasContents = false;
		truncated = false;
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes)
					throws SAXException {
		text.setLength(0);
		if (S3Api.QNAME_COMMON_PREFIXES.equals(qName)) {
			insideCommonPrefixes = true;
		} else if (S3Api.QNAME_ITEM.equals(qName)) {
			hasContents = true; // indicate there are direct children
		}
	}

	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		text.append(ch, start, length);
	}

	private String consume() {
		final String v = text.toString().trim();
		text.setLength(0);
		return v;
	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		switch (qName) {
		case S3Api.QNAME_PREFIX:
			if (insideCommonPrefixes) {
				final String p = consume();
				if (!p.isEmpty()) {
					commonPrefixes.add(p);
				}
			} else {
				consume();
			}
			break;
		case S3Api.QNAME_COMMON_PREFIXES:
			consume();
			insideCommonPrefixes = false;
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			truncated = Boolean.parseBoolean(consume());
			break;
		default:
			consume();
			break;
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
