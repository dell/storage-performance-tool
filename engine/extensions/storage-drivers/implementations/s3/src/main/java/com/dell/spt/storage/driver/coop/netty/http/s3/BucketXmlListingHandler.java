package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
Created by andrey on 02.12.16.
*/
public final class BucketXmlListingHandler<I extends Item>
				extends DefaultHandler implements S3XmlListingHandler {

	private final List<I> itemsBuffer;
	private final String path;
	private final String prefix;
	private final ItemFactory<I> itemFactory;
	private final int idRadix;
	private final List<I> pageItems = new ArrayList<>();
	private final Deque<String> elementStack = new ArrayDeque<>();
	private final StringBuilder oid = new StringBuilder();
	private final StringBuilder strSize = new StringBuilder();
	private final StringBuilder truncateFlag = new StringBuilder();

	private boolean documentElementSeen;
	private boolean rootClosed;
	private boolean isInsideItem;
	private String activeScalar;
	private boolean itemIdSeen;
	private boolean itemSizeSeen;
	private boolean truncateFlagSeen;
	private boolean isTruncatedFlag;

	public BucketXmlListingHandler(
					final List<I> itemsBuffer, final String path, final ItemFactory<I> itemFactory,
					final int idRadix) {
		this(itemsBuffer, path, null, itemFactory, idRadix);
	}

	public BucketXmlListingHandler(
					final List<I> itemsBuffer, final String path, final String prefix,
					final ItemFactory<I> itemFactory, final int idRadix) {
		this.itemsBuffer = itemsBuffer;
		this.path = path == null ? "" : (path.endsWith("/") ? path : path + "/");
		this.prefix = prefix == null || prefix.isEmpty()
						? ""
						: (prefix.startsWith("/") ? prefix.substring(1) : prefix);
		this.itemFactory = itemFactory;
		this.idRadix = idRadix;
	}

	@Override
	public void startDocument() {
		documentElementSeen = false;
		rootClosed = false;
		isInsideItem = false;
		activeScalar = null;
		itemIdSeen = false;
		itemSizeSeen = false;
		truncateFlagSeen = false;
		isTruncatedFlag = false;
		pageItems.clear();
		elementStack.clear();
		oid.setLength(0);
		strSize.setLength(0);
		truncateFlag.setLength(0);
	}

	@Override
	public void startElement(
					final String uri, final String localName, final String qName,
					final Attributes attrs) throws SAXException {
		if (activeScalar != null) {
			throw new SAXException(
							"Nested element " + qName + " is invalid inside S3 LIST field "
											+ activeScalar);
		}
		if (elementStack.isEmpty()) {
			if (documentElementSeen) {
				throw new SAXException("S3 listing contains content after its root element");
			}
			documentElementSeen = true;
			if (!S3Api.QNAME_LIST_BUCKET_RESULT.equals(qName)) {
				throw new SAXException("Expected S3 ListBucketResult root but found " + qName);
			}
			elementStack.push(qName);
			return;
		}
		if (rootClosed) {
			throw new SAXException("S3 listing contains content after its root element");
		}
		final String parent = elementStack.peek();
		switch (qName) {
		case S3Api.QNAME_ITEM:
			requireDirectChild(qName, parent, S3Api.QNAME_LIST_BUCKET_RESULT);
			if (isInsideItem) {
				throw new SAXException("Nested S3 Contents entry");
			}
			isInsideItem = true;
			itemIdSeen = false;
			itemSizeSeen = false;
			oid.setLength(0);
			strSize.setLength(0);
			break;
		case S3Api.QNAME_ITEM_ID:
			requireItemChild(qName, parent);
			if (itemIdSeen) {
				throw new SAXException("Duplicate Key in S3 Contents entry");
			}
			itemIdSeen = true;
			activeScalar = qName;
			break;
		case S3Api.QNAME_ITEM_SIZE:
			requireItemChild(qName, parent);
			if (itemSizeSeen) {
				throw new SAXException("Duplicate Size in S3 Contents entry");
			}
			itemSizeSeen = true;
			activeScalar = qName;
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			requireDirectChild(qName, parent, S3Api.QNAME_LIST_BUCKET_RESULT);
			if (isInsideItem) {
				throw new SAXException("IsTruncated is invalid inside S3 Contents");
			}
			if (truncateFlagSeen) {
				throw new SAXException("Duplicate IsTruncated in S3 listing");
			}
			truncateFlagSeen = true;
			truncateFlag.setLength(0);
			activeScalar = qName;
			break;
		case S3Api.QNAME_LIST_BUCKET_RESULT:
			throw new SAXException("Nested S3 ListBucketResult root");
		default:
			break;
		}
		elementStack.push(qName);
	}

	private void requireItemChild(final String child, final String parent) throws SAXException {
		if (!isInsideItem) {
			throw new SAXException(child + " is invalid outside S3 Contents");
		}
		requireDirectChild(child, parent, S3Api.QNAME_ITEM);
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
	public void endElement(
					final String uri, final String localName, final String qName) throws SAXException {
		if (elementStack.isEmpty() || !qName.equals(elementStack.peek())) {
			throw new SAXException("Mismatched S3 listing element close: " + qName);
		}
		switch (qName) {
		case S3Api.QNAME_ITEM_ID:
			activeScalar = null;
			break;
		case S3Api.QNAME_ITEM_SIZE:
			activeScalar = null;
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			activeScalar = null;
			final String value = truncateFlag.toString().trim();
			if (!"true".equals(value) && !"false".equals(value)) {
				throw new SAXException("Invalid IsTruncated value: " + value);
			}
			isTruncatedFlag = Boolean.parseBoolean(value);
			break;
		case S3Api.QNAME_ITEM:
			finishItem();
			isInsideItem = false;
			break;
		case S3Api.QNAME_LIST_BUCKET_RESULT:
			if (elementStack.size() != 1) {
				throw new SAXException("S3 listing root closed at an invalid hierarchy position");
			}
			rootClosed = true;
			break;
		default:
			break;
		}
		elementStack.pop();
	}

	private void finishItem() throws SAXException {
		if (!itemIdSeen || oid.length() == 0) {
			throw new SAXException("S3 Contents entry is missing a nonempty Key");
		}
		if (!itemSizeSeen) {
			throw new SAXException("S3 Contents entry is missing Size for key \"" + oid + "\"");
		}
		final long size;
		try {
			size = Long.parseLong(strSize.toString().trim());
		} catch (final NumberFormatException e) {
			throw new SAXException("Invalid object size for key \"" + oid + "\"", e);
		}
		if (size < 0) {
			throw new SAXException("Negative object size for key \"" + oid + "\"");
		}
		final long offset;
		try {
			offset = Long.parseLong(generatedItemId(oid.toString()), idRadix);
		} catch (final NumberFormatException e) {
			throw new SAXException(
							"Failed to parse the generated item id from object key \"" + oid + "\"", e);
		}
		pageItems.add(itemFactory.getItem(path + oid, offset, size));
	}

	private String generatedItemId(final String objectKey) throws NumberFormatException {
		String relativeKey = objectKey;
		if (!prefix.isEmpty()) {
			if (!relativeKey.startsWith(prefix)) {
				throw new NumberFormatException(
								"Object key does not start with the configured prefix \"" + prefix + "\"");
			}
			relativeKey = relativeKey.substring(prefix.length());
		}
		final int lastSlash = relativeKey.lastIndexOf('/');
		if (lastSlash >= 0) {
			relativeKey = relativeKey.substring(lastSlash + 1);
		}
		if (relativeKey.isEmpty()) {
			throw new NumberFormatException("Object key has no generated leaf id");
		}
		return relativeKey;
	}

	@Override
	public void characters(final char[] buff, final int start, final int length) {
		if (S3Api.QNAME_ITEM_ID.equals(activeScalar)) {
			oid.append(buff, start, length);
		} else if (S3Api.QNAME_ITEM_SIZE.equals(activeScalar)) {
			strSize.append(buff, start, length);
		} else if (S3Api.QNAME_IS_TRUNCATED.equals(activeScalar)) {
			truncateFlag.append(buff, start, length);
		}
	}

	@Override
	public void endDocument() throws SAXException {
		if (!documentElementSeen || !rootClosed) {
			throw new SAXException("S3 listing has no complete ListBucketResult root");
		}
		if (!elementStack.isEmpty()) {
			throw new SAXException("S3 listing ended with unclosed elements");
		}
		if (isInsideItem) {
			throw new SAXException("S3 listing ended inside a Contents entry");
		}
		if (!truncateFlagSeen) {
			throw new SAXException("S3 listing is missing IsTruncated");
		}
		itemsBuffer.addAll(pageItems);
	}

	@Override
	public boolean isTruncated() {
		return isTruncatedFlag;
	}
}
