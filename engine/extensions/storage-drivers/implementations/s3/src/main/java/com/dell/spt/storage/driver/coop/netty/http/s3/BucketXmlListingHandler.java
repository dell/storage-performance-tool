package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import java.util.List;

/**
Created by andrey on 02.12.16.
*/
public final class BucketXmlListingHandler<I extends Item>
				extends DefaultHandler implements S3XmlListingHandler {

	private boolean isInsideItem = false;
	private boolean itIsItemId = false;
	private boolean itIsItemSize = false;
	private boolean itIsTruncateFlag = false;
	private boolean isTruncatedFlag = false;
	private boolean documentElementSeen;
	private boolean itemIdSeen;
	private boolean itemSizeSeen;
	private boolean truncateFlagSeen;
	private final StringBuilder oid = new StringBuilder();
	private final StringBuilder strSize = new StringBuilder();
	private final StringBuilder truncateFlag = new StringBuilder();

	private final List<I> itemsBuffer;
	private final String path;
	private final String prefix;
	private final ItemFactory<I> itemFactory;
	private final int idRadix;

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
	public final void startElement(
					final String uri, final String localName, final String qName, Attributes attrs) throws SAXException {
		if (!documentElementSeen) {
			documentElementSeen = true;
			if (!S3Api.QNAME_LIST_BUCKET_RESULT.equals(qName)) {
				throw new SAXException("Expected S3 ListBucketResult root but found " + qName);
			}
		}
		if (S3Api.QNAME_ITEM.equals(qName)) {
			if (isInsideItem) {
				throw new SAXException("Nested S3 Contents entry");
			}
			isInsideItem = true;
			itemIdSeen = false;
			itemSizeSeen = false;
			oid.setLength(0);
			strSize.setLength(0);
		} else if (isInsideItem && S3Api.QNAME_ITEM_ID.equals(qName)) {
			if (itemIdSeen) {
				throw new SAXException("Duplicate Key in S3 Contents entry");
			}
			itemIdSeen = true;
			itIsItemId = true;
		} else if (isInsideItem && S3Api.QNAME_ITEM_SIZE.equals(qName)) {
			if (itemSizeSeen) {
				throw new SAXException("Duplicate Size in S3 Contents entry");
			}
			itemSizeSeen = true;
			itIsItemSize = true;
		} else if (S3Api.QNAME_IS_TRUNCATED.equals(qName)) {
			if (truncateFlagSeen) {
				throw new SAXException("Duplicate IsTruncated in S3 listing");
			}
			truncateFlagSeen = true;
			truncateFlag.setLength(0);
			itIsTruncateFlag = true;
		}
		super.startElement(uri, localName, qName, attrs);
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void endElement(
					final String uri, final String localName, final String qName) throws SAXException {

		if (itIsItemId && S3Api.QNAME_ITEM_ID.equals(qName)) {
			itIsItemId = false;
		}
		if (itIsItemSize && S3Api.QNAME_ITEM_SIZE.equals(qName)) {
			itIsItemSize = false;
		}
		if (itIsTruncateFlag && S3Api.QNAME_IS_TRUNCATED.equals(qName)) {
			itIsTruncateFlag = false;
			final String value = truncateFlag.toString().trim();
			if (!"true".equals(value) && !"false".equals(value)) {
				throw new SAXException("Invalid IsTruncated value: " + value);
			}
			isTruncatedFlag = Boolean.parseBoolean(value);
		}

		if (isInsideItem && S3Api.QNAME_ITEM.equals(qName)) {
			isInsideItem = false;
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
				throw new SAXException("Failed to parse the generated item id from object key \"" + oid + "\"", e);
			}
			itemsBuffer.add(itemFactory.getItem(path + oid, offset, size));
		}

		super.endElement(uri, localName, qName);
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
	public final void characters(final char buff[], final int start, final int length)
					throws SAXException {
		if (itIsItemId) {
			oid.append(buff, start, length);
		} else if (itIsItemSize) {
			strSize.append(buff, start, length);
		} else if (itIsTruncateFlag) {
			truncateFlag.append(buff, start, length);
		}
		super.characters(buff, start, length);
	}

	@Override
	public void endDocument() throws SAXException {
		if (!documentElementSeen) {
			throw new SAXException("S3 listing has no document element");
		}
		if (isInsideItem) {
			throw new SAXException("S3 listing ended inside a Contents entry");
		}
		if (!truncateFlagSeen) {
			throw new SAXException("S3 listing is missing IsTruncated");
		}
		super.endDocument();
	}

	@Override
	public final boolean isTruncated() {
		return isTruncatedFlag;
	}
}
