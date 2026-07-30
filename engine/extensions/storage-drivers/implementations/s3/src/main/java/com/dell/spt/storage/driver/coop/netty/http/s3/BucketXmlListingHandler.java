package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;

import org.apache.logging.log4j.Level;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.List;

/**
Created by andrey on 02.12.16.
*/
public final class BucketXmlListingHandler<I extends Item>
				extends DefaultHandler implements S3XmlListingHandler {

	private int count = 0;
	private boolean isInsideItem = false;
	private boolean itIsItemId = false;
	private boolean itIsItemSize = false;
	private boolean itIsTruncateFlag = false;
	private boolean isTruncatedFlag = false;
	private String oid = null, strSize = null;
	private long offset;
	private I nextItem;

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
		this.prefix = prefix == null ? "" : prefix;
		this.itemFactory = itemFactory;
		this.idRadix = idRadix;
	}

	@Override
	public final void startElement(
					final String uri, final String localName, final String qName, Attributes attrs) throws SAXException {
		isInsideItem = isInsideItem || S3Api.QNAME_ITEM.equals(qName);
		itIsItemId = isInsideItem && S3Api.QNAME_ITEM_ID.equals(qName);
		itIsItemSize = isInsideItem && S3Api.QNAME_ITEM_SIZE.equals(qName);
		itIsTruncateFlag = S3Api.QNAME_IS_TRUNCATED.equals(qName);
		super.startElement(uri, localName, qName, attrs);
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void endElement(
					final String uri, final String localName, final String qName) throws SAXException {

		itIsItemId = itIsItemId && !S3Api.QNAME_ITEM_ID.equals(qName);
		itIsItemSize = itIsItemSize && !S3Api.QNAME_ITEM_SIZE.equals(qName);
		itIsTruncateFlag = itIsTruncateFlag && !S3Api.QNAME_IS_TRUNCATED.equals(qName);

		if (isInsideItem && S3Api.QNAME_ITEM.equals(qName)) {
			isInsideItem = false;

			long size = -1;

			if (strSize != null && strSize.length() > 0) {
				try {
					size = Long.parseLong(strSize);
				} catch (final NumberFormatException e) {
					LogUtil.exception(
									Level.WARN, e, "Data object size should be a 64 bit number");
				}
			} else {
				Loggers.ERR.trace("No \"{}\" element or empty", S3Api.QNAME_ITEM_SIZE);
			}

			if (oid != null && oid.length() > 0 && size > -1) {
				try {
					offset = Long.parseLong(generatedItemId(oid), idRadix);
				} catch (final NumberFormatException e) {
					throw new SAXException("Failed to parse the generated item id from object key \"" + oid + "\"", e);
				}
				nextItem = itemFactory.getItem(path + oid, offset, size);
				itemsBuffer.add(nextItem);
				count++;
			} else {
				Loggers.ERR.trace("Invalid object id ({}) or size ({})", oid, strSize);
			}
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
			oid = new String(buff, start, length);
		} else if (itIsItemSize) {
			strSize = new String(buff, start, length);
		} else if (itIsTruncateFlag) {
			isTruncatedFlag = Boolean.parseBoolean(new String(buff, start, length));
		}
		super.characters(buff, start, length);
	}

	@Override
	public final boolean isTruncated() {
		return isTruncatedFlag;
	}
}
