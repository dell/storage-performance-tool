package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListedObject;
import java.util.ArrayList;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** Strict, all-or-nothing SAX parser for integrity-discovery LIST pages. */
final class ListObjectsXmlHandler extends DefaultHandler {

	private enum EntryKind {
		CONTENT, VERSION, DELETE_MARKER
	}

	private record PageEntry(String key, long size, EntryKind kind) {}

	private final ListOperation<?> listOp;
	private final boolean includeVersions;
	private final boolean fetchMetadata;
	private final StringBuilder text = new StringBuilder();
	private final List<PageEntry> pageEntries = new ArrayList<>();

	private boolean documentElementSeen;
	private boolean rootClosed;
	private EntryKind currentEntry;
	private boolean currentKeySeen;
	private boolean currentSizeSeen;
	private String currentKey;
	private long currentSize;
	private boolean truncationSeen;
	private boolean truncated;
	private boolean nextContinuationTokenSeen;
	private String nextContinuationToken;
	private boolean nextKeyMarkerSeen;
	private String nextKeyMarker;
	private boolean nextVersionIdMarkerSeen;
	private String nextVersionIdMarker;

	ListObjectsXmlHandler(
					final ListOperation<?> listOp, final boolean includeVersions, final boolean fetchMetadata) {
		this.listOp = listOp;
		this.includeVersions = includeVersions;
		this.fetchMetadata = fetchMetadata;
	}

	@Override
	public void startDocument() {
		documentElementSeen = false;
		rootClosed = false;
		currentEntry = null;
		pageEntries.clear();
		truncationSeen = false;
		truncated = false;
		nextContinuationTokenSeen = false;
		nextContinuationToken = null;
		nextKeyMarkerSeen = false;
		nextKeyMarker = null;
		nextVersionIdMarkerSeen = false;
		nextVersionIdMarker = null;
	}

	@Override
	public void startElement(
					final String uri,
					final String localName,
					final String qName,
					final Attributes attributes)
					throws SAXException {
		text.setLength(0);
		if (!documentElementSeen) {
			documentElementSeen = true;
			final String expectedRoot = includeVersions
							? S3Api.QNAME_LIST_VERSIONS_RESULT
							: S3Api.QNAME_LIST_BUCKET_RESULT;
			if (!expectedRoot.equals(qName)) {
				throw new SAXException("Expected S3 " + expectedRoot + " root but found " + qName);
			}
			return;
		}
		switch (qName) {
		case S3Api.QNAME_ITEM:
			startEntry(EntryKind.CONTENT);
			break;
		case S3Api.QNAME_VERSION_ENTRY:
			if (!includeVersions) {
				throw new SAXException("Version entry is invalid in ListBucketResult");
			}
			startEntry(EntryKind.VERSION);
			break;
		case S3Api.QNAME_DELETE_MARKER:
			if (!includeVersions) {
				throw new SAXException("DeleteMarker entry is invalid in ListBucketResult");
			}
			startEntry(EntryKind.DELETE_MARKER);
			break;
		case S3Api.QNAME_ITEM_ID:
			if (currentEntry != null) {
				if (currentKeySeen) {
					throw new SAXException("Duplicate Key in S3 LIST entry");
				}
				currentKeySeen = true;
			}
			break;
		case S3Api.QNAME_ITEM_SIZE:
			if (currentEntry != null) {
				if (currentEntry == EntryKind.DELETE_MARKER) {
					throw new SAXException("DeleteMarker must not contain Size");
				}
				if (currentSizeSeen) {
					throw new SAXException("Duplicate Size in S3 LIST entry");
				}
				currentSizeSeen = true;
			}
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			if (truncationSeen) {
				throw new SAXException("Duplicate IsTruncated in S3 LIST result");
			}
			truncationSeen = true;
			break;
		case S3Api.QNAME_NEXT_CONTINUATION_TOKEN:
			if (nextContinuationTokenSeen) {
				throw new SAXException("Duplicate NextContinuationToken in S3 LIST result");
			}
			nextContinuationTokenSeen = true;
			break;
		case S3Api.QNAME_NEXT_KEY_MARKER:
			if (nextKeyMarkerSeen) {
				throw new SAXException("Duplicate NextKeyMarker in S3 LIST result");
			}
			nextKeyMarkerSeen = true;
			break;
		case S3Api.QNAME_NEXT_VERSION_ID_MARKER:
			if (nextVersionIdMarkerSeen) {
				throw new SAXException("Duplicate NextVersionIdMarker in S3 LIST result");
			}
			nextVersionIdMarkerSeen = true;
			break;
		default:
			break;
		}
	}

	private void startEntry(final EntryKind kind) throws SAXException {
		if (currentEntry != null) {
			throw new SAXException("Nested S3 LIST entry");
		}
		if (includeVersions == (kind == EntryKind.CONTENT)) {
			throw new SAXException("Unexpected " + kind + " entry for S3 LIST result type");
		}
		currentEntry = kind;
		currentKeySeen = false;
		currentSizeSeen = false;
		currentKey = null;
		currentSize = 0;
	}

	@Override
	public void characters(final char[] ch, final int start, final int length) {
		text.append(ch, start, length);
	}

	@Override
	public void endElement(final String uri, final String localName, final String qName)
					throws SAXException {
		switch (qName) {
		case S3Api.QNAME_ITEM_ID:
			if (currentEntry != null) {
				currentKey = exactValue();
			}
			break;
		case S3Api.QNAME_ITEM_SIZE:
			if (currentEntry != null) {
				currentSize = parseSize(grammarValue());
			}
			break;
		case S3Api.QNAME_IS_TRUNCATED:
			truncated = parseBoolean(grammarValue());
			break;
		case S3Api.QNAME_NEXT_CONTINUATION_TOKEN:
			nextContinuationToken = exactValue();
			break;
		case S3Api.QNAME_NEXT_KEY_MARKER:
			nextKeyMarker = exactValue();
			break;
		case S3Api.QNAME_NEXT_VERSION_ID_MARKER:
			nextVersionIdMarker = exactValue();
			break;
		case S3Api.QNAME_ITEM:
			finishEntry(EntryKind.CONTENT);
			break;
		case S3Api.QNAME_VERSION_ENTRY:
			finishEntry(EntryKind.VERSION);
			break;
		case S3Api.QNAME_DELETE_MARKER:
			finishEntry(EntryKind.DELETE_MARKER);
			break;
		case S3Api.QNAME_LIST_BUCKET_RESULT:
		case S3Api.QNAME_LIST_VERSIONS_RESULT:
			rootClosed = true;
			break;
		default:
			break;
		}
		text.setLength(0);
	}

	private void finishEntry(final EntryKind expectedKind) throws SAXException {
		if (currentEntry != expectedKind) {
			throw new SAXException("Mismatched S3 LIST entry close: " + expectedKind);
		}
		if (!currentKeySeen || currentKey == null || currentKey.isEmpty()) {
			throw new SAXException("S3 LIST entry is missing a nonempty Key");
		}
		if (currentEntry != EntryKind.DELETE_MARKER && !currentSizeSeen) {
			throw new SAXException("S3 LIST entry is missing Size for key \"" + currentKey + "\"");
		}
		pageEntries.add(new PageEntry(currentKey, currentSize, currentEntry));
		currentEntry = null;
	}

	@Override
	public void endDocument() throws SAXException {
		if (!documentElementSeen || !rootClosed) {
			throw new SAXException("S3 LIST result has no complete expected root element");
		}
		if (currentEntry != null) {
			throw new SAXException("S3 LIST result ended inside an object entry");
		}
		if (!truncationSeen) {
			throw new SAXException("S3 LIST result is missing IsTruncated");
		}
		if (truncated) {
			if (!includeVersions && isEmpty(nextContinuationToken)) {
				throw new SAXException("Truncated S3 LIST result is missing NextContinuationToken");
			}
			if (includeVersions && (isEmpty(nextKeyMarker) || isEmpty(nextVersionIdMarker))) {
				throw new SAXException(
								"Truncated S3 version LIST result is missing its next markers");
			}
		}
		commitPage();
	}

	private void commitPage() throws SAXException {
		final List<ListedObject> listedObjects = new ArrayList<>();
		String firstKey = null;
		String lastKey = null;
		long bytesTotal = 0;
		for (final PageEntry entry : pageEntries) {
			if (firstKey == null) {
				firstKey = entry.key();
			}
			lastKey = entry.key();
			if (entry.kind() != EntryKind.DELETE_MARKER) {
				listedObjects.add(new ListedObject(entry.key(), entry.size()));
				if (fetchMetadata) {
					try {
						bytesTotal = Math.addExact(bytesTotal, entry.size());
					} catch (final ArithmeticException e) {
						throw new SAXException("S3 LIST byte total exceeds signed 64-bit range", e);
					}
				}
			}
		}
		listOp.objectsListed(pageEntries.size());
		listOp.listedObjects(List.copyOf(listedObjects));
		listOp.bytesListed(fetchMetadata ? bytesTotal : 0);
		listOp.truncated(truncated);
		if (firstKey != null) {
			listOp.pageFirstKey(firstKey);
		}
		listOp.continuationToken(includeVersions ? nextKeyMarker : nextContinuationToken);
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

	private String exactValue() {
		return text.toString();
	}

	private String grammarValue() {
		return text.toString().trim();
	}

	private static boolean parseBoolean(final String value) throws SAXException {
		if (!"true".equals(value) && !"false".equals(value)) {
			throw new SAXException("Invalid IsTruncated value: " + value);
		}
		return Boolean.parseBoolean(value);
	}

	private static long parseSize(final String value) throws SAXException {
		final long result;
		try {
			result = Long.parseLong(value);
		} catch (final NumberFormatException e) {
			throw new SAXException("Invalid S3 LIST object size: " + value, e);
		}
		if (result < 0) {
			throw new SAXException("Negative S3 LIST object size: " + value);
		}
		return result;
	}

	private static boolean isEmpty(final String value) {
		return value == null || value.isEmpty();
	}
}
