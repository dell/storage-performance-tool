package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.op.deletion.DeleteTransportTargetResult;
import java.util.ArrayList;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** Strict parser for the identity-bearing entries in an S3 DeleteObjects response. */
final class DeleteObjectsXmlHandler extends DefaultHandler {

	private static final String ROOT = "DeleteResult";
	private static final String DELETED = "Deleted";
	private static final String ERROR = "Error";
	private static final String KEY = "Key";
	private static final String VERSION_ID = "VersionId";
	private static final String CODE = "Code";
	private static final String MESSAGE = "Message";
	private static final String DELETE_MARKER = "DeleteMarker";
	private static final String DELETE_MARKER_VERSION_ID = "DeleteMarkerVersionId";

	private final List<DeleteTransportTargetResult> results = new ArrayList<>();
	private final StringBuilder text = new StringBuilder();
	private String entry;
	private String scalar;
	private String key;
	private String versionId;
	private String code;
	private String message;
	private boolean rootSeen;
	private boolean rootClosed;

	List<DeleteTransportTargetResult> results() {
		return results;
	}

	@Override
	public void startDocument() {
		results.clear();
		entry = null;
		scalar = null;
		key = null;
		versionId = null;
		code = null;
		message = null;
		rootSeen = false;
		rootClosed = false;
		text.setLength(0);
	}

	@Override
	public void startElement(
					final String uri,
					final String localName,
					final String qName,
					final Attributes attributes) throws SAXException {
		if (!rootSeen) {
			if (!ROOT.equals(qName)) {
				throw new SAXException("Expected S3 DeleteResult root but found " + qName);
			}
			rootSeen = true;
			return;
		}
		if (rootClosed) {
			throw new SAXException("S3 multi-delete response contains content after its root");
		}
		if (scalar != null) {
			throw new SAXException("Nested element " + qName + " inside S3 multi-delete field " + scalar);
		}
		if (entry == null) {
			if (!DELETED.equals(qName) && !ERROR.equals(qName)) {
				throw new SAXException("Unexpected S3 multi-delete result element " + qName);
			}
			entry = qName;
			key = null;
			versionId = null;
			code = null;
			message = null;
			return;
		}
		if (!KEY.equals(qName)
						&& !VERSION_ID.equals(qName)
						&& !CODE.equals(qName)
						&& !MESSAGE.equals(qName)
						&& !DELETE_MARKER.equals(qName)
						&& !DELETE_MARKER_VERSION_ID.equals(qName)) {
			throw new SAXException("Unexpected S3 multi-delete entry element " + qName);
		}
		scalar = qName;
		text.setLength(0);
	}

	@Override
	public void characters(final char[] chars, final int start, final int length) {
		if (scalar != null) {
			text.append(chars, start, length);
		}
	}

	@Override
	public void endElement(final String uri, final String localName, final String qName)
					throws SAXException {
		if (qName.equals(scalar)) {
			setScalar(qName, text.toString());
			scalar = null;
			text.setLength(0);
			return;
		}
		if (qName.equals(entry)) {
			if (key == null || key.isEmpty()) {
				throw new SAXException("S3 multi-delete response entry is missing its key");
			}
			results.add(DELETED.equals(entry)
							? new DeleteTransportTargetResult(key, versionId, true, null)
							: new DeleteTransportTargetResult(key, versionId, false, errorMessage()));
			entry = null;
			return;
		}
		if (ROOT.equals(qName)) {
			if (entry != null || scalar != null) {
				throw new SAXException("S3 multi-delete response closed with an incomplete entry");
			}
			rootClosed = true;
			return;
		}
		throw new SAXException("Unexpected S3 multi-delete closing element " + qName);
	}

	@Override
	public void endDocument() throws SAXException {
		if (!rootSeen || !rootClosed) {
			throw new SAXException("Incomplete S3 multi-delete response document");
		}
	}

	private void setScalar(final String name, final String value) throws SAXException {
		switch (name) {
		case KEY:
			key = setOnce(key, value, KEY);
			break;
		case VERSION_ID:
			versionId = setOnce(versionId, value, VERSION_ID);
			break;
		case CODE:
			code = setOnce(code, value, CODE);
			break;
		case MESSAGE:
			message = setOnce(message, value, MESSAGE);
			break;
		default:
			// Delete-marker response metadata is not part of the requested identity.
		}
	}

	private static String setOnce(
					final String current, final String value, final String field) throws SAXException {
		if (current != null) {
			throw new SAXException("Duplicate " + field + " in S3 multi-delete response entry");
		}
		return value;
	}

	private String errorMessage() {
		if (code == null || code.isEmpty()) {
			return message;
		}
		if (message == null || message.isEmpty()) {
			return code;
		}
		return code + ": " + message;
	}
}
