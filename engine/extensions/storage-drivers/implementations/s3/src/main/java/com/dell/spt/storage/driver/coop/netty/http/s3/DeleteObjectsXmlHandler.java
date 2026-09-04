package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteTransportTargetResult;
import java.util.ArrayList;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** Strict parser for the identity-bearing entries in an S3 DeleteObjects response. */
final class DeleteObjectsXmlHandler extends DefaultHandler {

	enum FailureClass {
		ENTRY_LIMIT, INVALID_ENTRY, INVALID_STRUCTURE
	}

	enum StructuralContext {
		DOCUMENT_END, DOCUMENT_ROOT, ENTRY_FIELD, RESULT_ENTRY, ROOT_CONTENT
	}

	static final class ParseFailure extends SAXException {

		private final FailureClass failureClass;
		private final StructuralContext structuralContext;

		private ParseFailure(
						final FailureClass failureClass,
						final StructuralContext structuralContext) {
			super("Rejected S3 multi-delete response: " + failureClass + '/' + structuralContext);
			this.failureClass = failureClass;
			this.structuralContext = structuralContext;
		}

		FailureClass failureClass() {
			return failureClass;
		}

		StructuralContext structuralContext() {
			return structuralContext;
		}
	}

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
				throw failure(FailureClass.INVALID_STRUCTURE, StructuralContext.DOCUMENT_ROOT);
			}
			rootSeen = true;
			return;
		}
		if (rootClosed) {
			throw failure(FailureClass.INVALID_STRUCTURE, StructuralContext.ROOT_CONTENT);
		}
		if (scalar != null) {
			throw failure(FailureClass.INVALID_STRUCTURE, StructuralContext.ENTRY_FIELD);
		}
		if (entry == null) {
			if (!DELETED.equals(qName) && !ERROR.equals(qName)) {
				throw failure(FailureClass.INVALID_STRUCTURE, StructuralContext.ROOT_CONTENT);
			}
			if (results.size() >= DeleteRequest.MAX_TARGET_COUNT) {
				throw failure(FailureClass.ENTRY_LIMIT, StructuralContext.RESULT_ENTRY);
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
			throw failure(FailureClass.INVALID_STRUCTURE, StructuralContext.ENTRY_FIELD);
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
				throw failure(FailureClass.INVALID_ENTRY, StructuralContext.RESULT_ENTRY);
			}
			results.add(DELETED.equals(entry)
							? new DeleteTransportTargetResult(key, versionId, true, null)
							: new DeleteTransportTargetResult(key, versionId, false, errorMessage()));
			entry = null;
			return;
		}
		if (ROOT.equals(qName)) {
			if (entry != null || scalar != null) {
				throw failure(FailureClass.INVALID_ENTRY, StructuralContext.RESULT_ENTRY);
			}
			rootClosed = true;
			return;
		}
		throw failure(FailureClass.INVALID_STRUCTURE, StructuralContext.ROOT_CONTENT);
	}

	@Override
	public void endDocument() throws SAXException {
		if (!rootSeen || !rootClosed) {
			throw failure(FailureClass.INVALID_STRUCTURE, StructuralContext.DOCUMENT_END);
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
			throw failure(FailureClass.INVALID_ENTRY, StructuralContext.ENTRY_FIELD);
		}
		return value;
	}

	private static ParseFailure failure(
					final FailureClass failureClass,
					final StructuralContext structuralContext) {
		return new ParseFailure(failureClass, structuralContext);
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
