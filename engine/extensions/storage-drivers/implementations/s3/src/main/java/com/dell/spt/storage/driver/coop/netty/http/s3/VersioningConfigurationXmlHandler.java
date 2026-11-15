package com.dell.spt.storage.driver.coop.netty.http.s3;

import java.io.ByteArrayInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Reads a bucket's {@code GET ?versioning} body: a {@code VersioningConfiguration} root and its
 * {@code Status} child, matched by local name whatever their prefix, namespace declarations or
 * attributes. Anything it can't read with certainty is {@link Kind#UNRECOGNIZED}, which never
 * authorizes a change to the bucket.
 */
final class VersioningConfigurationXmlHandler extends DefaultHandler {

	/** What the body says about the bucket's versioning. */
	enum Kind {
		/** The root has no {@code Status}, or an empty one: the bucket has never been versioned. */
		NEVER, ENABLED, SUSPENDED,
		/** A {@code Status} other than {@code Enabled} or {@code Suspended}. */
		OTHER,
		/** A body that doesn't parse, an empty one, or one with another root or an ambiguous {@code Status}. */
		UNRECOGNIZED
	}

	/** @param status the {@code Status} text, for {@link Kind#OTHER} only */
	record State(Kind kind, String status) {}

	private static final String ROOT = "VersioningConfiguration";
	private static final String STATUS = "Status";

	private int depth;
	private boolean rootMatches;
	private int statuses;
	private boolean inStatus;
	private boolean statusHasChildren;
	private final StringBuilder status = new StringBuilder();

	private VersioningConfigurationXmlHandler() {}

	static State read(final byte[] body) {
		if (body.length == 0) {
			return new State(Kind.UNRECOGNIZED, null);
		}
		final var handler = new VersioningConfigurationXmlHandler();
		try {
			S3XmlParser.parseNamespaceAware(new ByteArrayInputStream(body), handler);
		} catch (final Exception e) {
			return new State(Kind.UNRECOGNIZED, null);
		}
		return handler.state();
	}

	@Override
	public void startElement(final String uri, final String localName, final String qName, final Attributes attributes) {
		depth++;
		if (depth == 1) {
			rootMatches = ROOT.equals(localName);
		} else if (inStatus) {
			statusHasChildren = true;
		} else if (depth == 2 && STATUS.equals(localName)) {
			statuses++;
			inStatus = true;
		}
	}

	@Override
	public void endElement(final String uri, final String localName, final String qName) {
		if (depth == 2 && inStatus) {
			inStatus = false;
		}
		depth--;
	}

	@Override
	public void characters(final char[] ch, final int start, final int length) {
		if (inStatus && statuses == 1) {
			status.append(ch, start, length);
		}
	}

	private State state() {
		if (!rootMatches || statuses > 1 || statusHasChildren) {
			return new State(Kind.UNRECOGNIZED, null);
		}
		final var text = status.toString().strip();
		if (statuses == 0 || text.isEmpty()) {
			return new State(Kind.NEVER, null);
		}
		return switch (text) {
		case "Enabled" -> new State(Kind.ENABLED, null);
		case "Suspended" -> new State(Kind.SUSPENDED, null);
		default -> new State(Kind.OTHER, text);
		};
	}
}
