package com.dell.spt.storage.driver.coop.netty.http.s3;

import java.io.IOException;
import java.io.InputStream;
import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.EntityResolver2;
import org.xml.sax.helpers.DefaultHandler;

/** Central fail-closed parser for every S3 XML response path. */
final class S3XmlParser {

	private static final String DISALLOW_DOCTYPE = "http://apache.org/xml/features/disallow-doctype-decl";
	private static final String EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
	private static final String EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
	private static final String LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

	private static final EntityResolver2 REJECTING_ENTITY_RESOLVER = new EntityResolver2() {

		@Override
		public InputSource getExternalSubset(final String name, final String baseUri)
						throws SAXException {
			throw externalEntityFailure(name, baseUri);
		}

		@Override
		public InputSource resolveEntity(
						final String name,
						final String publicId,
						final String baseUri,
						final String systemId)
						throws SAXException {
			throw externalEntityFailure(name, systemId);
		}

		@Override
		public InputSource resolveEntity(final String publicId, final String systemId)
						throws SAXException {
			throw externalEntityFailure(publicId, systemId);
		}
	};

	private S3XmlParser() {}

	static void parse(final InputStream input, final DefaultHandler handler)
					throws IOException, ParserConfigurationException, SAXException {
		final SAXParser parser = acquire();
		final var reader = parser.getXMLReader();
		reader.setContentHandler(handler);
		reader.setDTDHandler(handler);
		reader.setErrorHandler(handler);
		reader.setEntityResolver(REJECTING_ENTITY_RESOLVER);
		reader.parse(new InputSource(input));
	}

	static void clearThreadLocalForTest() {
		S3StorageDriver.THREAD_LOCAL_XML_PARSER.remove();
	}

	private static SAXParser acquire() throws ParserConfigurationException, SAXException {
		SAXParser parser = S3StorageDriver.THREAD_LOCAL_XML_PARSER.get();
		if (parser == null) {
			parser = newHardenedParser();
			S3StorageDriver.THREAD_LOCAL_XML_PARSER.set(parser);
		} else {
			try {
				parser.reset();
			} catch (final UnsupportedOperationException e) {
				throw parserConfigurationFailure("S3 XML parser reset is unsupported", e);
			}
		}
		// reset() may restore provider defaults, so this is the single mandatory
		// post-acquisition point for reapplying every fail-closed control.
		applyRequiredControls(parser);
		return parser;
	}

	private static SAXParser newHardenedParser()
					throws ParserConfigurationException, SAXException {
		final SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setNamespaceAware(false);
		try {
			factory.setXIncludeAware(false);
		} catch (final UnsupportedOperationException e) {
			throw parserConfigurationFailure("S3 XML parser cannot disable XInclude", e);
		}
		setRequiredFeature(factory, XMLConstants.FEATURE_SECURE_PROCESSING, true);
		setRequiredFeature(factory, DISALLOW_DOCTYPE, true);
		setRequiredFeature(factory, EXTERNAL_GENERAL_ENTITIES, false);
		setRequiredFeature(factory, EXTERNAL_PARAMETER_ENTITIES, false);
		setRequiredFeature(factory, LOAD_EXTERNAL_DTD, false);
		return factory.newSAXParser();
	}

	private static void applyRequiredControls(final SAXParser parser) throws SAXException {
		parser.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
		parser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
		final var reader = parser.getXMLReader();
		reader.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
		reader.setFeature(DISALLOW_DOCTYPE, true);
		reader.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
		reader.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
		reader.setFeature(LOAD_EXTERNAL_DTD, false);
	}

	private static void setRequiredFeature(
					final SAXParserFactory factory, final String feature, final boolean enabled)
					throws ParserConfigurationException, SAXException {
		factory.setFeature(feature, enabled);
	}

	private static ParserConfigurationException parserConfigurationFailure(
					final String message, final Throwable cause) {
		final var failure = new ParserConfigurationException(message);
		failure.initCause(cause);
		return failure;
	}

	private static SAXException externalEntityFailure(
					final String name, final String systemId) {
		return new SAXException(
						"External entities are forbidden in S3 XML responses: name="
										+ name + ", systemId=" + systemId);
	}
}
