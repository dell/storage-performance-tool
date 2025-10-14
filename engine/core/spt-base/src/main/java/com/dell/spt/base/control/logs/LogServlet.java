package com.dell.spt.base.control.logs;

import static com.dell.spt.base.Constants.MIB;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.eclipse.jetty.server.InclusiveByteRange.satisfiableRanges;

import com.dell.spt.base.Constants;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender;
import org.apache.logging.log4j.core.async.AsyncLogger;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.InclusiveByteRange;

public final class LogServlet extends HttpServlet {

	private static final String KEY_STEP_ID = "stepId";
	private static final String KEY_LOGGER_NAME = "loggerName";
	private static final Pattern PATTERN_URI_PATH = Pattern.compile(
					"/logs/(?<" + KEY_STEP_ID + ">[\\w\\-_.,;:~=+@]+)/(?<" + KEY_LOGGER_NAME + ">[\\w_.]+)");
	private static final Pattern PATTERN_INDEX = Pattern.compile(
					"/logs/(?<" + KEY_STEP_ID + ">[\\w\\-_.,;:~=+@]+)/index\\.json");
	private static final String PATTERN_STEP_ID_SUBST = "${ctx:" + Constants.KEY_STEP_ID + "}";
	private static final int LOG_PAGE_SIZE_LIMIT = MIB;

	private final Map<String, String> logFileNamePatternByName;

	public LogServlet() {
		logFileNamePatternByName = Arrays.stream(Loggers.class.getFields())
						.map(
										field -> {
											try {
												return field.get(null);
											} catch (final Exception e) {
												throw new AssertionError(e);
											}
										})
						.filter(fieldVal -> fieldVal instanceof Logger)
						.map(o -> (Logger) o)
						.filter(logger -> logger.getName().startsWith(Loggers.BASE))
						.collect(
										Collectors.toMap(
														logger -> logger.getName().substring(Loggers.BASE.length()),
														logger -> ((AsyncLogger) logger)
																		.getAppenders().values().stream()
																		.filter(
																						appender -> appender instanceof RollingRandomAccessFileAppender)
																		.map(
																						appender -> ((RollingRandomAccessFileAppender) appender)
																										.getFilePattern())
																		.findAny()
																		.orElse("")));
	}

	@Override
	protected final void doGet(final HttpServletRequest req, final HttpServletResponse resp)
					throws IOException {
		try {
			// Handle per-step index first
			final String uri = req.getRequestURI();
			final Matcher mIndex = PATTERN_INDEX.matcher(uri);
			if (mIndex.matches()) {
				final String stepId = mIndex.group(KEY_STEP_ID);
				writeIndexJson(resp, stepId);
				resp.setStatus(HttpStatus.OK_200);
				return;
			}

			logFilePath(req)
							.ifPresentOrElse(
											path -> {
												try {
													try {
														respondFileContent(path, req, resp);
														resp.setStatus(HttpStatus.OK_200);
													} catch (final MultipleByteRangesException e) {
														resp.sendError(HttpStatus.RANGE_NOT_SATISFIABLE_416, e.getMessage());
													}
												} catch (final Exception e) {
													throwUnchecked(e);
												}
											},
											() -> {
												try {
													writeLogNamesJson(resp.getOutputStream());
													resp.setStatus(HttpStatus.OK_200);
												} catch (final IOException e) {
													throwUnchecked(e);
												}
											});
		} catch (final NoLoggerException | InvalidUriPathException e) {
			// Uniform 404 for unknown logger/step to simplify client logic
			resp.sendError(HttpStatus.NOT_FOUND_404, e.getMessage());
		} catch (final NoLogFileException e) {
			resp.sendError(HttpStatus.NOT_FOUND_404, e.getMessage());
		}
	}

	@Override
	protected final void doHead(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
		try {
			final var pathOpt = logFilePath(req);
			if (pathOpt.isPresent()) {
				final Path path = pathOpt.get();
				if (Files.exists(path)) {
					try {
						final long size = Files.size(path);
						final long mtime = Files.getLastModifiedTime(path).toMillis();
						resp.setContentLengthLong(size);
						resp.setDateHeader(HttpHeader.LAST_MODIFIED.asString(), mtime);
						resp.setStatus(HttpStatus.OK_200);
					} catch (final IOException ioe) {
						resp.sendError(HttpStatus.INTERNAL_SERVER_ERROR_500, ioe.getMessage());
					}
				} else {
					resp.sendError(HttpStatus.NOT_FOUND_404);
				}
			} else {
				// For HEAD we only support /logs/<stepId>/<logger>
				resp.sendError(HttpStatus.NOT_FOUND_404);
			}
		} catch (final NoLoggerException | InvalidUriPathException e) {
			resp.sendError(HttpStatus.NOT_FOUND_404, e.getMessage());
		} catch (final NoLogFileException e) {
			resp.sendError(HttpStatus.NOT_FOUND_404, e.getMessage());
		}
	}

	@Override
	protected final void doDelete(final HttpServletRequest req, final HttpServletResponse resp)
					throws IOException {
		try {
			logFilePath(req)
							.ifPresentOrElse(
											path -> {
												try {
													Files.delete(path);
													resp.setStatus(HttpStatus.OK_200);
												} catch (final IOException e) {
													resp.setStatus(
																	HttpStatus.INTERNAL_SERVER_ERROR_500, "Failed to delete the log file");
												}
											},
											() -> {
												try {
													resp.sendError(HttpStatus.BAD_REQUEST_400);
												} catch (final IOException e) {
													throwUnchecked(e);
												}
											});

		} catch (final NoLoggerException | InvalidUriPathException e) {
			resp.sendError(HttpStatus.BAD_REQUEST_400, e.getMessage());
		} catch (final NoLogFileException e) {
			resp.sendError(HttpStatus.NOT_FOUND_404, e.getMessage());
		}
	}

	private Optional<Path> logFilePath(final HttpServletRequest req)
					throws NoLoggerException, NoLogFileException, InvalidUriPathException {
		final String reqUri = req.getRequestURI();
		final Matcher matcher = PATTERN_URI_PATH.matcher(reqUri);
		if (matcher.find()) {
			final String stepId = matcher.group(KEY_STEP_ID);
			final String loggerName = matcher.group(KEY_LOGGER_NAME);
			final String logFileNamePattern = logFileNamePatternByName.get(loggerName);
			if (null == logFileNamePattern) {
				throw new NoLoggerException("No such logger: \"" + loggerName + "\"");
			} else if (logFileNamePattern.isEmpty()) {
				throw new NoLogFileException(
								"Unable to determine the log file for the logger \"" + loggerName + "\"");
			} else {
				final String logFile = logFileNamePattern.replace(PATTERN_STEP_ID_SUBST, stepId);
				return Optional.of(Paths.get(logFile));
			}
		} else {
			return Optional.empty();
		}
	}

	private static void respondFileContent(
					final Path filePath, final HttpServletRequest req, final HttpServletResponse resp)
					throws IOException, MultipleByteRangesException, NoLogFileException {
		if (Files.exists(filePath)) {
			final Enumeration<String> rangeHeaders = req.getHeaders(HttpHeader.RANGE.asString());
			final List<InclusiveByteRange> byteRanges = satisfiableRanges(rangeHeaders, Files.size(filePath));
			final long offset;
			final long size;
			if (byteRanges == null || 0 == byteRanges.size()) {
				offset = 0;
				size = LOG_PAGE_SIZE_LIMIT - 1;
			} else if (1 == byteRanges.size()) {
				final InclusiveByteRange byteRange = byteRanges.get(0);
				offset = byteRange.getFirst();
				size = byteRange.getLast() + 1;
			} else {
				throw new MultipleByteRangesException("Unable to process more than 1 range header");
			}
			writeFileRange(filePath, offset, size, resp.getOutputStream(), resp.getBufferSize());
		} else {
			throw new NoLogFileException("The log file doesn't exist");
		}
	}

	private static void writeFileRange(
					final Path filePath,
					final long offset,
					final long size,
					final OutputStream out,
					final int buffSize)
					throws IOException {
		final int sizeLimit = (int) Math.min(size, LOG_PAGE_SIZE_LIMIT);
		try (final InputStream in = Files.newInputStream(filePath)) {

			long skipped = 0;
			while (offset > skipped) {
				skipped += in.skip(offset - skipped);
			}

			final byte[] buff = new byte[Math.min(sizeLimit, buffSize)];
			long done = 0;
			int n;
			while (done < sizeLimit && -1 != (n = in.read(buff))) {
				out.write(buff, 0, n);
				done += n;
			}
		}
	}

	private static void writeLogNamesJson(final OutputStream out)
					throws IOException {
		final var jsonFactory = new JsonFactory();
		final var mapper = new ObjectMapper(jsonFactory);
		mapper
						.configure(SerializationFeature.INDENT_OUTPUT, true)
						.configure(Feature.AUTO_CLOSE_TARGET, false)
						.configure(Feature.FLUSH_PASSED_TO_STREAM, false)
						.writerWithDefaultPrettyPrinter()
						.writeValue(out, Loggers.DESCRIPTIONS_BY_NAME);
		out.write(System.lineSeparator().getBytes());
		out.flush();
	}

	private void writeIndexJson(final HttpServletResponse resp, final String stepId) throws IOException {
		resp.setContentType("application/json");
		final var mapper = new ObjectMapper();
		final var root = mapper.createObjectNode();
		root.put("step_id", stepId);
		final var items = mapper.createArrayNode();
		for (final var e : logFileNamePatternByName.entrySet()) {
			final String logger = e.getKey();
			final String pattern = e.getValue();
			if (pattern == null || pattern.isEmpty())
				continue;
			final String file = pattern.replace(PATTERN_STEP_ID_SUBST, stepId);
			final Path p = Paths.get(file);
			if (!Files.exists(p))
				continue;
			final var item = mapper.createObjectNode();
			item.put("logger", logger);
			item.put("href", "/logs/" + stepId + "/" + logger);
			try {
				item.put("size", Files.size(p));
				final long mtime = Files.getLastModifiedTime(p).toMillis();
				item.put("modified", java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
								.withZone(java.time.ZoneOffset.UTC)
								.format(java.time.Instant.ofEpochMilli(mtime)));
			} catch (final IOException ignore) {}
			String ctype = null;
			try {
				ctype = Files.probeContentType(p);
			} catch (final IOException ignore) {}
			if (ctype == null) {
				final String name = p.getFileName().toString().toLowerCase();
				if (name.endsWith(".csv"))
					ctype = "text/csv";
				else if (name.endsWith(".log") || name.endsWith(".txt"))
					ctype = "text/plain";
				else
					ctype = "application/octet-stream";
			}
			item.put("content_type", ctype);
			items.add(item);
		}
		root.set("items", items);
		resp.getWriter().write(mapper.writeValueAsString(root));
	}
}
