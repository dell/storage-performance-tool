package com.dell.spt.base.metrics.snapshot;

import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.TimingMetricType;
import org.apache.logging.log4j.Level;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// During the test step we only want to aggregate mean metrics. But we also want to know the quantile values.
// Hence each worker during the step writes latency and duration values of successful operation into a local tmp file.
// ItemTimingMetricsOutputAggregator retrieves the data from worker to entry node tmp files. Then this class reads
// local files and creates a histogram for latency or duration and saves the results for specific quantiles.

// we list all the files according to the filepath regex and get either first or second column to get either latency or duration

public class TimingMetricQuantileResultsImpl implements Closeable {
	private final Map<Double, Long> metricsValues;
	private final TimingMetricType metricType;
	private final String metricsFilesDirPath; // e.g. /tmp/spt/
	private final String metricsFilePattern; // e.g. timingMetrics_linear_20210209.183605.102
	private final File[] listOfMetricsFiles;
	//TODO: probably word metrics is excessive here, but arguable

	public TimingMetricQuantileResultsImpl(final List<Double> quantileValues, final TimingMetricType metricType,
					final int nodeAmount, final String metricsFilesDirPath,
					final String metricsFilePattern, final boolean timingPersist) {
		this.metricType = metricType;
		this.metricsFilesDirPath = metricsFilesDirPath;
		this.metricsFilePattern = metricsFilePattern;
		listOfMetricsFiles = findAllWorkersMetricsFiles();

		if (0 == listOfMetricsFiles.length) {
			Loggers.ERR.warn("No local timing metrics files found in {} by pattern {}. " +
							"Skipping lat/dur analysis", metricsFilesDirPath, metricsFilePattern);
			metricsValues = new LinkedHashMap<>(0);
			return;
		}

		if (!timingPersist) {
			metricsValues = new LinkedHashMap<>(0);
			return;
		}

		final List<List<Long>> tmpMetrics = Stream.of(listOfMetricsFiles)
						.map(file -> {
							List<Long> metricsFromFile = null;
							try {
								metricsFromFile = readArrayFromInputStream(new FileInputStream(file));
								Collections.sort(metricsFromFile);
								if (metricsFromFile.isEmpty()) {
									Loggers.ERR.warn("One of the aggregated timing metrics local files is empty: {}", file);
								}
							} catch (final FileNotFoundException e) {
								LogUtil.exception(
												Level.WARN, e, "Failed to find one of the timing metrics files: {}",
												file.toString());
							}
							return metricsFromFile;
						})
						.collect(Collectors.toList());
		if (tmpMetrics.size() != nodeAmount) {
			Loggers.ERR.warn(
							"Expected to aggregate timing metrics from {} node(s), but found {} local file(s) after aggregation for pattern {}",
							nodeAmount, tmpMetrics.size(), metricsFilePattern);
		}

		// merge sorted metrics arrays from each aggregated local tmp file and take the quantile values from it
		// we do not want to store the whole values list in the class as we can eventually reach a few Gb size array of
		// latency/duration values.
		// So we only store a few values of specified quantiles
		metricsValues = retrieveQuantileValues(quantileValues, mergeSort(tmpMetrics));
	}

	private File[] findAllWorkersMetricsFiles() {
		final File dir = new File(metricsFilesDirPath);
		final File[] files = dir.listFiles((ignored, name) -> name.startsWith(metricsFilePattern));
		return files != null ? files : new File[0];
	}

	// FileManagerImpl already has a method to get raw bytes but converting that to List<long> can be cumbersome and wasteful
	// For several reasons: we don't want to keep in memory the whole files, but only the half of it needed for
	// chosen metric
	private List<Long> readArrayFromInputStream(InputStream inputStream) {
		List<Long> tmpArray = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
			String line;
			while ((line = br.readLine()) != null) {
				final Long parsedValue = parseMetricValue(line, metricType);
				if (parsedValue != null) {
					tmpArray.add(parsedValue);
				}
			}
		} catch (IOException e) {
			LogUtil.exception(
							Level.WARN, e, "Failed to read one of the timing metrics files: {}", inputStream.toString());
		}
		return tmpArray;
	}

	private List<Long> mergeSort(final List<List<Long>> listOfMetricsArrays) {

		List<Long> finalSortedMetrics = listOfMetricsArrays.get(0);
		for (int i = 1; i < listOfMetricsArrays.size(); i++) {
			finalSortedMetrics = mergeSortTwoArrays(finalSortedMetrics, listOfMetricsArrays.get(i));
		}
		return finalSortedMetrics;
	}

	private List<Long> mergeSortTwoArrays(final List<Long> arr1, final List<Long> arr2) {
		int i = 0;
		int j = 0;
		int n1 = arr1.size();
		int n2 = arr2.size();
		List<Long> mergeSortedAray = new ArrayList<>(arr1.size() + arr2.size());

		// mergeSort until we reached the end of shorter array
		while (i < n1 && j < n2) {
			if (arr1.get(i) < arr2.get(j))
				mergeSortedAray.add(arr1.get(i++));
			else
				mergeSortedAray.add(arr2.get(j++));
		}

		// if one of the arrays reached the end we can store the leftovers of the bigger array
		if (n1 > n2) {
			while (i < n1) {
				mergeSortedAray.add(arr1.get(i++));
			}
		} else {
			while (j < n2) {
				mergeSortedAray.add(arr2.get(j++));
			}
		}
		return mergeSortedAray;
	}

	// when quantile values are parsed at the start of the test we check that values are in [0,1).
	private Map<Double, Long> retrieveQuantileValues(final List<Double> quantiles, final List<Long> metricsArray) {
		// for the metrics csv output it's important we iterate in the order passed by user to avoid things like:
		// Quantile 0.7:              4542
		// Quantile 0.95:             13961
		// Quantile 0.9:              6521
		// Quantile 0.1:              1679
		// so linkedHashMap is used over HashMap
		final Map<Double, Long> arrayQuantileValues = new LinkedHashMap<>(quantiles.size());
		final int metricsArrayLength = metricsArray.size();
		if (metricsArrayLength == 0) {
			Loggers.ERR.warn("No timing metrics available to compute quantiles for type {}", metricType);
			return arrayQuantileValues;
		}
		for (final Double quantile : quantiles) {
			int index = (int) (quantile * metricsArrayLength);
			if (index >= metricsArrayLength) {
				index = metricsArrayLength - 1;
			}
			arrayQuantileValues.put(quantile, metricsArray.get(index));

		}
		return arrayQuantileValues;
	}

	static Long parseMetricValue(final String line, final TimingMetricType metricType) {
		if (line == null) {
			return null;
		}
		final int targetColumn = switch (metricType) {
		case LATENCY -> 0;
		case DURATION -> 1;
		};
		int index = 0;
		final int length = line.length();
		// Skip leading whitespace
		while (index < length && Character.isWhitespace(line.charAt(index))) {
			index++;
		}
		if (index >= length) {
			return null;
		}
		int currentColumn = 0;
		while (index < length) {
			final int tokenStart = index;
			while (index < length && !Character.isWhitespace(line.charAt(index))) {
				index++;
			}
			if (currentColumn == targetColumn) {
				final String token = line.substring(tokenStart, index);
				try {
					return Long.parseLong(token);
				} catch (NumberFormatException ex) {
					LogUtil.exception(Level.WARN, ex, "Failed to parse timing metric token '{}' for {}", token, metricType);
					return null;
				}
			}
			currentColumn++;
			while (index < length && Character.isWhitespace(line.charAt(index))) {
				index++;
			}
		}
		Loggers.ERR.warn(
						"Timing metrics line '{}' does not contain column {} for {}", line, targetColumn, metricType);
		return null;
	}

	public Map<Double, Long> getMetricsValues() {
		return metricsValues;
	}

	@Override
	public final void close() throws IOException {
		for (File metricsFile : listOfMetricsFiles) {
			if (!metricsFile.delete()) {
				// for some reason java.io.File.delete() method can return false when the file is actually deleted
				if (metricsFile.exists()) {
					throw new FileSystemException(metricsFile.getName(), null, "Failed to delete a metrics file in " + metricsFilesDirPath);
				}
			}
		}
	}
}
