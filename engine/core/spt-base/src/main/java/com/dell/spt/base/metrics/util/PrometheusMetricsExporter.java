package com.dell.spt.base.metrics.util;

import java.util.List;

/** Fluent builder-style exporter for Prometheus metric families. */
public interface PrometheusMetricsExporter {

	PrometheusMetricsExporter quantile(final double value);

	PrometheusMetricsExporter quantiles(final double[] values);

	PrometheusMetricsExporter quantiles(final List<Double> values);

	PrometheusMetricsExporter label(final String name, final String value);

	PrometheusMetricsExporter labels(final String[] names, final String[] values);

	PrometheusMetricsExporter help(final String helpInfo);
}
