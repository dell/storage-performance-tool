package tui

// getDisplaySamples is a test-only helper retained for unit tests.
func (cr *GenericChartRenderer) getDisplaySamples(samples []PerformanceMetric, maxSamples int) []PerformanceMetric {
	if len(samples) <= maxSamples {
		return samples
	}
	return samples[len(samples)-maxSamples:]
}
