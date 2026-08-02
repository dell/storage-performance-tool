/*
Copyright © 2025 Dell Technologies
*/

package constants

import "time"

// Timeout constants for various operations
const (
	// API and connection timeouts
	APIPollingTimeout               = 500 * time.Millisecond // Timeout for API polling requests
	APIReadinessPollInterval        = 500 * time.Millisecond // Interval between API readiness probes
	APIReadinessTimeout             = 30 * time.Second       // Timeout for waiting for APIs to be ready
	PortCheckTimeout                = 2 * time.Second        // Timeout for checking if a port is open
	AutoResultsTrackerPollInterval  = 500 * time.Millisecond // Completion-tracker polling interval
	AutoResultsTrackerIdleGrace     = 20 * time.Second       // Stable idle duration required for fallback completion
	AutoResultsStartupTimeout       = 2 * time.Minute        // Maximum time to observe the first real run activity
	AutoResultsUnavailableTimeout   = 2 * time.Minute        // Maximum continuous loss of all completion-tracker API signals
	AutoResultsCancelCleanupTimeout = 15 * time.Second       // Best-effort evidence and shutdown budget after cancellation
	AutoResultsDiscoveryInterval    = 500 * time.Millisecond // Step discovery polling interval
	AutoResultsShutdownTimeout      = 30 * time.Second       // Graceful API shutdown request budget
	AutoResultsShutdownSettleDelay  = 1 * time.Second        // Artifact settle delay before shutdown
	AutoResultsOptionalWaitTimeout  = 2 * time.Second        // Optional wait for a background results pass
	ResultsDiscoveryHTTPTimeout     = 5 * time.Second        // Step-identity discovery request timeout
	IntegrityMetricsHTTPTimeout     = 10 * time.Second       // Required corruption-metrics request timeout

	// Container lifecycle timeouts
	ContainerShutdownGrace       = 2 * time.Second        // Grace period for container graceful shutdown
	APILingerDefault             = 5 * time.Second        // Default /status linger window after /shutdown
	APILingerPollInterval        = 200 * time.Millisecond // Interval between post-shutdown status probes
	DiagnosticsCollectionTimeout = 5 * time.Minute        // Per-host timeout for copying JVM diagnostics artifacts

	// Metrics collection intervals
	DefaultMetricsInterval = 500 * time.Millisecond // Default interval for metrics collection
	DefaultStatusInterval  = 2 * time.Second        // Default interval for status updates
	MetricsPollInterval    = 1 * time.Second        // Interval for polling metrics from nodes

	// Metrics poller backoff
	MetricsBackoffInitial = 1 * time.Second        // Initial backoff when a poll fails
	MetricsBackoffMax     = 8 * time.Second        // Maximum backoff between poll attempts
	MetricsBackoffJitter  = 250 * time.Millisecond // Randomized jitter added to backoff delays

	// RMI coordination timeouts
	RMIReadinessTimeout = 30 * time.Second // Timeout for waiting for RMI registries to be ready
	RMIRetryInterval    = 1 * time.Second  // Interval between RMI readiness check retries

	// Health monitoring
	MetricsHealthTimeout = 30 * time.Second // Timeout for metrics health monitoring
)

// AutoResultsTrackerStableConfirmations is the number of unchanged successful
// probes required before a step artifact is considered complete.
const AutoResultsTrackerStableConfirmations = 2
