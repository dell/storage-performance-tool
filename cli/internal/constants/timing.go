/*
Copyright © 2025 Dell Technologies
*/

package constants

import "time"

// Timeout constants for various operations
const (
	// API and connection timeouts
	APIPollingTimeout             = 500 * time.Millisecond // Timeout for API polling requests
	APIReadinessTimeout           = 30 * time.Second       // Timeout for waiting for APIs to be ready
	PortCheckTimeout              = 2 * time.Second        // Timeout for checking if a port is open
	AutoResultsUnavailableTimeout = 2 * time.Minute        // Maximum continuous loss of all completion-tracker API signals

	// Container lifecycle timeouts
	ContainerShutdownGrace       = 2 * time.Second // Grace period for container graceful shutdown
	APILingerDefault             = 5 * time.Second // Default /status linger window after /shutdown
	DiagnosticsCollectionTimeout = 5 * time.Minute // Per-host timeout for copying JVM diagnostics artifacts

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
