package tui

import "github.com/dell/storage-performance-tool/cli/internal/deletemetrics"

// DeleteMetrics preserves the public TUI/API surface for schema-v4 metrics.
type DeleteMetrics = deletemetrics.Metrics

// DeleteCompletionMetrics aliases the shared completion contract.
type DeleteCompletionMetrics = deletemetrics.Completion

// DeleteMetricUnits aliases the shared unit contract.
type DeleteMetricUnits = deletemetrics.Units

// DeleteRequestMetrics aliases the shared request contract.
type DeleteRequestMetrics = deletemetrics.Requests

// DeleteObjectMetrics aliases the shared object contract.
type DeleteObjectMetrics = deletemetrics.Objects

// DeleteBatchMetrics aliases the shared batch contract.
type DeleteBatchMetrics = deletemetrics.Batches

// DeleteVersionMetrics aliases the shared version contract.
type DeleteVersionMetrics = deletemetrics.Versions

// DeleteBucketMetrics aliases the shared bounded bucket contract.
type DeleteBucketMetrics = deletemetrics.Bucket

// DeletePhaseMetrics aliases the shared lifecycle phase contract.
type DeletePhaseMetrics = deletemetrics.Phases

// DeleteResultIdentity aliases the shared run identity contract.
type DeleteResultIdentity = deletemetrics.Identity

// DeleteFailurePolicy aliases the shared failure policy contract.
type DeleteFailurePolicy = deletemetrics.FailurePolicy

// DeleteTimingMetrics aliases the shared request timing contract.
type DeleteTimingMetrics = deletemetrics.Timing

// DeletePerformanceApplicability aliases the shared applicability contract.
type DeletePerformanceApplicability = deletemetrics.Performance

// DeleteVerificationMetrics aliases the shared verification contract.
type DeleteVerificationMetrics = deletemetrics.Verification
