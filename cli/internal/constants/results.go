/*
Copyright © 2025 Dell Technologies
*/

// Package constants defines shared constants used across results ingestion and reporting.
package constants

// Results manifest and metadata filenames.
const (
	ResultsManifestFileName         = "index.json"
	ResultsMetadataFileName         = "spt_run_params.json"
	ResultsPreparedDefaultsFileName = "defaults.yaml"
	ResultsSummaryFilePrefix        = "spt_"
	ResultsSummaryFileSuffix        = "_results_summary.txt"
	ResultsSummaryFilePattern       = ResultsSummaryFilePrefix + "%s" + ResultsSummaryFileSuffix
	// MetricsLocalContributorID is the engine's canonical identity for the entry JVM slice.
	MetricsLocalContributorID = "local"
)

// Results artifact suffixes.
const (
	ResultsArtifactSuffixMetricsTotal          = "metrics.total.csv"
	ResultsArtifactSuffixConfig                = "config.yaml"
	ResultsArtifactSuffixCLIArgs               = "cli.args.log"
	ResultsArtifactSuffixMessages              = "messages.log"
	ResultsArtifactSuffixErrors                = "errors.log"
	ResultsArtifactSuffixMetrics               = "metrics.csv"
	ResultsArtifactSuffixScenario              = "scenario.txt"
	ResultsArtifactSuffixMetricsThreshold      = "metrics.threshold.total.csv"
	ResultsArtifactSuffixOpTrace               = "op.trace.csv"
	ResultsArtifactSuffixWritten               = "written.csv"
	ResultsArtifactSuffixWrittenCompletion     = "written.complete.json"
	ResultsArtifactSuffixVerifyInput           = "verify-input.csv"
	ResultsArtifactSuffixVerifyInputCompletion = "verify-input.complete.json"
	ResultsArtifactSuffixVerified              = "verified.csv"
	ResultsArtifactSuffixVerifiedCompletion    = "verified.complete.json"
	ResultsArtifactSuffixVerifyRemaining       = "verify-remaining.csv"
	ResultsArtifactSuffixIntegrityFailures     = "integrity.failures.csv"
	ResultsArtifactSuffixIntegrityPerformance  = "integrity.performance.csv"
	ResultsArtifactSuffixMultipartLifecycle    = "multipart.lifecycle.csv"
	ResultsArtifactSuffixMultipart             = "multipart.csv"
	ResultsArtifactSuffixTablesMetrics         = "tables.metrics.log"
	ResultsArtifactSuffixItems                 = "items.csv"
	ResultsArtifactSuffixPutRemaining          = "put-remaining.csv"
	ResultsArtifactSuffixExtResults            = "result.xml"
	ResultsArtifactSuffixExtResultsThreshold   = "result-threshold.xml"
)
