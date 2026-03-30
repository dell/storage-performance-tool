/*
Copyright © 2025 Dell Technologies
*/

// Package constants defines shared constants used across results ingestion and reporting.
package constants

// Results manifest and metadata filenames.
const (
	ResultsManifestFileName   = "index.json"
	ResultsMetadataFileName   = "spt_run_params.json"
	ResultsSummaryFilePrefix  = "spt_"
	ResultsSummaryFileSuffix  = "_results_summary.txt"
	ResultsSummaryFilePattern = ResultsSummaryFilePrefix + "%s" + ResultsSummaryFileSuffix
)

// Results artifact suffixes.
const (
	ResultsArtifactSuffixMetricsTotal     = "metrics.total.csv"
	ResultsArtifactSuffixConfig           = "config.yaml"
	ResultsArtifactSuffixCLIArgs          = "cli.args.log"
	ResultsArtifactSuffixMessages         = "messages.log"
	ResultsArtifactSuffixErrors           = "errors.log"
	ResultsArtifactSuffixMetrics          = "metrics.csv"
	ResultsArtifactSuffixScenario         = "scenario.txt"
	ResultsArtifactSuffixMetricsThreshold = "metrics.threshold.total.csv"
	ResultsArtifactSuffixOpTrace          = "op.trace.csv"
	ResultsArtifactSuffixMultipart        = "multipart.csv"
	ResultsArtifactSuffixTablesMetrics    = "tables.metrics.log"
	ResultsArtifactSuffixItems            = "items.csv"
)
