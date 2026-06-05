package replay

import "net/http"

const (
	defaultLabel = "replay"

	severityWarning = "warning"
	severityError   = "error"
)

// Options contains user/local runtime inputs for importing and generating a replay.
type Options struct {
	SourceURL     string
	Endpoints     []string
	AccessKey     string
	SecretKey     string
	Bucket        string
	AuthVersion   int
	TestHosts     string
	Label         string
	S3Driver      string
	BaseTimestamp string
	HTTPClient    *http.Client
}

// Diagnostic describes a conversion/import issue surfaced during replay planning.
type Diagnostic struct {
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// RunScript is the parsed, sanitized shape of an archived run script.
type RunScript struct {
	Exports          map[string]string
	ScenarioPath     string
	ItemOutputPath   string
	StorageNodeAddrs string
	ClientAddrs      string
}

// Artifacts contains fetched PDC artifacts required for replay conversion.
type Artifacts struct {
	FolderURL      string
	RunScriptName  string
	ScenarioName   string
	RunScript      RunScript
	ScenarioBody   []byte
	ScenarioFormat string
}

// StepSummary records the replay-visible shape of a converted step.
type StepSummary struct {
	ArchiveID   string `json:"archiveId,omitempty"`
	StepID      string `json:"stepId"`
	Operation   string `json:"operation"`
	Size        string `json:"size,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	Duration    string `json:"duration,omitempty"`
	Count       int64  `json:"count,omitempty"`
}

// PathRewrite records an item-file path conversion from legacy labels to canonical step IDs.
type PathRewrite struct {
	ArchiveID string `json:"archiveId"`
	StepID    string `json:"stepId"`
	From      string `json:"from"`
	To        string `json:"to"`
}

// Generated is the complete generate-only replay output.
type Generated struct {
	Artifacts       Artifacts
	ScenarioJS      []byte
	DefaultsYAML    []byte
	MetadataJSON    []byte
	Preflight       string
	Diagnostics     []Diagnostic
	Steps           []StepSummary
	PathRewrites    []PathRewrite
	EffectiveBucket string
}

// OutputPaths records where generated artifacts were written.
type OutputPaths struct {
	Dir      string
	Scenario string
	Defaults string
	Metadata string
}
