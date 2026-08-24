package replay

import (
	"net/http"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const (
	defaultLabel = "replay"

	severityWarning = "warning"
	severityError   = "error"
)

// Options contains user/local runtime inputs for importing and generating a replay.
type Options struct {
	SourceURL     string
	RunID         int64
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
	Code     string `json:"code,omitempty"`
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
	ArchiveID           string `json:"archiveId,omitempty"`
	StepID              string `json:"stepId"`
	Operation           string `json:"operation"`
	Size                string `json:"size,omitempty"`
	Concurrency         int    `json:"concurrency,omitempty"`
	concurrencyExplicit bool
	Duration            string `json:"duration,omitempty"`
	Count               int64  `json:"count,omitempty"`
}

// PathRewrite records an item-file path conversion from legacy labels to canonical step IDs.
type PathRewrite struct {
	ArchiveID string `json:"archiveId"`
	StepID    string `json:"stepId"`
	From      string `json:"from"`
	To        string `json:"to"`
}

// CommandOperation records how an archived command step was handled.
type CommandOperation struct {
	Action  string `json:"action"`
	Command string `json:"command"`
	Detail  string `json:"detail,omitempty"`
}

// Generated is the complete generate-only replay output.
type Generated struct {
	Artifacts       Artifacts
	Params          scenario.Params `json:"-"`
	ScenarioJS      []byte
	DefaultsYAML    []byte
	MetadataJSON    []byte
	Preflight       string
	Diagnostics     []Diagnostic
	Steps           []StepSummary
	PathRewrites    []PathRewrite
	CommandOps      []CommandOperation
	EffectiveBucket string
}

// OutputPaths records where generated artifacts were written.
type OutputPaths struct {
	Dir      string
	Scenario string
	Defaults string
	Metadata string
}

// WriteGeneratedOptions controls which replay artifacts are persisted.
type WriteGeneratedOptions struct {
	OutputDir       string
	IncludeDefaults bool
}
