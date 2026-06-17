package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// Generate imports artifacts and generates replay scenario/defaults/metadata.
func Generate(ctx context.Context, opts Options) (*Generated, error) {
	artifacts, err := FetchArtifacts(ctx, opts.SourceURL, opts.HTTPClient)
	if err != nil {
		return nil, ensureClassifiedError(err)
	}
	if strings.TrimSpace(opts.BaseTimestamp) == "" {
		opts.BaseTimestamp = scenario.BaseTimestamp()
	}

	generated, err := convertScenario(artifacts, opts)
	if generated == nil {
		generated = &Generated{}
	}
	generated.Artifacts = artifacts
	generated.Diagnostics = append(generated.Diagnostics, replayWarnings(artifacts, opts)...)
	if err != nil {
		generated.MetadataJSON = buildMetadata(generated)
		generated.Preflight = BuildPreflight(generated, opts)
		return generated, ensureClassifiedError(err)
	}

	localAccess := strings.TrimSpace(opts.AccessKey)
	localSecret := strings.TrimSpace(opts.SecretKey)
	if localAccess == "" {
		localAccess = "local_access_key"
		generated.Diagnostics = append(generated.Diagnostics, Diagnostic{Severity: severityWarning, Message: "local S3 access key not set; generated defaults use a placeholder"})
	}
	if localSecret == "" {
		localSecret = "local_secret_key"
		generated.Diagnostics = append(generated.Diagnostics, Diagnostic{Severity: severityWarning, Message: "local S3 secret key not set; generated defaults use a placeholder"})
	}

	params := scenario.Params{
		WorkloadType: "write",
		Endpoints:    opts.Endpoints,
		AccessKey:    localAccess,
		SecretKey:    localSecret,
		Bucket:       generated.EffectiveBucket,
		Threads:      replayDefaultConcurrency(generated.Steps),
		AuthVersion:  opts.AuthVersion,
		S3Driver:     opts.S3Driver,
		// Legacy Mongoose scenarios use the engine's repeatable data stream by default;
		// replay should not opt into SPT's anti-dedupe stamping unless explicitly designed.
		ObjectDataDedupable: true,
	}
	applyReplayShapeToParams(&params, generated.Steps)
	if len(opts.Endpoints) == 1 {
		params.Endpoint = opts.Endpoints[0]
	}
	defaults, err := scenario.GenerateDefaults(params)
	if err != nil {
		return generated, newClassifiedError(failureReplayDefaultsGeneration, fmt.Sprintf("generate replay defaults: %v", err), err)
	}
	generated.Params = params
	generated.DefaultsYAML = defaults
	generated.MetadataJSON = buildMetadata(generated)
	generated.Preflight = BuildPreflight(generated, opts)
	return generated, nil
}

func convertScenario(artifacts Artifacts, opts Options) (*Generated, error) {
	switch artifacts.ScenarioFormat {
	case "json":
		return ConvertJSON(artifacts.ScenarioBody, artifacts.RunScript, opts)
	case "js":
		return ConvertJS(artifacts.ScenarioBody, artifacts.RunScript, opts)
	default:
		return nil, classifiedErrorf(failureScenarioFormatUnsupported, "scenario format %q is not implemented for replay", artifacts.ScenarioFormat)
	}
}

func replayWarnings(artifacts Artifacts, opts Options) []Diagnostic {
	var warnings []Diagnostic
	switch artifacts.ScenarioFormat {
	case "json":
		warnings = append(warnings, Diagnostic{Severity: severityWarning, Message: "converted legacy JSON scenario to JS"})
	case "js":
		warnings = append(warnings, Diagnostic{Severity: severityWarning, Message: "adapted legacy JS scenario for replay"})
	}
	if strings.HasPrefix(strings.ToLower(artifacts.FolderURL), "http://") {
		warnings = append(warnings, Diagnostic{Severity: severityWarning, Message: "source uses unauthenticated HTTP"})
	}
	if archivedNodes := splitCSV(artifacts.RunScript.StorageNodeAddrs); len(archivedNodes) > 0 && len(opts.Endpoints) > 0 && len(archivedNodes) != len(opts.Endpoints) {
		warnings = append(warnings, Diagnostic{Severity: severityWarning, Message: fmt.Sprintf("local endpoint count (%d) differs from archived storage node count (%d); concurrency was not rescaled", len(opts.Endpoints), len(archivedNodes))})
	}
	localHosts := splitCSV(opts.TestHosts)
	if len(localHosts) == 0 {
		localHosts = []string{"127.0.0.1"}
	}
	if archivedClients := splitCSV(artifacts.RunScript.ClientAddrs); len(archivedClients) > 0 && len(archivedClients) != len(localHosts) {
		warnings = append(warnings, Diagnostic{Severity: severityWarning, Message: fmt.Sprintf("local test-host count (%d) differs from archived client count (%d); workload shape was preserved", len(localHosts), len(archivedClients))})
	}
	return warnings
}

// WriteGenerated writes generated replay artifacts to a private temp or explicit output directory.
func WriteGenerated(g *Generated, outputDir string) (OutputPaths, error) {
	return WriteGeneratedWithOptions(g, WriteGeneratedOptions{
		OutputDir:       outputDir,
		IncludeDefaults: true,
	})
}

// WriteGeneratedWithOptions writes generated replay artifacts with explicit persistence controls.
func WriteGeneratedWithOptions(g *Generated, opts WriteGeneratedOptions) (OutputPaths, error) {
	dir := strings.TrimSpace(opts.OutputDir)
	var err error
	if dir == "" {
		dir, err = os.MkdirTemp("", "spt-replay-*")
		if err != nil {
			return OutputPaths{}, err
		}
	} else if err = os.MkdirAll(dir, 0o700); err != nil {
		return OutputPaths{}, err
	}
	if chmodErr := os.Chmod(dir, 0o700); chmodErr != nil {
		return OutputPaths{}, chmodErr
	}
	paths := OutputPaths{
		Dir:      dir,
		Scenario: filepath.Join(dir, "replay-scenario.js"),
		Defaults: filepath.Join(dir, "defaults.yaml"),
		Metadata: filepath.Join(dir, "replay-metadata.json"),
	}
	if err := os.WriteFile(paths.Scenario, g.ScenarioJS, 0o600); err != nil {
		return OutputPaths{}, err
	}
	if opts.IncludeDefaults {
		if err := os.WriteFile(paths.Defaults, g.DefaultsYAML, 0o600); err != nil {
			return OutputPaths{}, err
		}
	} else {
		paths.Defaults = ""
	}
	if err := os.WriteFile(paths.Metadata, g.MetadataJSON, 0o600); err != nil {
		return OutputPaths{}, err
	}
	return paths, nil
}

// BuildPreflight renders a safe replay preflight summary.
func BuildPreflight(g *Generated, opts Options) string {
	var b strings.Builder
	b.WriteString("Replay preflight\n")
	b.WriteString("\nSource\n")
	fmt.Fprintf(&b, "  Folder: %s\n", g.Artifacts.FolderURL)
	fmt.Fprintf(&b, "  Run script: %s\n", g.Artifacts.RunScriptName)
	fmt.Fprintf(&b, "  Scenario: %s\n", g.Artifacts.ScenarioName)
	fmt.Fprintf(&b, "  Format: %s\n", strings.ToUpper(g.Artifacts.ScenarioFormat))
	b.WriteString("\nTarget\n")
	b.WriteString("  Protocol: S3\n")
	fmt.Fprintf(&b, "  Endpoints: %d configured locally\n", len(opts.Endpoints))
	if archivedNodes := splitCSV(g.Artifacts.RunScript.StorageNodeAddrs); len(archivedNodes) > 0 {
		fmt.Fprintf(&b, "  Archived storage nodes: %d\n", len(archivedNodes))
	}
	localHosts := splitCSV(opts.TestHosts)
	if len(localHosts) == 0 {
		localHosts = []string{"127.0.0.1"}
	}
	fmt.Fprintf(&b, "  Local test hosts: %d\n", len(localHosts))
	if archivedClients := splitCSV(g.Artifacts.RunScript.ClientAddrs); len(archivedClients) > 0 {
		fmt.Fprintf(&b, "  Archived clients: %d\n", len(archivedClients))
	}
	fmt.Fprintf(&b, "  Bucket: %s\n", g.EffectiveBucket)
	b.WriteString("  Auth: local environment\n")
	b.WriteString("\nWorkload\n")
	for _, step := range g.Steps {
		fmt.Fprintf(&b, "  %s -> %s  %s", step.ArchiveID, step.StepID, step.Operation)
		if step.Size != "" {
			fmt.Fprintf(&b, "  size=%s", step.Size)
		}
		if step.Concurrency > 0 {
			fmt.Fprintf(&b, "  concurrency=%d", step.Concurrency)
		}
		if step.Duration != "" {
			fmt.Fprintf(&b, "  duration=%s", step.Duration)
		}
		if step.Count > 0 {
			fmt.Fprintf(&b, "  count=%d", step.Count)
		}
		b.WriteByte('\n')
	}
	if len(g.PathRewrites) > 0 {
		b.WriteString("\nPath rewrites\n")
		for _, rewrite := range g.PathRewrites {
			fmt.Fprintf(&b, "  %s -> %s\n", rewrite.ArchiveID, rewrite.StepID)
		}
	}
	if len(g.CommandOps) > 0 {
		b.WriteString("\nCommand operations\n")
		for _, op := range g.CommandOps {
			fmt.Fprintf(&b, "  - %s: %s", op.Action, safeCommandSummary(op.Command))
			if strings.TrimSpace(op.Detail) != "" {
				fmt.Fprintf(&b, " (%s)", op.Detail)
			}
			b.WriteByte('\n')
		}
	}
	if hasDiagnosticSeverity(g.Diagnostics, severityError) {
		b.WriteString("\nErrors\n")
		for _, d := range g.Diagnostics {
			if d.Severity == severityError {
				b.WriteString("  - ")
				b.WriteString(d.Message)
				b.WriteByte('\n')
			}
		}
	}
	if hasDiagnosticSeverity(g.Diagnostics, severityWarning) {
		b.WriteString("\nWarnings\n")
		for _, d := range g.Diagnostics {
			if d.Severity == severityWarning {
				b.WriteString("  - ")
				b.WriteString(d.Message)
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}

func hasDiagnosticSeverity(diagnostics []Diagnostic, severity string) bool {
	for _, d := range diagnostics {
		if d.Severity == severity {
			return true
		}
	}
	return false
}

func safeCommandSummary(command string) string {
	command = strings.TrimSpace(jsLineComment(command))
	if command == "" {
		return "<empty>"
	}
	return command
}

func replayDefaultConcurrency(steps []StepSummary) int {
	var common int
	for _, step := range steps {
		if !step.concurrencyExplicit || step.Concurrency <= 0 {
			return 1
		}
		if common == 0 {
			common = step.Concurrency
			continue
		}
		if step.Concurrency != common {
			return 1
		}
	}
	if common > 0 {
		return common
	}
	return 1
}

func applyReplayShapeToParams(params *scenario.Params, steps []StepSummary) {
	if params == nil || len(steps) == 0 {
		return
	}
	if size, ok := commonReplaySize(steps); ok {
		params.ObjectSize = size
	}
	if duration, ok := commonReplayDuration(steps); ok {
		params.Duration = duration
	}
	if count, ok := commonReplayCount(steps); ok {
		params.ObjectCount = count
	}
}

func commonReplaySize(steps []StepSummary) (string, bool) {
	var common string
	for _, step := range steps {
		size := strings.TrimSpace(step.Size)
		if size == "" {
			return "", false
		}
		if common == "" {
			common = size
			continue
		}
		if size != common {
			return "", false
		}
	}
	return common, common != ""
}

func commonReplayDuration(steps []StepSummary) (string, bool) {
	var common string
	for _, step := range steps {
		duration := strings.TrimSpace(step.Duration)
		if duration == "" || step.Count > 0 {
			return "", false
		}
		if common == "" {
			common = duration
			continue
		}
		if duration != common {
			return "", false
		}
	}
	return common, common != ""
}

func commonReplayCount(steps []StepSummary) (int, bool) {
	var common int64
	maxInt := int64(^uint(0) >> 1)
	for _, step := range steps {
		if step.Count <= 0 || strings.TrimSpace(step.Duration) != "" || step.Count > maxInt {
			return 0, false
		}
		if common == 0 {
			common = step.Count
			continue
		}
		if step.Count != common {
			return 0, false
		}
	}
	if common == 0 {
		return 0, false
	}
	return int(common), true
}

func buildMetadata(g *Generated) []byte {
	type metadata struct {
		GeneratedAt     string             `json:"generatedAt"`
		SourceURL       string             `json:"sourceUrl"`
		RunScript       string             `json:"runScript"`
		Scenario        string             `json:"scenario"`
		ScenarioFormat  string             `json:"scenarioFormat"`
		EffectiveBucket string             `json:"effectiveBucket"`
		Steps           []StepSummary      `json:"steps"`
		PathRewrites    []PathRewrite      `json:"pathRewrites,omitempty"`
		CommandOps      []CommandOperation `json:"commandOperations,omitempty"`
		Diagnostics     []Diagnostic       `json:"diagnostics,omitempty"`
	}
	data, _ := json.MarshalIndent(metadata{
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		SourceURL:       g.Artifacts.FolderURL,
		RunScript:       g.Artifacts.RunScriptName,
		Scenario:        g.Artifacts.ScenarioName,
		ScenarioFormat:  g.Artifacts.ScenarioFormat,
		EffectiveBucket: g.EffectiveBucket,
		Steps:           g.Steps,
		PathRewrites:    g.PathRewrites,
		CommandOps:      g.CommandOps,
		Diagnostics:     g.Diagnostics,
	}, "", "  ")
	return data
}
