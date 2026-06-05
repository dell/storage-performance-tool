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
		return nil, err
	}
	if artifacts.ScenarioFormat != "json" {
		return nil, fmt.Errorf("scenario format %q is not implemented for replay", artifacts.ScenarioFormat)
	}
	if strings.TrimSpace(opts.BaseTimestamp) == "" {
		opts.BaseTimestamp = scenario.BaseTimestamp()
	}

	generated, err := ConvertJSON(artifacts.ScenarioBody, artifacts.RunScript, opts)
	if generated == nil {
		generated = &Generated{}
	}
	generated.Artifacts = artifacts
	if err != nil {
		return generated, err
	}
	generated.Diagnostics = append(generated.Diagnostics, replayWarnings(artifacts, opts)...)

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
		Threads:      maxConcurrency(generated.Steps),
		AuthVersion:  opts.AuthVersion,
		S3Driver:     opts.S3Driver,
	}
	defaults, err := scenario.GenerateDefaults(params)
	if err != nil {
		return generated, fmt.Errorf("generate replay defaults: %w", err)
	}
	generated.DefaultsYAML = defaults
	generated.MetadataJSON = buildMetadata(generated)
	generated.Preflight = BuildPreflight(generated, opts)
	return generated, nil
}

func replayWarnings(artifacts Artifacts, opts Options) []Diagnostic {
	warnings := []Diagnostic{{Severity: severityWarning, Message: "converted legacy JSON scenario to JS"}}
	if strings.HasPrefix(strings.ToLower(artifacts.FolderURL), "http://") {
		warnings = append(warnings, Diagnostic{Severity: severityWarning, Message: "source uses unauthenticated HTTP; command operations are limited to recognized replay operations"})
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
	dir := strings.TrimSpace(outputDir)
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
	if err := os.WriteFile(paths.Defaults, g.DefaultsYAML, 0o600); err != nil {
		return OutputPaths{}, err
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
	b.WriteString(fmt.Sprintf("  Folder: %s\n", g.Artifacts.FolderURL))
	b.WriteString(fmt.Sprintf("  Run script: %s\n", g.Artifacts.RunScriptName))
	b.WriteString(fmt.Sprintf("  Scenario: %s\n", g.Artifacts.ScenarioName))
	b.WriteString(fmt.Sprintf("  Format: %s\n", strings.ToUpper(g.Artifacts.ScenarioFormat)))
	b.WriteString("\nTarget\n")
	b.WriteString("  Protocol: S3\n")
	b.WriteString(fmt.Sprintf("  Endpoints: %d configured locally\n", len(opts.Endpoints)))
	if archivedNodes := splitCSV(g.Artifacts.RunScript.StorageNodeAddrs); len(archivedNodes) > 0 {
		b.WriteString(fmt.Sprintf("  Archived storage nodes: %d\n", len(archivedNodes)))
	}
	localHosts := splitCSV(opts.TestHosts)
	if len(localHosts) == 0 {
		localHosts = []string{"127.0.0.1"}
	}
	b.WriteString(fmt.Sprintf("  Local test hosts: %d\n", len(localHosts)))
	if archivedClients := splitCSV(g.Artifacts.RunScript.ClientAddrs); len(archivedClients) > 0 {
		b.WriteString(fmt.Sprintf("  Archived clients: %d\n", len(archivedClients)))
	}
	b.WriteString(fmt.Sprintf("  Bucket: %s\n", g.EffectiveBucket))
	b.WriteString("  Auth: local environment\n")
	b.WriteString("\nWorkload\n")
	for _, step := range g.Steps {
		b.WriteString(fmt.Sprintf("  %s -> %s  %s", step.ArchiveID, step.StepID, step.Operation))
		if step.Size != "" {
			b.WriteString(fmt.Sprintf("  size=%s", step.Size))
		}
		if step.Concurrency > 0 {
			b.WriteString(fmt.Sprintf("  concurrency=%d", step.Concurrency))
		}
		if step.Duration != "" {
			b.WriteString(fmt.Sprintf("  duration=%s", step.Duration))
		}
		if step.Count > 0 {
			b.WriteString(fmt.Sprintf("  count=%d", step.Count))
		}
		b.WriteByte('\n')
	}
	if len(g.PathRewrites) > 0 {
		b.WriteString("\nPath rewrites\n")
		for _, rewrite := range g.PathRewrites {
			b.WriteString(fmt.Sprintf("  %s -> %s\n", rewrite.ArchiveID, rewrite.StepID))
		}
	}
	if len(g.Diagnostics) > 0 {
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

func maxConcurrency(steps []StepSummary) int {
	max := 1
	for _, step := range steps {
		if step.Concurrency > max {
			max = step.Concurrency
		}
	}
	return max
}

func buildMetadata(g *Generated) []byte {
	type metadata struct {
		GeneratedAt     string        `json:"generatedAt"`
		SourceURL       string        `json:"sourceUrl"`
		RunScript       string        `json:"runScript"`
		Scenario        string        `json:"scenario"`
		ScenarioFormat  string        `json:"scenarioFormat"`
		EffectiveBucket string        `json:"effectiveBucket"`
		Steps           []StepSummary `json:"steps"`
		PathRewrites    []PathRewrite `json:"pathRewrites,omitempty"`
		Diagnostics     []Diagnostic  `json:"diagnostics,omitempty"`
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
		Diagnostics:     g.Diagnostics,
	}, "", "  ")
	return data
}
