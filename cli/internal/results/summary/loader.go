package summary

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"gopkg.in/yaml.v3"
)

// Loader ingests fetched run artifacts into structured data for later aggregation.
type Loader struct {
	artifactBySuffix map[string]*results.ArtifactSpec
}

// NewLoader builds a Loader with defaults that mirror the artifact fetcher.
func NewLoader() *Loader {
	suffixMap := make(map[string]*results.ArtifactSpec, len(results.DefaultArtifacts))
	for i := range results.DefaultArtifacts {
		spec := &results.DefaultArtifacts[i]
		suffixMap[spec.Suffix] = spec
	}
	return &Loader{artifactBySuffix: suffixMap}
}

// RunData captures the raw ingestion outputs required for summary generation.
type RunData struct {
	RunDir               string
	RunID                string
	Manifest             *results.Manifest
	Params               *RunParams
	Steps                map[string]*StepData
	StepOrder            []string
	MissingExpectedSteps []string
	ManifestPath         string
	MetadataPath         string
	EngineInfo           *engineinfo.Manifest
	EngineInfoPath       string
}

// StepData captures per-step artifact availability and metrics totals.
type StepData struct {
	StepID            string
	Manifest          *results.StepManifest
	Metrics           *MetricsTotals
	Delete            *deletemetrics.Metrics
	DeleteEvidence    *DeleteArtifactEvidence
	Status            StepStatus
	MetricsSuppressed bool
	MissingRequired   []string
	MissingOptional   []string
	Notes             []string
}

// StepStatus indicates ingestion completeness for a step.
type StepStatus string

const (
	// StepStatusUnknown indicates the ingestion pipeline has not determined a status yet.
	StepStatusUnknown StepStatus = "unknown"
	// StepStatusComplete means all required artifacts were ingested successfully.
	StepStatusComplete StepStatus = "complete"
	// StepStatusPartial means some required artifacts are missing but others were ingested.
	StepStatusPartial StepStatus = "partial"
	// StepStatusMissing indicates the step was expected but no artifacts were retrieved.
	StepStatusMissing StepStatus = "missing"
	// StepStatusError marks steps where artifacts existed but failed to parse cleanly.
	StepStatusError StepStatus = "error"
)

const (
	fileStatusOK      = "ok"
	fileStatusMissing = "missing"
)

// Load ingests results artifacts located under runDir. The returned RunData is populated
// even when recoverable issues occur; such issues are joined into the returned error.
func (l *Loader) Load(ctx context.Context, runDir string) (*RunData, error) {
	if runDir == "" {
		return nil, errors.New("run directory not provided")
	}
	manifestPath := filepath.Join(runDir, constants.ResultsManifestFileName)
	manifest, err := l.loadManifest(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}

	metadataPath := filepath.Join(runDir, constants.ResultsMetadataFileName)
	params, err := l.loadRunParams(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("load run metadata: %w", err)
	}

	runID := filepath.Base(runDir)
	if params.ResultsRoot != "" {
		runID = filepath.Base(params.ResultsRoot)
	}

	data := &RunData{
		RunDir:       runDir,
		RunID:        runID,
		Manifest:     manifest,
		Params:       params,
		Steps:        make(map[string]*StepData, len(manifest.Steps)),
		ManifestPath: manifestPath,
		MetadataPath: metadataPath,
	}
	if params.EngineInfoFile != "" {
		if params.EngineInfoFile != constants.EngineInfoManifestName || filepath.Base(params.EngineInfoFile) != params.EngineInfoFile {
			return data, fmt.Errorf("engine identity manifest reference %q is invalid", params.EngineInfoFile)
		}
		if !manifestListsRunFile(manifest, params.EngineInfoFile) {
			return data, fmt.Errorf("engine identity manifest is not listed in index.json")
		}
		engineInfoPath := filepath.Join(runDir, params.EngineInfoFile)
		engineIdentity, loadErr := engineinfo.LoadManifest(engineInfoPath)
		if loadErr != nil {
			return data, fmt.Errorf("load engine identity manifest: %w", loadErr)
		}
		if params.EngineConsistency != engineIdentity.Consistency.Status {
			return data, fmt.Errorf(
				"engine identity consistency index %q does not match manifest %q",
				params.EngineConsistency, engineIdentity.Consistency.Status,
			)
		}
		if params.ScenarioParams.RunID != engineIdentity.RunID {
			return data, fmt.Errorf(
				"engine identity manifest run_id %d does not match current run_id %d",
				engineIdentity.RunID, params.ScenarioParams.RunID,
			)
		}
		data.EngineInfo = engineIdentity
		data.EngineInfoPath = engineInfoPath
	}
	if params.DeleteArtifactsVersion != 0 &&
		params.DeleteArtifactsVersion != constants.ResultsDeleteArtifactsVersionV1 &&
		params.DeleteArtifactsVersion != constants.ResultsDeleteArtifactsVersion {
		return data, fmt.Errorf("unsupported DELETE artifact version %d", params.DeleteArtifactsVersion)
	}
	deleteArtifactSteps := stringSet(params.DeleteArtifactStepIDs)
	if params.DeleteArtifactsVersion > 0 &&
		(len(deleteArtifactSteps) == 0 || len(deleteArtifactSteps) != len(params.DeleteArtifactStepIDs) ||
			contains(deleteArtifactSteps, "")) {
		return data, fmt.Errorf("DELETE artifact step identity is missing or invalid")
	}
	if params.DeleteArtifactsVersion > 0 {
		manifestStepCounts := make(map[string]int, len(manifest.Steps))
		for i := range manifest.Steps {
			manifestStepCounts[manifest.Steps[i].StepID]++
		}
		for stepID := range deleteArtifactSteps {
			if manifestStepCounts[stepID] != 1 {
				return data, fmt.Errorf(
					"DELETE artifact step identity %q does not resolve exactly once in fetched manifest", stepID,
				)
			}
		}
	}

	data.StepOrder = make([]string, 0, len(manifest.Steps))

	var stepErrs []error
	for i := range manifest.Steps {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return data, fmt.Errorf("context canceled: %w", ctx.Err())
			default:
			}
		}
		sm := &manifest.Steps[i]
		step := &StepData{
			StepID:   sm.StepID,
			Manifest: sm,
			Status:   StepStatusUnknown,
			Delete:   params.DeleteMetrics[sm.StepID],
		}
		step.MetricsSuppressed = l.metricsSummaryPersistSuppressed(runDir, sm)
		required, optional := l.categorizeMissing(sm)
		if len(required) > 0 {
			step.MissingRequired = append([]string(nil), required...)
		}
		if step.MetricsSuppressed {
			step.MissingRequired = removeString(step.MissingRequired, constants.ResultsArtifactSuffixMetricsTotal)
		}
		if len(optional) > 0 {
			step.MissingOptional = append([]string(nil), optional...)
		}

		metricsEntry := l.findMetricsEntry(sm)
		if metricsEntry != nil && metricsEntry.Status == fileStatusOK {
			metricsPath := filepath.Join(runDir, metricsEntry.Name)
			totals, parseErr := parseMetricsTotals(sm.StepID, metricsPath)
			if parseErr != nil {
				step.Status = StepStatusError
				step.Notes = append(step.Notes, fmt.Sprintf("metrics totals parse error: %v", parseErr))
				stepErrs = append(stepErrs, fmt.Errorf("step %s: %w", sm.StepID, parseErr))
			} else {
				step.Metrics = totals
			}
		} else if !step.MetricsSuppressed {
			step.MissingRequired = appendUnique(step.MissingRequired, constants.ResultsArtifactSuffixMetricsTotal)
			if metricsEntry != nil && metricsEntry.Status != fileStatusOK && metricsEntry.Error != "" {
				step.Notes = append(step.Notes, metricsEntry.Error)
			}
		}

		deleteArtifactsExpected := params.DeleteArtifactsVersion > 0 &&
			contains(deleteArtifactSteps, sm.StepID)
		deleteEvidence, durableDelete, deleteErr := loadDeleteEvidence(
			ctx, runDir, sm, params.DeleteArtifactsVersion, deleteArtifactsExpected,
		)
		if deleteErr != nil {
			step.Status = StepStatusError
			step.Notes = append(step.Notes, fmt.Sprintf("DELETE artifact validation error: %v", deleteErr))
			stepErrs = append(stepErrs, fmt.Errorf("step %s: %w", sm.StepID, deleteErr))
		} else if deleteEvidence != nil {
			step.DeleteEvidence = deleteEvidence
			if deleteArtifactsExpected && step.Delete == nil {
				deleteErr = fmt.Errorf("schema-v4 DELETE metrics unavailable")
			} else {
				step.Delete, deleteErr = preferDeleteTotalsV1(step.Delete, durableDelete, deleteEvidence)
			}
			if deleteErr != nil {
				step.Status = StepStatusError
				step.Notes = append(step.Notes, fmt.Sprintf("DELETE totals validation error: %v", deleteErr))
				stepErrs = append(stepErrs, fmt.Errorf("step %s: %w", sm.StepID, deleteErr))
			}
		}

		if step.Status == StepStatusUnknown {
			if len(step.MissingRequired) > 0 {
				step.Status = StepStatusPartial
			} else {
				step.Status = StepStatusComplete
			}
		}

		data.Steps[step.StepID] = step
		data.StepOrder = append(data.StepOrder, step.StepID)
	}

	expected := make(map[string]struct{}, len(params.ExpectedStepIDs))
	for _, id := range params.ExpectedStepIDs {
		expected[id] = struct{}{}
	}
	for id := range data.Steps {
		delete(expected, id)
	}
	if len(expected) > 0 {
		data.MissingExpectedSteps = make([]string, 0, len(expected))
		for id := range expected {
			data.MissingExpectedSteps = append(data.MissingExpectedSteps, id)
		}
		sort.Strings(data.MissingExpectedSteps)
	}

	if len(stepErrs) > 0 {
		return data, errors.Join(stepErrs...)
	}
	return data, nil
}

func manifestListsRunFile(manifest *results.Manifest, name string) bool {
	for _, file := range manifest.RunFiles {
		if file.Name == name && file.Status == fileStatusOK {
			return true
		}
	}
	return false
}

func (l *Loader) loadManifest(path string) (*results.Manifest, error) {
	content, err := os.ReadFile(path) // #nosec G304 -- path restricted to sanitized results directory
	if err != nil {
		return nil, err
	}
	man := &results.Manifest{}
	if err := json.Unmarshal(content, man); err != nil {
		return nil, err
	}
	return man, nil
}

func (l *Loader) loadRunParams(path string) (*RunParams, error) {
	content, err := os.ReadFile(path) // #nosec G304 -- path restricted to sanitized results directory
	if err != nil {
		return nil, err
	}
	params := &RunParams{}
	if err := json.Unmarshal(content, params); err != nil {
		return nil, err
	}
	return params, nil
}

func (l *Loader) categorizeMissing(sm *results.StepManifest) (required []string, optional []string) {
	for _, file := range sm.Files {
		suffix, spec := l.matchStepSuffix(sm.StepID, file.Name)
		if spec == nil {
			continue
		}
		if file.Status == fileStatusOK {
			continue
		}
		if spec.Required {
			required = append(required, suffix)
		} else {
			optional = append(optional, suffix)
		}
	}
	if len(required) > 1 {
		sort.Strings(required)
	}
	if len(optional) > 1 {
		sort.Strings(optional)
	}
	return required, optional
}

func (l *Loader) matchStepSuffix(stepID, name string) (string, *results.ArtifactSpec) {
	for suffix, spec := range l.artifactBySuffix {
		if name == stepID+"."+suffix {
			return suffix, spec
		}
	}
	return l.matchSuffix(name)
}

func (l *Loader) matchSuffix(name string) (string, *results.ArtifactSpec) {
	var bestSuffix string
	var bestSpec *results.ArtifactSpec
	for suffix, spec := range l.artifactBySuffix {
		if strings.HasSuffix(name, suffix) && len(suffix) > len(bestSuffix) {
			bestSuffix = suffix
			bestSpec = spec
		}
	}
	return bestSuffix, bestSpec
}

func (l *Loader) findMetricsEntry(sm *results.StepManifest) *results.FileStatus {
	expectedName := sm.StepID + "." + constants.ResultsArtifactSuffixMetricsTotal
	for i := range sm.Files {
		fs := &sm.Files[i]
		if fs.Name == expectedName {
			return fs
		}
	}
	return nil
}

func (l *Loader) findConfigEntry(sm *results.StepManifest) *results.FileStatus {
	for i := range sm.Files {
		fs := &sm.Files[i]
		if strings.HasSuffix(fs.Name, constants.ResultsArtifactSuffixConfig) {
			return fs
		}
	}
	return nil
}

type stepConfigProbe struct {
	Output struct {
		Metrics struct {
			Summary struct {
				Persist *bool `yaml:"persist"`
			} `yaml:"summary"`
		} `yaml:"metrics"`
	} `yaml:"output"`
}

func (l *Loader) metricsSummaryPersistSuppressed(runDir string, sm *results.StepManifest) bool {
	entry := l.findConfigEntry(sm)
	if entry == nil || entry.Status != fileStatusOK {
		return false
	}
	content, err := os.ReadFile(filepath.Join(runDir, entry.Name)) // #nosec G304 -- path restricted to fetched results directory
	if err != nil {
		return false
	}
	var config stepConfigProbe
	if err := yaml.Unmarshal(content, &config); err != nil {
		return false
	}
	return config.Output.Metrics.Summary.Persist != nil && !*config.Output.Metrics.Summary.Persist
}

func appendUnique(list []string, value string) []string {
	for _, v := range list {
		if v == value {
			return list
		}
	}
	return append(list, value)
}

func removeString(list []string, value string) []string {
	if len(list) == 0 {
		return list
	}
	out := list[:0]
	for _, item := range list {
		if item != value {
			out = append(out, item)
		}
	}
	return out
}

// RunParams mirrors the structure of spt_run_params.json for ingestion purposes.
type RunParams struct {
	GeneratedAt            time.Time                         `json:"generatedAt"`
	WorkloadType           string                            `json:"workloadType"`
	SptImage               string                            `json:"sptImage"`
	APIPort                string                            `json:"apiPort"`
	BaseURL                string                            `json:"baseUrl"`
	TraceFile              string                            `json:"traceFile"`
	TraceAuto              bool                              `json:"traceAuto"`
	Label                  string                            `json:"label"`
	ResultsDir             string                            `json:"resultsDir"`
	ResultsRoot            string                            `json:"resultsRoot"`
	ScenarioFile           string                            `json:"scenarioFile"`
	ScenarioStoredPath     string                            `json:"scenarioStoredPath"`
	ScenarioParams         ScenarioParams                    `json:"scenarioParams"`
	Hosts                  []RunHost                         `json:"hosts"`
	TestHosts              string                            `json:"testHosts"`
	ExpectedStepIDs        []string                          `json:"expectedStepIds"`
	ActualStepIDs          []string                          `json:"actualStepIds"`
	DiscoveredStepIDs      []string                          `json:"discoveredStepIds"`
	DeleteMetrics          map[string]*deletemetrics.Metrics `json:"deleteMetrics,omitempty"`
	DeleteArtifactsVersion int                               `json:"deleteArtifactsVersion,omitempty"`
	DeleteArtifactStepIDs  []string                          `json:"deleteArtifactStepIds,omitempty"`
	ResultsOptions         RunResultsOptions                 `json:"resultsOptions"`
	CLI                    RunCLI                            `json:"cli"`
	MultiHost              RunMultiHost                      `json:"multiHost"`
	EngineInfoFile         string                            `json:"engineInfoFile,omitempty"`
	EngineConsistency      engineinfo.ConsistencyStatus      `json:"engineConsistency,omitempty"`
}

// ScenarioParams captures key scenario tunables stored with the run.
type ScenarioParams struct {
	RunID          int64    `json:"RunID"`
	WorkloadType   string   `json:"WorkloadType"`
	Endpoint       string   `json:"Endpoint"`
	Endpoints      []string `json:"Endpoints"`
	AccessKey      string   `json:"AccessKey"`
	SecretKey      string   `json:"SecretKey"`
	Bucket         string   `json:"Bucket"`
	Prefix         string   `json:"Prefix"`
	Threads        int      `json:"Threads"`
	ObjectSize     string   `json:"ObjectSize"`
	ObjectCount    int64    `json:"ObjectCount"`
	GetDistrib     int      `json:"GetDistrib"`
	PutDistrib     int      `json:"PutDistrib"`
	DeleteDistrib  int      `json:"DeleteDistrib"`
	StatDistrib    int      `json:"StatDistrib"`
	Duration       string   `json:"Duration"`
	Cleanup        bool     `json:"Cleanup"`
	KeepScenario   bool     `json:"KeepScenario"`
	SliceEndpoints bool     `json:"SliceEndpoints"`
}

// RunHost describes a host entry in the captured metadata.
type RunHost struct {
	Host       string `json:"host"`
	User       string `json:"user"`
	IsLocal    bool   `json:"isLocal"`
	Original   string `json:"original"`
	DockerHost string `json:"dockerHost"`
}

// RunCLI captures the command vector and sanitized flag values.
type RunCLI struct {
	Version      string            `json:"version,omitempty"`
	Revision     string            `json:"revision,omitempty"`
	BuildTime    string            `json:"buildTime,omitempty"`
	Command      []string          `json:"command"`
	ChangedFlags map[string]string `json:"changedFlags"`
}

// RunResultsOptions stores auto-results settings used during ingestion.
type RunResultsOptions struct {
	AutoResults           bool   `json:"autoResults"`
	ResultsDir            string `json:"resultsDir"`
	Label                 string `json:"label"`
	Debug                 bool   `json:"debug"`
	ShutdownOnComplete    bool   `json:"shutdownOnComplete"`
	ShutdownLingerSeconds int    `json:"shutdownLingerSeconds"`
}

// RunMultiHost records the multi-host orchestration settings.
type RunMultiHost struct {
	Enabled        bool   `json:"enabled"`
	HostCount      int    `json:"hostCount"`
	MinHosts       int    `json:"minHosts"`
	AttachExisting bool   `json:"attachExisting"`
	NetworkMode    string `json:"networkMode"`
	RMIPortStart   int    `json:"rmiPortStart"`
	RMIPortCount   int    `json:"rmiPortCount"`
}
