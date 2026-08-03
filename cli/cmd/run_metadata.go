package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/cmdline"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// runMetadata captures the launch context for a single spt run.
type runMetadata struct {
	GeneratedAt             time.Time                                               `json:"generatedAt"`
	WorkloadType            string                                                  `json:"workloadType"`
	SptImage                string                                                  `json:"sptImage"`
	APIPort                 string                                                  `json:"apiPort"`
	BaseURL                 string                                                  `json:"baseUrl"`
	TraceFile               string                                                  `json:"traceFile,omitempty"`
	TraceAuto               bool                                                    `json:"traceAuto,omitempty"`
	Label                   string                                                  `json:"label"`
	ResultsDir              string                                                  `json:"resultsDir"`
	ResultsRoot             string                                                  `json:"resultsRoot"`
	ScenarioFile            string                                                  `json:"scenarioFile"`
	ScenarioStoredPath      string                                                  `json:"scenarioStoredPath,omitempty"`
	PreparedDefaultsSHA256  string                                                  `json:"preparedDefaultsSha256,omitempty"`
	PreparedDefaultsBytes   int                                                     `json:"preparedDefaultsBytes,omitempty"`
	ScenarioParams          scenario.Params                                         `json:"scenarioParams"`
	Hosts                   []runHostMetadata                                       `json:"hosts"`
	TestHosts               string                                                  `json:"testHosts"`
	ExpectedStepIDs         []string                                                `json:"expectedStepIds,omitempty"`
	ActualStepIDs           []string                                                `json:"actualStepIds,omitempty"`
	DiscoveredStepIDs       []string                                                `json:"discoveredStepIds,omitempty"`
	StepLifecycles          map[string]string                                       `json:"stepLifecycles,omitempty"`
	ResultsOptions          resultsOptionsSnapshot                                  `json:"resultsOptions"`
	CLI                     runCLIInfo                                              `json:"cli"`
	MultiHost               *runMultiHostMetadata                                   `json:"multiHost,omitempty"`
	RuntimeIdentity         *tui.DistributedRuntimeIdentityEvidence                 `json:"runtimeIdentity,omitempty"`
	RuntimeIdentityError    string                                                  `json:"runtimeIdentityError,omitempty"`
	AutoTerminateSeconds    int                                                     `json:"autoTerminateSeconds,omitempty"`
	Lifecycle               *runLifecycleMetadata                                   `json:"lifecycle,omitempty"`
	runtimeIdentityProvider func() (*tui.DistributedRuntimeIdentityEvidence, error) `json:"-"`
	resourceFinalization    *runcontrol.FinalizationOutcome                         `json:"-"`
	preparedCleanup         func(context.Context) error                             `json:"-"`
	preparedInputs          bool                                                    `json:"-"`
	preparedScenarioJS      []byte                                                  `json:"-"`
	preparedDefaultsYAML    []byte                                                  `json:"-"`
}

type runLifecycleMetadata struct {
	Workload            lifecyclePhaseMetadata         `json:"workload"`
	Artifacts           lifecyclePhaseMetadata         `json:"artifacts"`
	Shutdown            lifecyclePhaseMetadata         `json:"shutdown"`
	Diagnostics         lifecyclePhaseMetadata         `json:"diagnostics"`
	Removal             lifecyclePhaseMetadata         `json:"removal"`
	PreparedInputs      lifecyclePhaseMetadata         `json:"preparedInputs"`
	Summary             lifecyclePhaseMetadata         `json:"summary"`
	ResourceDisposition runcontrol.ResourceDisposition `json:"resourceDisposition"`
}

type lifecyclePhaseMetadata struct {
	Started         bool   `json:"started"`
	Completed       bool   `json:"completed"`
	TimedOut        bool   `json:"timedOut"`
	Error           string `json:"error,omitempty"`
	State           string `json:"state,omitempty"`
	FailureStepID   string `json:"failureStepId,omitempty"`
	FailureCategory string `json:"failureCategory,omitempty"`
	FailureMessage  string `json:"failureMessage,omitempty"`
}

type runHostMetadata struct {
	Host       string `json:"host"`
	User       string `json:"user,omitempty"`
	IsLocal    bool   `json:"isLocal"`
	Original   string `json:"original"`
	DockerHost string `json:"dockerHost"`
}

type resultsOptionsSnapshot struct {
	AutoResults        bool   `json:"autoResults"`
	ResultsDir         string `json:"resultsDir"`
	Label              string `json:"label"`
	Debug              bool   `json:"debug"`
	ShutdownOnComplete bool   `json:"shutdownOnComplete"`
	ShutdownLingerSec  int    `json:"shutdownLingerSeconds"`
}

type runCLIInfo struct {
	Command []string `json:"command"`
	// ChangedFlags holds only flags the user explicitly passed on the
	// command line. EnvAppliedFlags holds flags applyEnvDefaultsToRunFlags
	// injected from .env/OS env because the user did not — pflag's
	// FlagSet.Set() marks a flag Changed=true regardless of caller, so
	// these must be tracked separately to keep this distinction (see
	// markEnvApplied/captureChangedFlags).
	ChangedFlags    map[string]string `json:"changedFlags,omitempty"`
	EnvAppliedFlags map[string]string `json:"envAppliedFlags,omitempty"`
}

type runMultiHostMetadata struct {
	Enabled        bool   `json:"enabled"`
	HostCount      int    `json:"hostCount"`
	MinHosts       int    `json:"minHosts"`
	AttachExisting bool   `json:"attachExisting"`
	NetworkMode    string `json:"networkMode,omitempty"`
	RMIPortStart   int    `json:"rmiPortStart,omitempty"`
	RMIPortCount   int    `json:"rmiPortCount,omitempty"`
}

type runMetadataInput struct {
	WorkloadType         string
	Params               scenario.Params
	ScenarioPath         string
	ResultsOptions       ResultsOptions
	HostInfos            []*hostparse.HostInfo
	TestHostsRaw         string
	MinHosts             int
	AttachExisting       bool
	NetworkMode          string
	RMIPortStart         int
	RMIPortCount         int
	APIPort              string
	BaseURL              string
	ExpectedStepIDs      []string
	SptImage             string
	Command              *cobra.Command
	AutoTerminateSeconds int
}

func buildRunMetadata(in runMetadataInput) *runMetadata {
	hosts := make([]runHostMetadata, 0, len(in.HostInfos))
	for _, h := range in.HostInfos {
		if h == nil {
			continue
		}
		hosts = append(hosts, runHostMetadata{
			Host:       h.Host,
			User:       h.User,
			IsLocal:    h.IsLocal,
			Original:   h.Original,
			DockerHost: h.GetDockerHost(),
		})
	}

	paramsSnapshot := maskScenarioParams(in.Params)
	expected := append([]string(nil), in.ExpectedStepIDs...)

	cliInfo := runCLIInfo{
		Command:         sanitizeCommandArgs(os.Args),
		ChangedFlags:    captureChangedFlags(in.Command),
		EnvAppliedFlags: captureEnvAppliedFlags(in.Command),
	}

	meta := &runMetadata{
		WorkloadType:         in.WorkloadType,
		SptImage:             in.SptImage,
		APIPort:              in.APIPort,
		BaseURL:              in.BaseURL,
		Label:                in.ResultsOptions.Label,
		ResultsDir:           in.ResultsOptions.ResultsDir,
		ScenarioFile:         filepath.Base(in.ScenarioPath),
		ScenarioParams:       paramsSnapshot,
		Hosts:                hosts,
		TestHosts:            in.TestHostsRaw,
		ExpectedStepIDs:      expected,
		ResultsOptions:       snapshotResultsOptions(in.ResultsOptions),
		CLI:                  cliInfo,
		AutoTerminateSeconds: in.AutoTerminateSeconds,
	}

	if len(in.HostInfos) > 1 {
		meta.MultiHost = &runMultiHostMetadata{
			Enabled:        true,
			HostCount:      len(in.HostInfos),
			MinHosts:       in.MinHosts,
			AttachExisting: in.AttachExisting,
			NetworkMode:    in.NetworkMode,
			RMIPortStart:   in.RMIPortStart,
			RMIPortCount:   in.RMIPortCount,
		}
	} else {
		meta.MultiHost = &runMultiHostMetadata{
			Enabled:        false,
			HostCount:      len(in.HostInfos),
			MinHosts:       in.MinHosts,
			AttachExisting: in.AttachExisting,
		}
	}

	return meta
}

func snapshotResultsOptions(opts ResultsOptions) resultsOptionsSnapshot {
	return resultsOptionsSnapshot(opts)
}

func maskScenarioParams(p scenario.Params) scenario.Params {
	clone := p
	clone.AccessKey = maskSecret(clone.AccessKey)
	clone.SecretKey = maskSecret(clone.SecretKey)
	clone.EngineOverrides = secretmask.EngineOverrides(clone.EngineOverrides)
	return clone
}

func maskSecret(v string) string {
	if v == "" {
		return ""
	}
	if len(v) <= 3 {
		return maskedPlaceholder
	}
	return v[:3] + maskedPlaceholder
}

// captureChangedFlags returns only flags the user explicitly passed on the
// command line. pflag's FlagSet.Set() marks a flag Changed=true regardless
// of who called it, so flags applyEnvDefaultsToRunFlags injected from
// .env/OS env (recorded via markEnvApplied) are excluded here — see
// captureEnvAppliedFlags for those.
func captureChangedFlags(cmd *cobra.Command) map[string]string {
	if cmd == nil {
		return nil
	}
	changed := map[string]string{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if !f.Changed {
			return
		}
		if _, envApplied := cmd.Annotations[envAppliedAnnotationPrefix+f.Name]; envApplied {
			return
		}
		val := f.Value.String()
		switch {
		case f.Name == flagEngineOverride:
			overrides, err := cmd.Flags().GetStringArray(f.Name)
			if err != nil {
				val = secretmask.EngineOverrideList(val)
			} else {
				val = strings.Join(secretmask.EngineOverrides(overrides), "; ")
			}
		case isSensitiveFlag(f.Name):
			val = maskSecret(val)
		}
		changed[f.Name] = val
	})
	if len(changed) == 0 {
		return nil
	}
	return changed
}

// captureEnvAppliedFlags returns flags applyEnvDefaultsToRunFlags injected
// from .env/OS env because the user did not pass them explicitly.
func captureEnvAppliedFlags(cmd *cobra.Command) map[string]string {
	if cmd == nil || len(cmd.Annotations) == 0 {
		return nil
	}
	applied := map[string]string{}
	for key, val := range cmd.Annotations {
		name, ok := strings.CutPrefix(key, envAppliedAnnotationPrefix)
		if !ok {
			continue
		}
		if name == flagEngineOverride {
			val = secretmask.EngineOverrideList(val)
		} else if isSensitiveFlag(name) {
			val = maskSecret(val)
		}
		applied[name] = val
	}
	if len(applied) == 0 {
		return nil
	}
	return applied
}

func isSensitiveFlag(name string) bool {
	switch name {
	case "secret-key", "access-key":
		return true
	default:
		return false
	}
}

func sanitizeCommandArgs(args []string) []string {
	return cmdline.SanitizeArgs(args)
}

func copyScenarioForResults(srcPath, destDir string) (string, error) {
	if srcPath == "" {
		return "", nil
	}
	stat, err := os.Stat(srcPath)
	if err != nil {
		return "", err
	}
	if stat.IsDir() {
		return "", fmt.Errorf("scenario path %s is a directory", srcPath)
	}

	base := filepath.Base(srcPath)
	dest := filepath.Join(destDir, base)
	srcAbs, _ := filepath.Abs(srcPath)
	destAbs, _ := filepath.Abs(dest)
	if srcAbs == destAbs {
		return base, nil
	}

	src, err := os.Open(srcPath) // #nosec G304 -- srcPath already validated by checkScenario
	if err != nil {
		return "", err
	}
	defer func() { _ = src.Close() }()

	tmp, err := os.CreateTemp(destDir, ".scenario-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := os.Rename(tmpName, dest); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	return base, nil
}

func archivePreparedRunInputs(meta *runMetadata, scenarioPath, destDir string) error {
	if meta == nil || !meta.preparedInputs {
		storedPath, err := copyScenarioForResults(scenarioPath, destDir)
		if meta != nil && err == nil {
			meta.ScenarioStoredPath = storedPath
		}
		return err
	}

	scenarioName := filepath.Base(scenarioPath)
	if scenarioName == "." || scenarioName == string(filepath.Separator) || scenarioName == "" {
		return fmt.Errorf("invalid prepared scenario path %q", scenarioPath)
	}
	var archiveErrs []error
	if err := writePreparedInputDurable(
		filepath.Join(destDir, scenarioName), meta.preparedScenarioJS, 0o600); err != nil {
		archiveErrs = append(archiveErrs, fmt.Errorf("archive prepared scenario: %w", err))
	} else {
		meta.ScenarioStoredPath = scenarioName
	}
	defaultsDigest := sha256.Sum256(meta.preparedDefaultsYAML)
	meta.PreparedDefaultsSHA256 = fmt.Sprintf("%x", defaultsDigest)
	meta.PreparedDefaultsBytes = len(meta.preparedDefaultsYAML)
	return errors.Join(archiveErrs...)
}

func writePreparedInputDurable(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".prepared-input-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	keepTemp := true
	defer func() {
		if keepTemp {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	keepTemp = false
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

func writeRunMetadata(meta *runMetadata, root string) error {
	if meta == nil {
		return nil
	}
	if meta.runtimeIdentityProvider != nil && meta.RuntimeIdentity == nil {
		evidence, err := meta.runtimeIdentityProvider()
		if err != nil {
			meta.RuntimeIdentityError = err.Error()
		} else {
			meta.RuntimeIdentity = evidence
			meta.RuntimeIdentityError = ""
		}
	}
	meta.GeneratedAt = time.Now().UTC()
	meta.ResultsRoot = root

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run metadata: %w", err)
	}
	tmp, err := os.CreateTemp(root, ".runmeta-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp metadata: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write metadata: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close metadata: %w", err)
	}
	finalPath := filepath.Join(root, constants.ResultsMetadataFileName)
	if err := os.Rename(tmpName, finalPath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename metadata: %w", err)
	}
	return nil
}
