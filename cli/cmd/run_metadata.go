package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/cmdline"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// runMetadata captures the launch context for a single spt run.
type runMetadata struct {
	GeneratedAt          time.Time              `json:"generatedAt"`
	WorkloadType         string                 `json:"workloadType"`
	SptImage             string                 `json:"sptImage"`
	APIPort              string                 `json:"apiPort"`
	BaseURL              string                 `json:"baseUrl"`
	TraceFile            string                 `json:"traceFile,omitempty"`
	TraceAuto            bool                   `json:"traceAuto,omitempty"`
	Label                string                 `json:"label"`
	ResultsDir           string                 `json:"resultsDir"`
	ResultsRoot          string                 `json:"resultsRoot"`
	ScenarioFile         string                 `json:"scenarioFile"`
	ScenarioStoredPath   string                 `json:"scenarioStoredPath,omitempty"`
	ScenarioParams       scenario.Params        `json:"scenarioParams"`
	Hosts                []runHostMetadata      `json:"hosts"`
	TestHosts            string                 `json:"testHosts"`
	ExpectedStepIDs      []string               `json:"expectedStepIds,omitempty"`
	ActualStepIDs        []string               `json:"actualStepIds,omitempty"`
	DiscoveredStepIDs    []string               `json:"discoveredStepIds,omitempty"`
	ResultsOptions       resultsOptionsSnapshot `json:"resultsOptions"`
	CLI                  runCLIInfo             `json:"cli"`
	MultiHost            *runMultiHostMetadata  `json:"multiHost,omitempty"`
	AutoTerminateSeconds int                    `json:"autoTerminateSeconds,omitempty"`
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
	Command      []string          `json:"command"`
	ChangedFlags map[string]string `json:"changedFlags,omitempty"`
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
		Command:      sanitizeCommandArgs(os.Args),
		ChangedFlags: captureChangedFlags(in.Command),
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

func captureChangedFlags(cmd *cobra.Command) map[string]string {
	if cmd == nil {
		return nil
	}
	changed := map[string]string{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if !f.Changed {
			return
		}
		val := f.Value.String()
		if isSensitiveFlag(f.Name) {
			val = maskSecret(val)
		}
		changed[f.Name] = val
	})
	if len(changed) == 0 {
		return nil
	}
	return changed
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

func writeRunMetadata(meta *runMetadata, root string) error {
	if meta == nil {
		return nil
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
