package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

const (
	dockerDiagnosticsMount              = "/spt-diagnostics"
	dockerDiagnosticsResultsDirName     = "diagnostics"
	dockerDiagnosticsManifestFileName   = "diagnostics_manifest.json"
	dockerDiagnosticsStopTimeoutSeconds = 60
	diagnosticsDefaultHost              = "localhost"
	remoteDiagnosticsBaseDir            = "/tmp/spt-diagnostics"
)

type diagnosticsRecord struct {
	Host               string   `json:"host"`
	Role               string   `json:"role"`
	ContainerID        string   `json:"containerId,omitempty"`
	ContainerDir       string   `json:"containerDir"`
	RemoteDir          string   `json:"remoteDir,omitempty"`
	LocalDir           string   `json:"localDir,omitempty"`
	Files              []string `json:"files,omitempty"`
	Collected          bool     `json:"collected"`
	RemoteDirRemoved   bool     `json:"remoteDirRemoved,omitempty"`
	PreservedRemoteDir bool     `json:"preservedRemoteDir,omitempty"`
	Error              string   `json:"error,omitempty"`
}

type diagnosticsManifest struct {
	SchemaVersion int                 `json:"schemaVersion"`
	GeneratedAt   string              `json:"generatedAt"`
	ContainerDir  string              `json:"containerDir"`
	Records       []diagnosticsRecord `json:"records"`
}

type diagnosticsCollector interface {
	collectDiagnostics(context.Context) (*diagnosticsRecord, error)
	diagnosticsRecord() *diagnosticsRecord
}

func diagnosticsEnabled() bool {
	return strings.TrimSpace(os.Getenv(constants.EnvSptJavaOpts)) != ""
}

func diagnosticsHostComponent(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		host = diagnosticsDefaultHost
	}
	return sanitizeHostComponent(host)
}

func diagnosticsRunID(resultsRoot string) string {
	if strings.TrimSpace(resultsRoot) == "" {
		return fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	return sanitizeHostComponent(filepath.Base(resultsRoot))
}

func diagnosticsLocalDir(resultsRoot, host, role string) string {
	return filepath.Join(
		resultsRoot,
		dockerDiagnosticsResultsDirName,
		diagnosticsHostComponent(host),
		diagnosticsHostComponent(role),
	)
}

func listDiagnosticFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if entry.Name() == dockerDiagnosticsManifestFileName {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	return files, nil
}

func writeDiagnosticsRecordManifest(localDir string, record diagnosticsRecord) error {
	if strings.TrimSpace(localDir) == "" {
		return nil
	}
	if err := os.MkdirAll(localDir, 0o750); err != nil {
		return err
	}
	manifest := diagnosticsManifest{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		ContainerDir:  dockerDiagnosticsMount,
		Records:       []diagnosticsRecord{record},
	}
	return writeDiagnosticsManifestFile(filepath.Join(localDir, dockerDiagnosticsManifestFileName), manifest)
}

func writeDiagnosticsAggregateManifest(resultsRoot string, records []diagnosticsRecord) error {
	if strings.TrimSpace(resultsRoot) == "" || len(records) == 0 {
		return nil
	}
	root := filepath.Join(resultsRoot, dockerDiagnosticsResultsDirName)
	if err := os.MkdirAll(root, 0o750); err != nil {
		return err
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Host == records[j].Host {
			return records[i].Role < records[j].Role
		}
		return records[i].Host < records[j].Host
	})
	manifest := diagnosticsManifest{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		ContainerDir:  dockerDiagnosticsMount,
		Records:       records,
	}
	return writeDiagnosticsManifestFile(filepath.Join(root, dockerDiagnosticsManifestFileName), manifest)
}

func writeDiagnosticsManifestFile(path string, manifest diagnosticsManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}
