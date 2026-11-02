package server

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// catalogArtifact is one file in a results folder the UI can download.
// Only files with a known SPT suffix are listed; credentials never appear here.
type catalogArtifact struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	Type string `json:"type"`
	Size int64  `json:"size"`
}

type artifactKind struct {
	suffix string
	kind   string
	label  string
}

// artifactKinds is the allow-list of SPT result suffixes the dashboard may offer.
// Order is display preference within a kind; kind decides the UI group label.
var artifactKinds = []artifactKind{
	{constants.ResultsArtifactSuffixMetricsTotal, "csv", "Metrics totals (CSV)"},
	{constants.ResultsArtifactSuffixMetrics, "csv", "Metrics samples (CSV)"},
	{constants.ResultsArtifactSuffixMetricsThreshold, "csv", "Threshold metrics (CSV)"},
	{constants.ResultsArtifactSuffixOpTrace, "csv", "Operation trace (CSV)"},
	{constants.ResultsArtifactSuffixExtResults, "xml", "Engine results (XML)"},
	{constants.ResultsArtifactSuffixExtResultsThreshold, "xml", "Threshold results (XML)"},
	{constants.ResultsArtifactSuffixMessages, "log", "Messages log"},
	{constants.ResultsArtifactSuffixErrors, "log", "Errors log"},
	{constants.ResultsArtifactSuffixCLIArgs, "log", "CLI args log"},
	{constants.ResultsArtifactSuffixConfig, "config", "Engine config"},
	{constants.ResultsArtifactSuffixScenario, "config", "Scenario text"},
}

const (
	artifactKindSummary = "summary"
	artifactKindOther   = "other"
	maxArtifactBytes    = 32 << 20
)

func classifyArtifact(name string) (kind, label string, ok bool) {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, constants.ResultsSummaryFilePrefix) && strings.HasSuffix(lower, constants.ResultsSummaryFileSuffix) {
		return artifactKindSummary, "Results summary", true
	}
	for _, k := range artifactKinds {
		if strings.HasSuffix(lower, "."+strings.ToLower(k.suffix)) {
			return k.kind, k.label, true
		}
	}
	return "", "", false
}

// listCatalogArtifacts returns the downloadable files in a run folder, newest first.
// Symlinks and files over maxArtifactBytes are omitted so the UI never offers a dead link.
func listCatalogArtifacts(dir string) ([]catalogArtifact, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := []catalogArtifact{}
	for _, e := range entries {
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := e.Name()
		kind, label, ok := classifyArtifact(name)
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		if info.Size() > maxArtifactBytes {
			continue
		}
		out = append(out, catalogArtifact{Name: name, Kind: kind, Type: label, Size: info.Size()})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// resolveCatalogArtifactPath returns an absolute path inside run.dir for a basename
// that matches an allow-listed artifact suffix. Symlinks are rejected so an
// allow-listed name cannot be used to read a blocked file via a link.
func resolveCatalogArtifactPath(run catalogRun, name string) (string, bool) {
	if name == "" || name != filepath.Base(name) || strings.Contains(name, "..") || strings.ContainsAny(name, `/\`) {
		return "", false
	}
	if _, _, ok := classifyArtifact(name); !ok {
		return "", false
	}
	full := filepath.Join(run.dir, name)
	info, err := os.Lstat(full)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", false
	}
	if !strings.HasPrefix(full, run.dir+string(filepath.Separator)) && full != run.dir {
		return "", false
	}
	// EvalSymlinks still guards against a directory component that is a symlink.
	resolved, err := filepath.EvalSymlinks(full)
	if err != nil {
		return full, true
	}
	root, err := filepath.EvalSymlinks(run.dir)
	if err != nil {
		root = run.dir
	}
	if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		return "", false
	}
	return resolved, true
}

func (a *app) handleCatalogArtifacts(w http.ResponseWriter, r *http.Request) {
	if !a.loopbackHost(r) {
		writeJSON(w, 403, map[string]any{keyError: msgLoopbackOnly})
		return
	}
	run, ok := a.findCatalogRun(r.URL.Query().Get("id"))
	if !ok {
		writeJSON(w, 404, map[string]any{keyError: msgCatalogRunNotFound})
		return
	}
	items, err := listCatalogArtifacts(run.dir)
	if err != nil {
		writeJSON(w, 500, map[string]any{keyError: "Could not list artifacts for this run."})
		return
	}
	writeJSON(w, 200, map[string]any{keyRunID: run.ID, "artifacts": items, "read_only": true})
}

func (a *app) handleCatalogArtifact(w http.ResponseWriter, r *http.Request) {
	if !a.loopbackHost(r) {
		writeJSON(w, 403, map[string]any{keyError: msgLoopbackOnly})
		return
	}
	run, ok := a.findCatalogRun(r.URL.Query().Get("id"))
	if !ok {
		writeJSON(w, 404, map[string]any{keyError: msgCatalogRunNotFound})
		return
	}
	path, ok := resolveCatalogArtifactPath(run, r.URL.Query().Get("file"))
	if !ok {
		writeJSON(w, 404, map[string]any{keyError: "Artifact not found for this run."})
		return
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		writeJSON(w, 404, map[string]any{keyError: "Artifact not found for this run."})
		return
	}
	if info.Size() > maxArtifactBytes {
		writeJSON(w, 413, map[string]any{keyError: "Artifact is too large to download through the dashboard."})
		return
	}
	f, err := os.Open(path) // #nosec G304 -- path is an allow-listed basename inside a catalog run folder resolved above
	if err != nil {
		writeJSON(w, 500, map[string]any{keyError: "Could not read the artifact."})
		return
	}
	defer func() { _ = f.Close() }()
	name := filepath.Base(path)
	w.Header().Set("Content-Type", artifactContentType(name))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	if !artifactNeedsRedaction(name) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, io.LimitReader(f, maxArtifactBytes))
		return
	}
	data, err := io.ReadAll(io.LimitReader(f, maxArtifactBytes))
	if err != nil {
		writeJSON(w, 500, map[string]any{keyError: "Could not read the artifact."})
		return
	}
	out := redactArtifactBytes(data)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(out)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

func artifactNeedsRedaction(name string) bool {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".log"), strings.HasSuffix(lower, ".txt"),
		strings.HasSuffix(lower, ".csv"), strings.HasSuffix(lower, ".json"),
		strings.HasSuffix(lower, ".yaml"), strings.HasSuffix(lower, ".yml"),
		strings.HasSuffix(lower, ".xml"):
		return true
	default:
		return false
	}
}

// redactArtifactBytes applies the same secret scrubbing as the live run log,
// line by line, so downloaded message/error logs do not leak credentials.
func redactArtifactBytes(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	var b strings.Builder
	b.Grow(len(data))
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] != '\n' {
			continue
		}
		line := string(data[start:i])
		if strings.HasSuffix(line, "\r") {
			line = line[:len(line)-1]
			b.WriteString(redactLine(line))
			b.WriteString("\r\n")
		} else {
			b.WriteString(redactLine(line))
			b.WriteByte('\n')
		}
		start = i + 1
	}
	if start < len(data) {
		line := string(data[start:])
		b.WriteString(redactLine(line))
	}
	return []byte(b.String())
}

func artifactContentType(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".xml"):
		return "application/xml; charset=utf-8"
	case strings.HasSuffix(lower, ".csv"):
		return "text/csv; charset=utf-8"
	case strings.HasSuffix(lower, ".json"):
		return "application/json; charset=utf-8"
	case strings.HasSuffix(lower, ".yaml"), strings.HasSuffix(lower, ".yml"):
		return "text/yaml; charset=utf-8"
	case strings.HasSuffix(lower, ".log"), strings.HasSuffix(lower, ".txt"):
		return "text/plain; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}
