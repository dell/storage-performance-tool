package results

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// ArtifactSpec defines a log endpoint and its output filename suffix.
type ArtifactSpec struct {
	Loggers  []string
	Suffix   string
	Required bool
}

// DefaultArtifacts is the initial list we fetch per step.
var DefaultArtifacts = []ArtifactSpec{
	{Loggers: []string{"metrics.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsTotal, Required: true},
	{Loggers: []string{"Config"}, Suffix: constants.ResultsArtifactSuffixConfig, Required: true},
	{Loggers: []string{"Cli"}, Suffix: constants.ResultsArtifactSuffixCLIArgs, Required: true},
	{Loggers: []string{"Messages"}, Suffix: constants.ResultsArtifactSuffixMessages, Required: true},
	{Loggers: []string{"Errors"}, Suffix: constants.ResultsArtifactSuffixErrors, Required: true},
	// Optional below
	{Loggers: []string{"metrics.File"}, Suffix: constants.ResultsArtifactSuffixMetrics, Required: false},
	{Loggers: []string{"Scenario"}, Suffix: constants.ResultsArtifactSuffixScenario, Required: false},
	{Loggers: []string{"metrics.threshold.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsThreshold, Required: false},
	{Loggers: []string{"OpTraces"}, Suffix: constants.ResultsArtifactSuffixOpTrace, Required: false},
	// Multipart per-part timings (name standardized to multipart.csv); try common loggers
	{Loggers: []string{"PartsUpload", "Parts.Upload", "parts.upload.csv"}, Suffix: constants.ResultsArtifactSuffixMultipart, Required: false},
	// S3 Tables metrics (only present on s3-tables runs)
	{Loggers: []string{"TablesMetrics"}, Suffix: constants.ResultsArtifactSuffixTablesMetrics, Required: false},
	// Items CSV (only present when --save-items is used on write workloads)
	{Loggers: []string{"items.csv"}, Suffix: constants.ResultsArtifactSuffixItems, Required: false},
	// Ext results XML (Mongoose 3.6 compatible result.xml)
	{Loggers: []string{"metrics.ExtResultsFile"}, Suffix: constants.ResultsArtifactSuffixExtResults, Required: false},
	{Loggers: []string{"metrics.threshold.ExtResultsFile"}, Suffix: constants.ResultsArtifactSuffixExtResultsThreshold, Required: false},
}

// FileStatus records outcome for a single artifact.
type FileStatus struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	Status      string `json:"status"` // ok|missing|error
	Error       string `json:"error,omitempty"`
	Modified    string `json:"modified,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

// StepManifest summarizes files saved for a step.
type StepManifest struct {
	StepID      string       `json:"stepId"`
	CompletedAt time.Time    `json:"completedAt"`
	Files       []FileStatus `json:"files"`
}

// Manifest is the top-level results summary.
type Manifest struct {
	BaseURL     string         `json:"baseUrl"`
	OutputDir   string         `json:"outputDir"`
	GeneratedAt time.Time      `json:"generatedAt"`
	Steps       []StepManifest `json:"steps"`
}

// Fetcher downloads artifacts for steps via the /logs endpoints.
type Fetcher struct {
	BaseURL    string
	OutputDir  string
	HTTPClient *http.Client
	Retries    int
	RetryDelay time.Duration
	Sleeper    func(time.Duration)
	Artifacts  []ArtifactSpec
}

// NewFetcher constructs a Fetcher with sensible defaults.
func NewFetcher(baseURL, outputDir string) *Fetcher {
	return &Fetcher{
		BaseURL:    strings.TrimSuffix(baseURL, "/"),
		OutputDir:  outputDir,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
		Retries:    3,
		RetryDelay: 200 * time.Millisecond,
		Sleeper:    time.Sleep,
		Artifacts:  DefaultArtifacts,
	}
}

// FetchArtifactsForSteps fetches artifacts for the provided step IDs and writes a manifest.
// It returns an error only if all steps fail to retrieve the required metrics.total.csv.
func (f *Fetcher) FetchArtifactsForSteps(ctx context.Context, stepIDs []string) (*Manifest, error) {
	if err := os.MkdirAll(f.OutputDir, 0o750); err != nil {
		return nil, fmt.Errorf("create results dir: %w", err)
	}

	man := &Manifest{
		BaseURL:     f.BaseURL,
		OutputDir:   f.OutputDir,
		GeneratedAt: time.Now().UTC(),
		Steps:       make([]StepManifest, 0, len(stepIDs)),
	}

	haveAnyTotals := false
	for _, stepID := range stepIDs {
		sm := f.fetchStep(ctx, stepID)
		man.Steps = append(man.Steps, sm)
		// Consider success if required metrics.total.csv present
		for _, fs := range sm.Files {
			if strings.HasSuffix(fs.Name, ".metrics.total.csv") && fs.Status == "ok" {
				haveAnyTotals = true
				break
			}
		}
	}

	// Write manifest to disk
	if err := f.writeManifest(man); err != nil {
		return nil, err
	}

	if !haveAnyTotals {
		return man, fmt.Errorf("failed to retrieve required metrics.total.csv for all steps")
	}
	return man, nil
}

func (f *Fetcher) writeManifest(m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	tmp, err := os.CreateTemp(f.OutputDir, ".index.json.tmp-*")
	if err != nil {
		return fmt.Errorf("create temp manifest: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp manifest: %w", err)
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp manifest: %w", err)
	}
	final := filepath.Join(f.OutputDir, constants.ResultsManifestFileName)
	if err = os.Rename(tmpPath, final); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename manifest: %w", err)
	}
	return nil
}

func (f *Fetcher) fetchStep(ctx context.Context, stepID string) StepManifest {
	sm := StepManifest{StepID: stepID, CompletedAt: time.Now().UTC()}
	sm.Files = make([]FileStatus, 0, len(f.Artifacts))
	// Try per-step index.json first to discover available artifacts
	indexMap, idxErr := f.fetchStepIndex(ctx, stepID)
	if idxErr != nil || len(indexMap) == 0 {
		// Without index.json we consider this a failure in the current server contract; mark missing
		for _, a := range f.Artifacts {
			name := fmt.Sprintf("%s.%s", stepID, a.Suffix)
			st := FileStatus{Name: name, Size: 0, Status: "missing"}
			if a.Required {
				if idxErr != nil {
					st.Error = fmt.Sprintf("index.json fetch failed: %v", idxErr)
				} else {
					st.Error = "index.json empty"
				}
			}
			sm.Files = append(sm.Files, st)
		}
		return sm
	}
	for _, a := range f.Artifacts {
		name := fmt.Sprintf("%s.%s", stepID, a.Suffix)
		// If index is available, prefer the first listed logger that exists
		selected := ""
		if len(indexMap) > 0 {
			for _, lg := range a.Loggers {
				if _, ok := indexMap[lg]; ok {
					selected = lg
					break
				}
			}
			// If none of the expected aliases were listed but index is present, treat as missing
			if selected == "" {
				if a.Required {
					sm.Files = append(sm.Files, FileStatus{Name: name, Size: 0, Status: "missing", Error: "not listed in index.json"})
				} else {
					sm.Files = append(sm.Files, FileStatus{Name: name, Size: 0, Status: "missing"})
				}
				continue
			}
		}
		idxItem := indexMap[selected]
		outPath := filepath.Join(f.OutputDir, name)
		size, err := f.downloadOne(ctx, stepID, selected, outPath)
		if err != nil {
			status := "missing"
			if !errors.Is(err, errNotFound) {
				status = "error"
			}
			sm.Files = append(sm.Files, FileStatus{Name: name, Size: 0, Status: status, Error: err.Error()})
			continue
		}
		// Normalize result XML: strip stale wrappers and re-wrap in a clean root element.
		switch a.Suffix {
		case constants.ResultsArtifactSuffixExtResults:
			size = normalizeResultXML(outPath, "result")
		case constants.ResultsArtifactSuffixExtResultsThreshold:
			size = normalizeResultXML(outPath, "result-with-threshold")
		}
		sm.Files = append(sm.Files, FileStatus{Name: name, Size: size, Status: "ok", Modified: idxItem.Modified, ContentType: idxItem.ContentType})
	}
	return sm
}

var errNotFound = errors.New("not found")

// normalizeResultXML strips any stale wrapper tags from the downloaded file and
// re-wraps the self-closing <result .../> entries in a clean root element.
// This makes the CLI the single owner of XML structure, independent of log4j2
// header/footer timing.  Returns the new file size, or the original size on error.
func normalizeResultXML(filePath, rootTag string) int64 {
	raw, err := os.ReadFile(filePath) // #nosec G304 -- path constructed internally from results directory
	if err != nil {
		return 0
	}

	openTag := "<" + rootTag + ">"
	closeTag := "</" + rootTag + ">"

	// Collect only lines that are actual result entries (self-closing XML elements).
	var entries []string
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed == openTag || trimmed == closeTag {
			continue
		}
		entries = append(entries, trimmed)
	}

	var buf strings.Builder
	buf.WriteString(openTag)
	buf.WriteByte('\n')
	for _, e := range entries {
		buf.WriteString(e)
		buf.WriteByte('\n')
	}
	buf.WriteString(closeTag)
	buf.WriteByte('\n')

	out := buf.String()
	if err := os.WriteFile(filePath, []byte(out), 0o600); err != nil {
		return int64(len(raw))
	}
	return int64(len(out))
}

func (f *Fetcher) downloadOne(ctx context.Context, stepID, logger, outPath string) (int64, error) {
	u, err := url.Parse(f.BaseURL)
	if err != nil {
		return 0, fmt.Errorf("parse base url: %w", err)
	}
	u.Path = path.Join(u.Path, "/logs/", stepID, logger)

	var lastErr error
	var bytesWritten int64
	for attempt := 0; attempt < max(1, f.Retries); attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return 0, fmt.Errorf("new request: %w", err)
		}
		resp, err := f.HTTPClient.Do(req)
		if err != nil {
			lastErr = err
		} else {
			func() {
				defer func() { _ = resp.Body.Close() }()
				switch resp.StatusCode {
				case http.StatusOK:
					// Write to temp then rename
					tmpDir := filepath.Dir(outPath)
					if err := os.MkdirAll(tmpDir, 0o750); err != nil {
						lastErr = fmt.Errorf("mkdir: %w", err)
						return
					}
					tmp, err := os.CreateTemp(tmpDir, ".part-*")
					if err != nil {
						lastErr = fmt.Errorf("create temp: %w", err)
						return
					}
					n, copyErr := io.Copy(tmp, resp.Body)
					if closeErr := tmp.Close(); closeErr != nil && copyErr == nil {
						copyErr = closeErr
					}
					if copyErr != nil {
						_ = os.Remove(tmp.Name())
						lastErr = fmt.Errorf("write file: %w", copyErr)
						return
					}
					if err := os.Rename(tmp.Name(), outPath); err != nil {
						_ = os.Remove(tmp.Name())
						lastErr = fmt.Errorf("rename file: %w", err)
						return
					}
					// success
					lastErr = nil
					bytesWritten = n
					return
				case http.StatusNotFound:
					lastErr = errNotFound
				default:
					b, _ := io.ReadAll(resp.Body)
					if len(b) > 0 {
						lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
					} else {
						lastErr = fmt.Errorf("status %d", resp.StatusCode)
					}
				}
			}()
		}

		if lastErr == nil {
			return bytesWritten, nil
		}

		// Stop retrying on 404
		if errors.Is(lastErr, errNotFound) {
			break
		}
		// Delay before next attempt unless last attempt
		if attempt < f.Retries-1 && f.RetryDelay > 0 && f.Sleeper != nil {
			f.Sleeper(f.RetryDelay)
		}
	}

	if lastErr == nil {
		// Shouldn't happen; treat as error
		return 0, fmt.Errorf("unknown download state")
	}
	return 0, lastErr
}

// stepIndexItem represents one item in /logs/<stepId>/index.json
type stepIndexItem struct {
	Logger      string `json:"logger"`
	Href        string `json:"href"`
	Size        int64  `json:"size"`
	Modified    string `json:"modified"`
	ContentType string `json:"content_type"`
}

// fetchStepIndex retrieves /logs/<stepId>/index.json and returns a map logger->item.
// It retries up to Retries times when the response is empty (engine may still be
// flushing log files shortly after completion).
func (f *Fetcher) fetchStepIndex(ctx context.Context, stepID string) (map[string]stepIndexItem, error) {
	for attempt := 0; attempt < max(1, f.Retries); attempt++ {
		out, err := f.doFetchStepIndex(ctx, stepID)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 {
			return out, nil
		}
		// Empty index — engine may still be flushing; retry after delay.
		if attempt < f.Retries-1 && f.RetryDelay > 0 && f.Sleeper != nil {
			f.Sleeper(f.RetryDelay)
		}
	}
	// All retries exhausted with empty response — return empty map.
	return make(map[string]stepIndexItem), nil
}

// doFetchStepIndex performs a single HTTP request for the step index.
func (f *Fetcher) doFetchStepIndex(ctx context.Context, stepID string) (map[string]stepIndexItem, error) {
	u, err := url.Parse(f.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base url: %w", err)
	}
	u.Path = path.Join(u.Path, "/logs/", stepID, "index.json")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	resp, err := f.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("index status %d", resp.StatusCode)
	}
	var payload struct {
		Items []stepIndexItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode index json: %w", err)
	}
	out := make(map[string]stepIndexItem, len(payload.Items))
	for _, it := range payload.Items {
		if it.Logger != "" {
			out[it.Logger] = it
		}
	}
	return out, nil
}
