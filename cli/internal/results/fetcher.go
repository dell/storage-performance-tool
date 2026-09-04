package results

import (
	"bytes"
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
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
)

const (
	fileStatusError   = "error"
	fileStatusMissing = "missing"
	fileStatusOK      = "ok"
)

var integrityNodeSourcePattern = regexp.MustCompile(`^(written|verify-input|verified|integrity\.failures|integrity\.performance|multipart\.lifecycle|delete\.metrics\.total|delete\.requests|delete\.objects|delete\.verification|items)\.node-[0-9]{3}\.csv$`)

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
	{Loggers: []string{"Cli"}, Suffix: constants.ResultsArtifactSuffixCLIArgs, Required: false},
	{Loggers: []string{"Messages"}, Suffix: constants.ResultsArtifactSuffixMessages, Required: true},
	{Loggers: []string{"Errors"}, Suffix: constants.ResultsArtifactSuffixErrors, Required: true},
	// Optional below
	{Loggers: []string{"metrics.File"}, Suffix: constants.ResultsArtifactSuffixMetrics, Required: false},
	{Loggers: []string{"Scenario"}, Suffix: constants.ResultsArtifactSuffixScenario, Required: false},
	{Loggers: []string{"metrics.threshold.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsThreshold, Required: false},
	{Loggers: []string{"OpTraces"}, Suffix: constants.ResultsArtifactSuffixOpTrace, Required: false},
	{Loggers: []string{"DeleteMetricsTotal"}, Suffix: constants.ResultsArtifactSuffixDeleteMetricsTotal, Required: false},
	{Loggers: []string{"DeleteRequests"}, Suffix: constants.ResultsArtifactSuffixDeleteRequests, Required: false},
	{Loggers: []string{"DeleteObjects"}, Suffix: constants.ResultsArtifactSuffixDeleteObjects, Required: false},
	{Loggers: []string{"DeleteVerification"}, Suffix: constants.ResultsArtifactSuffixDeleteVerification, Required: false},
	{Loggers: []string{"DeleteCompletion"}, Suffix: constants.ResultsArtifactSuffixDeleteCompletion, Required: false},
	{Loggers: []string{"written.csv"}, Suffix: constants.ResultsArtifactSuffixWritten, Required: false},
	{Loggers: []string{"written.complete.json"}, Suffix: constants.ResultsArtifactSuffixWrittenCompletion, Required: false},
	{Loggers: []string{"DeleteSelection", "verify-input.csv"}, Suffix: constants.ResultsArtifactSuffixVerifyInput, Required: false},
	{Loggers: []string{"DeleteSelectionCompletion", "verify-input.complete.json"}, Suffix: constants.ResultsArtifactSuffixVerifyInputCompletion, Required: false},
	{Loggers: []string{"verified.csv"}, Suffix: constants.ResultsArtifactSuffixVerified, Required: false},
	{Loggers: []string{"verified.complete.json"}, Suffix: constants.ResultsArtifactSuffixVerifiedCompletion, Required: false},
	{Loggers: []string{"IntegrityFailures"}, Suffix: constants.ResultsArtifactSuffixIntegrityFailures, Required: false},
	{Loggers: []string{"IntegrityPerformance"}, Suffix: constants.ResultsArtifactSuffixIntegrityPerformance, Required: false},
	{Loggers: []string{"MultipartLifecycle"}, Suffix: constants.ResultsArtifactSuffixMultipartLifecycle, Required: false},
	// Multipart per-part timings (name standardized to multipart.csv); try common loggers
	{Loggers: []string{"PartsUpload", "Parts.Upload", "parts.upload.csv"}, Suffix: constants.ResultsArtifactSuffixMultipart, Required: false},
	// S3 Tables metrics (only present on s3-tables runs)
	{Loggers: []string{"TablesMetrics"}, Suffix: constants.ResultsArtifactSuffixTablesMetrics, Required: false},
	// Created-object inventory for writes, or the conservative residual for standalone DELETE.
	{Loggers: []string{"DeleteResidual", "items.csv"}, Suffix: constants.ResultsArtifactSuffixItems, Required: false},
	// PUT-created CSV (only present on mixed workloads; exact remaining-set generation is deferred)
	{Loggers: []string{"put-remaining.csv"}, Suffix: constants.ResultsArtifactSuffixPutRemaining, Required: false},
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
	BaseURL     string            `json:"baseUrl"`
	OutputDir   string            `json:"outputDir"`
	GeneratedAt time.Time         `json:"generatedAt"`
	Steps       []StepManifest    `json:"steps"`
	RunFiles    []FileStatus      `json:"runFiles,omitempty"`
	Integrity   *IntegritySummary `json:"integrity,omitempty"`
}

// IntegritySummary is the stable machine-readable verification outcome embedded in index.json.
type IntegritySummary struct {
	Complete                   bool                        `json:"complete"`
	FinalizationError          string                      `json:"finalization_error,omitempty"`
	SelectionCountsValid       bool                        `json:"selection_counts_valid"`
	SelectionSourceCount       int64                       `json:"selection_source_count"`
	SelectionUniqueCount       int64                       `json:"selection_unique_count"`
	SelectionCount             int64                       `json:"selection_count"`
	ExcludedDeleteMarkerCount  int64                       `json:"excluded_delete_marker_count"`
	VerificationAttemptedCount int64                       `json:"verification_attempted_count"`
	VerificationDeferred       bool                        `json:"verification_deferred"`
	VerifiedCount              int64                       `json:"verified_count"`
	CorruptCount               int64                       `json:"corrupt_count"`
	RemainingCount             int64                       `json:"remaining_count"`
	EmptySelection             bool                        `json:"empty_selection"`
	EmptyAllowed               bool                        `json:"empty_allowed"`
	DigestPerformance          IntegrityPerformanceSummary `json:"digest_performance"`
}

// IntegrityPerformanceSummary reports digest cost independently from ordinary S3 operation metrics.
type IntegrityPerformanceSummary struct {
	Objects                         int64                   `json:"objects"`
	Bytes                           int64                   `json:"bytes"`
	HashWorkerSeconds               float64                 `json:"hash_worker_seconds"`
	MeanWorkerHashMiBPerSecond      float64                 `json:"mean_worker_hash_mib_per_second"`
	InitialWriteDelaySecondsMaxNode *float64                `json:"initial_write_delay_seconds_max_node,omitempty"`
	AdditionalPayloadPasses         int64                   `json:"additional_payload_passes"`
	Phases                          []IntegrityPhaseSummary `json:"phases"`
}

// IntegrityPhaseSummary is the same bounded digest-cost summary for one stable integrity phase.
type IntegrityPhaseSummary struct {
	Phase                      string  `json:"phase"`
	Objects                    int64   `json:"objects"`
	Bytes                      int64   `json:"bytes"`
	HashWorkerSeconds          float64 `json:"hash_worker_seconds"`
	MeanWorkerHashMiBPerSecond float64 `json:"mean_worker_hash_mib_per_second"`
	AdditionalPayloadPasses    int64   `json:"additional_payload_passes"`
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
		haveAnyTotals = haveAnyTotals || hasSuccessfulGenericTotals(sm)
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

func hasSuccessfulGenericTotals(sm StepManifest) bool {
	expected := sm.StepID + "." + constants.ResultsArtifactSuffixMetricsTotal
	for _, file := range sm.Files {
		if file.Name == expected && file.Status == fileStatusOK {
			return true
		}
	}
	return false
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
			st := FileStatus{Name: name, Size: 0, Status: fileStatusMissing}
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
					sm.Files = append(sm.Files, FileStatus{Name: name, Size: 0, Status: fileStatusMissing, Error: "not listed in index.json"})
				} else {
					sm.Files = append(sm.Files, FileStatus{Name: name, Size: 0, Status: fileStatusMissing})
				}
				continue
			}
		}
		idxItem := indexMap[selected]
		outPath := filepath.Join(f.OutputDir, name)
		size, err := f.downloadOne(ctx, stepID, selected, outPath, idxItem.Size)
		if err != nil {
			status := fileStatusMissing
			if !errors.Is(err, errNotFound) {
				status = fileStatusError
			}
			sm.Files = append(sm.Files, FileStatus{Name: name, Size: 0, Status: status, Error: err.Error()})
			continue
		}
		// Normalize result XML: strip stale wrappers and re-wrap in a clean root element.
		switch a.Suffix {
		case constants.ResultsArtifactSuffixConfig:
			var sanitizeErr error
			size, sanitizeErr = sanitizeConfigArtifact(outPath)
			if sanitizeErr != nil {
				_ = os.Remove(outPath)
				sm.Files = append(sm.Files, FileStatus{Name: name, Size: 0, Status: fileStatusError, Error: sanitizeErr.Error()})
				continue
			}
		case constants.ResultsArtifactSuffixExtResults:
			size = normalizeResultXML(outPath, "result")
		case constants.ResultsArtifactSuffixExtResultsThreshold:
			size = normalizeResultXML(outPath, "result-with-threshold")
		}
		sm.Files = append(sm.Files, FileStatus{Name: name, Size: size, Status: fileStatusOK, Modified: idxItem.Modified, ContentType: idxItem.ContentType})
	}
	// Preserve every distributed integrity source as step-prefixed evidence. Canonical artifacts
	// remain handled by the fixed registry above; these dynamically discovered files are never
	// promoted over them.
	var nodeSources []string
	for logger := range indexMap {
		if integrityNodeSourcePattern.MatchString(logger) {
			nodeSources = append(nodeSources, logger)
		}
	}
	sort.Strings(nodeSources)
	for _, logger := range nodeSources {
		item := indexMap[logger]
		name := fmt.Sprintf("%s.%s", stepID, logger)
		size, err := f.downloadOne(ctx, stepID, logger, filepath.Join(f.OutputDir, name), item.Size)
		if err != nil {
			sm.Files = append(sm.Files, FileStatus{Name: name, Status: fileStatusError, Error: err.Error()})
			continue
		}
		sm.Files = append(sm.Files, FileStatus{Name: name, Size: size, Status: fileStatusOK, Modified: item.Modified, ContentType: item.ContentType})
	}
	return sm
}

var errNotFound = errors.New("not found")

func sanitizeConfigArtifact(filePath string) (int64, error) {
	raw, err := os.ReadFile(filePath) // #nosec G304 -- path constructed internally from results directory
	if err != nil {
		return 0, fmt.Errorf("read config artifact: %w", err)
	}
	masked := secretmask.YAML(raw)
	if err := os.WriteFile(filePath, masked, 0o600); err != nil {
		return 0, fmt.Errorf("write sanitized config artifact: %w", err)
	}
	return int64(len(masked)), nil
}

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

func (f *Fetcher) downloadOne(ctx context.Context, stepID, logger, outPath string, indexedSize int64) (int64, error) {
	u, err := url.Parse(f.BaseURL)
	if err != nil {
		return 0, fmt.Errorf("parse base url: %w", err)
	}
	u.Path = path.Join(u.Path, "/logs/", stepID, logger)
	expectedSize, err := f.resolveArtifactSize(ctx, u.String(), indexedSize)
	if err != nil {
		return 0, err
	}
	if expectedSize > constants.EnginePlainLogArtifactMaxBytes {
		return f.downloadOneRanged(ctx, u.String(), outPath, expectedSize)
	}
	return f.downloadOnePlain(ctx, u.String(), outPath, expectedSize)
}

func (f *Fetcher) resolveArtifactSize(ctx context.Context, url string, indexedSize int64) (int64, error) {
	if indexedSize > 0 {
		return indexedSize, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, fmt.Errorf("new HEAD request: %w", err)
	}
	resp, err := f.HTTPClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("resolve artifact size with HEAD: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		if resp.ContentLength >= 0 {
			return resp.ContentLength, nil
		}
		return 0, fmt.Errorf("artifact size missing in index and HEAD response")
	case http.StatusNotFound:
		return 0, errNotFound
	default:
		return 0, responseStatusError(resp)
	}
}

func (f *Fetcher) downloadOnePlain(ctx context.Context, url, outPath string, expectedSize int64) (int64, error) {
	var lastErr error
	var bytesWritten int64
	for attempt := 0; attempt < max(1, f.Retries); attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
					n, err := writeResponseToFile(resp.Body, outPath)
					if err != nil {
						lastErr = err
						return
					}
					if n < expectedSize {
						_ = os.Remove(outPath)
						lastErr = fmt.Errorf("downloaded size %d is smaller than index size %d", n, expectedSize)
						return
					}
					lastErr = nil
					bytesWritten = n
				case http.StatusNotFound:
					lastErr = errNotFound
				default:
					lastErr = responseStatusError(resp)
				}
			}()
		}
		if lastErr == nil {
			return bytesWritten, nil
		}
		if errors.Is(lastErr, errNotFound) {
			break
		}
		f.sleepBeforeRetry(attempt)
	}
	if lastErr == nil {
		return 0, fmt.Errorf("unknown download state")
	}
	return 0, lastErr
}

func (f *Fetcher) downloadOneRanged(ctx context.Context, url, outPath string, expectedSize int64) (int64, error) {
	tmpDir := filepath.Dir(outPath)
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return 0, fmt.Errorf("mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(tmpDir, ".part-*")
	if err != nil {
		return 0, fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	var bytesWritten int64
	defer func() { _ = os.Remove(tmpName) }()
	// Keep chunks aligned with LogServlet.LOG_PAGE_SIZE_LIMIT;
	// the engine caps each ranged response at that size.
	for start := int64(0); start < expectedSize; start += constants.EngineLogArtifactPageSize {
		end := min(start+constants.EngineLogArtifactPageSize-1, expectedSize-1)
		chunk, err := f.downloadRangeChunk(ctx, url, start, end)
		if err != nil {
			_ = tmp.Close()
			return 0, err
		}
		want := end - start + 1
		if int64(len(chunk)) != want {
			_ = tmp.Close()
			return 0, fmt.Errorf("range %d-%d returned %d bytes, want %d", start, end, len(chunk), want)
		}
		n, err := tmp.Write(chunk)
		if err != nil {
			_ = tmp.Close()
			return 0, fmt.Errorf("write range %d-%d: %w", start, end, err)
		}
		if n != len(chunk) {
			_ = tmp.Close()
			return 0, fmt.Errorf("write range %d-%d: %w", start, end, io.ErrShortWrite)
		}
		bytesWritten += int64(n)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("close temp: %w", err)
	}
	if bytesWritten != expectedSize {
		return 0, fmt.Errorf("downloaded size %d does not match index size %d", bytesWritten, expectedSize)
	}
	if err := os.Rename(tmpName, outPath); err != nil {
		return 0, fmt.Errorf("rename file: %w", err)
	}
	return bytesWritten, nil
}

func (f *Fetcher) downloadRangeChunk(ctx context.Context, url string, start, end int64) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < max(1, f.Retries); attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("new request: %w", err)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		resp, err := f.HTTPClient.Do(req)
		if err != nil {
			lastErr = err
		} else {
			var chunk []byte
			func() {
				defer func() { _ = resp.Body.Close() }()
				switch resp.StatusCode {
				case http.StatusOK, http.StatusPartialContent:
					var buf bytes.Buffer
					_, lastErr = io.Copy(&buf, resp.Body)
					if lastErr != nil {
						lastErr = fmt.Errorf("read range %d-%d: %w", start, end, lastErr)
						return
					}
					chunk = buf.Bytes()
				case http.StatusNotFound:
					lastErr = errNotFound
				default:
					lastErr = responseStatusError(resp)
				}
			}()
			if lastErr == nil {
				return chunk, nil
			}
		}
		if errors.Is(lastErr, errNotFound) {
			break
		}
		f.sleepBeforeRetry(attempt)
	}
	if lastErr == nil {
		return nil, fmt.Errorf("unknown range download state")
	}
	return nil, lastErr
}

func (f *Fetcher) sleepBeforeRetry(attempt int) {
	if attempt < f.Retries-1 && f.RetryDelay > 0 && f.Sleeper != nil {
		f.Sleeper(f.RetryDelay)
	}
}

func writeResponseToFile(body io.Reader, outPath string) (int64, error) {
	tmpDir := filepath.Dir(outPath)
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return 0, fmt.Errorf("mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(tmpDir, ".part-*")
	if err != nil {
		return 0, fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	n, copyErr := io.Copy(tmp, body)
	if closeErr := tmp.Close(); closeErr != nil && copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(tmpName)
		return 0, fmt.Errorf("write file: %w", copyErr)
	}
	if err := os.Rename(tmpName, outPath); err != nil {
		_ = os.Remove(tmpName)
		return 0, fmt.Errorf("rename file: %w", err)
	}
	return n, nil
}

func responseStatusError(resp *http.Response) error {
	b, _ := io.ReadAll(resp.Body)
	if len(b) > 0 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	return fmt.Errorf("status %d", resp.StatusCode)
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
