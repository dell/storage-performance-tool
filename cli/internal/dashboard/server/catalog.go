package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// The results catalog is a read-only index of SPT run folders under one results
// root, for both dashboard runs (mt-dashboard-*/<label>-<ts>/) and runs started from
// the CLI (<results-dir>/<label>-<ts>/). It never writes, renames, or deletes files.
const (
	msgCatalogRunNotFound = "Run not found in the results catalog."
	flagNoData            = "no_data"

	catalogMaxDepth   = 3
	catalogMaxDirs    = 5000
	catalogMaxRuns    = 1000
	catalogMaxPoints  = 2000
	catalogMaxCompare = 6
	catalogParamsMax  = 1 << 20
	catalogEngineMax  = 64 << 10
)

type catalogPhase struct {
	Key          string   `json:"key"`
	Step         string   `json:"step"`
	OpType       string   `json:"op_type"`
	SourceFile   string   `json:"source_file"`
	Successful   float64  `json:"successful_ops"`
	Failed       float64  `json:"failed_ops"`
	Corrupt      *float64 `json:"corrupt_ops"`
	IOPS         *float64 `json:"iops"`
	Bandwidth    *float64 `json:"bandwidth_mib_s"`
	P50          *float64 `json:"p50_latency_us"`
	P99          *float64 `json:"p99_latency_us"`
	LatencyMean  *float64 `json:"latency_mean_us"`
	LatencyMax   *float64 `json:"latency_max_us"`
	Duration     *float64 `json:"duration_seconds"`
	Bytes        *float64 `json:"bytes_processed"`
	NodeCount    *float64 `json:"node_count"`
	metricsCSV   string
	metricsTotal string
}

type catalogRun struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Source        string         `json:"source"`
	Status        string         `json:"status"`
	Integrity     string         `json:"integrity"`
	StartedAt     string         `json:"started_at,omitempty"`
	Workload      string         `json:"workload,omitempty"`
	Label         string         `json:"label,omitempty"`
	ObjectSize    string         `json:"object_size,omitempty"`
	ObjectCount   int64          `json:"object_count,omitempty"`
	Threads       int64          `json:"threads,omitempty"`
	Duration      string         `json:"duration,omitempty"`
	S3Driver      string         `json:"s3_driver,omitempty"`
	HostCount     int64          `json:"host_count,omitempty"`
	EndpointHost  string         `json:"endpoint_host,omitempty"`
	CLIVersion    string         `json:"cli_version,omitempty"`
	EngineVersion string         `json:"engine_version,omitempty"`
	Phases        []catalogPhase `json:"phases"`
	Issues        []string       `json:"issues"`
	SummaryFile   string         `json:"summary_file,omitempty"`
	ModifiedAt    string         `json:"modified_at"`
	dir           string
}

type catalogCache struct {
	mu   sync.Mutex
	runs map[string]cachedCatalogRun
}

type cachedCatalogRun struct {
	stamp string
	run   catalogRun
}

// runParams is an allow-list view of spt_run_params.json. Only these fields are
// decoded; everything else in the file (including ScenarioParams.AccessKey and
// ScenarioParams.SecretKey) is skipped by encoding/json and never stored.
type runParams struct {
	GeneratedAt    string            `json:"generatedAt"`
	WorkloadType   string            `json:"workloadType"`
	Label          string            `json:"label"`
	StepLifecycles map[string]string `json:"stepLifecycles"`
	ExpectedSteps  []string          `json:"expectedStepIds"`
	CLI            struct {
		Version string `json:"version"`
	} `json:"cli"`
	MultiHost struct {
		HostCount int64 `json:"hostCount"`
	} `json:"multiHost"`
	Scenario struct {
		WorkloadType string   `json:"WorkloadType"`
		Endpoint     string   `json:"Endpoint"`
		Endpoints    []string `json:"Endpoints"`
		Threads      int64    `json:"Threads"`
		ObjectSize   string   `json:"ObjectSize"`
		ObjectCount  int64    `json:"ObjectCount"`
		Duration     string   `json:"Duration"`
		S3Driver     string   `json:"S3Driver"`
	} `json:"scenarioParams"`
}

// catalogDisplayRoot names the catalog root for the UI: "~/…" when it is under the
// user's home directory, otherwise the absolute path.
func (a *app) catalogDisplayRoot() string {
	dir := a.catalogDir()
	if a.home != "" {
		if rel, err := filepath.Rel(a.home, dir); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			if rel == "." {
				return "~"
			}
			return "~/" + filepath.ToSlash(rel)
		}
	}
	return dir
}

func (a *app) catalogDir() string {
	if a.catalogRoot != "" {
		return a.catalogRoot
	}
	return a.results
}

// scanCatalog walks the results root without following symlinks and returns the
// run folders it finds, newest first. A malformed folder becomes an entry with
// issues instead of failing the whole listing.
func (a *app) scanCatalog() ([]catalogRun, bool) {
	root := a.catalogDir()
	runs := []catalogRun{}
	dirs := 0
	truncated := false
	seen := map[string]bool{}
	var walk func(dir, rel string, depth int)
	walk = func(dir, rel string, depth int) {
		if truncated {
			return
		}
		dirs++
		if dirs > catalogMaxDirs || len(runs) >= catalogMaxRuns {
			truncated = true
			return
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		isRun := false
		for _, e := range entries {
			if e.Type()&os.ModeSymlink != 0 || !e.Type().IsRegular() {
				continue
			}
			n := e.Name()
			if n == constants.ResultsMetadataFileName || isTotalsFile(n) {
				isRun = true
				break
			}
		}
		if isRun && rel != "" {
			seen[rel] = true
			runs = append(runs, a.catalogEntry(dir, rel))
			return
		}
		if depth >= catalogMaxDepth {
			return
		}
		for _, e := range entries {
			if !e.IsDir() || e.Type()&os.ModeSymlink != 0 || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			childRel := e.Name()
			if rel != "" {
				childRel = rel + "/" + e.Name()
			}
			walk(filepath.Join(dir, e.Name()), childRel, depth+1)
		}
	}
	walk(root, "", 0)
	a.catalog.mu.Lock()
	for id := range a.catalog.runs {
		if !seen[id] {
			delete(a.catalog.runs, id)
		}
	}
	a.catalog.mu.Unlock()
	sort.SliceStable(runs, func(i, j int) bool {
		x, y := firstNonempty(runs[i].StartedAt, runs[i].ModifiedAt), firstNonempty(runs[j].StartedAt, runs[j].ModifiedAt)
		if x != y {
			return x > y
		}
		return runs[i].ID < runs[j].ID
	})
	return runs, truncated
}

func isTotalsFile(name string) bool {
	return strings.HasSuffix(name, "."+constants.ResultsArtifactSuffixMetricsTotal) && !strings.HasSuffix(name, "."+constants.ResultsArtifactSuffixDeleteMetricsTotal)
}

// folderStamp changes whenever a file in the folder is added, removed, or rewritten.
func folderStamp(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	var b strings.Builder
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		b.WriteString(e.Name())
		b.WriteString(info.ModTime().UTC().Format(time.RFC3339Nano))
		b.WriteString(strconv.FormatInt(info.Size(), 10))
		b.WriteByte('|')
	}
	return b.String()
}

func (a *app) catalogEntry(dir, rel string) catalogRun {
	stamp := folderStamp(dir)
	a.catalog.mu.Lock()
	if a.catalog.runs == nil {
		a.catalog.runs = map[string]cachedCatalogRun{}
	}
	if c, ok := a.catalog.runs[rel]; ok && c.stamp == stamp {
		a.catalog.mu.Unlock()
		return c.run
	}
	a.catalog.mu.Unlock()
	run := a.readCatalogRun(dir, rel)
	a.catalog.mu.Lock()
	a.catalog.runs[rel] = cachedCatalogRun{stamp: stamp, run: run}
	a.catalog.mu.Unlock()
	return run
}

func (a *app) readCatalogRun(dir, rel string) catalogRun {
	run := catalogRun{ID: rel, Name: filepath.Base(dir), Source: "cli", Phases: []catalogPhase{}, Issues: []string{}, dir: dir}
	if info, err := os.Stat(dir); err == nil {
		run.ModifiedAt = info.ModTime().UTC().Format(time.RFC3339)
	}
	top := strings.SplitN(rel, "/", 2)[0]
	if runPattern.MatchString(top) && strings.HasPrefix(top, "mt-dashboard-") {
		run.Source = sourceDashboard
	}
	paramsOK := false
	paramsPath := filepath.Join(dir, constants.ResultsMetadataFileName)
	if info, err := os.Lstat(paramsPath); err == nil && info.Mode().IsRegular() {
		if info.Size() > catalogParamsMax {
			run.Issues = append(run.Issues, "Run parameters file is larger than the catalog reads.")
		} else if b, err := os.ReadFile(paramsPath); err != nil { // #nosec G304 -- path is a regular file found by walking the configured results root without symlinks
			run.Issues = append(run.Issues, "Run parameters could not be read.")
		} else {
			var p runParams
			if err := json.Unmarshal(b, &p); err != nil {
				run.Issues = append(run.Issues, "Run parameters are not valid JSON.")
			} else {
				paramsOK = true
				applyParams(&run, p)
			}
		}
	} else {
		run.Issues = append(run.Issues, "No "+constants.ResultsMetadataFileName+" in this folder.")
	}
	if run.Label == sourceDashboard {
		run.Source = sourceDashboard
	}
	run.EngineVersion = engineVersion(filepath.Join(dir, "engine.info.json"))
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Type()&os.ModeSymlink != 0 || !e.Type().IsRegular() {
			continue
		}
		n := e.Name()
		if strings.HasPrefix(n, constants.ResultsSummaryFilePrefix) && strings.HasSuffix(n, constants.ResultsSummaryFileSuffix) {
			run.SummaryFile = n
		}
		if !isTotalsFile(n) {
			continue
		}
		phases, err := a.readTotals(filepath.Join(dir, n))
		if err != nil {
			run.Issues = append(run.Issues, "Could not read "+n+".")
			continue
		}
		run.Phases = append(run.Phases, phases...)
	}
	sort.SliceStable(run.Phases, func(i, j int) bool { return run.Phases[i].SourceFile < run.Phases[j].SourceFile })
	switch {
	case !paramsOK && len(run.Phases) == 0:
		run.Status = "malformed"
	case !paramsOK || len(run.Phases) == 0 || len(run.Issues) > 0:
		run.Status = "partial"
	default:
		run.Status = "complete"
	}
	run.Integrity = integrityOf(run)
	return run
}

func applyParams(run *catalogRun, p runParams) {
	run.StartedAt = p.GeneratedAt
	run.Workload = strings.ToLower(firstNonempty(p.WorkloadType, p.Scenario.WorkloadType))
	run.Label = p.Label
	run.ObjectSize = p.Scenario.ObjectSize
	run.ObjectCount = p.Scenario.ObjectCount
	run.Threads = p.Scenario.Threads
	run.Duration = p.Scenario.Duration
	run.S3Driver = p.Scenario.S3Driver
	run.HostCount = p.MultiHost.HostCount
	run.CLIVersion = p.CLI.Version
	endpoint := p.Scenario.Endpoint
	if endpoint == "" && len(p.Scenario.Endpoints) > 0 {
		endpoint = p.Scenario.Endpoints[0]
	}
	run.EndpointHost = endpointHost(endpoint)
	for _, step := range p.ExpectedSteps {
		state, ok := p.StepLifecycles[step]
		if !ok {
			run.Issues = append(run.Issues, "Step "+stepName(step)+" has no recorded lifecycle.")
		} else if state != "completed" {
			run.Issues = append(run.Issues, "Step "+stepName(step)+" ended as "+state+".")
		}
	}
}

// endpointHost keeps only the host of an endpoint URL so no path, query, or userinfo is shown.
func endpointHost(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Hostname()
}

func engineVersion(path string) string {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > catalogEngineMax {
		return ""
	}
	b, err := os.ReadFile(path) // #nosec G304 -- regular file inside a run folder found under the results root
	if err != nil {
		return ""
	}
	var doc struct {
		Builds []struct {
			Version string `json:"version"`
		} `json:"builds"`
	}
	if json.Unmarshal(b, &doc) != nil || len(doc.Builds) == 0 {
		return ""
	}
	return doc.Builds[0].Version
}

// stepName returns the trailing step word of an SPT step id, e.g. "create" from
// "label-001-20261023.202619.791-create".
func stepName(id string) string {
	if i := strings.LastIndex(id, "-"); i >= 0 && i < len(id)-1 {
		return id[i+1:]
	}
	return id
}

func (a *app) readTotals(path string) ([]catalogPhase, error) {
	base := filepath.Base(path)
	stepID := strings.TrimSuffix(base, "."+constants.ResultsArtifactSuffixMetricsTotal)
	step := stepName(stepID)
	data, err := a.parseMetricsFile(path, "", "", false, true)
	if err != nil {
		return nil, err
	}
	rows, _ := data[keySamples].([]map[string]any)
	latest := map[string]map[string]any{}
	order := []string{}
	for _, r := range rows {
		op := strings.ToUpper(str(r[keyOpType]))
		if _, ok := latest[op]; !ok {
			order = append(order, op)
		}
		latest[op] = r
	}
	out := []catalogPhase{}
	interval := filepath.Join(filepath.Dir(path), stepID+"."+constants.ResultsArtifactSuffixMetrics)
	if !regularFile(interval) {
		interval = ""
	}
	for _, op := range order {
		r := latest[op]
		key := strings.ToUpper(step)
		if len(order) > 1 {
			key += ":" + op
		}
		p := catalogPhase{Key: key, Step: step, OpType: op, SourceFile: base, metricsCSV: interval, metricsTotal: path}
		p.Successful = numberValue(r, keyCumulativeOps)
		p.Failed = numberValue(r, "count_failed")
		p.Corrupt = optFloat(r, metricCorrupt)
		p.IOPS = optFloat(r, keyIntervalIOPS)
		p.Bandwidth = optFloat(r, metricBandwidth)
		p.P50 = optFloat(r, metricP50Latency)
		p.P99 = optFloat(r, sptP99Latency)
		p.LatencyMean = optFloat(r, "latency_mean_us")
		p.LatencyMax = optFloat(r, "latency_max_us")
		p.Duration = optFloat(r, "duration_seconds")
		p.Bytes = optFloat(r, "bytes_processed")
		p.NodeCount = optFloat(r, "node_count")
		out = append(out, p)
	}
	return out, nil
}

func optFloat(m map[string]any, key string) *float64 {
	if v, ok := num(m, key); ok {
		return &v
	}
	return nil
}

// integrityOf summarizes what the recorded results say about data integrity.
// "not_recorded" means the workload did not verify data, not that it passed.
func integrityOf(run catalogRun) string {
	verify := false
	for _, p := range run.Phases {
		if p.Corrupt != nil && *p.Corrupt > 0 {
			return integrityFailed
		}
		if strings.Contains(strings.ToLower(p.Step), "verify") {
			verify = true
			if p.Failed > 0 {
				return integrityFailed
			}
		}
	}
	if verify {
		return integrityPassed
	}
	if strings.Contains(run.Workload, "verify") {
		return "incomplete"
	}
	return "not_recorded"
}

func (a *app) findCatalogRun(id string) (catalogRun, bool) {
	if id == "" || strings.Contains(id, "\\") || strings.HasPrefix(id, "/") {
		return catalogRun{}, false
	}
	runs, _ := a.scanCatalog()
	for _, r := range runs {
		if r.ID == id {
			return r, true
		}
	}
	return catalogRun{}, false
}

func (a *app) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if !a.loopbackHost(r) {
		writeJSON(w, 403, map[string]any{keyError: msgLoopbackOnly})
		return
	}
	runs, truncated := a.scanCatalog()
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	source := r.URL.Query().Get("source")
	workload := strings.ToLower(r.URL.Query().Get("workload"))
	status := r.URL.Query().Get(keyStatus)
	out := []catalogRun{}
	for _, run := range runs {
		if source != "" && run.Source != source {
			continue
		}
		if workload != "" && run.Workload != workload {
			continue
		}
		if status != "" && run.Status != status && run.Integrity != status {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(strings.Join([]string{run.ID, run.Label, run.Workload, run.ObjectSize, run.EndpointHost, run.StartedAt}, " ")), q) {
			continue
		}
		out = append(out, run)
	}
	writeJSON(w, 200, map[string]any{"root": a.catalogDisplayRoot(), keyRuns: out, "total": len(runs), keyTruncated: truncated, "read_only": true})
}

func (a *app) handleCatalogRun(w http.ResponseWriter, r *http.Request) {
	if !a.loopbackHost(r) {
		writeJSON(w, 403, map[string]any{keyError: msgLoopbackOnly})
		return
	}
	run, ok := a.findCatalogRun(r.URL.Query().Get("id"))
	if !ok {
		writeJSON(w, 404, map[string]any{keyError: msgCatalogRunNotFound})
		return
	}
	writeJSON(w, 200, map[string]any{keyRun: run, "series": a.runSeries(run)})
}

type seriesPoint struct {
	T    float64  `json:"t"`
	IOPS *float64 `json:"iops"`
	BW   *float64 `json:"bandwidth_mib_s"`
	P50  *float64 `json:"p50_latency_us"`
	P99  *float64 `json:"p99_latency_us"`
}

// runSeries returns each phase's interval samples on an axis of seconds since that
// phase's first sample, so runs that started at different times overlay directly.
func (a *app) runSeries(run catalogRun) map[string][]seriesPoint {
	out := map[string][]seriesPoint{}
	for _, p := range run.Phases {
		if p.metricsCSV == "" {
			continue
		}
		data, err := a.parseMetricsFile(p.metricsCSV, "", "", false, false)
		if err != nil {
			continue
		}
		rows, _ := data[keySamples].([]map[string]any)
		points := []seriesPoint{}
		start := -1.0
		for _, r := range rows {
			if !strings.EqualFold(str(r[keyOpType]), p.OpType) {
				continue
			}
			t := numberValue(r, keyElapsedSeconds)
			if start < 0 {
				start = t
			}
			points = append(points, seriesPoint{T: t - start, IOPS: optFloat(r, keyIntervalIOPS), BW: optFloat(r, metricBandwidth), P50: optFloat(r, metricP50Latency), P99: optFloat(r, sptP99Latency)})
		}
		out[p.Key] = downsample(points, catalogMaxPoints)
	}
	return out
}

// downsample averages consecutive points into at most limit buckets; nulls stay gaps.
func downsample(points []seriesPoint, limit int) []seriesPoint {
	if len(points) <= limit || limit < 1 {
		return points
	}
	out := make([]seriesPoint, 0, limit)
	size := float64(len(points)) / float64(limit)
	for b := 0; b < limit; b++ {
		lo, hi := int(float64(b)*size), int(float64(b+1)*size)
		if hi > len(points) {
			hi = len(points)
		}
		if lo >= hi {
			continue
		}
		bucket := points[lo:hi]
		avg := func(get func(seriesPoint) *float64) *float64 {
			sum, n := 0.0, 0
			for _, p := range bucket {
				if v := get(p); v != nil {
					sum += *v
					n++
				}
			}
			if n == 0 {
				return nil
			}
			v := sum / float64(n)
			return &v
		}
		out = append(out, seriesPoint{T: bucket[len(bucket)-1].T, IOPS: avg(func(p seriesPoint) *float64 { return p.IOPS }), BW: avg(func(p seriesPoint) *float64 { return p.BW }), P50: avg(func(p seriesPoint) *float64 { return p.P50 }), P99: avg(func(p seriesPoint) *float64 { return p.P99 })})
	}
	return out
}
