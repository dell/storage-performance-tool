package server

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (a *app) sameOrigin(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	allowed := "localhost:" + a.port
	if host != "127.0.0.1:"+a.port && host != allowed {
		return false
	}
	origin := r.Header.Get("Origin")
	u, err := url.Parse(origin)
	return err == nil && u.Scheme == "http" && strings.ToLower(u.Host) == host && (u.Path == "" || u.Path == "/") && u.RawQuery == "" && u.Fragment == ""
}

func (a *app) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/runs" || r.URL.RawQuery != "" {
		writeJSON(w, 404, map[string]any{keyError: msgEndpointNotFound})
		return
	}
	if !a.sameOrigin(r) {
		writeJSON(w, 403, map[string]any{keyError: "Run requests must come from this dashboard through its SSH tunnel."})
		return
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		writeJSON(w, 415, map[string]any{keyError: "Send JSON dashboard settings."})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var opts options
	if err := dec.Decode(&opts); err != nil {
		writeJSON(w, 400, map[string]any{keyError: "Choose a supported object count, size, thread count, and maximum runtime."})
		return
	}
	if err := ensureEOF(dec); err != nil {
		writeJSON(w, 400, map[string]any{keyError: "Send one JSON dashboard settings object."})
		return
	}
	if err := a.validateOptions(opts); err != nil {
		writeJSON(w, 400, map[string]any{keyError: err.Error()})
		return
	}
	target := a.targetConfiguration()
	if opts.Driver == driverMock {
		// The mock driver never contacts S3, so only the client machines must verify.
		if reasons := a.mockReasons(); len(reasons) > 0 {
			writeJSON(w, 503, map[string]any{keyError: "SPT is not ready: " + strings.Join(reasons, " ")})
			return
		}
		if !a.nodesVerified(target) {
			writeJSON(w, 409, map[string]any{keyError: "Verify the client machines before starting a mock run."})
			return
		}
	} else {
		if target["ready"] != true {
			reasons, _ := target["reasons"].([]string)
			writeJSON(w, 503, map[string]any{keyError: "SPT is not ready: " + strings.Join(reasons, " ")})
			return
		}
		if !a.verified(target) {
			writeJSON(w, 409, map[string]any{keyError: "Verify the storage configuration before starting a run."})
			return
		}
	}
	a.mu.Lock()
	if a.stopping || a.verifying {
		a.mu.Unlock()
		writeJSON(w, 409, map[string]any{keyError: "Storage verification is in progress. Wait for it to finish before starting a run."})
		return
	}
	lock, err := a.lockRun()
	if err != nil {
		a.mu.Unlock()
		writeJSON(w, 409, map[string]any{keyError: "An SPT dashboard run is already in progress. Wait for it to finish before starting another."})
		return
	}
	runID := "mt-dashboard-" + time.Now().UTC().Format("20060102T150405") + "-" + randomHex4()
	runDir := filepath.Join(a.results, runID)
	if filepath.Dir(runDir) != a.results {
		unlockClose(lock)
		a.mu.Unlock()
		writeJSON(w, 500, map[string]any{keyError: "Could not allocate a unique SPT result directory."})
		return
	}
	// SPT's results directory does not exist until the first run on a host.
	err = os.MkdirAll(a.results, 0700)
	if err == nil {
		err = os.Mkdir(runDir, 0700)
	}
	if err != nil {
		unlockClose(lock)
		a.mu.Unlock()
		writeJSON(w, 500, map[string]any{keyError: "Could not create the SPT result directory: " + err.Error()})
		return
	}
	logPath := filepath.Join(a.stateDir, runID+".log")
	argv := a.buildArgv(runDir, runID, opts, target)
	cmd := exec.Command(a.sptBin, argv[1:]...) // #nosec G204 -- no shell; arguments are whitelisted options, validated hosts and server-side config
	cmd.Dir = a.root
	cmd.Env = a.childEnvironment(opts, target)
	cmd.Stdin = nil
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = os.RemoveAll(runDir)
		unlockClose(lock)
		a.mu.Unlock()
		writeJSON(w, 500, map[string]any{keyError: "Could not start SPT on the VM: " + err.Error()})
		return
	}
	cmd.Stderr = cmd.Stdout
	prepareProcess(cmd, lock)
	if err = cmd.Start(); err != nil {
		_ = os.RemoveAll(runDir)
		unlockClose(lock)
		a.mu.Unlock()
		writeJSON(w, 500, map[string]any{keyError: "Could not start SPT on the VM: " + err.Error()})
		return
	}
	drained := make(chan struct{})
	go func() { defer close(drained); drainOutput(stdout, logPath) }()
	started := time.Now().UTC()
	job := map[string]any{keyRunID: runID, keyStatus: jobRunning, "created_at": started.Format(time.RFC3339Nano), keyStartedAt: started.Format(time.RFC3339Nano), "started_epoch": float64(started.UnixNano()) / 1e9, "pid": cmd.Process.Pid, "returncode": nil, keyOptions: opts, "prefix": "dashboard/" + runID + "/", "results_dir": runDir, "log_file": logPath, keyMessage: "SPT workload started on the configured environment.", keyPhase: "SPT setup"}
	if err := a.writeState(job); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		unlockClose(lock)
		_ = os.RemoveAll(runDir)
		a.mu.Unlock()
		writeJSON(w, 500, map[string]any{keyError: "Could not save dashboard run state."})
		return
	}
	// Publish the new run only after saving it. A failed save must preserve
	// the previous job and its live history rather than leave a phantom run.
	a.activeJob = job
	a.activeCmd = cmd
	a.activeLock = lock
	a.liveRunID = ""
	a.liveSamples = nil
	a.liveSignature = ""
	a.processes.Add(2)
	a.mu.Unlock()
	go a.watchProcess(runID, cmd, lock, drained)
	go a.collectLiveHistory(runID)
	writeJSON(w, 202, map[string]any{keyJob: publicJob(job), keyMessage: "SPT run started on the VM."})
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("extra value")
		}
		return err
	}
	return nil
}

func (a *app) validateOptions(o options) error {
	if o.Driver != "" && o.Driver != driverS3 && o.Driver != driverMock {
		return userError("Choose the S3 or mock storage driver.")
	}
	switch o.Mode {
	case "", modeGuided:
		return a.validateGuided(o)
	case modeExpert:
		return a.validateExpert(o)
	default:
		return userError("Choose the guided or expert run mode.")
	}
}

func (a *app) validateGuided(o options) error {
	objects, sizes, threads, runtimes := a.guidedLimitSets()
	if !objects[o.ObjectCount] {
		return userError("Object count is outside the configured guided choices.")
	}
	if !sizes[o.ObjectSize] {
		return userError("Object size is outside the configured guided choices.")
	}
	if !threads[o.Threads] {
		return userError("Thread count is outside the configured guided choices.")
	}
	if !runtimes[o.MaxRuntimeSeconds] {
		return userError("Maximum runtime is outside the configured guided choices.")
	}
	return nil
}

func (a *app) validateExpert(o options) error {
	e := a.effectiveSettings().Limits.Expert
	if o.ObjectCount < e.ObjectCountMin || o.ObjectCount > e.ObjectCountMax {
		return userErrorf("Object count must be between %d and %d.", e.ObjectCountMin, e.ObjectCountMax)
	}
	size, ok := parseSize(o.ObjectSize)
	if !ok || size < e.ObjectSizeMin || size > e.ObjectSizeMax {
		return userErrorf("Object size must be between %d and %d bytes using a whole number of KiB, MiB, or GiB.", e.ObjectSizeMin, e.ObjectSizeMax)
	}
	if o.Threads < e.ThreadsMin || o.Threads > e.ThreadsMax {
		return userErrorf("Thread count must be between %d and %d.", e.ThreadsMin, e.ThreadsMax)
	}
	if o.MaxRuntimeSeconds < e.RuntimeMin || o.MaxRuntimeSeconds > e.RuntimeMax {
		return userErrorf("Maximum runtime must be between %d and %d seconds.", e.RuntimeMin, e.RuntimeMax)
	}
	if size > e.MaxTotalBytes/int64(o.ObjectCount) {
		return userErrorf("Total test data must not exceed %d GiB.", e.MaxTotalBytes>>30)
	}
	return nil
}

// parseSize accepts only canonical sizes such as "512KiB", so a value can never be read as an SPT option.
func parseSize(value string) (int64, bool) {
	m := sizePattern.FindStringSubmatch(value)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	shift := map[string]uint{"KiB": 10, "MiB": 20, "GiB": 30}[m[2]]
	return n << shift, true
}

func randomHex4() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}

func (a *app) lockRun() (*os.File, error) {
	f, err := os.OpenFile(a.lockFile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func unlockClose(f *os.File) {
	if f != nil {
		_ = unlockFile(f)
		_ = f.Close()
	}
}

func (a *app) buildArgv(runDir, runID string, o options, target map[string]any) []string {
	if o.Driver == driverMock {
		// "spt run mock" selects the dummy-mock driver; it takes no endpoint, bucket, or credentials.
		return []string{a.sptBin, "run", "mock", "--headless", "--object-count", strconv.Itoa(o.ObjectCount), "--object-size", o.ObjectSize, "--threads", strconv.Itoa(o.Threads), "--test-hosts", targetHosts(target), "--service-threads", strconv.Itoa(o.Threads), "--auto-terminate-seconds", strconv.Itoa(o.MaxRuntimeSeconds), "--label", sourceDashboard, "--results-dir", runDir}
	}
	return []string{a.sptBin, "run", driverWriteVerify, "--headless", "--endpoints", fmt.Sprint(target["endpoint"]), "--bucket", fmt.Sprint(target["bucket"]), "--prefix", "dashboard/" + runID + "/", "--object-count", strconv.Itoa(o.ObjectCount), "--object-size", o.ObjectSize, "--threads", strconv.Itoa(o.Threads), "--test-hosts", targetHosts(target), "--service-threads", strconv.Itoa(o.Threads), "--auto-terminate-seconds", strconv.Itoa(o.MaxRuntimeSeconds), "--s3-driver", "default", "--auth-version", "4", "--object-data-compressibility", "0", "--object-data-dedupable=true", "--cleanup", "--label", "dashboard", "--results-dir", runDir}
}

func (a *app) childEnvironment(o options, target map[string]any) []string {
	values := a.configValues()
	set := map[string]string{"S3_ENDPOINT": fmt.Sprint(target["endpoint"]), "S3_ENDPOINTS": fmt.Sprint(target["endpoint"]), "S3_BUCKET": fmt.Sprint(target["bucket"]), "HOSTS": targetHosts(target), "THREADS": strconv.Itoa(o.Threads), "SPT_ENGINE_OVERRIDES": "", "SPT_SERVICE_THREADS": strconv.Itoa(o.Threads), "SPT_S3_DRIVER": "default", "SPT_IMAGE": "", "SPT_SKIP_IMAGE_PULL": "false", "SPT_OBJECT_DATA_COMPRESSIBILITY": "0", "SPT_OBJECT_DATA_DEDUPABLE": "true", "SPT_DEFER_VERIFICATION": "false", "SPT_CHECKSUM": "", "AWS_EC2_METADATA_DISABLED": "true"}
	for k, v := range set {
		values[k] = v
	}
	if o.Driver == driverMock {
		// A mock run has no storage target, so it never receives S3 settings or credentials.
		for _, k := range []string{"S3_ENDPOINT", "S3_ENDPOINTS", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
			delete(values, k)
		}
	}
	return envList(values)
}

func readDotenv(path string, existing map[string]string) map[string]string {
	out := map[string]string{}
	f, err := os.Open(path) // #nosec G304 -- fixed .env locations chosen by the server, never by a request
	if err != nil {
		return out
	}
	defer func() { _ = f.Close() }()
	scan := bufio.NewScanner(io.LimitReader(f, 1_000_000))
	scan.Buffer(make([]byte, 4096), 1_000_000)
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		if !keyPattern.MatchString(k) {
			continue
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && ((v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'')) {
			v = v[1 : len(v)-1]
		} else if i := strings.Index(v, " #"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
		out[k] = v
	}
	ref := regexp.MustCompile(`\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?`)
	for k, v := range out {
		out[k] = ref.ReplaceAllStringFunc(v, func(m string) string {
			n := strings.Trim(m, "$")
			n = strings.Trim(n, "{}")
			if x, ok := out[n]; ok {
				return x
			}
			if x, ok := existing[n]; ok {
				return x
			}
			return os.Getenv(n)
		})
	}
	return out
}

func (a *app) targetConfiguration() map[string]any {
	cfg := a.configValues()
	saved := a.storageSnapshot()
	source := "env"
	ep := cfg["S3_ENDPOINTS"]
	if ep == "" {
		ep = cfg["S3_ENDPOINT"]
	}
	if saved.Endpoint != "" {
		ep, source = saved.Endpoint, "dashboard"
	}
	endpoints := []string{}
	for _, x := range strings.Split(ep, ",") {
		if strings.TrimSpace(x) != "" {
			endpoints = append(endpoints, strings.TrimSpace(x))
		}
	}
	endpoint, label, bucket := "", "", strings.TrimSpace(cfg["S3_BUCKET"])
	reasons := []string{}
	if len(endpoints) != 1 {
		reasons = append(reasons, "Configure exactly one S3 endpoint in SPT on the VM.")
	} else {
		var ok bool
		if endpoint, label, ok = validateEndpoint(endpoints[0]); !ok {
			reasons = append(reasons, "The configured S3 endpoint must be a valid HTTP or HTTPS URL without embedded credentials.")
		}
	}
	if !namePattern.MatchString(bucket) {
		reasons = append(reasons, "Configure one valid S3 bucket name in SPT on the VM.")
	}
	creds := cfg["S3_ACCESS_KEY"] != "" && cfg["S3_SECRET_KEY"] != ""
	if !creds {
		reasons = append(reasons, "Configure S3_ACCESS_KEY and S3_SECRET_KEY in SPT on the VM.")
	}
	if !executable(a.sptBin) {
		reasons = append(reasons, "SPT is not executable on this VM.")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		reasons = append(reasons, "Docker is not installed on this VM.")
	}
	mockReasons := a.mockReasons()
	return map[string]any{"ready": len(reasons) == 0, "reasons": reasons, "mock_ready": len(mockReasons) == 0, "mock_reasons": mockReasons, "endpoint": endpoint, "endpoint_label": label, "bucket": bucket, "credentials_configured": creds, "endpoint_source": source, "hosts": saved.Hosts}
}

// mockReasons lists what a mock-driver run still needs: only SPT and Docker on this VM.
func (a *app) mockReasons() []string {
	reasons := []string{}
	if !executable(a.sptBin) {
		reasons = append(reasons, "SPT is not executable on this VM.")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		reasons = append(reasons, "Docker is not installed on this VM.")
	}
	return reasons
}
