// Package server is the local web dashboard behind "spt dashboard": it
// serves the embedded browser UI, starts and stops SPT runs, and reads the
// results catalog.
package server

import (
	"embed"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sync"
)

// The browser UI ships inside the Go server binary.
//
//go:embed index.html
var assets embed.FS

const (
	maxRequestBytes = 4096
	maxCSVBytes     = 10_000_000
	maxLogBytes     = 2_000_000
	maxMetricsBytes = 1_000_000
	// Live samples kept per run: enough for the longest run at the 500 ms poll.
	maxSamples      = 1500
	maxSamplesBytes = 4_000_000
	metricsContract = "SPT live metrics API"

	// Expert mode accepts any value in these ranges; guided mode only the presets below.
	expertMinObjects     = 1
	expertMaxObjects     = 100_000
	expertMinObjectBytes = 1 << 10
	expertMaxObjectBytes = 1 << 30
	expertMinThreads     = 1
	expertMaxThreads     = 64
	expertMinRuntime     = 10
	expertMaxRuntime     = 3600
	expertMaxTotalBytes  = 100 << 30

	modeGuided = "guided"
	modeExpert = "expert"
)

var (
	sizePattern    = regexp.MustCompile(`^([1-9][0-9]{0,6})(KiB|MiB|GiB)$`)
	namePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{1,61}[A-Za-z0-9]$`)
	runPattern     = regexp.MustCompile(`^mt-[A-Za-z0-9_.-]+$`)
	keyPattern     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	runTimePattern = regexp.MustCompile(`^mt-dashboard-(\d{8}T\d{6})-`)
)

type options struct {
	// Mode is "guided" or "expert"; an empty mode is treated as guided.
	Mode              string `json:"mode,omitempty"`
	ObjectCount       int    `json:"object_count"`
	ObjectSize        string `json:"object_size"`
	Threads           int    `json:"threads"`
	MaxRuntimeSeconds int    `json:"max_runtime_seconds"`
	// Driver is "s3" (default, write + verify against the configured bucket) or
	// "mock" (SPT's dummy-mock storage driver; no S3 endpoint or credentials).
	Driver string `json:"driver,omitempty"`
}

const (
	driverS3   = "s3"
	driverMock = "mock"
)

type app struct {
	root       string
	home       string
	results    string
	stateDir   string
	stateFile  string
	lockFile   string
	port       string
	sptAPIPort string
	sptBin     string
	metricsURL string
	// nodeMetricsURL overrides each client machine's /metrics/json address (tests only).
	nodeMetricsURL func(host string) string
	// catalogRoot is the read-only results tree the catalog scans (dashboard and CLI runs).
	catalogRoot string
	catalog     catalogCache
	settings    dashboardSettings

	storageFile    string
	verifyFile     string
	endpointClient *http.Client
	verifyNodes    verificationRunner
	storageMu      sync.Mutex
	storage        storageConfig
	verification   verification

	mu            sync.Mutex
	verifying     bool
	stopping      bool
	processes     sync.WaitGroup
	activeJob     map[string]any
	activeCmd     *exec.Cmd
	activeLock    *os.File
	attachedHost  string
	attachedRunID string
	attachedRows  []map[string]any
	liveRunID     string
	liveSamples   []map[string]any
	liveSignature string
}

func executable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && (runtime.GOOS == "windows" || info.Mode()&0111 != 0)
}

type limitedWriter struct {
	w    io.Writer
	left int64
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	n := len(p)
	if int64(n) > l.left {
		n = int(l.left)
	}
	if n > 0 {
		_, _ = l.w.Write(p[:n])
		l.left -= int64(n)
	}
	return len(p), nil
}
