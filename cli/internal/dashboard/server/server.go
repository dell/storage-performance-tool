package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// Options configures a dashboard started by Run.
type Options struct {
	SPTBinary   string
	WorkDir     string
	StateDir    string
	Port        string
	ConfigFile  string
	Output      io.Writer
	OpenBrowser bool
	// ResultsRoot is the read-only tree the results catalog scans. Defaults to
	// DASHBOARD_RESULTS_ROOT, then ~/results.
	ResultsRoot string
}

// Run serves the dashboard on 127.0.0.1 until ctx is cancelled, then stops
// any SPT process it started.
func Run(ctx context.Context, opts Options) error {
	if ctx.Err() != nil {
		return nil
	}
	a, err := newApp(opts)
	if err != nil {
		return err
	}
	defer a.stopProcesses()
	listener, err := net.Listen("tcp", "127.0.0.1:"+a.port)
	if err != nil {
		return fmt.Errorf("start dashboard: %w", err)
	}
	// server.Shutdown normally closes the listener first, so a second close error is expected.
	defer func() { _ = listener.Close() }()
	a.port = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	server := a.httpServer()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			a.mu.Lock()
			a.stopping = true
			a.mu.Unlock()
			shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdown); err != nil {
				_ = server.Close()
			}
		case <-done:
		}
	}()
	announce(opts.Output, "http://127.0.0.1:"+a.port, opts.OpenBrowser)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// announce prints the dashboard URL to output (stdout when nil) and, when
// openInBrowser is set, opens it as openBrowser does. A browser that cannot
// be opened is reported, not returned: the dashboard keeps serving.
func announce(output io.Writer, url string, openInBrowser bool) {
	if output == nil {
		output = os.Stdout
	}
	_, _ = fmt.Fprintf(output, "Dashboard running at %s\nPress Ctrl+C to stop.\n", url)
	if !openInBrowser {
		return
	}
	if err := openBrowser(url); err != nil {
		_, _ = fmt.Fprintf(output, "Could not open browser: %v. Open the URL above manually.\n", err)
	}
}

func newApp(opts Options) (*app, error) {
	root := opts.WorkDir
	if root == "" {
		executable, err := os.Executable()
		if err != nil {
			return nil, err
		}
		root = filepath.Dir(executable)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	port := opts.Port
	if port == "" {
		port = os.Getenv("DASHBOARD_PORT")
		if port == "" {
			port = "8000"
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return nil, errors.New("dashboard port (--port or DASHBOARD_PORT) must be a number between 0 and 65535")
	}
	sptBin := opts.SPTBinary
	if sptBin == "" {
		sptBin, err = os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve SPT executable: %w", err)
		}
	}
	stateDir := opts.StateDir
	if stateDir == "" {
		stateDir = filepath.Join(root, ".dashboard-state")
	}
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(stateDir, 0700); err != nil { // #nosec G302 -- This is a directory; 0700 grants access only to its owner.
		return nil, err
	}
	a := &app{root: root, home: home, results: filepath.Join(home, "results"), stateDir: stateDir,
		stateFile: filepath.Join(stateDir, "latest-job.json"), lockFile: filepath.Join(stateDir, "job.lock"), port: strconv.Itoa(n), sptBin: sptBin,
		storageFile: filepath.Join(stateDir, "storage-config.json"), verifyFile: filepath.Join(stateDir, "storage-verification.json")}
	a.catalogRoot = opts.ResultsRoot
	if a.catalogRoot == "" {
		a.catalogRoot = os.Getenv("DASHBOARD_RESULTS_ROOT")
	}
	if a.catalogRoot != "" {
		if a.catalogRoot, err = filepath.Abs(a.catalogRoot); err != nil {
			return nil, err
		}
	}
	a.settings, err = loadSettings(opts.ConfigFile)
	if err != nil {
		return nil, err
	}
	a.loadStorage()
	return a, nil
}

func (a *app) httpServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.handleRoot)
	mux.HandleFunc("GET /index.html", a.handleRoot)
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /api/state", a.handleState)
	mux.HandleFunc("POST /api/attach", a.handleAttach)
	mux.HandleFunc("DELETE /api/attach", a.handleDetach)
	mux.HandleFunc("GET /api/runs", a.handleRunHistory)
	mux.HandleFunc("GET /api/runs/{runID}/csv", a.handleRunCSV)
	mux.HandleFunc("GET /api/runs/{runID}/log", a.handleRunLog)
	mux.HandleFunc("GET /api/catalog", a.handleCatalog)
	mux.HandleFunc("GET /api/catalog/run", a.handleCatalogRun)
	mux.HandleFunc("GET /api/catalog/compare", a.handleCatalogCompare)
	mux.HandleFunc("GET /api/catalog/log", a.handleCatalogLog)
	mux.HandleFunc("GET /api/catalog/artifacts", a.handleCatalogArtifacts)
	mux.HandleFunc("GET /api/catalog/artifact", a.handleCatalogArtifact)
	mux.HandleFunc("GET /api/runs/{runID}", a.handleSavedRun)
	mux.HandleFunc("POST /api/runs", a.handleRun)
	mux.HandleFunc("POST /api/storage/verify", a.handleVerify)
	return &http.Server{Handler: securityHeaders(a.apiGuard(mux)), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
}

func (a *app) stopProcesses() {
	a.mu.Lock()
	a.stopping = true
	commands := []*exec.Cmd{a.activeCmd}
	a.mu.Unlock()
	for _, cmd := range commands {
		if cmd != nil {
			if err := interruptProcess(cmd.Process); err != nil && !errors.Is(err, os.ErrProcessDone) {
				_ = cmd.Process.Kill()
			}
		}
	}
	a.processes.Wait()
}
