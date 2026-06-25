package cmd

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	updater "github.com/dell/storage-performance-tool/cli/internal/update"
)

func TestUpdateCheckReportsAvailableWithoutDefaultLog(t *testing.T) {
	withUpdateCommandTestHooks(t, "5.10.4", func(server *httptest.Server) {
		dir := t.TempDir()
		oldWD, err := os.Getwd()
		if err != nil {
			t.Fatalf("Getwd: %v", err)
		}
		if err := os.Chdir(dir); err != nil {
			t.Fatalf("Chdir: %v", err)
		}
		defer os.Chdir(oldWD)

		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check"})
		var out, errOut bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&errOut)
		err = cmd.Execute()
		var exitErr *ExitCodeError
		if !errors.As(err, &exitErr) || exitErr.Code != 10 {
			t.Fatalf("Execute() error = %v, want ExitCodeError 10", err)
		}
		if got := strings.TrimSpace(out.String()); got != "current=5.10.4 latest=5.10.5 available=true" {
			t.Fatalf("stdout = %q", got)
		}
		if errOut.String() != "" {
			t.Fatalf("stderr = %q", errOut.String())
		}
		if _, err := os.Stat(filepath.Join(dir, "spt.log")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("default spt.log was created or stat failed: %v", err)
		}
	})
}

func TestUpdateOutputDownloadsVerifiesAndWritesBinary(t *testing.T) {
	withUpdateCommandTestHooks(t, "dev", func(server *httptest.Server) {
		outPath := filepath.Join(t.TempDir(), "spt-release")
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--output", outPath})
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		got, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("ReadFile output: %v", err)
		}
		if string(got) != "release binary" {
			t.Fatalf("output file = %q", got)
		}
		if !strings.Contains(out.String(), "Wrote spt 5.10.5") {
			t.Fatalf("stdout = %q", out.String())
		}
	})
}

func TestUpdateRefusesDevBuildSelfReplace(t *testing.T) {
	withUpdateCommandTestHooks(t, "5.10.4-dev+abc1234", func(server *httptest.Server) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--yes"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted dev build self-replace")
		}
		if !strings.Contains(errOut.String(), "cannot self-update this build") {
			t.Fatalf("stderr = %q", errOut.String())
		}
	})
}

func TestUpdateWarnsForImageOverrideWithYes(t *testing.T) {
	withUpdateCommandTestHooks(t, "5.10.4", func(server *httptest.Server) {
		t.Setenv(constants.EnvSptImage, "example.com/spt:pinned")
		target := filepath.Join(t.TempDir(), "current-spt")
		if err := os.WriteFile(target, []byte("old"), 0o755); err != nil {
			t.Fatalf("WriteFile target: %v", err)
		}
		oldExecutable := updateExecutable
		updateExecutable = func() (string, error) { return target, nil }
		defer func() { updateExecutable = oldExecutable }()

		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--yes"})
		var errOut bytes.Buffer
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&errOut)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if !strings.Contains(errOut.String(), constants.EnvSptImage+" is set") {
			t.Fatalf("stderr = %q", errOut.String())
		}
	})
}

func withUpdateCommandTestHooks(t *testing.T, currentVersion string, fn func(*httptest.Server)) {
	t.Helper()
	binary := []byte("release binary")
	archive := gzipTestBytes(t, binary)
	sum := sha256.Sum256(archive)
	checksumLine := fmt.Sprintf("%s  spt-5.10.5-linux-amd64.gz\n", hex.EncodeToString(sum[:]))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	mux.HandleFunc("/repos/dell/storage-performance-tool/releases", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("per_page") != "100" {
			t.Fatalf("per_page = %q", r.URL.Query().Get("per_page"))
		}
		writeUpdateJSON(t, w, []updater.Release{{
			TagName: "v5.10.5",
			Assets: []updater.Asset{
				{Name: "spt-5.10.5-linux-amd64.gz", BrowserDownloadURL: server.URL + "/assets/spt.gz", Size: int64(len(archive))},
				{Name: updater.ChecksumAssetName, BrowserDownloadURL: server.URL + "/assets/SHA256SUMS", Size: int64(len(checksumLine))},
			},
		}})
	})
	mux.HandleFunc("/assets/spt.gz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(archive)
	})
	mux.HandleFunc("/assets/SHA256SUMS", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(checksumLine))
	})
	defer server.Close()

	oldVersion := updateCurrentVersion
	oldGOOS := updateRuntimeGOOS
	oldGOARCH := updateRuntimeGOARCH
	oldClient := updateNewGitHubClient
	updateCurrentVersion = func() string { return currentVersion }
	updateRuntimeGOOS = func() string { return "linux" }
	updateRuntimeGOARCH = func() string { return "amd64" }
	updateNewGitHubClient = func(timeout time.Duration, token string) updater.GitHubClient {
		return updater.GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: updater.DefaultGitHubOwner, Repo: updater.DefaultGitHubRepo, Token: token}
	}
	defer func() {
		updateCurrentVersion = oldVersion
		updateRuntimeGOOS = oldGOOS
		updateRuntimeGOARCH = oldGOARCH
		updateNewGitHubClient = oldClient
	}()

	fn(server)
}

func gzipTestBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func writeUpdateJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("json encode: %v", err)
	}
}
