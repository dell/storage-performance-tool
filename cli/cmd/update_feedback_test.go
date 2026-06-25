package cmd

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	updater "github.com/dell/storage-performance-tool/cli/internal/update"
)

func TestUpdateCheckDoesNotRequirePlatformAsset(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{
		currentVersion: "5.10.4",
		releaseAssets:  []updater.Asset{{Name: updater.ChecksumAssetName, BrowserDownloadURL: "https://example.com/SHA256SUMS"}},
	}, func(_ *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check"})
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		err := cmd.Execute()
		var exitErr *ExitCodeError
		if !errors.As(err, &exitErr) || exitErr.Code != 10 {
			t.Fatalf("Execute() error = %v, want exit 10", err)
		}
		if !strings.Contains(out.String(), "available=true") {
			t.Fatalf("stdout = %q", out.String())
		}
	})
}

func TestUpdateCheckDevBuildReportsAvailableFalse(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "dev"}, func(_ *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check"})
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute() error = %v, want nil for dev check", err)
		}
		if got := strings.TrimSpace(out.String()); got != "current=dev latest=5.10.5 available=false" {
			t.Fatalf("stdout = %q", got)
		}
	})
}

func TestUpdateAlreadyUpToDateDoesNotDownload(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.5"}, func(s *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--yes"})
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if s.assetDownloads != 0 {
			t.Fatalf("assetDownloads = %d, want 0", s.assetDownloads)
		}
		if !strings.Contains(out.String(), "already up to date") {
			t.Fatalf("stdout = %q", out.String())
		}
	})
}

func TestUpdateChecksumMismatchDoesNotReplace(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4", badChecksum: true}, func(_ *customUpdateServer) {
		target := filepath.Join(t.TempDir(), "spt")
		if err := os.WriteFile(target, []byte("old"), 0o755); err != nil {
			t.Fatalf("WriteFile target: %v", err)
		}
		withUpdateExecutable(t, target)

		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--yes"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted checksum mismatch")
		}
		got, readErr := os.ReadFile(target)
		if readErr != nil {
			t.Fatalf("ReadFile target: %v", readErr)
		}
		if string(got) != "old" {
			t.Fatalf("target was replaced on checksum mismatch: %q", got)
		}
		if !strings.Contains(errOut.String(), "checksum mismatch") {
			t.Fatalf("stderr = %q", errOut.String())
		}
	})
}

func TestUpdateReplaceAccessPrecheckRunsBeforeDownload(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4"}, func(s *customUpdateServer) {
		target := filepath.Join(t.TempDir(), "spt")
		if err := os.WriteFile(target, []byte("old"), 0o755); err != nil {
			t.Fatalf("WriteFile target: %v", err)
		}
		withUpdateExecutable(t, target)
		oldVerify := updateVerifyReplaceAccess
		updateVerifyReplaceAccess = func(string) error { return errors.New("permission denied") }
		defer func() { updateVerifyReplaceAccess = oldVerify }()

		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--yes"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted inaccessible target")
		}
		if s.assetDownloads != 0 {
			t.Fatalf("assetDownloads = %d, want 0 before access check failure", s.assetDownloads)
		}
		for _, want := range []string{"cannot replace", "use --output", "sudo"} {
			if !strings.Contains(errOut.String(), want) {
				t.Fatalf("stderr = %q missing %q", errOut.String(), want)
			}
		}
	})
}

func TestUpdateBlocksWindowsSelfReplaceBeforeDownload(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4", goos: "windows", goarch: "amd64"}, func(s *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--yes"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted Windows self-replace")
		}
		if s.assetDownloads != 0 {
			t.Fatalf("assetDownloads = %d, want 0", s.assetDownloads)
		}
		if !strings.Contains(errOut.String(), "use --output") {
			t.Fatalf("stderr = %q", errOut.String())
		}
	})
}

func TestUpdateUnsupportedPlatformFailsAtCommandLevel(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4", goos: "plan9", goarch: "mips"}, func(_ *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--yes"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted unsupported platform")
		}
		if !strings.Contains(errOut.String(), "unsupported platform") {
			t.Fatalf("stderr = %q", errOut.String())
		}
	})
}

func TestConfirmUpdateIfNeededReadsLine(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "empty defaults no", input: "\n", wantErr: true},
		{name: "n cancels", input: "n\n", wantErr: true},
		{name: "yes proceeds", input: "yes\n", wantErr: false},
		{name: "y proceeds", input: "y\n", wantErr: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newUpdateCommand()
			cmd.SetIn(strings.NewReader(tt.input))
			cmd.SetErr(&bytes.Buffer{})
			oldIsTerminal := updateIsTerminalInput
			updateIsTerminalInput = func(io.Reader) bool { return true }
			defer func() { updateIsTerminalInput = oldIsTerminal }()
			err := confirmUpdateIfNeeded(cmd, &updateOptions{}, "5.10.5", "/tmp/spt", "https://example.com/release", "5.10.4")
			if (err != nil) != tt.wantErr {
				t.Fatalf("confirmUpdateIfNeeded error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type customUpdateServerOptions struct {
	currentVersion string
	releaseAssets  []updater.Asset
	badChecksum    bool
	goos           string
	goarch         string
}

type customUpdateServer struct {
	server         *httptest.Server
	assetDownloads int
}

func withCustomUpdateServer(t *testing.T, opts customUpdateServerOptions, fn func(*customUpdateServer)) {
	t.Helper()
	binary := []byte("release binary")
	archive := gzipFeedbackBytes(t, binary)
	sum := sha256.Sum256(archive)
	if opts.badChecksum {
		sum = sha256.Sum256([]byte("wrong"))
	}
	checksumLine := fmt.Sprintf("%s  spt-5.10.5-linux-amd64.gz\n", hex.EncodeToString(sum[:]))
	state := &customUpdateServer{}
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	state.server = server
	assets := opts.releaseAssets
	if assets == nil {
		assets = []updater.Asset{
			{Name: "spt-5.10.5-linux-amd64.gz", BrowserDownloadURL: server.URL + "/assets/spt.gz", Size: int64(len(archive))},
			{Name: updater.ChecksumAssetName, BrowserDownloadURL: server.URL + "/assets/SHA256SUMS", Size: int64(len(checksumLine))},
		}
	}
	mux.HandleFunc("/repos/dell/storage-performance-tool/releases", func(w http.ResponseWriter, _ *http.Request) {
		writeFeedbackJSON(t, w, []updater.Release{{TagName: "v5.10.5", HTMLURL: "https://github.com/dell/storage-performance-tool/releases/tag/v5.10.5", Assets: assets}})
	})
	mux.HandleFunc("/assets/spt.gz", func(w http.ResponseWriter, _ *http.Request) {
		state.assetDownloads++
		_, _ = w.Write(archive)
	})
	mux.HandleFunc("/assets/SHA256SUMS", func(w http.ResponseWriter, _ *http.Request) {
		state.assetDownloads++
		_, _ = w.Write([]byte(checksumLine))
	})
	defer server.Close()

	oldVersion := updateCurrentVersion
	oldGOOS := updateRuntimeGOOS
	oldGOARCH := updateRuntimeGOARCH
	oldClient := updateNewGitHubClient
	oldVerify := updateVerifyReplaceAccess
	updateCurrentVersion = func() string { return opts.currentVersion }
	updateRuntimeGOOS = func() string {
		if opts.goos != "" {
			return opts.goos
		}
		return "linux"
	}
	updateRuntimeGOARCH = func() string {
		if opts.goarch != "" {
			return opts.goarch
		}
		return "amd64"
	}
	updateNewGitHubClient = func(timeout time.Duration, token string) updater.GitHubClient {
		return updater.GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: updater.DefaultGitHubOwner, Repo: updater.DefaultGitHubRepo, Token: token, UserAgent: "spt/test"}
	}
	updateVerifyReplaceAccess = func(string) error { return nil }
	defer func() {
		updateCurrentVersion = oldVersion
		updateRuntimeGOOS = oldGOOS
		updateRuntimeGOARCH = oldGOARCH
		updateNewGitHubClient = oldClient
		updateVerifyReplaceAccess = oldVerify
	}()

	fn(state)
}

func withUpdateExecutable(t *testing.T, target string) {
	t.Helper()
	oldExecutable := updateExecutable
	updateExecutable = func() (string, error) { return target, nil }
	t.Cleanup(func() { updateExecutable = oldExecutable })
}

func gzipFeedbackBytes(t *testing.T, data []byte) []byte {
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

func writeFeedbackJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("json encode: %v", err)
	}
}

var _ = runtime.GOOS
