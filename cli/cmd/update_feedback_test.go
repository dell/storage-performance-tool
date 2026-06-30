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

	"github.com/dell/storage-performance-tool/cli/internal/constants"
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

func TestUpdateCheckUnsupportedPlatformFails(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4", goos: "linux", goarch: "386"}, func(_ *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted unsupported check platform")
		}
		if !strings.Contains(errOut.String(), "unsupported platform linux/386") {
			t.Fatalf("stderr = %q", errOut.String())
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

func TestUpdateCheckIgnoresEnvTokenWhenUnauthenticatedLookupSucceeds(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "stale-token")
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4"}, func(s *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check"})
		var out, errOut bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		var exitErr *ExitCodeError
		if !errors.As(err, &exitErr) || exitErr.Code != 10 {
			t.Fatalf("Execute() error = %v, want exit 10", err)
		}
		if got := s.releaseAuthHeaders; len(got) != 1 || got[0] != "" {
			t.Fatalf("release auth headers = %#v, want unauthenticated request", got)
		}
		if errOut.String() != "" {
			t.Fatalf("stderr = %q", errOut.String())
		}
		if !strings.Contains(out.String(), "available=true") {
			t.Fatalf("stdout = %q", out.String())
		}
	})
}

func TestUpdateCheckRetriesWithEnvTokenOnRateLimit(t *testing.T) {
	t.Setenv("SPT_GITHUB_TOKEN", "good-token")
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4", rateLimitUnauth: true, acceptedToken: "good-token"}, func(s *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check"})
		var out, errOut bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		var exitErr *ExitCodeError
		if !errors.As(err, &exitErr) || exitErr.Code != 10 {
			t.Fatalf("Execute() error = %v, want exit 10", err)
		}
		want := []string{"", "Bearer good-token"}
		if got := s.releaseAuthHeaders; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("release auth headers = %#v, want %#v", got, want)
		}
		if errOut.String() != "" {
			t.Fatalf("stderr = %q", errOut.String())
		}
		if !strings.Contains(out.String(), "available=true") {
			t.Fatalf("stdout = %q", out.String())
		}
	})
}

// Explicit --token expresses intent, so it must be sent (and validated) on the
// first request even when an unauthenticated lookup would have succeeded.
func TestUpdateCheckReportsExplicitBadTokenEvenWhenUnauthWouldSucceed(t *testing.T) {
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4", acceptedToken: "good-token"}, func(s *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check", "--token", "bad-token"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted bad explicit token")
		}
		for _, want := range []string{"--token", "invalid"} {
			if !strings.Contains(errOut.String(), want) {
				t.Fatalf("stderr = %q missing %q", errOut.String(), want)
			}
		}
		if got := s.releaseAuthHeaders; len(got) != 1 || got[0] != "Bearer bad-token" {
			t.Fatalf("release auth headers = %#v, want a single authenticated request", got)
		}
	})
}

// A bad ambient token must not trigger a doomed unauthenticated retry: the
// original rate-limit error is surfaced and the token is reported as ignored.
func TestUpdateCheckWarnsAndReturnsRateLimitWhenEnvTokenIsBad(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "bad-token")
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "5.10.4", rateLimitUnauth: true, acceptedToken: "good-token"}, func(s *customUpdateServer) {
		cmd := newUpdateCommand()
		cmd.SetArgs([]string{"--check"})
		var errOut bytes.Buffer
		cmd.SetErr(&errOut)
		err := cmd.Execute()
		if err == nil {
			t.Fatal("Execute() accepted rate-limited unauthenticated lookup with bad env token")
		}
		wantHeaders := []string{"", "Bearer bad-token"}
		if got := s.releaseAuthHeaders; len(got) != len(wantHeaders) || got[0] != wantHeaders[0] || got[1] != wantHeaders[1] {
			t.Fatalf("release auth headers = %#v, want %#v (no doomed third request)", got, wantHeaders)
		}
		for _, want := range []string{"warning: GITHUB_TOKEN appears invalid", "rate limit"} {
			if !strings.Contains(errOut.String(), want) {
				t.Fatalf("stderr = %q missing %q", errOut.String(), want)
			}
		}
	})
}

// The client returned from the rate-limit retry must be the authenticated one,
// and it must carry the token through to the asset download.
func TestUpdateDownloadsWithAuthClientAfterRateLimitRetry(t *testing.T) {
	t.Setenv("SPT_GITHUB_TOKEN", "good-token")
	withCustomUpdateServer(t, customUpdateServerOptions{currentVersion: "dev", rateLimitUnauth: true, acceptedToken: "good-token", assetUsesAPIURL: true}, func(s *customUpdateServer) {
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
		wantReleaseHeaders := []string{"", "Bearer good-token"}
		if h := s.releaseAuthHeaders; len(h) != len(wantReleaseHeaders) || h[0] != wantReleaseHeaders[0] || h[1] != wantReleaseHeaders[1] {
			t.Fatalf("release auth headers = %#v, want %#v", h, wantReleaseHeaders)
		}
		if len(s.assetAuthHeaders) == 0 {
			t.Fatal("no asset downloads recorded")
		}
		for i, h := range s.assetAuthHeaders {
			if h != "Bearer good-token" {
				t.Fatalf("asset auth header[%d] = %q, want authenticated download", i, h)
			}
		}
	})
}

func TestResolveUpdateTokenPrecedence(t *testing.T) {
	cases := []struct {
		name      string
		flag      string
		sptEnv    string
		githubEnv string
		wantValue string
		wantName  string
		wantSrc   updateTokenSource
	}{
		{name: "flag wins over env", flag: "flag-token", sptEnv: "spt-token", githubEnv: "gh-token", wantValue: "flag-token", wantName: "--token", wantSrc: updateTokenSourceFlag},
		{name: "spt env wins over github env", sptEnv: "spt-token", githubEnv: "gh-token", wantValue: "spt-token", wantName: constants.EnvSptGitHubToken, wantSrc: updateTokenSourceEnv},
		{name: "github env used when others unset", githubEnv: "gh-token", wantValue: "gh-token", wantName: "GITHUB_TOKEN", wantSrc: updateTokenSourceEnv},
		{name: "none when all unset", wantValue: "", wantSrc: updateTokenSourceNone},
		{name: "whitespace flag is ignored", flag: "   ", githubEnv: "gh-token", wantValue: "gh-token", wantName: "GITHUB_TOKEN", wantSrc: updateTokenSourceEnv},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(constants.EnvSptGitHubToken, tt.sptEnv)
			t.Setenv("GITHUB_TOKEN", tt.githubEnv)
			got := resolveUpdateToken(tt.flag)
			if got.Value != tt.wantValue {
				t.Fatalf("Value = %q, want %q", got.Value, tt.wantValue)
			}
			if got.Src != tt.wantSrc {
				t.Fatalf("Src = %q, want %q", got.Src, tt.wantSrc)
			}
			if tt.wantName != "" && got.Name != tt.wantName {
				t.Fatalf("Name = %q, want %q", got.Name, tt.wantName)
			}
		})
	}
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
	currentVersion  string
	releaseAssets   []updater.Asset
	badChecksum     bool
	goos            string
	goarch          string
	rateLimitUnauth bool
	acceptedToken   string
	assetUsesAPIURL bool
}

type customUpdateServer struct {
	server             *httptest.Server
	assetDownloads     int
	releaseAuthHeaders []string
	assetAuthHeaders   []string
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
		if opts.assetUsesAPIURL {
			// Setting the asset API URL makes the client authenticate the
			// download, so we can observe which client performed the fetch.
			assets[0].URL = server.URL + "/assets/spt.gz"
			assets[1].URL = server.URL + "/assets/SHA256SUMS"
		}
	}
	mux.HandleFunc("/repos/dell/storage-performance-tool/releases", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		state.releaseAuthHeaders = append(state.releaseAuthHeaders, auth)
		if opts.rateLimitUnauth && auth == "" {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", "1782420000")
			http.Error(w, "rate limited", http.StatusForbidden)
			return
		}
		if opts.acceptedToken != "" && auth != "Bearer "+opts.acceptedToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"Bad credentials","status":"401"}`))
			return
		}
		writeFeedbackJSON(t, w, []updater.Release{{TagName: "v5.10.5", HTMLURL: "https://github.com/dell/storage-performance-tool/releases/tag/v5.10.5", Assets: assets}})
	})
	mux.HandleFunc("/assets/spt.gz", func(w http.ResponseWriter, r *http.Request) {
		state.assetDownloads++
		state.assetAuthHeaders = append(state.assetAuthHeaders, r.Header.Get("Authorization"))
		_, _ = w.Write(archive)
	})
	mux.HandleFunc("/assets/SHA256SUMS", func(w http.ResponseWriter, r *http.Request) {
		state.assetDownloads++
		state.assetAuthHeaders = append(state.assetAuthHeaders, r.Header.Get("Authorization"))
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
		return updater.GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: updater.DefaultGitHubOwner, Repo: updater.DefaultGitHubRepo, Token: token, UserAgent: "spt/test", AllowInsecureLocalHTTP: true}
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
