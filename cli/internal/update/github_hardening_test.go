package update

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGitHubClientSetsUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "spt/5.10.4" {
			t.Fatalf("User-Agent = %q", got)
		}
		writeJSON(t, w, []Release{})
	}))
	defer server.Close()

	client := GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: "dell", Repo: "storage-performance-tool", UserAgent: "spt/5.10.4", AllowInsecureLocalHTTP: true}
	if _, err := client.ListReleases(context.Background()); err != nil {
		t.Fatalf("ListReleases returned error: %v", err)
	}
}

func TestGitHubClientRateLimitErrorIsActionable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", "1782420000")
		http.Error(w, "rate limited", http.StatusForbidden)
	}))
	defer server.Close()

	client := GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: "dell", Repo: "storage-performance-tool", AllowInsecureLocalHTTP: true}
	_, err := client.ListReleases(context.Background())
	if err == nil {
		t.Fatal("ListReleases accepted rate-limit response")
	}
	msg := err.Error()
	for _, want := range []string{"rate limit", "SPT_GITHUB_TOKEN", "GITHUB_TOKEN", "1782420000"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("rate-limit error %q missing %q", msg, want)
		}
	}
}

func TestGitHubClientRejectsInsecureBaseURL(t *testing.T) {
	client := GitHubClient{BaseURL: "http://api.github.com", Owner: "dell", Repo: "storage-performance-tool"}
	if _, err := client.ListReleases(context.Background()); err == nil {
		t.Fatal("ListReleases accepted insecure base URL")
	}
}

func TestGitHubClientRejectsInsecureAssetURL(t *testing.T) {
	client := GitHubClient{}
	_, err := client.DownloadAsset(context.Background(), Asset{Name: "asset.gz", BrowserDownloadURL: "http://example.com/asset.gz"})
	if err == nil {
		t.Fatal("DownloadAsset accepted insecure asset URL")
	}
}

func TestGitHubClientRejectsNonGitHubAuthenticatedAssetURL(t *testing.T) {
	client := GitHubClient{Token: "token"}
	_, err := client.DownloadAsset(context.Background(), Asset{Name: "asset.gz", URL: "https://evil.example.com/asset"})
	if err == nil {
		t.Fatal("DownloadAsset accepted non-GitHub authenticated asset URL")
	}
}

func TestGitHubClientPageCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", `<`+serverURL(t, r)+`?per_page=100&page=2>; rel="next"`)
		writeJSON(t, w, []Release{})
	}))
	defer server.Close()

	client := GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: "dell", Repo: "storage-performance-tool", MaxPages: 1, AllowInsecureLocalHTTP: true}
	_, err := client.ListReleases(context.Background())
	if err == nil {
		t.Fatal("ListReleases followed pagination beyond cap")
	}
	var pageErr *PageLimitError
	if !errors.As(err, &pageErr) {
		t.Fatalf("error = %T %v, want PageLimitError", err, err)
	}
}
