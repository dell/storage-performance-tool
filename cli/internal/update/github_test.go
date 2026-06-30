package update

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGitHubClientListReleasesPaginates(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
			t.Fatalf("Accept header = %q", got)
		}
		if got := r.URL.Path; got != "/repos/dell/storage-performance-tool/releases" {
			t.Fatalf("path = %q", got)
		}
		pages = append(pages, r.URL.Query().Get("page"))
		switch r.URL.Query().Get("page") {
		case "", "1":
			w.Header().Set("Link", `<`+serverURL(t, r)+`?per_page=100&page=2>; rel="next"`)
			writeJSON(t, w, []Release{{TagName: "v5.10.4"}})
		case "2":
			writeJSON(t, w, []Release{{TagName: "v5.11.0"}})
		default:
			t.Fatalf("unexpected page %q", r.URL.Query().Get("page"))
		}
	}))
	defer server.Close()

	client := GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: "dell", Repo: "storage-performance-tool", AllowInsecureLocalHTTP: true}
	releases, err := client.ListReleases(context.Background())
	if err != nil {
		t.Fatalf("ListReleases returned error: %v", err)
	}
	if len(releases) != 2 {
		t.Fatalf("len(releases) = %d, want 2", len(releases))
	}
	if len(pages) != 2 || pages[0] != "1" || pages[1] != "2" {
		t.Fatalf("pages = %#v, want [1 2]", pages)
	}
}

func TestGitHubClientListReleasesUsesToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization header = %q", got)
		}
		writeJSON(t, w, []Release{})
	}))
	defer server.Close()

	client := GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: "dell", Repo: "storage-performance-tool", Token: "test-token", AllowInsecureLocalHTTP: true}
	if _, err := client.ListReleases(context.Background()); err != nil {
		t.Fatalf("ListReleases returned error: %v", err)
	}
}

func TestGitHubClientListReleasesRateLimitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		http.Error(w, "rate limited", http.StatusForbidden)
	}))
	defer server.Close()

	client := GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: "dell", Repo: "storage-performance-tool", AllowInsecureLocalHTTP: true}
	if _, err := client.ListReleases(context.Background()); err == nil {
		t.Fatal("ListReleases accepted rate-limit response")
	}
}

func TestGitHubClientListReleasesSecondaryRateLimitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Secondary/abuse limits return 403 with Retry-After and no
		// X-RateLimit-Remaining: 0.
		w.Header().Set("Retry-After", "60")
		http.Error(w, "secondary rate limit", http.StatusForbidden)
	}))
	defer server.Close()

	client := GitHubClient{HTTPClient: server.Client(), BaseURL: server.URL, Owner: "dell", Repo: "storage-performance-tool", AllowInsecureLocalHTTP: true}
	_, err := client.ListReleases(context.Background())
	if err == nil {
		t.Fatal("ListReleases accepted secondary rate-limit response")
	}
	if !IsRateLimitError(err) {
		t.Fatalf("err = %v, want RateLimitError for secondary rate limit", err)
	}
}

func serverURL(t *testing.T, r *http.Request) string {
	t.Helper()
	return "http://" + r.Host + r.URL.Path
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("json encode: %v", err)
	}
}
