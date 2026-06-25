package update

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultGitHubAPIBaseURL = "https://api.github.com"
	DefaultGitHubOwner      = "dell"
	DefaultGitHubRepo       = "storage-performance-tool"
	defaultMaxReleasePages  = 10
)

type GitHubClient struct {
	HTTPClient *http.Client
	BaseURL    string
	Owner      string
	Repo       string
	Token      string
	UserAgent  string
	MaxPages   int
}

type PageLimitError struct {
	Limit int
}

func (e *PageLimitError) Error() string {
	return fmt.Sprintf("GitHub release pagination exceeded %d pages", e.Limit)
}

func NewGitHubClient(timeout time.Duration, token string) GitHubClient {
	return GitHubClient{
		HTTPClient: &http.Client{Timeout: timeout},
		BaseURL:    DefaultGitHubAPIBaseURL,
		Owner:      DefaultGitHubOwner,
		Repo:       DefaultGitHubRepo,
		Token:      token,
	}
}

func (c GitHubClient) ListReleases(ctx context.Context) ([]Release, error) {
	client := c.hardenedHTTPClient()
	baseURL := strings.TrimRight(c.BaseURL, "/")
	if baseURL == "" {
		baseURL = DefaultGitHubAPIBaseURL
	}
	if err := validateGitHubURL(baseURL); err != nil {
		return nil, err
	}
	owner := c.Owner
	if owner == "" {
		owner = DefaultGitHubOwner
	}
	repo := c.Repo
	if repo == "" {
		repo = DefaultGitHubRepo
	}

	pageURL := fmt.Sprintf("%s/repos/%s/%s/releases?per_page=100&page=1", baseURL, url.PathEscape(owner), url.PathEscape(repo))
	maxPages := c.MaxPages
	if maxPages <= 0 {
		maxPages = defaultMaxReleasePages
	}
	var releases []Release
	for page := 1; pageURL != ""; page++ {
		if page > maxPages {
			return nil, &PageLimitError{Limit: maxPages}
		}
		var releasePage []Release
		resp, err := c.getJSON(ctx, client, pageURL, &releasePage)
		if err != nil {
			return nil, err
		}
		releases = append(releases, releasePage...)
		pageURL = nextLink(resp.Header.Get("Link"))
	}
	return releases, nil
}

func (c GitHubClient) getJSON(ctx context.Context, client *http.Client, rawURL string, out any) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", c.userAgent())
	if strings.TrimSpace(c.Token) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(c.Token))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests) && resp.Header.Get("X-RateLimit-Remaining") == "0" {
			return nil, rateLimitError(resp)
		}
		return nil, fmt.Errorf("GitHub API request failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return nil, err
	}
	return resp, nil
}

func nextLink(linkHeader string) string {
	for _, part := range strings.Split(linkHeader, ",") {
		sections := strings.Split(part, ";")
		if len(sections) < 2 {
			continue
		}
		target := strings.TrimSpace(sections[0])
		if !strings.HasPrefix(target, "<") || !strings.HasSuffix(target, ">") {
			continue
		}
		for _, param := range sections[1:] {
			if strings.TrimSpace(param) == `rel="next"` {
				return strings.TrimSuffix(strings.TrimPrefix(target, "<"), ">")
			}
		}
	}
	return ""
}

func (c GitHubClient) DownloadAsset(ctx context.Context, asset Asset) ([]byte, error) {
	client := c.hardenedHTTPClient()
	downloadURL := asset.BrowserDownloadURL
	accept := "application/octet-stream"
	usingAssetAPI := strings.TrimSpace(c.Token) != "" && asset.URL != ""
	if usingAssetAPI {
		downloadURL = asset.URL
	}
	if downloadURL == "" {
		return nil, fmt.Errorf("asset %q has no download URL", asset.Name)
	}
	if err := validateAssetDownloadURL(downloadURL, usingAssetAPI); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", c.userAgent())
	if strings.TrimSpace(c.Token) != "" && asset.URL != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(c.Token))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("asset download failed: %s", resp.Status)
	}
	limit := int64(MaxCompressedAssetBytes + 1)
	if asset.Size > 0 && asset.Size < limit {
		limit = asset.Size + 1
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, err
	}
	if len(data) >= int(limit) {
		return nil, fmt.Errorf("asset %q exceeds compressed size limit %s", asset.Name, strconv.FormatInt(limit-1, 10))
	}
	return data, nil
}

func (c GitHubClient) hardenedHTTPClient() *http.Client {
	base := c.HTTPClient
	if base == nil {
		base = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *base
	previous := base.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := validateAssetDownloadURL(req.URL.String(), false); err != nil {
			return err
		}
		if len(via) > 0 && !sameHost(via[len(via)-1].URL, req.URL) {
			req.Header.Del("Authorization")
		}
		if previous != nil {
			return previous(req, via)
		}
		if len(via) >= 10 {
			return http.ErrUseLastResponse
		}
		return nil
	}
	return &clone
}

func (c GitHubClient) userAgent() string {
	if strings.TrimSpace(c.UserAgent) != "" {
		return strings.TrimSpace(c.UserAgent)
	}
	return "spt/unknown"
}

func validateGitHubURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if !secureURL(u) {
		return fmt.Errorf("refusing non-HTTPS GitHub URL %q", rawURL)
	}
	if isLocalHTTP(u) {
		return nil
	}
	if host := strings.ToLower(u.Hostname()); host != "api.github.com" {
		return fmt.Errorf("refusing non-GitHub API host %q", host)
	}
	return nil
}

func validateAssetDownloadURL(rawURL string, authenticatedAPI bool) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if !secureURL(u) {
		return fmt.Errorf("refusing non-HTTPS asset URL %q", rawURL)
	}
	if isLocalHTTP(u) {
		return nil
	}
	host := strings.ToLower(u.Hostname())
	if authenticatedAPI && host != "api.github.com" {
		return fmt.Errorf("refusing to send authenticated asset request to non-GitHub API host %q", host)
	}
	if !allowedGitHubAssetHost(host) {
		return fmt.Errorf("refusing non-GitHub asset host %q", host)
	}
	return nil
}

func secureURL(u *url.URL) bool {
	return u.Scheme == "https" || isLocalHTTP(u)
}

func isLocalHTTP(u *url.URL) bool {
	if u.Scheme != "http" {
		return false
	}
	host := u.Hostname()
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func allowedGitHubAssetHost(host string) bool {
	return host == "api.github.com" || host == "github.com" || strings.HasSuffix(host, ".githubusercontent.com")
}

func sameHost(a, b *url.URL) bool {
	return strings.EqualFold(a.Hostname(), b.Hostname())
}

func rateLimitError(resp *http.Response) error {
	reset := resp.Header.Get("X-RateLimit-Reset")
	if reset == "" {
		reset = "unknown"
	}
	return fmt.Errorf("GitHub API rate limit exceeded; set SPT_GITHUB_TOKEN or GITHUB_TOKEN to raise the limit and retry (X-RateLimit-Reset=%s)", reset)
}
