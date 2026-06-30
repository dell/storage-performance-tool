package update

import (
	"context"
	"encoding/json"
	"errors"
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
	// DefaultGitHubAPIBaseURL is the default GitHub REST API base URL.
	DefaultGitHubAPIBaseURL = "https://api.github.com"
	// DefaultGitHubOwner is the GitHub owner that publishes SPT releases.
	DefaultGitHubOwner = "dell"
	// DefaultGitHubRepo is the GitHub repository that publishes SPT releases.
	DefaultGitHubRepo      = "storage-performance-tool"
	githubAPIHost          = "api.github.com"
	githubWebHost          = "github.com"
	defaultMaxReleasePages = 10
)

// GitHubClient lists releases and downloads release assets from GitHub.
type GitHubClient struct {
	HTTPClient             *http.Client
	BaseURL                string
	Owner                  string
	Repo                   string
	Token                  string
	UserAgent              string
	MaxPages               int
	AllowInsecureLocalHTTP bool
}

// PageLimitError reports that GitHub release pagination exceeded the configured cap.
type PageLimitError struct {
	Limit int
}

func (e *PageLimitError) Error() string {
	return fmt.Sprintf("GitHub release pagination exceeded %d pages", e.Limit)
}

// RateLimitError reports a GitHub API rate-limit response (primary or secondary).
type RateLimitError struct {
	Reset      string
	RetryAfter string
}

func (e *RateLimitError) Error() string {
	when := e.Reset
	label := "X-RateLimit-Reset"
	if when == "" && e.RetryAfter != "" {
		when = e.RetryAfter
		label = "Retry-After"
	}
	if when == "" {
		when = "unknown"
	}
	return fmt.Sprintf("GitHub API rate limit exceeded; set SPT_GITHUB_TOKEN or GITHUB_TOKEN to raise the limit and retry (%s=%s)", label, when)
}

// IsRateLimitError reports whether err is a GitHub API rate-limit response.
func IsRateLimitError(err error) bool {
	var rateErr *RateLimitError
	return errors.As(err, &rateErr)
}

// BadCredentialsError reports a GitHub API authentication failure.
type BadCredentialsError struct {
	Status string
	Body   string
}

func (e *BadCredentialsError) Error() string {
	return fmt.Sprintf("GitHub API request failed: %s: %s", e.Status, e.Body)
}

// IsBadCredentialsError reports whether err is a GitHub API authentication failure.
func IsBadCredentialsError(err error) bool {
	var authErr *BadCredentialsError
	return errors.As(err, &authErr)
}

// NewGitHubClient returns a GitHub client with default SPT repository settings.
func NewGitHubClient(timeout time.Duration, token string) GitHubClient {
	return GitHubClient{
		HTTPClient: &http.Client{Timeout: timeout},
		BaseURL:    DefaultGitHubAPIBaseURL,
		Owner:      DefaultGitHubOwner,
		Repo:       DefaultGitHubRepo,
		Token:      token,
		UserAgent:  "spt/unknown",
	}
}

// ListReleases returns all release pages needed for update selection.
func (c GitHubClient) ListReleases(ctx context.Context) ([]Release, error) {
	client := c.hardenedHTTPClient()
	baseURL := strings.TrimRight(c.BaseURL, "/")
	if baseURL == "" {
		baseURL = DefaultGitHubAPIBaseURL
	}
	if err := validateGitHubURL(baseURL, c.AllowInsecureLocalHTTP); err != nil {
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
		headers, err := c.getJSON(ctx, client, pageURL, &releasePage)
		if err != nil {
			return nil, err
		}
		releases = append(releases, releasePage...)
		pageURL = nextLink(headers.Get("Link"))
	}
	return releases, nil
}

func (c GitHubClient) getJSON(ctx context.Context, client *http.Client, rawURL string, out any) (http.Header, error) {
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
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		bodyText := strings.TrimSpace(string(body))
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
			// Primary rate limits report X-RateLimit-Remaining: 0; secondary
			// (abuse) limits report Retry-After without that header.
			if resp.Header.Get("X-RateLimit-Remaining") == "0" || resp.Header.Get("Retry-After") != "" {
				return nil, rateLimitError(resp)
			}
		}
		if resp.StatusCode == http.StatusUnauthorized {
			return nil, &BadCredentialsError{Status: resp.Status, Body: bodyText}
		}
		return nil, fmt.Errorf("GitHub API request failed: %s: %s", resp.Status, bodyText)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return nil, err
	}
	return resp.Header.Clone(), nil
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

// DownloadAsset downloads a release asset with size and transport checks.
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
	if err := validateAssetDownloadURL(downloadURL, usingAssetAPI, c.AllowInsecureLocalHTTP); err != nil {
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
	defer func() { _ = resp.Body.Close() }()
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
		if err := validateAssetDownloadURL(req.URL.String(), false, c.AllowInsecureLocalHTTP); err != nil {
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

func validateGitHubURL(rawURL string, allowInsecureLocalHTTP bool) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if !secureURL(u, allowInsecureLocalHTTP) {
		return fmt.Errorf("refusing non-HTTPS GitHub URL %q", rawURL)
	}
	if allowInsecureLocalHTTP && isLocalHTTP(u) {
		return nil
	}
	if host := strings.ToLower(u.Hostname()); host != githubAPIHost {
		return fmt.Errorf("refusing non-GitHub API host %q", host)
	}
	return nil
}

func validateAssetDownloadURL(rawURL string, authenticatedAPI, allowInsecureLocalHTTP bool) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if !secureURL(u, allowInsecureLocalHTTP) {
		return fmt.Errorf("refusing non-HTTPS asset URL %q", rawURL)
	}
	if allowInsecureLocalHTTP && isLocalHTTP(u) {
		return nil
	}
	host := strings.ToLower(u.Hostname())
	if authenticatedAPI && host != githubAPIHost {
		return fmt.Errorf("refusing to send authenticated asset request to non-GitHub API host %q", host)
	}
	if !allowedGitHubAssetHost(host) {
		return fmt.Errorf("refusing non-GitHub asset host %q", host)
	}
	return nil
}

func secureURL(u *url.URL, allowInsecureLocalHTTP bool) bool {
	return u.Scheme == "https" || (allowInsecureLocalHTTP && isLocalHTTP(u))
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
	return host == githubAPIHost || host == githubWebHost || strings.HasSuffix(host, ".githubusercontent.com")
}

func sameHost(a, b *url.URL) bool {
	return strings.EqualFold(a.Hostname(), b.Hostname())
}

func rateLimitError(resp *http.Response) error {
	return &RateLimitError{
		Reset:      resp.Header.Get("X-RateLimit-Reset"),
		RetryAfter: resp.Header.Get("Retry-After"),
	}
}
