package update

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
)

type GitHubClient struct {
	HTTPClient *http.Client
	BaseURL    string
	Owner      string
	Repo       string
	Token      string
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
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	baseURL := strings.TrimRight(c.BaseURL, "/")
	if baseURL == "" {
		baseURL = DefaultGitHubAPIBaseURL
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
	var releases []Release
	for pageURL != "" {
		var page []Release
		resp, err := c.getJSON(ctx, client, pageURL, &page)
		if err != nil {
			return nil, err
		}
		releases = append(releases, page...)
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
		if resp.StatusCode == http.StatusForbidden && resp.Header.Get("X-RateLimit-Remaining") == "0" {
			return nil, fmt.Errorf("GitHub API rate limit exceeded")
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
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	downloadURL := asset.BrowserDownloadURL
	accept := "application/octet-stream"
	if strings.TrimSpace(c.Token) != "" && asset.URL != "" {
		downloadURL = asset.URL
	}
	if downloadURL == "" {
		return nil, fmt.Errorf("asset %q has no download URL", asset.Name)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
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
