package replay

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

const maxArtifactBytes int64 = 16 << 20

var hrefRe = regexp.MustCompile(`(?i)href\s*=\s*["']?([^"'>\s]+)`)

// FetchArtifacts fetches a PDC-style folder listing, discovers the run script,
// and fetches the run script plus referenced scenario.
func FetchArtifacts(ctx context.Context, sourceURL string, client *http.Client) (Artifacts, error) {
	folderURL, err := normalizeFolderURL(sourceURL)
	if err != nil {
		return Artifacts{}, err
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	listing, err := fetchBytes(ctx, client, folderURL)
	if err != nil {
		return Artifacts{}, fmt.Errorf("fetch folder listing: %w", err)
	}
	links := parseLinks(string(listing))

	runHref, err := selectRunScript(links)
	if err != nil {
		return Artifacts{}, err
	}
	runURL, runName, err := resolveFolderLink(folderURL, runHref)
	if err != nil {
		return Artifacts{}, fmt.Errorf("resolve run script URL: %w", err)
	}
	runBody, err := fetchBytes(ctx, client, runURL)
	if err != nil {
		return Artifacts{}, fmt.Errorf("fetch run script %s: %w", runName, err)
	}
	runScript, err := ParseRunScript(string(runBody))
	if err != nil {
		return Artifacts{}, err
	}

	scenarioHref, err := selectScenario(links, runScript.ScenarioPath)
	if err != nil {
		return Artifacts{}, err
	}
	scenarioURL, scenarioName, err := resolveFolderLink(folderURL, scenarioHref)
	if err != nil {
		return Artifacts{}, fmt.Errorf("resolve scenario URL: %w", err)
	}
	scenarioBody, err := fetchBytes(ctx, client, scenarioURL)
	if err != nil {
		return Artifacts{}, fmt.Errorf("fetch scenario %s: %w", scenarioName, err)
	}

	return Artifacts{
		FolderURL:      folderURL.String(),
		RunScriptName:  runName,
		ScenarioName:   scenarioName,
		RunScript:      runScript,
		ScenarioBody:   scenarioBody,
		ScenarioFormat: strings.TrimPrefix(strings.ToLower(path.Ext(scenarioName)), "."),
	}, nil
}

func normalizeFolderURL(raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("--from URL is required")
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid --from URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported replay URL scheme %q; use http or https", u.Scheme)
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u, nil
}

func fetchBytes(ctx context.Context, client *http.Client, u *url.URL) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxArtifactBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxArtifactBytes {
		return nil, fmt.Errorf("artifact exceeds max size %d bytes", maxArtifactBytes)
	}
	return data, nil
}

func parseLinks(html string) []string {
	matches := hrefRe.FindAllStringSubmatch(html, -1)
	links := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, m := range matches {
		if len(m) != 2 {
			continue
		}
		link := strings.TrimSpace(m[1])
		if link == "" || link == "../" || strings.HasPrefix(link, "#") {
			continue
		}
		if _, ok := seen[link]; ok {
			continue
		}
		seen[link] = struct{}{}
		links = append(links, link)
	}
	sort.Strings(links)
	return links
}

func selectRunScript(links []string) (string, error) {
	var all []string
	var namedRun []string
	for _, link := range links {
		name := path.Base(link)
		if strings.EqualFold(path.Ext(name), ".sh") {
			all = append(all, link)
			if strings.Contains(strings.ToLower(name), "run") {
				namedRun = append(namedRun, link)
			}
		}
	}
	switch {
	case len(namedRun) == 1:
		return namedRun[0], nil
	case len(namedRun) > 1:
		return "", fmt.Errorf("multiple run script candidates found: %s", strings.Join(baseNames(namedRun), ", "))
	case len(all) == 1:
		return all[0], nil
	case len(all) > 1:
		return "", fmt.Errorf("multiple shell script candidates found: %s", strings.Join(baseNames(all), ", "))
	default:
		return "", fmt.Errorf("no .sh run script found in replay folder")
	}
}

func selectScenario(links []string, scenarioPath string) (string, error) {
	want := path.Base(strings.TrimSpace(scenarioPath))
	if want == "." || want == "/" || want == "" {
		want = ""
	}
	wantExt := strings.ToLower(path.Ext(want))
	if wantExt == "" {
		wantExt = ".json"
	}
	var scenarioLinks []string
	for _, link := range links {
		ext := strings.ToLower(path.Ext(path.Base(link)))
		if ext == wantExt {
			scenarioLinks = append(scenarioLinks, link)
		}
	}
	if len(scenarioLinks) == 0 {
		return "", fmt.Errorf("no %s scenario artifact found in replay folder", strings.TrimPrefix(wantExt, "."))
	}
	for _, link := range scenarioLinks {
		if path.Base(link) == want {
			return link, nil
		}
	}
	wantStem := strings.TrimSuffix(want, wantExt)
	if wantStem != "" {
		var prefixMatches []string
		for _, link := range scenarioLinks {
			stem := strings.TrimSuffix(path.Base(link), path.Ext(path.Base(link)))
			if strings.HasPrefix(stem, wantStem+".result.") {
				prefixMatches = append(prefixMatches, link)
			}
		}
		if len(prefixMatches) == 1 {
			return prefixMatches[0], nil
		}
		if len(prefixMatches) > 1 {
			return "", fmt.Errorf("multiple scenario candidates match %s: %s", want, strings.Join(baseNames(prefixMatches), ", "))
		}
	}
	return "", fmt.Errorf("scenario %s was not found in replay folder", want)
}

func resolveFolderLink(folderURL *url.URL, href string) (*url.URL, string, error) {
	rel, err := url.Parse(href)
	if err != nil {
		return nil, "", err
	}
	if rel.Scheme != "" && !strings.Contains(href, "://") {
		rel, err = url.Parse("./" + href)
		if err != nil {
			return nil, "", err
		}
	}
	resolved := folderURL.ResolveReference(rel)
	if resolved.Scheme != folderURL.Scheme || resolved.Host != folderURL.Host || !strings.HasPrefix(resolved.Path, folderURL.Path) {
		return nil, "", fmt.Errorf("artifact link %q escapes replay folder", href)
	}
	name := path.Base(resolved.Path)
	if name == "." || name == "/" || name == "" {
		return nil, "", fmt.Errorf("artifact link %q does not name a file", href)
	}
	return resolved, name, nil
}

func baseNames(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, path.Base(value))
	}
	return out
}
