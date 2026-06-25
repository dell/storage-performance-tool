package update

import "fmt"

type Channel int

const (
	ChannelStable Channel = iota
	ChannelPrerelease
)

type Release struct {
	TagName    string  `json:"tag_name"`
	Draft      bool    `json:"draft"`
	Prerelease bool    `json:"prerelease"`
	HTMLURL    string  `json:"html_url"`
	Assets     []Asset `json:"assets"`
	version    Version
}

type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	URL                string `json:"url"`
	Size               int64  `json:"size"`
}

func SelectLatestRelease(releases []Release, channel Channel) (Release, error) {
	var best Release
	found := false
	for _, r := range releases {
		v, ok := releaseVersionForChannel(r, channel)
		if !ok {
			continue
		}
		r.version = v
		if !found || v.Compare(best.version) > 0 {
			best = r
			found = true
		}
	}
	if !found {
		return Release{}, fmt.Errorf("no matching release found")
	}
	return best, nil
}

func releaseVersionForChannel(r Release, channel Channel) (Version, bool) {
	if r.Draft {
		return Version{}, false
	}
	v, err := ParseTag(r.TagName)
	if err != nil {
		return Version{}, false
	}
	if v.IsPrerelease() != r.Prerelease {
		return Version{}, false
	}
	if channel == ChannelStable && v.IsPrerelease() {
		return Version{}, false
	}
	return v, true
}
