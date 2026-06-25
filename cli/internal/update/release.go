package update

import "fmt"

// Channel selects which release channel should be considered for updates.
type Channel int

const (
	// ChannelStable selects only stable release tags.
	ChannelStable Channel = iota
	// ChannelPrerelease selects stable and prerelease tags.
	ChannelPrerelease
)

// Release is the GitHub release metadata used by update selection.
type Release struct {
	TagName    string  `json:"tag_name"`
	Draft      bool    `json:"draft"`
	Prerelease bool    `json:"prerelease"`
	HTMLURL    string  `json:"html_url"`
	Assets     []Asset `json:"assets"`
	version    Version
}

// Asset is the GitHub release asset metadata used by the updater.
type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	URL                string `json:"url"`
	Size               int64  `json:"size"`
}

// Version parses the release tag into a semantic version.
func (r Release) Version() (Version, error) {
	return ParseTag(r.TagName)
}

// SelectLatestRelease returns the highest-precedence release for channel.
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
