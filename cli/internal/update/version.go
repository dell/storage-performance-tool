package update

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	versionRE    = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z][0-9A-Za-z.-]*))?$`)
	numericIDRE  = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	devVersionRE = regexp.MustCompile(`(?:^dev$|-dev(?:[+.-]|$)|-SNAPSHOT$)`)
)

// Version is a parsed semantic version without build metadata.
type Version struct {
	major int
	minor int
	patch int
	pre   []string
}

// ParseVersion parses a semantic version string without a leading v prefix.
func ParseVersion(s string) (Version, error) {
	m := versionRE.FindStringSubmatch(s)
	if m == nil {
		return Version{}, fmt.Errorf("invalid semver version %q", s)
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])
	v := Version{major: major, minor: minor, patch: patch}
	if m[4] != "" {
		v.pre = strings.Split(m[4], ".")
		for _, id := range v.pre {
			if id == "" {
				return Version{}, fmt.Errorf("invalid empty prerelease identifier in %q", s)
			}
			if numericIDRE.MatchString(id) && len(id) > 1 && id[0] == '0' {
				return Version{}, fmt.Errorf("invalid numeric prerelease identifier %q", id)
			}
		}
	}
	return v, nil
}

// ParseTag parses a GitHub release tag with the required leading v prefix.
func ParseTag(tag string) (Version, error) {
	if !strings.HasPrefix(tag, "v") {
		return Version{}, fmt.Errorf("release tag %q does not start with v", tag)
	}
	return ParseVersion(strings.TrimPrefix(tag, "v"))
}

func (v Version) String() string {
	base := fmt.Sprintf("%d.%d.%d", v.major, v.minor, v.patch)
	if len(v.pre) == 0 {
		return base
	}
	return base + "-" + strings.Join(v.pre, ".")
}

// IsPrerelease reports whether v has a prerelease suffix.
func (v Version) IsPrerelease() bool {
	return len(v.pre) > 0
}

// Compare compares v with other using semantic-version precedence.
func (v Version) Compare(other Version) int {
	if c := compareInt(v.major, other.major); c != 0 {
		return c
	}
	if c := compareInt(v.minor, other.minor); c != 0 {
		return c
	}
	if c := compareInt(v.patch, other.patch); c != 0 {
		return c
	}
	return comparePrerelease(v.pre, other.pre)
}

// CanSelfUpdateCurrentBuild reports whether a current version may replace itself.
func CanSelfUpdateCurrentBuild(version string) (bool, string) {
	if version == "" {
		return false, "empty version"
	}
	if devVersionRE.MatchString(version) {
		return false, "local or snapshot build cannot self-update"
	}
	if _, err := ParseVersion(version); err != nil {
		return false, err.Error()
	}
	return true, ""
}

// Available reports whether latest has higher precedence than current.
func Available(current, latest Version) bool {
	return latest.Compare(current) > 0
}

func compareInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func comparePrerelease(a, b []string) int {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	if len(a) == 0 {
		return 1
	}
	if len(b) == 0 {
		return -1
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] == b[i] {
			continue
		}
		aNum, aIsNum := parseNumericIdentifier(a[i])
		bNum, bIsNum := parseNumericIdentifier(b[i])
		switch {
		case aIsNum && bIsNum:
			return compareInt(aNum, bNum)
		case aIsNum:
			return -1
		case bIsNum:
			return 1
		case a[i] < b[i]:
			return -1
		default:
			return 1
		}
	}
	return compareInt(len(a), len(b))
}

func parseNumericIdentifier(s string) (int, bool) {
	if !numericIDRE.MatchString(s) {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}
