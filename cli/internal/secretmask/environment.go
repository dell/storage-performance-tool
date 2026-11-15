package secretmask

import (
	"net"
	"os"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// credentialValueVars hold one credential each; the whole value is masked, at any length.
var credentialValueVars = []string{constants.EnvS3AccessKey, constants.EnvS3SecretKey}

// endpointValueVars hold S3 endpoint lists, separated by commas or whitespace.
var endpointValueVars = []string{constants.EnvS3Endpoint, constants.EnvS3Endpoints}

// maskValue is one value to mask; fold matches it in any letter case.
type maskValue struct {
	text string
	fold bool
}

// EnvironmentValues masks, in captured engine output such as log lines, the values of the S3
// credential and endpoint environment variables. Text masks credentials by their key name;
// this covers a bare value. A value is masked only as a whole token: the characters on either
// side must not be a letter, digit, '.', '-' or '_', so 10.0.0.1 is not masked inside
// 10.0.0.10, nor a key "test" inside "latest".
//
//   - S3_ACCESS_KEY and S3_SECRET_KEY: the whole value, exact case.
//   - S3_ENDPOINT and S3_ENDPOINTS: for each endpoint, its host:port and its host alone, in any
//     letter case; the scheme and path are not part of the match.
func EnvironmentValues(s string) string {
	values := environmentValues()
	if len(values) == 0 || s == "" {
		return s
	}
	return maskTokens(s, values)
}

func environmentValues() []maskValue {
	seen := make(map[maskValue]struct{})
	for _, name := range credentialValueVars {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			seen[maskValue{text: value}] = struct{}{}
		}
	}
	for _, name := range endpointValueVars {
		for _, endpoint := range strings.FieldsFunc(os.Getenv(name), func(r rune) bool {
			return r == ',' || unicode.IsSpace(r)
		}) {
			for _, value := range endpointForms(endpoint) {
				seen[maskValue{text: value, fold: true}] = struct{}{}
			}
		}
	}
	values := make([]maskValue, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	// Longest first, so host:port is masked whole before its host matches.
	sort.Slice(values, func(i, j int) bool {
		if len(values[i].text) != len(values[j].text) {
			return len(values[i].text) > len(values[j].text)
		}
		return values[i].text < values[j].text
	})
	return values
}

// endpointForms returns an endpoint's host:port (when it has a port) and its host, without
// scheme, user information or path.
func endpointForms(endpoint string) []string {
	hostPort := strings.TrimSpace(endpoint)
	if _, rest, ok := strings.Cut(hostPort, "://"); ok {
		hostPort = rest
	}
	if slash := strings.IndexAny(hostPort, "/?#"); slash >= 0 {
		hostPort = hostPort[:slash]
	}
	if at := strings.LastIndex(hostPort, "@"); at >= 0 {
		hostPort = hostPort[at+1:]
	}
	if hostPort == "" {
		return nil
	}
	if host, port, err := net.SplitHostPort(hostPort); err == nil && host != "" {
		forms := []string{host}
		if port != "" {
			forms = append(forms, hostPort)
		}
		return forms
	}
	return []string{strings.Trim(hostPort, "[]")}
}

// maskTokens replaces each whole-token occurrence of a value in s with the mask.
func maskTokens(s string, values []maskValue) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if n := matchAt(s, i, values); n > 0 {
			b.WriteString(masked)
			i += n
			continue
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		b.WriteString(s[i : i+size])
		i += size
	}
	return b.String()
}

// matchAt returns the length of the value that occurs at s[i:] as a whole token, or 0.
func matchAt(s string, i int, values []maskValue) int {
	if i > 0 {
		if r, _ := utf8.DecodeLastRuneInString(s[:i]); isTokenRune(r) {
			return 0
		}
	}
	for _, value := range values {
		end := i + len(value.text)
		if end > len(s) {
			continue
		}
		candidate := s[i:end]
		if matches := candidate == value.text || value.fold && strings.EqualFold(candidate, value.text); !matches {
			continue
		}
		if end < len(s) {
			if r, _ := utf8.DecodeRuneInString(s[end:]); isTokenRune(r) {
				continue
			}
		}
		return len(value.text)
	}
	return 0
}

func isTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' || r == '_'
}
