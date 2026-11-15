package secretmask

import (
	"os"
	"sort"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// minEnvironmentValueLength keeps a very short value, such as a one-letter test key, from
// masking every occurrence of that letter in the text.
const minEnvironmentValueLength = 4

// environmentValueVars are the environment-file settings whose values must not reach
// captured engine output: the S3 credentials and endpoints.
var environmentValueVars = []string{
	constants.EnvS3AccessKey,
	constants.EnvS3SecretKey,
	constants.EnvS3Endpoint,
	constants.EnvS3Endpoints,
}

// EnvironmentValues masks the values of the S3 credential and endpoint environment
// variables wherever they appear in s. Text masks credentials by their key name; this
// covers a bare value, such as an endpoint in an engine log line. Each endpoint is also
// masked without its URL scheme.
func EnvironmentValues(s string) string {
	values := environmentValues()
	if len(values) == 0 || s == "" {
		return s
	}
	pairs := make([]string, 0, 2*len(values))
	for _, value := range values {
		pairs = append(pairs, value, masked)
	}
	return strings.NewReplacer(pairs...).Replace(s)
}

func environmentValues() []string {
	seen := make(map[string]struct{})
	for _, name := range environmentValueVars {
		for _, field := range strings.FieldsFunc(os.Getenv(name), func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
		}) {
			field = strings.TrimSpace(field)
			for _, value := range []string{field, stripURLScheme(field)} {
				if len(value) >= minEnvironmentValueLength {
					seen[value] = struct{}{}
				}
			}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	// Longest first, so a URL is masked whole before its scheme-less form matches inside it.
	sort.Slice(values, func(i, j int) bool {
		if len(values[i]) != len(values[j]) {
			return len(values[i]) > len(values[j])
		}
		return values[i] < values[j]
	})
	return values
}

func stripURLScheme(value string) string {
	if _, rest, ok := strings.Cut(value, "://"); ok {
		return strings.TrimRight(rest, "/")
	}
	return value
}
