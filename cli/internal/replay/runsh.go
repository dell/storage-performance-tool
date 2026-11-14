package replay

import (
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	exportLineRe       = regexp.MustCompile(`^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)\s*$`)
	scenarioFlagRe     = regexp.MustCompile(`--(?:test-scenario-file|run-scenario|test-scenario)\s*=?\s*([^\s]+)`)
	itemOutputPathRe   = regexp.MustCompile(`--item-output-path\s*=?\s*([^\s]+)`)
	storageNodeAddrsRe = regexp.MustCompile(`--storage-net-node-addrs\s*=?\s*([^\s]+)`)
	clientAddrsRe      = regexp.MustCompile(`--storage-driver-addrs\s*=?\s*([^\s]+)`)
	// engineTreeFlagRe matches the name of a --storage-* or --load-* engine flag, which ends where
	// a value or the next word begins.
	engineTreeFlagRe = regexp.MustCompile(`(?:^|[\s"'=;(])--((?:storage|load)(?:-[A-Za-z0-9_]+)+)`)
)

// keptEngineTreeFlags are the --storage-* and --load-* flags ParseRunScript reads. Replay keeps no
// other flag of those trees.
var keptEngineTreeFlags = map[string]bool{
	"storage-net-node-addrs": true,
	"storage-driver-addrs":   true,
}

// ParseRunScript extracts replay-relevant data from an archived shell run script.
func ParseRunScript(script string) (RunScript, error) {
	rs := RunScript{Exports: map[string]string{}}
	lines := strings.Split(script, "\n")
	for _, line := range lines {
		m := exportLineRe.FindStringSubmatch(line)
		if len(m) != 3 {
			continue
		}
		rs.Exports[m[1]] = parseShellValue(m[2])
	}

	expanded := expandWithExports(script, rs.Exports)
	if m := scenarioFlagRe.FindStringSubmatch(expanded); len(m) == 2 {
		rs.ScenarioPath = strings.Trim(m[1], `"'`)
	}
	if m := itemOutputPathRe.FindStringSubmatch(expanded); len(m) == 2 {
		rs.ItemOutputPath = strings.Trim(m[1], `"'`)
	}
	if m := storageNodeAddrsRe.FindStringSubmatch(expanded); len(m) == 2 {
		rs.StorageNodeAddrs = strings.Trim(m[1], `"'`)
	}
	if m := clientAddrsRe.FindStringSubmatch(expanded); len(m) == 2 {
		rs.ClientAddrs = strings.Trim(m[1], `"'`)
	}
	rs.IgnoredFlags = ignoredEngineTreeFlags(lines, rs.Exports)

	if rs.ScenarioPath == "" {
		return rs, classifiedErrorf(failureRunScriptMissingScenario, "run script does not reference a scenario file")
	}
	return rs, nil
}

// ignoredEngineTreeFlags returns the names of the --storage-* and --load-* flags on the script's
// lines, exports expanded, that replay does not keep, once each in the order they first appear.
// An export line only assigns a variable, so it is not read for flags; a flag held in an exported
// variable is found where the variable is used. Values are never returned, since one can hold a
// credential or an endpoint.
func ignoredEngineTreeFlags(lines []string, exports map[string]string) []string {
	var ignored []string
	seen := map[string]bool{}
	for _, line := range lines {
		if exportLineRe.MatchString(line) {
			continue
		}
		for _, m := range engineTreeFlagRe.FindAllStringSubmatch(expandWithExports(line, exports), -1) {
			name := m[1]
			if keptEngineTreeFlags[name] || seen[name] {
				continue
			}
			seen[name] = true
			ignored = append(ignored, "--"+name)
		}
	}
	return ignored
}

func parseShellValue(raw string) string {
	v := strings.TrimSpace(raw)
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			if unquoted, err := strconv.Unquote(v); err == nil {
				return unquoted
			}
			return v[1 : len(v)-1]
		}
	}
	return v
}

func expandWithExports(s string, exports map[string]string) string {
	return os.Expand(s, func(key string) string {
		if v, ok := exports[key]; ok {
			return v
		}
		return ""
	})
}
