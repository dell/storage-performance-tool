package replay

import (
	"fmt"
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
)

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

	if rs.ScenarioPath == "" {
		return rs, fmt.Errorf("run script does not reference a scenario file")
	}
	return rs, nil
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
