/*
Copyright © 2026 Dell Technologies
*/

package engineformat

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// readVerdicts reads the shared fixture that EngineFormatFixtureTest keeps equal to the engine's
// parsers: [name] starts a section, # starts a comment, and each row is a quoted string.
func readVerdicts(t *testing.T) map[string][]string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve shared engine-format fixture source")
	}
	path := filepath.Join(
		filepath.Dir(source), "../../../engine/core/spt-base/src/test/resources/engine-format/verdicts.txt",
	)
	file, err := os.Open(path) // #nosec G304 -- path is rooted at this test source file
	if err != nil {
		t.Fatalf("open shared engine-format fixture: %v", err)
	}
	defer func() { _ = file.Close() }()

	sections := map[string][]string{}
	current := ""
	scanner := bufio.NewScanner(file)
	for number := 1; scanner.Scan(); number++ {
		line := scanner.Text()
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
		case strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]"):
			current = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			sections[current] = []string{}
		case current == "":
			t.Fatalf("fixture line %d: row before any section", number)
		default:
			value, err := strconv.Unquote(line)
			if err != nil {
				t.Fatalf("fixture line %d: %q is not a quoted string: %v", number, line, err)
			}
			sections[current] = append(sections[current], value)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read shared engine-format fixture: %v", err)
	}
	return sections
}

func section(t *testing.T, sections map[string][]string, name string, wantAtLeast int) []string {
	t.Helper()
	rows := sections[name]
	if len(rows) < wantAtLeast {
		t.Fatalf("fixture [%s] has %d rows, want at least %d", name, len(rows), wantAtLeast)
	}
	return rows
}

// The CLI check accepts every size the engine accepts: the full enumeration of short strings and
// every documented and edge form. This is the "never narrower" proof.
func TestCheckSizeAcceptsEverySizeTheEngineAccepts(t *testing.T) {
	sections := readVerdicts(t)
	// 1,335 enumerated values when the fixture was made; a much smaller set means a broken file.
	rows := append(section(t, sections, "size.enumerated", 1000), section(t, sections, "size.listed", 1)...)
	for _, value := range rows {
		if err := CheckSize(value); err != nil {
			t.Errorf("CheckSize(%q) = %v, but the engine accepts it", value, err)
		}
	}
}

func TestCheckDurationAcceptsEveryDurationTheEngineAccepts(t *testing.T) {
	sections := readVerdicts(t)
	// 220 enumerated values when the fixture was made.
	rows := append(section(t, sections, "duration.enumerated", 200), section(t, sections, "duration.listed", 1)...)
	for _, value := range rows {
		if err := CheckDuration(value); err != nil {
			t.Errorf("CheckDuration(%q) = %v, but the engine accepts it", value, err)
		}
	}
}

// The check stays useful: it refuses values the engine refuses.
func TestCheckSizeRefusesSizesTheEngineRefuses(t *testing.T) {
	for _, value := range section(t, readVerdicts(t), "size.refused", 1) {
		if err := CheckSize(value); !errors.Is(err, ErrSize) {
			t.Errorf("CheckSize(%q) = %v, want ErrSize", value, err)
		}
	}
}

func TestCheckDurationRefusesDurationsTheEngineRefuses(t *testing.T) {
	for _, value := range section(t, readVerdicts(t), "duration.refused", 1) {
		if err := CheckDuration(value); !errors.Is(err, ErrDuration) {
			t.Errorf("CheckDuration(%q) = %v, want ErrDuration", value, err)
		}
	}
}

// Values the engine refuses as written but that run today: the DELETE and integrity scenario
// generators trim them before the engine sees them. Any non-ASCII value, and a ',' before the
// range '-', is left to the engine.
func TestChecksAcceptWhatTheCLIPassesThrough(t *testing.T) {
	for _, value := range []string{" 5m", "5m ", "\t30s\n", " ", "5\u00a0m", "5\u212a"} {
		if err := CheckDuration(value); err != nil {
			t.Errorf("CheckDuration(%q) = %v, want nil", value, err)
		}
	}
	for _, value := range []string{" 1MB", "1MB ", "\n4KiB\n", " ", "1 \u212aB", "\u00e9", "1,5-6", "1KB,2-3KB"} {
		if err := CheckSize(value); err != nil {
			t.Errorf("CheckSize(%q) = %v, want nil", value, err)
		}
	}
}

// The IEC step drops only an i that follows a unit letter and precedes a b, in any ASCII case.
func TestDropIECInfix(t *testing.T) {
	for in, want := range map[string]string{
		"1KiB": "1KB", "1kib": "1kb", "1KIB": "1KB", "1Ki": "1Ki", "1KiiB": "1KiiB",
		"1KiB-2MiB,1": "1KB-2MB,1", "1EiB": "1EB", "1Bi": "1Bi", "KiKiB": "KiKB", "1iB": "1iB",
	} {
		if got := dropIECInfix(in); got != want {
			t.Errorf("dropIECInfix(%q) = %q, want %q", in, got, want)
		}
	}
}
