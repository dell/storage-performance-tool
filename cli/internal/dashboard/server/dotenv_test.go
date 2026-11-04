package server

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeEnvFixture(t *testing.T, dir, body string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Keep a variable genuinely absent while retaining testing's automatic restoration.
// An empty process variable is different: it deliberately overrides a file value.
func unsetEnvFixture(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
}

func TestReadDotenvSyntaxAndLiteralValues(t *testing.T) {
	body := "\r\n  # comment\r\n" +
		"export EXPORTED = value\r\n" +
		"PLAIN = first # ignored comment\r\n" +
		"DOUBLE = \" spaces # inside quotes \"\r\n" +
		"SINGLE = 'literal # inside quotes'\r\n" +
		"HASH = value#retained\r\n" +
		"EQUALS = first=second=third\r\n" +
		"EMPTY=\r\nEMPTY_DOUBLE=\"\"\r\nEMPTY_SINGLE=''\r\n" +
		"DUPLICATE=old\r\nDUPLICATE=new\r\n" +
		"NO_EQUALS\r\n=missing-key\r\nBAD-KEY=ignored\r\n9BAD=ignored\r\n" +
		"_VALID_2=accepted\r\n"
	path := writeEnvFixture(t, t.TempDir(), body)
	want := map[string]string{
		"EXPORTED": "value", "PLAIN": "first", "DOUBLE": " spaces # inside quotes ",
		"SINGLE": "literal # inside quotes", "HASH": "value#retained", "EQUALS": "first=second=third",
		"EMPTY": "", "EMPTY_DOUBLE": "", "EMPTY_SINGLE": "", "DUPLICATE": "new", "_VALID_2": "accepted",
	}
	if got := readDotenv(path, nil); !reflect.DeepEqual(got, want) {
		t.Errorf("parsed values = %#v, want %#v", got, want)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != body {
		t.Errorf("reading configuration changed the source file: %v", err)
	}
}

func TestReadDotenvReferenceSources(t *testing.T) {
	const local = "SPT_DASHBOARD_ENV_TEST_LOCAL"
	const inherited = "SPT_DASHBOARD_ENV_TEST_INHERITED"
	const process = "SPT_DASHBOARD_ENV_TEST_PROCESS"
	const absent = "SPT_DASHBOARD_ENV_TEST_ABSENT"
	t.Setenv(local, "process-local")
	t.Setenv(inherited, "process-inherited")
	t.Setenv(process, "process-value")
	unsetEnvFixture(t, absent)
	existing := map[string]string{local: "inherited-local", inherited: "inherited-value"}
	body := local + "=file-value\n" +
		"LOCAL_REF=${" + local + "}/$" + local + "\n" +
		"INHERITED_REF=${" + inherited + "}\n" +
		"PROCESS_REF=$" + process + "\n" +
		"ABSENT_REF=before-${" + absent + "}-after\n"
	got := readDotenv(writeEnvFixture(t, t.TempDir(), body), existing)
	for key, want := range map[string]string{
		"LOCAL_REF": "file-value/file-value", "INHERITED_REF": "inherited-value",
		"PROCESS_REF": "process-value", "ABSENT_REF": "before--after",
	} {
		if got[key] != want {
			t.Errorf("%s = %q, want %q", key, got[key], want)
		}
	}
	if existing[local] != "inherited-local" || existing[inherited] != "inherited-value" {
		t.Error("reading a file mutated its inherited configuration")
	}
}

func TestReadDotenvDoesNotExecuteValues(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "must-not-exist")
	values := map[string]string{
		"COMMAND":   "$(touch " + marker + ")",
		"BACKTICKS": "`touch " + marker + "`",
		"SHELL":     "; touch " + marker,
	}
	var body strings.Builder
	for key, value := range values {
		body.WriteString(key + "=" + value + "\n")
	}
	if got := readDotenv(writeEnvFixture(t, t.TempDir(), body.String()), nil); !reflect.DeepEqual(got, values) {
		t.Errorf("shell-like values were not preserved literally: %#v", got)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("configuration executed a command: %v", err)
	}
}

func TestReadDotenvOptionalAndBoundedFiles(t *testing.T) {
	for _, path := range []string{filepath.Join(t.TempDir(), "missing.env"), t.TempDir()} {
		if got := readDotenv(path, nil); len(got) != 0 {
			t.Errorf("unavailable configuration produced values: %#v", got)
		}
	}
	// The parser supports values larger than Scanner's default token buffer.
	large := strings.Repeat("x", 70_000)
	path := writeEnvFixture(t, t.TempDir(), "LARGE="+large+"\nAFTER=present\n")
	if got := readDotenv(path, nil); got["LARGE"] != large || got["AFTER"] != "present" {
		t.Error("a supported long value prevented later configuration from loading")
	}
	// Oversized input must not make the parser read beyond its bounded input.
	path = writeEnvFixture(t, t.TempDir(), "BEFORE=present\n"+strings.Repeat("x", 1_000_001)+"\nAFTER=not-read\n")
	if got := readDotenv(path, nil); !reflect.DeepEqual(got, map[string]string{"BEFORE": "present"}) {
		t.Errorf("oversized configuration = %#v", got)
	}
}
