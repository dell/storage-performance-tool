package scenario

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// An engine override of one URI argument or request header writes a value that YAML reads as a
// number or boolean as the text typed, so the engine sends it as typed. Every other value, and every
// other setting, keeps the encoding it had.
func TestApplyEngineOverrideTextMapEntries(t *testing.T) {
	const uriArg = "storage.net.http.uri.args.tid="
	tests := []struct {
		override string
		want     string // the line the generated defaults hold for the entry
	}{
		// Numbers and booleans are written as the text typed.
		{uriArg + "7", `tid: "7"`},
		{uriArg + "0012", `tid: "0012"`},
		{uriArg + "1e3", `tid: "1e3"`},
		{uriArg + "0x1F", `tid: "0x1F"`},
		{uriArg + "true", `tid: "true"`},
		{uriArg + "True", `tid: "True"`},
		{uriArg + "7.0", `tid: "7.0"`},
		{uriArg + "12345678901234567890", `tid: "12345678901234567890"`},
		{uriArg + " 7 ", `tid: "7"`},
		{uriArg + "7 #c", `tid: "7"`},
		{"storage-net-http-uri-args-tid=0012", `tid: "0012"`},
		{"storage.net.http.headers.X-Id=7", `X-Id: "7"`},
		{"storage.net.http.headers.X-Flag=false", `X-Flag: "false"`},
		// Strings, timestamps and nulls keep today's encoding.
		{uriArg + `"7"`, `tid: "7"`},
		{uriArg + `'0012'`, `tid: "0012"`},
		{uriArg + "x7", `tid: x7`},
		{uriArg + "2026-11-15", `tid: 2026-11-15T00:00:00Z`},
		{uriArg + "null", `tid: null`},
		{uriArg + "~", `tid: null`},
		{uriArg + "#x", `tid: null`},
		{uriArg + "&x", `tid: null`},
		{uriArg + "!x", `tid: ""`},
		{uriArg + "a #b", `tid: a`},
		{uriArg + "", `tid: ""`},
		{"storage.net.http.headers.X-Id=2026-11-15", `X-Id: 2026-11-15T00:00:00Z`},
		// Tags, whole-map overrides and other settings keep YAML's meaning.
		{"storage.object.tagging.tags.k=007", `k: 7`},
		{"storage.object.tagging.tags.k=true", `k: true`},
		{"storage.net.http.uri.args={tid: 0012}", `tid: 10`},
		{"storage.driver.threads=6", `threads: 6`},
		{"storage.auth.uid=0012", `uid: 10`},
	}
	for _, tt := range tests {
		t.Run(tt.override, func(t *testing.T) {
			data, err := applyEngineOverrides([]byte("{}\n"), []string{tt.override})
			if err != nil {
				t.Fatalf("applyEngineOverrides: %v", err)
			}
			if !containsLine(string(data), tt.want) {
				t.Fatalf("defaults do not hold %q:\n%s", tt.want, data)
			}
		})
	}
}

// The number or boolean of a URI argument reaches the engine's document as a string, and a typed
// setting stays a number.
func TestApplyEngineOverrideTextMapEntryDecodesAsString(t *testing.T) {
	data, err := applyEngineOverrides([]byte("{}\n"), []string{
		"storage.net.http.uri.args.tid=0012",
		"storage.net.http.headers.X-Id=7",
		"storage.driver.threads=6",
	})
	if err != nil {
		t.Fatalf("applyEngineOverrides: %v", err)
	}
	var root map[string]any
	if err := yaml.Unmarshal(data, &root); err != nil {
		t.Fatalf("unmarshal defaults: %v", err)
	}
	storage := root["storage"].(map[string]any)
	http := storage["net"].(map[string]any)["http"].(map[string]any)
	if got := http["uri"].(map[string]any)["args"].(map[string]any)["tid"]; got != "0012" {
		t.Errorf("tid = %#v, want the string 0012", got)
	}
	if got := http["headers"].(map[string]any)["X-Id"]; got != "7" {
		t.Errorf("X-Id = %#v, want the string 7", got)
	}
	if got := storage["driver"].(map[string]any)["threads"]; got != 6 {
		t.Errorf("threads = %#v, want the number 6", got)
	}
}

func containsLine(text, line string) bool {
	for _, l := range strings.Split(text, "\n") {
		if strings.TrimSpace(l) == line {
			return true
		}
	}
	return false
}
