package server

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func settingsFixture(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadSettingsDefaultsAndOverrides(t *testing.T) {
	t.Setenv("DASHBOARD_CONFIG", "")
	want := defaultSettings()
	got, err := loadSettings("")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults = %+v, %v; want %+v", got, err, want)
	}
	envPath := settingsFixture(t, `{"branding":{"product_name":"Environment console"}}`)
	t.Setenv("DASHBOARD_CONFIG", envPath)
	want.Branding.ProductName = "Environment console"
	got, err = loadSettings("")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("environment override = %+v, %v; want %+v", got, err, want)
	}
	// Explicit config replaces the environment-selected file, not its defaults.
	explicit := settingsFixture(t, `{"limits":{"expert":{"threads_max":8}}}`)
	t.Setenv("DASHBOARD_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	want = defaultSettings()
	want.Limits.Expert.ThreadsMax = 8
	got, err = loadSettings(explicit)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit override = %+v, %v; want %+v", got, err, want)
	}
}

func TestLoadSettingsRejectsInvalidFiles(t *testing.T) {
	t.Setenv("DASHBOARD_CONFIG", "")
	for _, tt := range []struct{ name, body, message string }{
		{"empty", "", "read dashboard settings"},
		{"malformed", "{", "read dashboard settings"},
		{"wrong shape", "[]", "read dashboard settings"},
		{"wrong field type", `{"limits":{"expert":{"threads_max":"8"}}}`, "read dashboard settings"},
		{"unknown top field", `{"limit":{}}`, "unknown field"},
		{"unknown nested field", `{"limits":{"expert":{"thread_max":8}}}`, "unknown field"},
		{"second object", "{} {}", "one JSON object"},
		{"trailing garbage", "{} extra", "one JSON object"},
		{"blank branding", `{"branding":{"product_name":" "}}`, "non-empty"},
		{"empty guided choices", `{"limits":{"guided":{"threads":[]}}}`, "at least one choice"},
		{"duplicate guided choices", `{"limits":{"guided":{"object_counts":[10,10]}}}`, "duplicate choices"},
		{"guided count below minimum", `{"limits":{"guided":{"object_counts":[0]}}}`, "object counts"},
		{"guided count above maximum", `{"limits":{"guided":{"object_counts":[1000001]}}}`, "object counts"},
		{"guided threads below minimum", `{"limits":{"guided":{"threads":[0]}}}`, "thread counts"},
		{"guided threads above maximum", `{"limits":{"guided":{"threads":[4097]}}}`, "thread counts"},
		{"guided runtime below minimum", `{"limits":{"guided":{"max_runtime_seconds":[0]}}}`, "runtimes"},
		{"guided runtime above maximum", `{"limits":{"guided":{"max_runtime_seconds":[2678401]}}}`, "runtimes"},
		{"noncanonical size", `{"limits":{"guided":{"object_sizes":["1MB"]}}}`, "invalid guided object size"},
		{"guided total exceeds cap", `{"limits":{"expert":{"max_total_bytes":1024}}}`, "maximum total data"},
		{"expert reversed range", `{"limits":{"expert":{"threads_min":9,"threads_max":8}}}`, "positive, ordered"},
		{"expert zero minimum", `{"limits":{"expert":{"object_count_min":0}}}`, "positive, ordered"},
		{"expert unsupported maximum", `{"limits":{"expert":{"threads_max":4097}}}`, "positive, ordered"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadSettings(settingsFixture(t, tt.body))
			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("error = %v; want containing %q", err, tt.message)
			}
		})
	}
	if _, err := loadSettings(filepath.Join(t.TempDir(), "missing.json")); err == nil || !strings.Contains(err.Error(), "open dashboard settings") {
		t.Fatalf("missing file error = %v", err)
	}
}
