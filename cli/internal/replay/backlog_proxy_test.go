package replay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBacklogProxyGate locks the CURRENT converter classification for one
// representative fixture per backlog class, derived from the full-catalog
// stress run (2026-06-12). It is the in-repo stand-in for the firewalled
// full-catalog regression gate: an implementation agent can run it offline to
// confirm a fix changed exactly the intended class and nothing else.
//
// Fixtures + expectations live in testdata/backlog/manifest.json.
//
//	disposition "lock" => by-design unsupported; converterClass must NOT change.
//	disposition "fix"  => P1 target. When you implement the feature, set the
//	                      fixture's converterClass to "" (expect success) in
//	                      manifest.json and add a positive assertion on the
//	                      generated scenario.
//
// converterClass is the COARSE class spt emits today (asserted here via
// ErrorClass). refinedClass is what cli/tools/replayqual.py re-buckets it into
// for reporting; see REPLAY_BACKLOG_WORK_CONTRACT.md for the full mapping.
type backlogFixture struct {
	File           string `json:"file"`
	Format         string `json:"format"`
	ConverterClass string `json:"converterClass"`
	RefinedClass   string `json:"refinedClass"`
	Disposition    string `json:"disposition"`
	Lever          string `json:"lever"`
	Note           string `json:"note"`
	// HarnessExcerptSubstrings are the lowercased tokens the out-of-repo
	// replayqual.py classifier keys on to map ConverterClass -> RefinedClass.
	// Asserting they remain in the converter's error message turns the
	// cross-repo coupling into an in-repo guardrail: a converter message change
	// that would silently break the harness's re-bucketing fails here instead.
	HarnessExcerptSubstrings []string `json:"harnessExcerptSubstrings,omitempty"`
}

type backlogManifest struct {
	Fixtures []backlogFixture `json:"fixtures"`
}

func TestBacklogProxyGate(t *testing.T) {
	dir := filepath.Join("testdata", "backlog")
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest backlogManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if len(manifest.Fixtures) == 0 {
		t.Fatal("manifest has no fixtures")
	}

	opts := Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		Bucket:        "replaybucket",
		Label:         "replayqual",
		BaseTimestamp: "20260612.000000.000",
	}

	for _, fx := range manifest.Fixtures {
		fx := fx
		t.Run(fx.File, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(dir, fx.File))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			runScript := RunScript{Exports: map[string]string{}}

			var convErr error
			switch fx.Format {
			case "json":
				_, convErr = ConvertJSON(body, runScript, opts)
			case "js":
				_, convErr = ConvertJS(body, runScript, opts)
			default:
				t.Fatalf("unknown fixture format %q", fx.Format)
			}

			if fx.ConverterClass == "" {
				if convErr != nil {
					t.Fatalf("expected successful conversion, got class %q: %v", ErrorClass(convErr), convErr)
				}
				return
			}
			if convErr == nil {
				t.Fatalf("expected converter class %q but conversion succeeded; if you implemented %q, update manifest.json to expect success and add a positive assertion", fx.ConverterClass, fx.File)
			}
			if got := ErrorClass(convErr); got != fx.ConverterClass {
				t.Fatalf("converter class = %q, want %q (disposition=%s); err=%v", got, fx.ConverterClass, fx.Disposition, convErr)
			}

			// Cross-repo coupling guard: the private replayqual.py classifier
			// re-buckets this case by matching these substrings against the
			// converter's error text. If a converter change drops one, the
			// harness would silently mis-bucket — fail here instead.
			lowerMsg := strings.ToLower(convErr.Error())
			for _, sub := range fx.HarnessExcerptSubstrings {
				if !strings.Contains(lowerMsg, strings.ToLower(sub)) {
					t.Errorf("converter error no longer contains harness signature %q (refined class %q would mis-bucket); err=%v", sub, fx.RefinedClass, convErr)
				}
			}
		})
	}
}
