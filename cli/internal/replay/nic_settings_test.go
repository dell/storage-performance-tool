package replay

import (
	"context"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const nicSettingWarningEnd = "pass --interfaces to use NICs"

// nicSettingWarnings returns the NIC-set notes, each of which must be a warning.
func nicSettingWarnings(t *testing.T, diagnostics []Diagnostic) []string {
	t.Helper()
	var out []string
	for _, diagnostic := range diagnostics {
		if strings.HasSuffix(diagnostic.Message, nicSettingWarningEnd) {
			if diagnostic.Severity != severityWarning {
				t.Fatalf("the NIC-set note is a %s, want a warning", diagnostic.Severity)
			}
			out = append(out, diagnostic.Message)
		}
	}
	return out
}

func TestConvertJSDropsAParentNicSetWithOneNote(t *testing.T) {
	raw := strings.Replace(maxS3SanityJS, `"net" : {
      "node" : {
        "port" : 9020
      }
    },`, `"net" : {
      "interfaces" : [ "ens1f0", 'ens1f1' ],
      "node" : {
        "port" : 9020
      },
      "nodeInterfaces" : ["auto", "none"]
    },`, 1)
	if raw == maxS3SanityJS {
		t.Fatal("parent fixture replacement failed")
	}
	got := convertJSWith(t, raw)
	want := "the source run used storage.net.interfaces=ens1f0,ens1f1 and storage.net.nodeInterfaces=auto,none; " +
		"replay ignores them, pass --interfaces to use NICs"
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 1 || warnings[0] != want {
		t.Fatalf("NIC-set warnings = %q, want [%q]", warnings, want)
	}
	if js := string(got.ScenarioJS); strings.Contains(js, "nterfaces") {
		t.Fatalf("the replayed scenario keeps an archived NIC set\n%s", js)
	}
}

func TestConvertJSDropsAnInlineNicSetAndKeepsTheRestOfTheConfig(t *testing.T) {
	got := convertJSWith(t, withInlineNet(t, `{ "interfaces" : archivedNics, "ssl" : { "enabled" : false } }`))
	want := "the source run used storage.net.interfaces=archivedNics; replay ignores it, pass --interfaces to use NICs"
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 1 || warnings[0] != want {
		t.Fatalf("NIC-set warnings = %q, want [%q]", warnings, want)
	}
	js := string(got.ScenarioJS)
	if strings.Contains(js, "interfaces") || strings.Contains(js, "archivedNics") {
		t.Fatalf("the inline step config keeps an archived NIC set\n%s", js)
	}
	for _, kept := range []string{`"ssl" : { "enabled" : false }`, `"concurrency" : 70`} {
		if !strings.Contains(js, kept) {
			t.Fatalf("dropping the NIC set must keep %s\n%s", kept, js)
		}
	}

	only := string(convertJSWith(t, withInlineNet(t, `{ "interfaces" : "auto" }`)).ScenarioJS)
	if strings.Contains(only, "interfaces") || !strings.Contains(only, `"net" : {  }`) {
		t.Fatalf("a net object with only the NIC set stays as an empty object\n%s", only)
	}
}

func TestConvertJSLeavesInterfacesKeysOutsideNetAlone(t *testing.T) {
	raw := strings.Replace(withInlineNet(t, `{ "ssl" : { "enabled" : false } }`), `"concurrency" : 70`,
		`"concurrency" : 70, "interfaces" : 2, "note" : "\"interfaces\" : [\"auto\"]"`, 1)
	got := convertJSWith(t, raw)
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 0 {
		t.Fatalf("NIC-set warnings = %q, want none", warnings)
	}
	if js := string(got.ScenarioJS); !strings.Contains(js, `"interfaces" : 2`) || !strings.Contains(js, `\"interfaces\" : [\"auto\"]`) {
		t.Fatalf("keys outside storage.net and strings stay as they were\n%s", js)
	}
}

func TestConvertJSONDropsAnArchivedNicSetWithOneNote(t *testing.T) {
	raw := strings.Replace(maxS3SanityJSON, `"net": {"node": {"port": 9020}},`,
		`"net": {"node": {"port": 9020}, "interfaces": ["ens1f0", "ens1f1"]},`, 1)
	raw = strings.Replace(raw, `"storage": {"driver": {"queue": {"input": 20000000}}},`,
		`"storage": {"driver": {"queue": {"input": 20000000}}, "net": {"interfaces": "auto", "nodeInterfaces": {"a": 1}}},`, 1)
	got, err := ConvertJSON([]byte(raw), RunScript{Exports: map[string]string{
		"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60",
	}, ItemOutputPath: "bucket"}, Options{Endpoints: []string{"https://s3.example.test:9021"}, BaseTimestamp: "20260605.121400.000"})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	want := "the source run used storage.net.interfaces=ens1f0,ens1f1 and storage.net.interfaces=auto and " +
		`storage.net.nodeInterfaces={"a":1}; replay ignores them, pass --interfaces to use NICs`
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 1 || warnings[0] != want {
		t.Fatalf("NIC-set warnings = %q, want [%q]", warnings, want)
	}
	if hasDiagnosticContaining(got.Diagnostics, severityWarning, "unmodeled JSON config path storage.net.interfaces") ||
		hasDiagnosticContaining(got.Diagnostics, severityWarning, "unmodeled JSON config path storage.net.nodeInterfaces") {
		t.Fatalf("the NIC set has its own note, not an unmodeled-path warning: %+v", got.Diagnostics)
	}
	if js := string(got.ScenarioJS); strings.Contains(js, "nterfaces") {
		t.Fatalf("the replayed scenario keeps an archived NIC set\n%s", js)
	}
}

func TestGenerateWritesOnlyTheFlagsNicSet(t *testing.T) {
	server := endpointSelectionArchive(t)
	options := func(interfaces []string) Options {
		return Options{
			SourceURL: server.URL, Endpoints: []string{"http://192.0.2.10:9020"}, Bucket: "local-bucket",
			BaseTimestamp: "20260605.121400.000", HTTPClient: server.Client(), Interfaces: interfaces,
		}
	}
	got, err := Generate(context.Background(), options([]string{"ens1f0", "192.0.2.21"}))
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if defaults := string(got.DefaultsYAML); !strings.Contains(defaults, "interfaces:\n") ||
		!strings.Contains(defaults, "- ens1f0\n") || !strings.Contains(defaults, "- 192.0.2.21\n") {
		t.Fatalf("replay defaults lack the flag's NIC set:\n%s", defaults)
	}
	without, err := Generate(context.Background(), options(nil))
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if strings.Contains(string(without.DefaultsYAML), "interfaces") {
		t.Fatalf("replay defaults without --interfaces mention interfaces:\n%s", without.DefaultsYAML)
	}
}

func TestGenerateRejectsANicSetTheReplayCannotServe(t *testing.T) {
	server := endpointSelectionArchive(t)
	_, err := Generate(context.Background(), Options{
		SourceURL: server.URL, Endpoints: []string{"http://192.0.2.10:9020"}, Bucket: "local-bucket",
		BaseTimestamp: "20260605.121400.000", HTTPClient: server.Client(),
		Interfaces: []string{"auto"}, InterfacesSource: "SPT_INTERFACES (environment or .env)", S3Driver: scenario.S3DriverAws,
	})
	if got := ErrorClass(err); got != failureInvalidInterfaces {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureInvalidInterfaces, err)
	}
	if want := "SPT_INTERFACES (environment or .env) requires the default Netty S3 driver, not --s3-driver aws"; !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v, want %q", err, want)
	}
}
