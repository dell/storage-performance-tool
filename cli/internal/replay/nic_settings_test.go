package replay

import (
	"context"
	"fmt"
	"slices"
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

func TestExtractJSNicSettingsRemovesAdjacentSettingsAndKeepsTheCodeValid(t *testing.T) {
	const (
		interfaces = `"interfaces":["auto"]`
		nodes      = `"nodeInterfaces":{"client-a.example":["ens1f0","ens1f1"]}`
	)
	declared := map[string]string{
		interfaces: "storage.net.interfaces=auto",
		nodes:      `storage.net.nodeInterfaces={"client-a.example":["ens1f0","ens1f1"]}`,
	}
	positions := map[string]struct{ net, want string }{
		"alone in net":     {`{%s,%s}`, `{}`},
		"at the beginning": {`{%s,%s,"port":9020}`, `{"port":9020}`},
		"in the middle":    {`{"ssl":true,%s,%s,"port":9020}`, `{"ssl":true,"port":9020}`},
		"at the end":       {`{"ssl":true,%s,%s}`, `{"ssl":true}`},
	}
	orders := map[string][2]string{"interfaces first": {interfaces, nodes}, "nodeInterfaces first": {nodes, interfaces}}
	for position, test := range positions {
		for order, keys := range orders {
			t.Run(position+", "+order, func(t *testing.T) {
				source := `({"storage":{"net":` + fmt.Sprintf(test.net, keys[0], keys[1]) + `},"keep":7})`
				archived := &archivedNicSettings{}
				got, rejected := extractJSNicSettings(source, archived)
				if want := `({"storage":{"net":` + test.want + `},"keep":7})`; got != want || rejected != nil {
					t.Fatalf("extractJSNicSettings(%s)\n got %s\nwant %s", source, got, want)
				}
				if want := []string{declared[keys[0]], declared[keys[1]]}; !slices.Equal(archived.declared, want) {
					t.Fatalf("recorded %q, want %q in source order", archived.declared, want)
				}
			})
		}
	}

	// The reported case: both settings end the object, and each took the comma between them.
	reported := `({"storage":{"net":{"interfaces":["auto"],"nodeInterfaces":{}}},"keep":7})`
	if got, _ := extractJSNicSettings(reported, &archivedNicSettings{}); got != `({"storage":{"net":{}},"keep":7})` {
		t.Fatalf("extractJSNicSettings(%s) = %s, want an empty net object", reported, got)
	}

	// Archived scenarios are indented, and comments may sit between the entries.
	indented := "({\"storage\" : {\n  \"net\" : {\n    \"port\" : 9020,\n    \"interfaces\" : [ \"auto\" ], // archived\n" +
		"    \"nodeInterfaces\" : { }\n  }},\n  \"keep\" : 7\n})"
	want := "({\"storage\" : {\n  \"net\" : {\n    \"port\" : 9020\n  }},\n  \"keep\" : 7\n})"
	if got, _ := extractJSNicSettings(indented, &archivedNicSettings{}); got != want {
		t.Fatalf("extractJSNicSettings(indented)\n got %q\nwant %q", got, want)
	}
}

func TestConvertJSDropsAdjacentInlineNicSettingsAndKeepsTheRestOfTheConfig(t *testing.T) {
	got := convertJSWith(t, withInlineNet(t, `{ "ssl" : { "enabled" : false }, "interfaces" : [ "auto" ], "nodeInterfaces" : { "a" : 1 } }`))
	want := `the source run used storage.net.interfaces=auto and storage.net.nodeInterfaces={ "a" : 1 }; ` +
		"replay ignores them, pass --interfaces to use NICs"
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 1 || warnings[0] != want {
		t.Fatalf("NIC-set warnings = %q, want [%q]", warnings, want)
	}
	js := string(got.ScenarioJS)
	if strings.Contains(js, "nterfaces") {
		t.Fatalf("the inline step config keeps an archived NIC set\n%s", js)
	}
	for _, kept := range []string{`"net" : { "ssl" : { "enabled" : false } },`, `"concurrency" : 70`} {
		if !strings.Contains(js, kept) {
			t.Fatalf("dropping the NIC set must keep %s\n%s", kept, js)
		}
	}
}

func TestConvertJSRejectsNicSettingsItCannotRemove(t *testing.T) {
	cases := map[string]string{
		"an object held in a variable":   "var oldNet = {\"interfaces\" : [\"old-nic\"]};\n" + withInlineNet(t, `oldNet`),
		"a net object outside storage":   "var unrelated = {\"net\" : {\"nodeInterfaces\" : {}}};\n" + withInlineNet(t, `{}`),
		"a member assignment":            "var oldNet = {};\noldNet.interfaces = [\"old-nic\"];\n" + withInlineNet(t, `oldNet`),
		"a bracket assignment":           "var oldNet = {};\noldNet[\"nodeInterfaces\"] = {};\n" + withInlineNet(t, `oldNet`),
		"an object passed to a function": "var oldNet = Object.assign({}, {\"interfaces\" : \"auto\"});\n" + withInlineNet(t, `oldNet`),
		"a named object by reference":    "var saved = {\"oldNetwork\" : {\"interfaces\" : \"ens1f0\"}};\n" + withInlineNet(t, `saved.oldNetwork`),
		"an escaped key in a variable":   "var oldNet = {\"interf\\u0061ces\" : \"ens1f0\"};\n" + withInlineNet(t, `oldNet`),
		"a flat key beside storage":      strings.Replace(withInlineNet(t, `{}`), `"storage" : {`, `"storage-net-interfaces" : "old-nic", "storage" : {`, 1),
		"a flat key in storage":          strings.Replace(withInlineNet(t, `{}`), `"storage" : {`, `"storage" : { "net-nodeInterfaces" : {},`, 1),
		"a flat parent key":              strings.Replace(withInlineNet(t, `{}`), `"storage" : {`, `"storage-net" : { "interfaces" : "old-nic" }, "storage" : {`, 1),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := convertJSResult(raw, nil)
			if ErrorClass(err) != failureInvalidInterfaces || !strings.Contains(err.Error(), "a form replay cannot remove") {
				t.Fatalf("want an invalid_interfaces rejection, got %v", err)
			}
			if got != nil && len(got.ScenarioJS) != 0 {
				t.Fatalf("a rejected archive must not produce a scenario:\n%s", got.ScenarioJS)
			}
		})
	}
}

func TestConvertJSDropsEmptyNicSettingsWithoutAWarning(t *testing.T) {
	got := convertJSWith(t, withInlineNet(t, `{ "interfaces" : [ ], "nodeInterfaces" : { }, "ssl" : { "enabled" : false } }`))
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 0 {
		t.Fatalf("an empty value is the default, so NIC-set warnings = %q, want none", warnings)
	}
	if js := string(got.ScenarioJS); strings.Contains(js, "nterfaces") {
		t.Fatalf("the empty settings stay\n%s", js)
	}
}

func TestExtractJSNicSettingsFindsAKeyAfterUnusualWhitespace(t *testing.T) {
	for name, space := range map[string]string{"no-break space": "\u00a0", "form feed": "\f", "vertical tab": "\v"} {
		for position, source := range map[string]string{
			"first in net": `{"storage":{"net":{` + space + `"interfaces":"x"}}}`,
			"after a key":  `{"storage":{"net":{"port":9020,` + space + `"interfaces":"x"}}}`,
		} {
			t.Run(name+", "+position, func(t *testing.T) {
				archived := &archivedNicSettings{}
				got, rejected := extractJSNicSettings(source, archived)
				if strings.Contains(got, "interfaces") || rejected != nil || len(archived.declared) != 1 {
					t.Fatalf("extractJSNicSettings(%q) = %q, rejected %v, declared %q", source, got, rejected, archived.declared)
				}
			})
		}
	}
}

func TestConvertJSDropsAnEscapedLiteralNicSetting(t *testing.T) {
	got := convertJSWith(t, withInlineNet(t, `{ "interf\u0061ces" : "auto" }`))
	want := "the source run used storage.net.interfaces=auto; replay ignores it, pass --interfaces to use NICs"
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 1 || warnings[0] != want {
		t.Fatalf("NIC-set warnings = %q, want [%q]", warnings, want)
	}
	if js := string(got.ScenarioJS); strings.Contains(js, "u0061") {
		t.Fatalf("the escaped setting stays\n%s", js)
	}
}

func TestConvertJSAcceptsCodeThatOnlyLooksLikeANicSetting(t *testing.T) {
	cases := map[string]string{
		"a ternary and a label": "var interfaces = 1;\nvar chosen = true ? interfaces : 0;\ninterfaces: while (false) {}\n" +
			withInlineNet(t, `{}`),
		"unrelated data with a literal net": "var stats = {\"totals\" : {\"net-interfaces\" : 1}};\nvar listed = {\"interfaces\" : 2};\n" +
			withInlineNet(t, `{}`),
		"a non-ASCII key prefix": "var labels = {\u670d\u52a1interfaces : 1};\n" + withInlineNet(t, `{}`),
		"a shared net and unrelated data": "var sharedNet = {\"ssl\" : {\"enabled\" : false}};\nvar inventory = {\"host\" : {\"interfaces\" : 2}};\n" +
			withInlineNet(t, `sharedNet`),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			got := convertJSWith(t, raw)
			if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 0 {
				t.Fatalf("NIC-set warnings = %q, want none", warnings)
			}
			if !strings.Contains(string(got.ScenarioJS), "nterfaces") {
				t.Fatalf("the scenario's own code stays\n%s", got.ScenarioJS)
			}
		})
	}
}

func TestUnremovableJSNicSettingsFollowsAStorageGivenByReference(t *testing.T) {
	const profile = `var profiles = {"fast":{"net-interfaces":"auto"}};`
	if _, rejected := extractJSNicSettings(profile+`Load.config({"storage":profiles.fast});`, &archivedNicSettings{}); rejected == nil {
		t.Fatal("a flat key in the object storage refers to must be rejected")
	}
	if _, rejected := extractJSNicSettings(profile+`Load.config({"storage":{"driver":{}}});`, &archivedNicSettings{}); rejected != nil {
		t.Fatalf("a flat key in an object nothing refers to is not a NIC setting: %v", rejected)
	}
	if _, rejected := extractJSNicSettings(`var shared = {"net-nodeInterfaces":{}};cfg.storage = shared;`, &archivedNicSettings{}); rejected == nil {
		t.Fatal("a flat key in an object with no key must be rejected when storage is assigned")
	}
}

func TestConvertJSFindsANicSettingAfterADivision(t *testing.T) {
	raw := "var count = 4; var half = count++ / 2; var unit = \"ops/s\";\n" + withInlineNet(t, `{ "interfaces" : "auto" }`)
	got := convertJSWith(t, raw)
	want := "the source run used storage.net.interfaces=auto; replay ignores it, pass --interfaces to use NICs"
	if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 1 || warnings[0] != want {
		t.Fatalf("NIC-set warnings = %q, want [%q]", warnings, want)
	}
}

func TestConvertJSFindsANicSettingAfterARegularExpression(t *testing.T) {
	for name, net := range map[string]string{
		"slashes that read as a comment": `{ "ssl" : { "note" : "a/b".replace(/\//g, "") }, "interfaces" : "auto" }`,
		"a slash in a character class":   `{ "ssl" : { "note" : "a/b".replace(/[/]/g, "") }, "interfaces" : "auto" }`,
	} {
		t.Run(name, func(t *testing.T) {
			got := convertJSWith(t, withInlineNet(t, net))
			want := "the source run used storage.net.interfaces=auto; replay ignores it, pass --interfaces to use NICs"
			if warnings := nicSettingWarnings(t, got.Diagnostics); len(warnings) != 1 || warnings[0] != want {
				t.Fatalf("NIC-set warnings = %q, want [%q]", warnings, want)
			}
			if js := string(got.ScenarioJS); strings.Contains(js, "interfaces") || !strings.Contains(js, "replace(/") {
				t.Fatalf("want the setting gone and the expression kept\n%s", js)
			}
		})
	}
}

func TestJSCodeMaskTellsADivisionFromARegularExpression(t *testing.T) {
	for source, want := range map[string]string{
		`a = b / 2; c = d / 3; // note`: `a = b / 2; c = d / 3;        `,
		`a = (b) / 2 / 3;`:              `a = (b) / 2 / 3;`,
		`a = n++ / 2; b = "c/d";`:       `a = n++ / 2; b = "c/d";`,
		`a = n-- / 2; b = "c/d";`:       `a = n-- / 2; b = "c/d";`,
		`a = s.return / 2; b = "c/d";`:  `a = s.return / 2; b = "c/d";`,
		`a = [1][0] / 2; b = "c/d";`:    `a = [1][0] / 2; b = "c/d";`,
		`a = /x\/y/g;`:                  `a =       g;`,
		`return /"/.test(a);`:           `return    .test(a);`,
		`a = /no end`:                   `a = /no end`,
		"a = /no end\nb = 1 / 2;":       "a = /no end\nb = 1 / 2;",
	} {
		if got := jsCodeMask(source); got != want {
			t.Fatalf("jsCodeMask(%q) = %q, want %q", source, got, want)
		}
	}
}

func TestFirstJSMemberAssignmentFindsOnlyAssignmentsOutsideStrings(t *testing.T) {
	found := map[string]string{
		`a.interfaces = 1`:            "interfaces",
		`a . nodeInterfaces=1`:        "nodeInterfaces",
		`a["interfaces"] = 1`:         "interfaces",
		`a[ 'nodeInterfaces' ]  =  1`: "nodeInterfaces",
		`a.interfaces == 1`:           "",
		`b = a.interfaces`:            "",
		`a.interfaces => 1`:           "",
		`a.networkInterfaces = 1`:     "",
		`"a.interfaces = 1"`:          "",
		`b = ["interfaces"]`:          "",
		`"unterminated`:               "",
	}
	for code, want := range found {
		if got := firstJSMemberAssignment(code, archivedNicSettingKeys...); got != want {
			t.Fatalf("firstJSMemberAssignment(%s) = %q, want %q", code, got, want)
		}
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
