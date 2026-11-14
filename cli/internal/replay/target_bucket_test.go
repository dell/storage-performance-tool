package replay

import (
	"strconv"
	"strings"
	"testing"
)

// Replay keeps an archived target bucket set; the replay bucket becomes its base.

func TestConvertJSONKeepsAnArchivedBucketCountWithTheReplayBucketAsBase(t *testing.T) {
	src := `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": {"count": 4}}},
  "steps": [
    {"type": "load", "config": {
      "item": {"data": {"size": "10KB"}, "output": {"path": "/archive", "file": "${MONGOOSE_DIR}/log/W/items.csv"}},
      "test": {"step": {"id": "W", "limit": {"count": 10}}}
    }},
    {"type": "load", "config": {
      "item": {"data": {"size": "10KB"}, "output": {"path": "/archive"}},
      "storage": {"targetBucket": {"count": 1}},
      "test": {"step": {"id": "W1", "limit": {"count": 10}}}
    }}
  ]
}`
	got, err := ConvertJSON([]byte(src), RunScript{ItemOutputPath: "archive", Exports: map[string]string{"MONGOOSE_DIR": "/opt/m"}}, Options{
		Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"})
	if err != nil {
		t.Fatal(err)
	}
	js := string(got.ScenarioJS)
	first, second, found := strings.Cut(js, "Starting replay step replay-002")
	if !found {
		t.Fatalf("second step missing:\n%s", js)
	}
	if !strings.Contains(first, `"targetBucket": {`) || !strings.Contains(first, `"count": 4`) || !strings.Contains(first, `"path": "/local"`) {
		t.Fatalf("first step must keep the set with the replay bucket as base:\n%s", first)
	}
	if strings.Contains(second, "targetBucket") {
		t.Fatalf("a count of 1 needs no setting:\n%s", second)
	}
	for _, d := range got.Diagnostics {
		if strings.Contains(d.Message, "targetBucket") {
			t.Fatalf("the count is modeled, not ignored: %s", d.Message)
		}
	}
}

// Replay keeps a list only when it can keep it exactly; anything else stops the replay before it
// runs, never turning unreadable JavaScript into bucket names or dropping the list.

// A JavaScript scenario is a program, so replay refuses any JavaScript archive that mentions
// targetBucket, however it spells it, rather than replay into other buckets.
func TestConvertJSRefusesAnArchiveThatSetsABucketSet(t *testing.T) {
	archive := func(storage string) []byte {
		return []byte(`var parentConfig_1 = {
  "storage" : ` + storage + `
};
Load
    .config(parentConfig_1)
    .config({
      "item" : { "data" : { "size" : "10KB" }, "output" : { "path" : "/alpha" } },
      "load" : { "step" : { "limit" : { "time" : 60 }, "id" : "W" } }
    })
    .run();
`)
	}
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"}
	for _, c := range []struct{ name, storage string }{
		{"list", `{ "targetBucket" : { "names" : [ "ps-eu", "PS_US" ] } }`},
		{"count", `{ "targetBucket" : { "count" : 4 } }`},
		{"unquoted keys", `{ targetBucket: { names: ["alpha", "beta"] } }`},
		{"dashed path", `{ "driver" : { "type" : "s3" } }, "storage-targetBucket-names" : ["alpha", "beta"]`},
		{"unicode escape", `{ "target\u0042ucket" : { "names" : ["alpha"] } }`},
		{"hex escape", `{ "target\x42ucket" : { "names" : ["alpha"] } }`},
		{"octal escape", `{ "target\102ucket" : { "names" : ["alpha"] } }`},
		{"identity escape", `{ "target\Bucket" : { "names" : ["alpha"] } }`},
		{"escaped identifier", `{ target\u0042ucket : { names : ["alpha"] } }`},
		{"JSON in a string", `JSON.parse("{\"target\\u0042ucket\": {\"names\": [\"alpha\"]}}")`},
		{"only a comment", `{ "driver" : { "type" : "s3" } } // targetBucket is not set here`},
	} {
		got, err := ConvertJS(archive(c.storage), RunScript{}, opts)
		if err == nil || !strings.Contains(err.Error(), "mentions storage.targetBucket") {
			t.Fatalf("%s: err = %v, want the refusal", c.name, err)
		}
		if got != nil && len(got.ScenarioJS) > 0 {
			t.Fatalf("%s: no scenario may be generated", c.name)
		}
	}
	explicit := opts
	explicit.Bucket = "local"
	got, err := ConvertJS(archive(`{ "driver" : { "type" : "s3" } }`), RunScript{}, explicit)
	if err != nil || got.EffectiveBucket != "local" {
		t.Fatalf("an archive without a bucket set replays as before: err = %v, bucket = %q", err, got.EffectiveBucket)
	}
}

// The refusal names the provisioning setting too, which spt run writes into its own scenarios, and
// says how to get past it. Its failure class is unchanged.
func TestConvertJSRefusalNamesTheProvisioningSetting(t *testing.T) {
	archive := `var parentConfig_1 = { "storage" : { "driver" : { "type" : "s3" } } };
var existingBucketsOnly = {"storage": {"targetBucket": {"provision": false}}};
Load
    .config(parentConfig_1)
    .config(existingBucketsOnly)
    .config({
      "item" : { "data" : { "size" : "10KB" }, "output" : { "path" : "/alpha" } },
      "load" : { "step" : { "limit" : { "time" : 60 }, "id" : "W" } }
    })
    .run();
`
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}
	_, err := ConvertJS([]byte(archive), RunScript{}, opts)
	want := "archived JavaScript scenario mentions storage.targetBucket (a bucket count, a bucket list or the " +
		"provisioning setting), which replay cannot keep exactly from a program; replay a JSON archive, or remove " +
		"the setting from the archived scenario"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	if got := ErrorClass(err); got != failureUnknownError {
		t.Fatalf("ErrorClass() = %q, want %q", got, failureUnknownError)
	}
}

func TestJSMentionsTargetBucketDecodesNestedEscapes(t *testing.T) {
	for _, source := range []string{`targetBucket`, `target\u0042ucket`, `target\\u0042ucket`, `"target\\\\x42ucket"`, `target\u{42}ucket`, `targe\164Bucket`} {
		if mentioned, err := jsMentionsTargetBucket(source); err != nil || !mentioned {
			t.Fatalf("%s: not detected (err %v)", source, err)
		}
	}
	for _, source := range []string{`target`, `"target" + "Bucket"`, `bucket`, `TargetBucket`} {
		if mentioned, err := jsMentionsTargetBucket(source); err != nil || mentioned {
			t.Fatalf("%s: detected (err %v)", source, err)
		}
	}
}

// A JSON configuration sets a bucket set only as storage.targetBucket; any other spelling would be
// dropped, so it is refused.
func TestConvertJSONRefusesATargetBucketOutsideItsPlace(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}
	archive := func(config string) []byte {
		return []byte(`{"type": "sequential", "config": ` + config + `,
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/alpha"}}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}}]}`)
	}
	for _, c := range []struct{ name, config, want string }{
		{"dashed path", `{"storage-targetBucket-names": ["alpha", "beta"]}`, "sets storage-targetBucket-names"},
		{"partly dashed", `{"storage": {"targetBucket-count": 4}}`, "sets storage.targetBucket-count"},
		{"elsewhere", `{"item": {"targetBucket": {"count": 4}}}`, "sets item.targetBucket"},
		{"unknown member", `{"storage": {"targetBucket": {"count": 4, "extra": 1}}}`, "storage.targetBucket.extra"},
	} {
		if _, err := ConvertJSON(archive(c.config), RunScript{}, opts); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: err = %v, want %q", c.name, err, c.want)
		}
	}
	if _, err := ConvertJSON(archive(`{"storage": {"targetBucket": {"count": 4, "provision": false}}}`), RunScript{}, opts); err != nil {
		t.Fatalf("storage.targetBucket with known members replays: %v", err)
	}
}

// A line continuation inside a string, or escapes nested to any depth, still spell targetBucket.
func TestJSMentionsTargetBucketThroughContinuationsAndDeepNesting(t *testing.T) {
	for _, terminator := range []string{"\n", "\r", "\r\n", "\u2028", "\u2029"} {
		if mentioned, err := jsMentionsTargetBucket(`{"target\` + terminator + `Bucket": {"names": ["alpha"]}}`); err != nil || !mentioned {
			t.Fatalf("continuation %q: not detected (err %v)", terminator, err)
		}
	}
	nested := `target\u0042ucket`
	for depth := 0; depth < 12; depth++ {
		nested = strings.ReplaceAll(nested, `\`, `\\`)
	}
	if mentioned, err := jsMentionsTargetBucket(nested); err != nil || !mentioned {
		t.Fatalf("twelve levels of escaping: not detected (err %v)", err)
	}
}

func TestConvertJSRefusesAContinuedTargetBucketKey(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}
	for _, terminator := range []string{"\n", "\r\n", "\u2028"} {
		archive := "var parentConfig_1 = {\n  \"storage\" : { \"target\\" + terminator + "Bucket\" : { \"names\" : [\"alpha\", \"beta\"] } }\n};\n" +
			"Load\n    .config(parentConfig_1)\n    .config({ \"item\" : { \"output\" : { \"path\" : \"/alpha\" } }, \"load\" : { \"step\" : { \"limit\" : { \"time\" : 60 }, \"id\" : \"W\" } } })\n    .run();\n"
		if _, err := ConvertJS([]byte(archive), RunScript{}, opts); err == nil || !strings.Contains(err.Error(), "mentions storage.targetBucket") {
			t.Fatalf("continuation %q: err = %v, want the refusal", terminator, err)
		}
	}
}

func TestConvertJSONComparesTargetBucketPathsMemberByMember(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}
	archive := func(config string) []byte {
		return []byte(`{"type": "sequential", "config": ` + config + `,
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/alpha"}}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}}]}`)
	}
	for _, c := range []struct{ name, config string }{
		{"in an array", `{"storage": [{"targetBucket": {"names": ["alpha", "beta"]}}]}`},
		{"dotted key", `{"storage.targetBucket": {"names": ["alpha", "beta"]}}`},
		{"nested in a member", `{"storage": {"targetBucket": {"count": {"targetBucket": {"names": ["alpha"]}}}}}`},
		{"names of the wrong type", `{"storage": {"targetBucket": {"names": {"a": "b"}}}}`},
		{"provision of the wrong type", `{"storage": {"targetBucket": {"provision": 1}}}`},
	} {
		if _, err := ConvertJSON(archive(c.config), RunScript{}, opts); err == nil || !strings.Contains(err.Error(), "archived configuration sets") {
			t.Fatalf("%s: err = %v, want the refusal", c.name, err)
		}
	}
}

// Decoding settles however long the chain of escapes, within a work limit: a harmless chain is
// read, and a chain built to exceed the limit is refused with the limit named.
func TestJSMentionsTargetBucketSettlesWithinAWorkLimit(t *testing.T) {
	harmless := `"\\` + strings.Repeat("u005c", 65) + `"`
	if mentioned, err := jsMentionsTargetBucket(harmless); err != nil || mentioned {
		t.Fatalf("a harmless chain of 65 escapes: mentioned %v, err %v", mentioned, err)
	}
	if _, err := ConvertJS([]byte("var note = "+harmless+";\nLoad.config({ \"item\" : { \"output\" : { \"path\" : \"/alpha\" } }, \"load\" : { \"step\" : { \"limit\" : { \"time\" : 60 }, \"id\" : \"W\" } } }).run();\n"),
		RunScript{}, Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}); err != nil {
		t.Fatalf("an archive with a harmless chain converts: %v", err)
	}
	if _, err := jsMentionsTargetBucket(`"\\` + strings.Repeat("u005c", 8000) + `"`); err != errJSDecodeLimit {
		t.Fatalf("a chain built to exceed the work limit: err = %v, want the limit", err)
	}
}

func jsonArchiveWithTargetBucket(scenario, step string) []byte {
	return []byte(`{"type": "sequential", "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": ` + scenario + `}},
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/archive"}},
    "storage": {"targetBucket": ` + step + `}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}},
  {"type": "command", "value": "sleep 1"}]}`)
}

// Replay cannot keep a bucket list: it targets every step at one bucket and reads the run script's
// exports as text, so it refuses any archive that lists buckets, wherever the list is.
func TestConvertJSONRefusesAnArchivedBucketList(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}
	for _, c := range []struct{ name, scenario, step string }{
		{"scenario list", `{"names": ["ps-eu", "PS_US"]}`, `{}`},
		{"step list", `{}`, `{"names": ["ps-eu", "PS_US"]}`},
		{"one name", `{"names": ["ps-eu"]}`, `{}`},
		{"comma string", `{"names": "ps-eu,PS_US"}`, `{}`},
		{"one-name string", `{}`, `{"names": "ps-eu"}`},
		{"exported names", `{"names": ["${BUCKET}-a"]}`, `{}`},
		{"list a step overrides", `{"names": ["ps-eu", "PS_US"]}`, `{"names": []}`},
	} {
		_, err := ConvertJSON(jsonArchiveWithTargetBucket(c.scenario, c.step), RunScript{Exports: map[string]string{"BUCKET": "archive"}}, opts)
		if err == nil || !strings.Contains(err.Error(), "sets a bucket list") {
			t.Fatalf("%s: err = %v, want the refusal", c.name, err)
		}
	}
	nested := []byte(`{"type": "sequential", "steps": [{"type": "parallel", "steps": [{"type": "load", "config": {"storage": {"targetBucket": {"names": ["a", "b"]}}}}]}]}`)
	if _, err := ConvertJSON(nested, RunScript{}, opts); err == nil || !strings.Contains(err.Error(), "sets a bucket list") {
		t.Fatalf("a nested step's list: err = %v, want the refusal", err)
	}
	for _, empty := range []string{`[]`, `""`, `[""]`} {
		got, err := ConvertJSON(jsonArchiveWithTargetBucket(`{"names": `+empty+`}`, `{}`), RunScript{}, opts)
		if err != nil || got.EffectiveBucket != "local" {
			t.Fatalf("names %s is no list, as the engine reads it: err = %v", empty, err)
		}
	}
}

// A count resolves with the archived exports only, counts by its value as a JSON number and as
// written as a string, and stops the replay when replay cannot read it.
func TestConvertJSONReadsAnArchivedCountExactly(t *testing.T) {
	exports := map[string]string{"BUCKET": "4", "COUNT": "6"}
	for _, c := range []struct {
		name, count string
		want        int
	}{
		{"integer", `4`, 4},
		{"whole with a fraction part", `4.0`, 4},
		{"exponent", `1e1`, 10},
		{"exported", `"${COUNT}"`, 6},
		{"exported bucket variable", `"${BUCKET}"`, 4},
	} {
		for _, replayBucket := range []string{"local", "2"} {
			got, err := ConvertJSON(jsonArchiveWithTargetBucket(`{"count": `+c.count+`}`, `{}`), RunScript{Exports: exports},
				Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: replayBucket, BaseTimestamp: "20260605.121400.000"})
			if err != nil {
				t.Fatalf("%s with replay bucket %q: %v", c.name, replayBucket, err)
			}
			if js := string(got.ScenarioJS); !strings.Contains(js, `"count": `+strconv.Itoa(c.want)) || !strings.Contains(js, `"path": "/`+replayBucket) {
				t.Fatalf("%s with replay bucket %q: want count %d over the replay bucket:\n%s", c.name, replayBucket, c.want, js)
			}
		}
	}
	for _, c := range []struct{ name, count, want string }{
		{"missing export", `"${MISSING}"`, "uses $MISSING"},
		{"not a number", `"four"`, "not a whole number"},
		{"string with a fraction", `"4.0"`, "not a whole number"},
		{"string with spaces", `" 4"`, "not a whole number"},
		{"fraction", `2.5`, "not a whole number"},
		{"zero", `0`, "not a whole number of at least 1"},
		{"negative", `-3`, "not a whole number of at least 1"},
		{"object", `{"a": 1}`, "archived configuration sets"},
	} {
		_, err := ConvertJSON(jsonArchiveWithTargetBucket(`{"count": `+c.count+`}`, `{}`), RunScript{Exports: exports},
			Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: err = %v, want %q", c.name, err, c.want)
		}
	}
}
