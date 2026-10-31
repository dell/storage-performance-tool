package replay

import (
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

// Replay keeps an archived bucket list. --bucket would replace only its placeholder, so it is
// rejected, and S3_BUCKET from the environment is ignored.

const jsonArchiveWithBucketList = `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": {"names": ["ps-eu", "PS_US", "ab"]}}},
  "steps": [
    {"type": "load", "config": {
      "item": {"data": {"size": "10KB"}, "output": {"path": "/ps-eu", "file": "${MONGOOSE_DIR}/log/W/items.csv"}},
      "test": {"step": {"id": "W", "limit": {"count": 10}}}
    }}
  ]
}`

func TestConvertJSONKeepsAnArchivedBucketList(t *testing.T) {
	for _, opts := range []Options{
		{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"},
		{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "from-env", BucketFromEnv: true, BaseTimestamp: "20260605.121400.000"},
	} {
		got, err := ConvertJSON([]byte(jsonArchiveWithBucketList), RunScript{Exports: map[string]string{"MONGOOSE_DIR": "/opt/m"}}, opts)
		if err != nil {
			t.Fatalf("bucket %q: %v", opts.Bucket, err)
		}
		js := string(got.ScenarioJS)
		if !strings.Contains(js, `"names": [`) || !strings.Contains(js, `"PS_US"`) || !strings.Contains(js, `"ab"`) {
			t.Fatalf("bucket %q: the list must be kept:\n%s", opts.Bucket, js)
		}
		if strings.Contains(js, "from-env") || got.EffectiveBucket != "ps-eu" {
			t.Fatalf("bucket %q: S3_BUCKET must not replace the placeholder (effective %q):\n%s", opts.Bucket, got.EffectiveBucket, js)
		}
		if want := []string{"ps-eu", "PS_US", "ab"}; strings.Join(got.ArchivedBucketNames, ",") != strings.Join(want, ",") {
			t.Fatalf("bucket %q: ArchivedBucketNames = %q, want %q", opts.Bucket, got.ArchivedBucketNames, want)
		}
		for _, d := range got.Diagnostics {
			if strings.Contains(d.Message, "targetBucket") {
				t.Fatalf("the list is modeled, not ignored: %s", d.Message)
			}
		}
	}
}

func TestReplayRejectsAnExplicitBucketForAnArchivedList(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}
	_, err := ConvertJSON([]byte(jsonArchiveWithBucketList), RunScript{Exports: map[string]string{"MONGOOSE_DIR": "/opt/m"}}, opts)
	if err == nil || !strings.Contains(err.Error(), "--bucket cannot be used") || !strings.Contains(err.Error(), "ps-eu ...") {
		t.Fatalf("err = %v, want the --bucket rejection naming the list", err)
	}
}

func TestReplayPreflightShowsAKeptList(t *testing.T) {
	g := &Generated{EffectiveBucket: "ps-eu", ArchivedBucketNames: []string{"ps-eu", "PS_US", "ab"},
		ArchivedBucketLists: []ArchivedBucketList{{Steps: []int{1, 3}, Names: []string{"ps-eu", "PS_US", "ab"}}, {Steps: []int{2}, Names: []string{"g", "h"}}}}
	preflight := BuildPreflight(g, Options{})
	if !strings.Contains(preflight, "Buckets: 3 (ps-eu ... ab), kept from the archive, steps 1, 3") ||
		!strings.Contains(preflight, "Buckets: 2 (g ... h), kept from the archive, step 2") || strings.Contains(preflight, "Bucket: ps-eu") {
		t.Fatalf("preflight:\n%s", preflight)
	}
	metadata := string(buildMetadata(g))
	if !strings.Contains(metadata, `"bucketLists": [`) || !strings.Contains(metadata, `"h"`) {
		t.Fatalf("replay metadata must keep every list:\n%s", metadata)
	}
	if strings.Contains(string(buildMetadata(&Generated{EffectiveBucket: "b"})), "bucketLists") {
		t.Fatal("a replay without a list adds no bucketLists")
	}
}

// Replay keeps a list only when it can keep it exactly; anything else stops the replay before it
// runs, never turning unreadable JavaScript into bucket names or dropping the list.

func TestConvertJSONRefusesABucketListItCannotKeepExactly(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"}
	archive := func(names string) []byte {
		return []byte(`{"type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": {"names": ` + names + `}}},
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/alpha"}}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}}]}`)
	}
	for _, c := range []struct {
		names string
		want  string
	}{
		{`["alpha", 5]`, "to a value replay does not know"},
		{`["alpha", ""]`, `must not be empty`},
		{`{"a": "b"}`, "to a value replay does not know"},
		{`"alpha,,beta"`, `must not be empty`},
	} {
		if _, err := ConvertJSON(archive(c.names), RunScript{}, opts); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("names %s: err = %v, want %q", c.names, err, c.want)
		}
	}
	got, err := ConvertJSON(archive(`"alpha,beta"`), RunScript{}, opts)
	if err != nil || strings.Join(got.ArchivedBucketNames, "|") != "alpha|beta" {
		t.Fatalf("comma string: err = %v, names = %q", err, got.ArchivedBucketNames)
	}
	explicit := opts
	explicit.Bucket = "local"
	empty, err := ConvertJSON(archive(`[]`), RunScript{}, explicit)
	if err != nil || empty.ArchivedBucketNames != nil || empty.EffectiveBucket != "local" {
		t.Fatalf("empty list: err = %v, names = %q, bucket = %q", err, empty.ArchivedBucketNames, empty.EffectiveBucket)
	}
}

func TestConvertJSONSplitsCommaStringsAsTheEngine(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"}
	archive := func(names string) []byte {
		return []byte(`{"type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": {"names": ` + names + `}}},
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/alpha"}}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}}]}`)
	}
	got, err := ConvertJSON(archive(`"alpha,beta,"`), RunScript{}, opts)
	if err != nil || strings.Join(got.ArchivedBucketNames, "|") != "alpha|beta" {
		t.Fatalf("trailing comma: err = %v, names = %q", err, got.ArchivedBucketNames)
	}
	explicit := opts
	explicit.Bucket = "local"
	for _, empty := range []string{`""`, `[""]`} {
		got, err := ConvertJSON(archive(empty), RunScript{}, explicit)
		if err != nil || got.ArchivedBucketNames != nil || got.EffectiveBucket != "local" {
			t.Fatalf("names %s is no list, as the engine reads it: err = %v, names = %q", empty, err, got.ArchivedBucketNames)
		}
	}
}

// A JavaScript scenario is a program, so replay refuses any JavaScript archive that sets a bucket
// set, however it spells targetBucket, rather than replay into other buckets.
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
		if err == nil || !strings.Contains(err.Error(), "sets storage.targetBucket") {
			t.Fatalf("%s: err = %v, want the refusal", c.name, err)
		}
		if got != nil && len(got.ScenarioJS) > 0 {
			t.Fatalf("%s: no scenario may be generated", c.name)
		}
	}
	explicit := opts
	explicit.Bucket = "local"
	got, err := ConvertJS(archive(`{ "driver" : { "type" : "s3" } }`), RunScript{}, explicit)
	if err != nil || got.EffectiveBucket != "local" || got.ArchivedBucketNames != nil {
		t.Fatalf("an archive without a bucket set replays as before: err = %v, bucket = %q", err, got.EffectiveBucket)
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
		if _, err := ConvertJS([]byte(archive), RunScript{}, opts); err == nil || !strings.Contains(err.Error(), "sets storage.targetBucket") {
			t.Fatalf("continuation %q: err = %v, want the refusal", terminator, err)
		}
	}
}

// Replay keeps what each load step uses: its own configuration merged over the scenario's.
func TestConvertJSONTakesTheBucketSetEachStepUses(t *testing.T) {
	archive := func(steps string) []byte {
		return []byte(`{"type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": {"names": ["unused-a", "unused-b"]}}},
  "steps": [` + steps + `]}`)
	}
	step := func(id, targetBucket string) string {
		return `{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/alpha"}},
      "storage": {"targetBucket": ` + targetBucket + `}, "test": {"step": {"id": "` + id + `", "limit": {"count": 10}}}}}`
	}
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}

	overridden, err := ConvertJSON(archive(step("W", `{"names": [], "count": 4}`)+","+step("R", `{"names": [], "count": 4}`)), RunScript{}, opts)
	if err != nil {
		t.Fatalf("every step overrides the list: %v", err)
	}
	js := string(overridden.ScenarioJS)
	if overridden.ArchivedBucketNames != nil || overridden.EffectiveBucket != "local" || strings.Contains(js, "unused-a") ||
		!strings.Contains(js, `"count": 4`) || !strings.Contains(js, `"/local`) {
		t.Fatalf("the steps' own count over the replay bucket must be used (names %q):\n%s", overridden.ArchivedBucketNames, js)
	}

	opts.Bucket = ""
	replaced, err := ConvertJSON(archive(step("W", `{"names": ["ps-eu", "PS_US"]}`)), RunScript{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(replaced.ArchivedBucketNames, "|") != "ps-eu|PS_US" || strings.Contains(string(replaced.ScenarioJS), "unused-a") {
		t.Fatalf("the step's list replaces the scenario's: names = %q", replaced.ArchivedBucketNames)
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

// A list resolves with the archived exports, never the replay's rebound ones, so the buckets are
// the ones the archived run used, in validation, metadata and the generated scenario alike.
func TestConvertJSONResolvesListNamesWithTheArchivedExports(t *testing.T) {
	src := []byte(`{"type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": {"names": ["${BUCKET}-a", "${BUCKET}-b", "${CLIENT_1}-c"]}}},
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/placeholder"}}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}}]}`)
	got, err := ConvertJSON(src, RunScript{ItemOutputPath: "placeholder", Exports: map[string]string{"BUCKET": "archive", "CLIENT_1": "host1"}},
		Options{Endpoints: []string{"http://10.0.0.1:9020"}, TestHosts: "local-host", BaseTimestamp: "20260605.121400.000"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.ArchivedBucketNames, "|") != "archive-a|archive-b|host1-c" {
		t.Fatalf("ArchivedBucketNames = %q", got.ArchivedBucketNames)
	}
	js := string(got.ScenarioJS)
	if !strings.Contains(js, `"archive-a"`) || !strings.Contains(js, `"host1-c"`) || strings.Contains(js, "placeholder-a") || strings.Contains(js, "local-host-c") {
		t.Fatalf("the generated scenario must name the archived buckets:\n%s", js)
	}
}

// Every element of names must be a string, in every configuration, even one a step overrides.
func TestConvertJSONRefusesNamesElementsThatAreNotStrings(t *testing.T) {
	src := []byte(`{"type": "sequential",
  "config": {"storage": {"targetBucket": {"names": [{"targetBucket": {"names": ["alpha", "beta"]}}]}}},
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/alpha"}},
    "storage": {"targetBucket": {"names": []}}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}}]}`)
	_, err := ConvertJSON(src, RunScript{}, Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"})
	if err == nil || !strings.Contains(err.Error(), "storage.targetBucket.names to a value replay does not know") {
		t.Fatalf("err = %v, want the refusal", err)
	}
}

// A list or count resolves only with the archived exports: a variable the archive does not
// export stops the replay, and retargeting the base never changes a count.
func TestConvertJSONResolvesBucketSetsOnlyWithArchivedExports(t *testing.T) {
	archive := func(targetBucket string) []byte {
		return []byte(`{"type": "sequential", "config": {"storage": {"driver": {"type": "s3"}, "targetBucket": ` + targetBucket + `}},
  "steps": [{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/archive"}}, "test": {"step": {"id": "W", "limit": {"count": 10}}}}}]}`)
	}
	exports := map[string]string{"BUCKET": "4", "MONGOOSE_DIR": "/opt/m"}
	for _, c := range []struct{ name, targetBucket, want string }{
		{"missing export in a name", `{"names": ["${PREFIX}-a", "${PREFIX}-b"]}`, "uses $PREFIX"},
		{"missing export as the list", `{"names": "${BUCKETS}"}`, "uses $BUCKETS"},
		{"missing export in a count", `{"count": "${COUNT}"}`, "uses $COUNT"},
		{"count that is not a number", `{"count": "four"}`, "not a whole number"},
		{"count of zero", `{"count": 0}`, "not a whole number of at least 1"},
		{"fractional count", `{"count": 2.5}`, "not a whole number"},
	} {
		_, err := ConvertJSON(archive(c.targetBucket), RunScript{Exports: exports}, Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: err = %v, want %q", c.name, err, c.want)
		}
	}
	for _, replayBucket := range []string{"local", "2"} {
		got, err := ConvertJSON(archive(`{"count": "${BUCKET}"}`), RunScript{Exports: exports}, Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: replayBucket, BaseTimestamp: "20260605.121400.000"})
		if err != nil {
			t.Fatalf("replay bucket %q: %v", replayBucket, err)
		}
		if js := string(got.ScenarioJS); !strings.Contains(js, `"count": 4`) {
			t.Fatalf("replay bucket %q: the archived count of 4 must stay:\n%s", replayBucket, js)
		}
	}
}

func TestConvertJSONReportsEveryStepsList(t *testing.T) {
	step := func(id, names string) string {
		return `{"type": "load", "config": {"item": {"data": {"size": "10KB"}, "output": {"path": "/alpha"}},
      "storage": {"targetBucket": {"names": ` + names + `}}, "test": {"step": {"id": "` + id + `", "limit": {"count": 10}}}}}`
	}
	src := []byte(`{"type": "sequential", "config": {"storage": {"driver": {"type": "s3"}}}, "steps": [` +
		step("W", `["alpha", "beta"]`) + "," + step("R", `["gamma", "delta", "epsilon"]`) + "," + step("D", `["alpha", "beta"]`) + `]}`)
	got, err := ConvertJSON(src, RunScript{}, Options{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ArchivedBucketLists) != 2 || strings.Join(got.ArchivedBucketLists[1].Names, "|") != "gamma|delta|epsilon" ||
		len(got.ArchivedBucketLists[0].Steps) != 2 {
		t.Fatalf("ArchivedBucketLists = %+v", got.ArchivedBucketLists)
	}
	if preflight := BuildPreflight(got, Options{}); !strings.Contains(preflight, "Buckets: 3 (gamma ... epsilon), kept from the archive, step 2") {
		t.Fatalf("preflight:\n%s", preflight)
	}
}
