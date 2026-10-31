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
	g := &Generated{EffectiveBucket: "ps-eu", ArchivedBucketNames: []string{"ps-eu", "PS_US", "ab"}}
	preflight := BuildPreflight(g, Options{})
	if !strings.Contains(preflight, "Buckets: 3 (ps-eu ... ab), kept from the archive") || strings.Contains(preflight, "Bucket: ps-eu") {
		t.Fatalf("preflight:\n%s", preflight)
	}
	if !strings.Contains(string(buildMetadata(g)), `"bucketNames": [`) {
		t.Fatal("replay metadata must keep the list")
	}
	if strings.Contains(string(buildMetadata(&Generated{EffectiveBucket: "b"})), "bucketNames") {
		t.Fatal("a replay without a list adds no bucketNames")
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
		{`["alpha", 5]`, "element 2 is not a string"},
		{`["alpha", ""]`, `must not be empty`},
		{`{"a": "b"}`, "not a list of names"},
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
	if _, err := ConvertJSON(archive(`""`), RunScript{}, opts); err == nil || !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("an empty string is one empty name, err = %v", err)
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
		if !jsMentionsTargetBucket(source) {
			t.Fatalf("%s: not detected", source)
		}
	}
	for _, source := range []string{`target`, `"target" + "Bucket"`, `bucket`, `TargetBucket`} {
		if jsMentionsTargetBucket(source) {
			t.Fatalf("%s: detected", source)
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
