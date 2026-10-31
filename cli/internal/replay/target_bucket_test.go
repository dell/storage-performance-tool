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

func TestConvertJSKeepsStepAndParentBucketCounts(t *testing.T) {
	src := `var parentConfig_1 = {
  "storage" : { "driver" : { "type" : "s3" }, "targetBucket" : { "count" : 3 } }
};
Load
    .config(parentConfig_1)
    .config({
      "item" : { "data" : { "size" : "10KB" }, "output" : { "path" : "/archive" } },
      "storage" : { "targetBucket" : { "count" : 4 } },
      "load" : { "step" : { "limit" : { "time" : 60 }, "id" : "W" } }
    })
    .run();
`
	got, err := ConvertJS([]byte(src), RunScript{ItemOutputPath: "archive"}, Options{
		Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"})
	if err != nil {
		t.Fatal(err)
	}
	js := string(got.ScenarioJS)
	parent, step, _ := strings.Cut(js[strings.Index(js, "var parentConfig_1"):], "Load\n")
	if !strings.Contains(parent, `"count": 3`) || !strings.Contains(parent, `"path": "/local"`) {
		t.Fatalf("parent config must keep its set:\n%s", parent)
	}
	if !strings.Contains(step, `"targetBucket" : { "count" : 4 }`) || !strings.Contains(step, `"path" : "/local"`) {
		t.Fatalf("step config must keep its set:\n%s", step)
	}
	for _, d := range got.Diagnostics {
		if strings.Contains(d.Message, "outside replay's modeled parent-config subset") {
			t.Fatalf("the count is modeled, not stripped: %s", d.Message)
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

const jsArchiveWithBucketList = `var parentConfig_1 = {
  "storage" : { "driver" : { "type" : "s3" }, "targetBucket" : { "names" : [ "ps-eu", "PS_US" ] } }
};
Load
    .config(parentConfig_1)
    .config({
      "item" : { "data" : { "size" : "10KB" }, "output" : { "path" : "/ps-eu" } },
      "storage" : { "targetBucket" : { "names" : [ "ps-eu", "PS_US", "ab" ] } },
      "load" : { "step" : { "limit" : { "time" : 60 }, "id" : "W" } }
    })
    .run();
`

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

func TestConvertJSKeepsStepAndParentBucketLists(t *testing.T) {
	for _, opts := range []Options{
		{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"},
		{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "from-env", BucketFromEnv: true, BaseTimestamp: "20260605.121400.000"},
	} {
		got, err := ConvertJS([]byte(jsArchiveWithBucketList), RunScript{}, opts)
		if err != nil {
			t.Fatalf("bucket %q: %v", opts.Bucket, err)
		}
		js := string(got.ScenarioJS)
		parent, step, _ := strings.Cut(js[strings.Index(js, "var parentConfig_1"):], "Load\n")
		if !strings.Contains(parent, `"names": [`) || !strings.Contains(parent, `"PS_US"`) {
			t.Fatalf("bucket %q: parent config must keep its list:\n%s", opts.Bucket, parent)
		}
		if !strings.Contains(step, `"names" : [ "ps-eu", "PS_US", "ab" ]`) {
			t.Fatalf("bucket %q: step config must keep its list:\n%s", opts.Bucket, step)
		}
		if strings.Contains(js, "from-env") || got.EffectiveBucket != "ps-eu" {
			t.Fatalf("bucket %q: S3_BUCKET must not replace the placeholder (effective %q):\n%s", opts.Bucket, got.EffectiveBucket, js)
		}
		for _, d := range got.Diagnostics {
			if strings.Contains(d.Message, "outside replay's modeled parent-config subset") {
				t.Fatalf("the list is modeled, not stripped: %s", d.Message)
			}
		}
	}
}

func TestReplayRejectsAnExplicitBucketForAnArchivedList(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, Bucket: "local", BaseTimestamp: "20260605.121400.000"}
	_, jsonErr := ConvertJSON([]byte(jsonArchiveWithBucketList), RunScript{Exports: map[string]string{"MONGOOSE_DIR": "/opt/m"}}, opts)
	_, jsErr := ConvertJS([]byte(jsArchiveWithBucketList), RunScript{}, opts)
	for kind, err := range map[string]error{"JSON": jsonErr, "JavaScript": jsErr} {
		if err == nil || !strings.Contains(err.Error(), "--bucket cannot be used") || !strings.Contains(err.Error(), "ps-eu ...") {
			t.Fatalf("%s: err = %v, want the --bucket rejection naming the list", kind, err)
		}
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

func jsArchiveWithParentNames(names string) string {
	return `var buckets = ["alpha", "beta"];
var parentConfig_1 = {
  "storage" : { "driver" : { "type" : "s3" }, "targetBucket" : { "names" : ` + names + ` } }
};
Load
    .config(parentConfig_1)
    .config({
      "item" : { "data" : { "size" : "10KB" }, "output" : { "path" : "/alpha" } },
      "load" : { "step" : { "limit" : { "time" : 60 }, "id" : "W" } }
    })
    .run();
`
}

func TestConvertJSRefusesABucketListItCannotKeepExactly(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"}
	for _, c := range []struct {
		names string
		want  string
	}{
		{`buckets`, "not a reference or expression"},
		{`buckets.concat(["gamma"])`, "not a reference or expression"},
		{`["alpha", buckets[1]]`, "element 2 is not a quoted name"},
		{`["alpha", 5]`, "element 2 is not a quoted name"},
		{`["alpha", ""]`, `bucket name 2 "" must not be empty`},
		{`["alpha,beta"]`, `must not contain ','`},
		{`["alpha", "alpha"]`, "repeats bucket name 1"},
		{`["a/b"]`, "must not contain '/'"},
	} {
		got, err := ConvertJS([]byte(jsArchiveWithParentNames(c.names)), RunScript{}, opts)
		if err == nil || !strings.Contains(err.Error(), "storage.targetBucket.names") || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("names %s: err = %v, want %q", c.names, err, c.want)
		}
		if got != nil && len(got.ScenarioJS) > 0 {
			t.Fatalf("names %s: no scenario may be generated", c.names)
		}
	}
}

func TestConvertJSKeepsLiteralListsWhateverTheKeyQuoting(t *testing.T) {
	opts := Options{Endpoints: []string{"http://10.0.0.1:9020"}, BaseTimestamp: "20260605.121400.000"}
	for _, c := range []struct {
		names string
		want  []string
	}{
		{`["alpha", "beta"]`, []string{"alpha", "beta"}},
		{`['alpha', "PS_US"]`, []string{"alpha", "PS_US"}},
		{`"alpha,beta"`, []string{"alpha", "beta"}},
	} {
		got, err := ConvertJS([]byte(jsArchiveWithParentNames(c.names)), RunScript{}, opts)
		if err != nil {
			t.Fatalf("names %s: %v", c.names, err)
		}
		if strings.Join(got.ArchivedBucketNames, "|") != strings.Join(c.want, "|") {
			t.Fatalf("names %s: ArchivedBucketNames = %q, want %q", c.names, got.ArchivedBucketNames, c.want)
		}
		parent, _, _ := strings.Cut(string(got.ScenarioJS)[strings.Index(string(got.ScenarioJS), "var parentConfig_1"):], "Load\n")
		for _, name := range c.want {
			if !strings.Contains(parent, `"`+name+`"`) {
				t.Fatalf("names %s: parent config must keep %q:\n%s", c.names, name, parent)
			}
		}
	}

	// Unquoted keys, in a step configuration and in a parent configuration.
	unquoted := `var parentConfig_1 = {
  storage : { driver : { type : "s3" }, targetBucket : { names : [ "ps-eu", "PS_US" ] } }
};
Load
    .config(parentConfig_1)
    .config({
      "item" : { "data" : { "size" : "10KB" }, "output" : { "path" : "/ps-eu" } },
      "storage" : { targetBucket : { names : [ "ps-eu", "PS_US" ] } },
      "load" : { "step" : { "limit" : { "time" : 60 }, "id" : "W" } }
    })
    .run();
`
	got, err := ConvertJS([]byte(unquoted), RunScript{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.ArchivedBucketNames, "|") != "ps-eu|PS_US" {
		t.Fatalf("unquoted keys: ArchivedBucketNames = %q", got.ArchivedBucketNames)
	}
	if parent := string(got.ScenarioJS); !strings.Contains(parent, `"names": [`) {
		t.Fatalf("unquoted keys: the parent config must keep the list:\n%s", parent)
	}
	explicit := opts
	explicit.Bucket = "local"
	if _, err := ConvertJS([]byte(unquoted), RunScript{}, explicit); err == nil || !strings.Contains(err.Error(), "--bucket cannot be used") {
		t.Fatalf("unquoted keys: an explicit --bucket must be rejected, err = %v", err)
	}

	// An empty list is no list: the replay bucket applies as for one bucket.
	empty, err := ConvertJS([]byte(jsArchiveWithParentNames(`[]`)), RunScript{}, explicit)
	if err != nil || empty.ArchivedBucketNames != nil || empty.EffectiveBucket != "local" {
		t.Fatalf("empty list: err = %v, names = %q, bucket = %q", err, empty.ArchivedBucketNames, empty.EffectiveBucket)
	}
}

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
