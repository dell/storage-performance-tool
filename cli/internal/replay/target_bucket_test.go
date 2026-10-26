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
