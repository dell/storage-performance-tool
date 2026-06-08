package replay

import (
	"strings"
	"testing"
)

const maxS3SanityJSON = `{
  "type": "sequential",
  "config": {
    "storage": {
      "net": {"node": {"port": 9020}},
      "driver": {"type": "s3"}
    }
  },
  "steps": [
    {
      "type": "load",
      "config": {
        "item": {
          "data": {"size": "10KB"},
          "output": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}
        },
        "test": {"step": {"id": "MAX-W10KB", "limit": {"time": "${RUN_TIME_FOR_SMALL_OBJ}"}}},
        "load": {"limit": {"concurrency": 160}}
      }
    },
    {"type": "command", "value": "sleep ${WAIT_TIME}", "blocking": true},
    {
      "type": "load",
      "config": {
        "storage": {"driver": {"queue": {"input": 20000000}}},
        "item": {
          "data": {"size": "10KB", "verify": true},
          "input": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}
        },
        "test": {"step": {"id": "MAX-R10KB", "limit": {"time": "${RUN_TIME}"}}},
        "load": {
          "type": "read",
          "limit": {"concurrency": 160},
          "generator": {"recycle": {"enabled": false}, "shuffle": true}
        }
      }
    }
  ]
}`

func TestConvertJSONRewritesItemPathsToCanonicalStepIDs(t *testing.T) {
	runScript := RunScript{
		Exports: map[string]string{
			"RUN_TIME":               "900",
			"RUN_TIME_FOR_SMALL_OBJ": "1800",
			"WAIT_TIME":              "60",
			"MONGOOSE_DIR":           "/opt/mongoose/current",
			"KEY":                    "archived-sensitive-value",
			"USER_ID":                "archived_user",
			"CLIENTS":                "archived-client",
			"CLIENT_2":               "archived-client-2",
			"DATA_NODES":             "192.0.2.10",
			"DATA_NODE_2":            "192.0.2.11",
			"BUCKET":                 "archive-bucket",
		},
		ItemOutputPath: "archive-bucket",
	}
	got, err := ConvertJSON([]byte(maxS3SanityJSON), runScript, Options{
		Endpoints:     []string{"http://10.0.0.1:9020", "http://10.0.0.2:9020"},
		Bucket:        "local-bucket",
		TestHosts:     "local-client",
		Label:         "replay",
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		"replay-001-20260605.121400.000-create",
		"replay-002-20260605.121400.000-read",
		"var DATA_NODE = \"10.0.0.1\";",
		"var DATA_NODE_2 = \"10.0.0.2\";",
		"var CLIENT = \"local-client\";",
		"var CLIENT_2 = \"local-client\";",
		"var BUCKET = \"local-bucket\";",
		"var itemsFile001 = sptHomeDir + \"/log/\" + \"replay-001-20260605.121400.000-create\" + \"/items.csv\";",
		`"file": itemsFile001`,
		`"time": "1800s"`,
		`pauseSeconds(60, "Archived wait 1");`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
	for _, forbidden := range []string{"archived-sensitive-value", "archived_user", "192.0.2.11", "archived-client-2", "/log/MAX-W10KB"} {
		if strings.Contains(js, forbidden) {
			t.Fatalf("scenario leaked/retained forbidden text %q\n%s", forbidden, js)
		}
	}
	if len(got.Steps) != 2 {
		t.Fatalf("len(Steps) = %d", len(got.Steps))
	}
	if len(got.PathRewrites) != 1 {
		t.Fatalf("len(PathRewrites) = %d, want 1", len(got.PathRewrites))
	}
}

func TestConvertJSONRejectsFSDriver(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "fs"}}},
  "steps": [{"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB"}}}}]
}`)
	_, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want unsupported fs error")
	}
	if !strings.Contains(err.Error(), "NFS/FS replay is not implemented") {
		t.Fatalf("error = %v", err)
	}
}

func TestConvertJSONReadsTestStepLimitCount(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{
    "type": "precondition",
    "config": {
      "test": {"step": {"id": "SEED", "limit": {"count": 600000}}},
      "storage": {"driver": {"concurrency": 64}}
    }
  }]
}`)
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	if !strings.Contains(js, `"count": 600000`) {
		t.Fatalf("scenario missing test.step.limit.count mapping:\n%s", js)
	}
	if !strings.Contains(js, `"concurrency": 64`) {
		t.Fatalf("scenario missing storage.driver.concurrency mapping:\n%s", js)
	}
}

func TestConvertJSONRejectsUnboundedCreateStep(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{
    "type": "load",
    "config": {"test": {"step": {"id": "MAX-W10KB"}}}
  }]
}`)
	_, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want unbounded step error")
	}
	if !strings.Contains(err.Error(), "has no time or count limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestConvertJSONRejectsUnresolvedLimitVariable(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{
    "type": "load",
    "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"time": "${RUN_TIME}"}}}}
  }]
}`)
	_, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want unresolved variable error")
	}
	if !strings.Contains(err.Error(), "unresolved variable RUN_TIME") {
		t.Fatalf("error = %v", err)
	}
}

func TestConvertJSONConvertsSafeFileCommand(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 10}}}}},
    {"type": "command", "value": "cd ${MONGOOSE_DIR}/log/MAX-W10KB ; split -l 5 items.csv; mv items.csv items.csv.1; mv xaa items.csv", "blocking": true}
  ]
}`)
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		`runReplayProcess("split", ["-l", "5", "items.csv"], sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create");`,
		`runReplayProcess("mv", ["items.csv", "items.csv.1"], sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create");`,
		`runReplayProcess("mv", ["xaa", "items.csv"], sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create");`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
	if strings.Contains(js, "/log/MAX-W10KB") {
		t.Fatalf("scenario retained legacy command path:\n%s", js)
	}
	if len(got.PathRewrites) != 1 {
		t.Fatalf("len(PathRewrites) = %d, want 1", len(got.PathRewrites))
	}
}

func TestConvertJSONRejectsUnsafeCommand(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 10}}}}},
    {"type": "command", "value": "rm -rf ${MONGOOSE_DIR}/log/MAX-W10KB", "blocking": true}
  ]
}`)
	_, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want unsupported command error")
	}
	if !strings.Contains(err.Error(), "unsupported command step") {
		t.Fatalf("error = %v", err)
	}
}

func TestConvertJSONRequiresLocalEndpoints(t *testing.T) {
	_, err := ConvertJSON([]byte(maxS3SanityJSON), RunScript{
		Exports: map[string]string{
			"RUN_TIME":               "900",
			"RUN_TIME_FOR_SMALL_OBJ": "1800",
			"WAIT_TIME":              "60",
			"DATA_NODES":             "192.0.2.10",
		},
		ItemOutputPath: "bucket",
	}, Options{BaseTimestamp: "20260605.121400.000"})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want local endpoints error")
	}
	if !strings.Contains(err.Error(), "local endpoints are required") {
		t.Fatalf("error = %v", err)
	}
}

func TestConvertJSONRejectsUnknownItemFileLabel(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{
    "type": "load",
    "config": {
      "load": {"type": "read"},
      "item": {"input": {"file": "${MONGOOSE_DIR}/log/UNKNOWN/items.csv"}},
      "test": {"step": {"id": "MAX-R10KB", "limit": {"count": 10}}}
    }
  }]
}`)
	_, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want unknown item-file label error")
	}
	if !strings.Contains(err.Error(), "unknown item-file path label UNKNOWN") {
		t.Fatalf("error = %v", err)
	}
}

func TestConvertJSONRewritesGenericMongooseDirPath(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "jobs": [{
    "type": "precondition",
    "config": {
      "item": {
        "data": {"size": "10kb"},
        "output": {"file": "${MONGOOSE_DIR}/log/object-list.csv"}
      },
      "test": {"step": {"limit": {"count": 600000}}},
      "storage": {"driver": {"concurrency": 160}}
    }
  }]
}`)
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	if !strings.Contains(js, `var itemsFile001 = sptHomeDir + "/log/object-list.csv";`) {
		t.Fatalf("scenario missing generic MONGOOSE_DIR rewrite:\n%s", js)
	}
	if strings.Contains(js, "${MONGOOSE_DIR}") {
		t.Fatalf("scenario retained literal MONGOOSE_DIR placeholder:\n%s", js)
	}
}

func TestConvertJSONMasksKeyLikeExports(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}}}]
}`)
	got, err := ConvertJSON(raw, RunScript{
		Exports: map[string]string{
			"S3_ACCESS_KEY": "local-access-like-value",
			"API_KEY":       "api-key-like-value",
		},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	if strings.Contains(js, "local-access-like-value") || strings.Contains(js, "api-key-like-value") {
		t.Fatalf("scenario emitted key-like exports:\n%s", js)
	}
}
