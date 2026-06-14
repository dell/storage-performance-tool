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
			"WAIT_TIME":              "60s",
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
	if len(got.CommandOps) != 1 {
		t.Fatalf("len(CommandOps) = %d, want 1", len(got.CommandOps))
	}
	if got.CommandOps[0].Action != "converted" || !strings.Contains(got.CommandOps[0].Detail, "pauseSeconds") {
		t.Fatalf("CommandOps[0] = %+v, want converted sleep command", got.CommandOps[0])
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

func TestConvertJSONClassifiesParallelUnimplemented(t *testing.T) {
	raw := []byte(`{
  "type": "parallel",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}}}]
}`)
	_, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want parallel unimplemented")
	}
	if got := ErrorClass(err); got != failureParallelUnimplemented {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureParallelUnimplemented, err)
	}
}

func TestConvertJSONClassifiesScenarioNoStepsOrJobs(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": []
}`)
	_, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want no steps/jobs")
	}
	if got := ErrorClass(err); got != failureScenarioNoStepsOrJobs {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureScenarioNoStepsOrJobs, err)
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

func TestConvertJSONSupportsUpdateOperation(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{
    "type": "load",
    "config": {
      "item": {
        "data": {"size": "10KB"},
        "output": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}
      },
      "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 10}}}
    }
  }, {
    "type": "load",
    "config": {
      "item": {
        "data": {"size": "10KB"},
        "input": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}
      },
      "test": {"step": {"id": "MAX-U10KB"}},
      "load": {"op": {"type": "update"}, "limit": {"concurrency": 64}}
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
	for _, want := range []string{
		`UpdateLoad`,
		`"type": "update"`,
		`"id": "replay-002-20260605.121400.000-update"`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
	if got.Steps[1].Operation != opTypeUpdate {
		t.Fatalf("Steps[1].Operation = %q, want update", got.Steps[1].Operation)
	}
}

func TestConvertJSONWarnsOnUnmodeledConfigAndUsesInheritedDefaults(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {
    "item": {
      "data": {"size": "10KB"},
      "naming": {"prefix": "obj_", "length": 11}
    },
    "storage": {
      "driver": {"type": "emcs3"},
      "net": {
        "node": {"port": 9020},
        "http": {
          "headers": {
            "x-amz-meta-name": "JohnDoe-%d[1-10]",
            "x-amz-meta-score": "%f{##.##}[2-10]"
          }
        }
      }
    }
  },
  "jobs": [{
    "type": "load",
    "config": {
      "item": {"naming": {"offset": 12}},
      "test": {"step": {"id": "MAX", "limit": {"count": 10}}},
      "load": {"limit": {"concurrency": 4}}
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
	if !strings.Contains(js, `"size": "10KB"`) {
		t.Fatalf("scenario did not inherit top-level item.data.size:\n%s", js)
	}
	if strings.Contains(js, "JohnDoe-%d[1-10]") || strings.Contains(js, `"naming"`) {
		t.Fatalf("scenario retained unmodeled config instead of warning:\n%s", js)
	}
	if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "step MAX ignores unmodeled JSON config path item.naming") {
		t.Fatalf("Diagnostics = %+v, want item.naming warning", got.Diagnostics)
	}
	if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "step MAX ignores unmodeled JSON config path storage.net.http") {
		t.Fatalf("Diagnostics = %+v, want storage.net.http warning", got.Diagnostics)
	}
}

func TestConvertJSONPreservesSSLConfigAndLegacyAliases(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {
    "storage": {
      "driver": {"type": "s3"},
      "net": {
        "ssl": {
          "enabled": true,
          "ciphers": ["TLS_AES_128_GCM_SHA256"],
          "protocols": ["TLSv1.2", "TLSv1.3"],
          "provider": "OPENSSL",
          "jsseProvider": "SunJSSE",
          "namedGroups": ["x25519"],
          "pqcMode": "hybrid"
        }
      }
    }
  },
  "steps": [{
    "type": "load",
    "config": {
      "test": {
        "step": {
          "id": "MAX-W10KB",
          "limit": {
            "count": 1,
            "fail": {"count": 7}
          },
          "metrics": {"threshold": 0.95}
        }
      }
    }
  }]
}`)
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"https://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		`"threshold": 0.95`,
		`"count": 7`,
		`"provider": "OPENSSL"`,
		`"jsseProvider": "SunJSSE"`,
		`"pqcMode": "hybrid"`,
		`"TLS_AES_128_GCM_SHA256"`,
		`"TLSv1.2"`,
		`"TLSv1.3"`,
		`"x25519"`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
	for _, unwanted := range []string{
		"step MAX-W10KB ignores unmodeled JSON config path storage.net.ssl",
		"step MAX-W10KB ignores unmodeled JSON config path test.step.limit.fail.count",
		"step MAX-W10KB ignores unmodeled JSON config path test.step.metrics.threshold",
	} {
		if hasDiagnosticContaining(got.Diagnostics, severityWarning, unwanted) {
			t.Fatalf("Diagnostics = %+v, unexpected warning %q", got.Diagnostics, unwanted)
		}
	}
}

func TestConvertJSONMapsScalarSSLToEnabled(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {
    "storage": {
      "driver": {"type": "s3"},
      "net": {
        "ssl": true
      }
    }
  },
  "steps": [{
    "type": "load",
    "config": {
      "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}
    }
  }]
}`)
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"https://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJSON() error = %v", err)
	}
	js := string(got.ScenarioJS)
	if !strings.Contains(js, `"enabled": true`) {
		t.Fatalf("scenario missing scalar SSL mapping:\n%s", js)
	}
	if hasDiagnosticContaining(got.Diagnostics, severityWarning, "step MAX-W10KB ignores unmodeled JSON config path storage.net.ssl") {
		t.Fatalf("Diagnostics = %+v, unexpected scalar SSL warning", got.Diagnostics)
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

func TestConvertJSONRejectsScenarioWithOnlyUnsupportedStepTypes(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "jobs": [{
    "type": "for",
    "value": "threads",
    "in": [1, 2],
    "jobs": [{
      "type": "load",
      "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 10}}}}
    }]
  }]
}`)
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want no-load error")
	}
	if !strings.Contains(err.Error(), "scenario contains no load steps") {
		t.Fatalf("error = %v", err)
	}
	if got == nil {
		t.Fatal("ConvertJSON() generated result = nil, want diagnostics")
	}
	if !hasDiagnosticContaining(got.Diagnostics, severityWarning, `unsupported step type "for" skipped`) {
		t.Fatalf("Diagnostics = %+v, want skipped for-loop warning", got.Diagnostics)
	}
}

func TestParseSleepCommandAcceptsIntegerSecondsWithOptionalSuffix(t *testing.T) {
	tests := []struct {
		name    string
		command string
		vars    map[string]string
		want    int
		wantOK  bool
	}{
		{name: "bare integer", command: "sleep 180", want: 180, wantOK: true},
		{name: "integer seconds suffix", command: "sleep 180s", want: 180, wantOK: true},
		{name: "expanded seconds suffix", command: "sleep ${WAIT_TIME}", vars: map[string]string{"WAIT_TIME": "180s"}, want: 180, wantOK: true},
		{name: "unsupported milliseconds suffix", command: "sleep 180ms", wantOK: false},
		{name: "unsupported minutes suffix", command: "sleep 3m", wantOK: false},
		{name: "negative duration", command: "sleep -1s", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseSleepCommand(tt.command, tt.vars)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("parseSleepCommand(%q) = %d, %v; want %d, %v", tt.command, got, ok, tt.want, tt.wantOK)
			}
		})
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
	if len(got.CommandOps) != 1 {
		t.Fatalf("len(CommandOps) = %d, want 1", len(got.CommandOps))
	}
	if got.CommandOps[0].Action != "converted" || !strings.Contains(got.CommandOps[0].Detail, "safe file command") {
		t.Fatalf("CommandOps[0] = %+v, want converted safe file command", got.CommandOps[0])
	}
}

func TestConvertJSONConvertsSedFilePrepCommand(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"test": {"step": {"id": "MAX-W10KB", "limit": {"count": 10}}}}},
    {"type": "command", "value": "sed '/^.\\{6\\}./d' ${MONGOOSE_DIR}/log/items.w.10KB.csv > ${MONGOOSE_DIR}/log/items.w.10KB.csv.1", "blocking": true}
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
	want := `runReplayProcessToFile("sed", ["/^.\\{6\\}./d", sptHomeDir + "/log/items.w.10KB.csv"], sptHomeDir, sptHomeDir + "/log/items.w.10KB.csv.1", false);`
	if !strings.Contains(js, want) {
		t.Fatalf("scenario missing sed file-prep conversion %q\n%s", want, js)
	}
	if len(got.CommandOps) != 1 || got.CommandOps[0].Action != "converted" {
		t.Fatalf("CommandOps = %+v, want converted sed command", got.CommandOps)
	}
}

func TestConvertJSONConvertsCpCatRmFilePrepCommand(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {
      "item": {"output": {"file": "${MONGOOSE_DIR}/log/MAX-W10MB/items.csv"}},
      "test": {"step": {"id": "MAX-W10MB", "limit": {"count": 10}}}
    }},
    {"type": "command", "value": "cp ${MONGOOSE_DIR}/log/MAX-W10MB/items.csv a; cat a a a a a >> ${MONGOOSE_DIR}/log/MAX-W10MB/items.csv; rm a", "blocking": true}
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
		`runReplayProcess("cp", [sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv", "a"], sptHomeDir);`,
		`runReplayProcessToFile("cat", ["a", "a", "a", "a", "a"], sptHomeDir, sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv", true);`,
		`runReplayProcess("rm", ["a"], sptHomeDir);`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
	if strings.Contains(js, "/log/MAX-W10MB") {
		t.Fatalf("scenario retained legacy cp/cat/rm path:\n%s", js)
	}
	if len(got.PathRewrites) != 1 {
		t.Fatalf("len(PathRewrites) = %d, want 1", len(got.PathRewrites))
	}
	if len(got.CommandOps) != 1 || got.CommandOps[0].Action != "converted" {
		t.Fatalf("CommandOps = %+v, want converted cp/cat/rm command", got.CommandOps)
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
	got, err := ConvertJSON(raw, RunScript{Exports: map[string]string{}, ItemOutputPath: "bucket"}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJSON() error = nil, want unsupported command error")
	}
	if !strings.Contains(err.Error(), "unsupported command step") {
		t.Fatalf("error = %v", err)
	}
	if got := ErrorClass(err); got != failureUnsupportedCommandStep {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureUnsupportedCommandStep, err)
	}
	if got == nil {
		t.Fatal("ConvertJSON() generated result = nil, want rejected command diagnostics")
	}
	if len(got.CommandOps) != 1 || got.CommandOps[0].Action != "rejected" {
		t.Fatalf("CommandOps = %+v, want rejected command", got.CommandOps)
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

func TestConvertJSONRewritesForwardReferencedItemFileLabel(t *testing.T) {
	raw := []byte(`{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [{
    "type": "load",
    "config": {
      "load": {"type": "read"},
      "item": {"input": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}},
      "test": {"step": {"id": "MAX-R10KB", "limit": {"count": 10}}}
    }
  }, {
    "type": "load",
    "config": {
      "item": {"output": {"file": "${MONGOOSE_DIR}/log/MAX-W10KB/items.csv"}},
      "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 10}}}
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
	want := `var itemsFile001 = sptHomeDir + "/log/" + "replay-002-20260605.121400.000-create" + "/items.csv";`
	if !strings.Contains(js, want) {
		t.Fatalf("scenario missing forward-reference rewrite %q\n%s", want, js)
	}
	if got.Steps[0].Operation != opTypeRead {
		t.Fatalf("Steps[0].Operation = %q, want read", got.Steps[0].Operation)
	}
	if len(got.PathRewrites) != 1 || got.PathRewrites[0].StepID != "replay-002-20260605.121400.000-create" {
		t.Fatalf("PathRewrites = %+v, want rewrite to future create step", got.PathRewrites)
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
