package replay

import (
	"strings"
	"testing"
)

const maxS3SanityJS = `var parentConfig_1 = {
  "storage" : {
    "net" : {
      "node" : {
        "port" : 9020
      }
    },
    "driver" : {
      "type" : "s3"
    }
  }
};

var cmd_1 = new java.lang.ProcessBuilder()
    .command("/bin/sh", "-c", "sleep ${WAIT_TIME}")
    .inheritIO()
    .start();
cmd_1.waitFor();

Load
    .config(parentConfig_1)
    .config({
      "item" : {
        "data" : {
          "size" : "10KB"
        },
        "output" : {
          "file" : "" + MONGOOSE_DIR + "/log/MAX-W10KB/items.csv"
        }
      },
      "storage" : {
        "driver" : {
          "limit" : {
            "concurrency" : 70
          }
        }
      },
      "load" : {
        "step" : {
          "limit" : {
            "time" : RUN_TIME_FOR_SMALL_OBJ
          },
          "id" : "MAX-W10KB"
        }
      }
    })
    .run();

ReadLoad
    .config(parentConfig_1)
    .config({
      "item" : {
        "data" : {
          "size" : "10KB",
          "verify" : true
        },
        "input" : {
          "file" : "" + MONGOOSE_DIR + "/log/MAX-W10KB/items.csv"
        }
      },
      "storage" : {
        "driver" : {
          "limit" : {
            "concurrency" : 160
          }
        }
      },
      "load" : {
        "op" : {
          "shuffle" : true,
          "recycle" : { "mode" : false }
        },
        "step" : {
          "limit" : {
            "time" : RUN_TIME
          },
          "id" : "MAX-R10KB"
        }
      }
    })
    .run();
`

func TestConvertJSAdaptsGeneratedS3Scenario(t *testing.T) {
	got, err := ConvertJS([]byte(maxS3SanityJS), RunScript{
		Exports: map[string]string{
			"RUN_TIME":               "900",
			"RUN_TIME_FOR_SMALL_OBJ": "1800",
			"WAIT_TIME":              "60s",
			"MONGOOSE_DIR":           "/opt/mongoose/current",
			"DATA_NODE":              "192.0.2.10",
			"DATA_NODE_2":            "192.0.2.11",
			"BUCKET":                 "archive-bucket",
			"KEY":                    "archived-sensitive-value",
			"CLIENT":                 "archived-client",
			"CLIENT_2":               "archived-client-2",
			"CLIENTS":                "archived-client,archived-client-2",
		},
		ItemOutputPath: "archive-bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:8333", "http://10.0.0.2:8334"},
		Bucket:        "local-bucket",
		TestHosts:     "root@wrk1,root@wrk2",
		Label:         "replay",
		S3Driver:      "aws",
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		"var sptHomeDir = org.apache.logging.log4j.ThreadContext.get(\"home_dir\");",
		"var MONGOOSE_DIR = sptHomeDir;",
		"var DATA_NODE = \"10.0.0.1\";",
		"var DATA_NODE_2 = \"10.0.0.2\";",
		"var CLIENT = \"wrk1\";",
		"var CLIENT_2 = \"wrk2\";",
		"var CLIENTS = \"wrk1,wrk2\";",
		"var BUCKET = \"local-bucket\";",
		`"type": "s3-aws"`,
		`"path" : "/local-bucket"`,
		`pauseSeconds(60, "Archived wait 1");`,
		`"id" : "replay-001-20260605.121400.000-create"`,
		`"id" : "replay-002-20260605.121400.000-read"`,
		`sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv"`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
	for _, forbidden := range []string{
		"new java.lang.ProcessBuilder",
		`"port" : 9020`,
		"/log/MAX-W10KB",
		"archived-sensitive-value",
		"192.0.2.11",
		"archive-bucket",
		"archived-client",
		"root@",
	} {
		if strings.Contains(js, forbidden) {
			t.Fatalf("scenario retained forbidden text %q\n%s", forbidden, js)
		}
	}
	if len(got.Steps) != 2 {
		t.Fatalf("len(Steps) = %d, want 2", len(got.Steps))
	}
	if got.Steps[0].ArchiveID != "MAX-W10KB" || got.Steps[0].Operation != "create" || got.Steps[0].Duration != "1800s" || got.Steps[0].Concurrency != 70 {
		t.Fatalf("Steps[0] = %+v", got.Steps[0])
	}
	if got.Steps[1].ArchiveID != "MAX-R10KB" || got.Steps[1].Operation != "read" || got.Steps[1].Duration != "900s" || got.Steps[1].Concurrency != 160 {
		t.Fatalf("Steps[1] = %+v", got.Steps[1])
	}
	if len(got.PathRewrites) != 1 {
		t.Fatalf("len(PathRewrites) = %d, want 1", len(got.PathRewrites))
	}
	if len(got.CommandOps) != 1 || got.CommandOps[0].Action != "converted" {
		t.Fatalf("CommandOps = %+v, want converted sleep command", got.CommandOps)
	}
}

func TestConvertJSRejectsEmptyLimitVariable(t *testing.T) {
	got, err := ConvertJS([]byte(maxS3SanityJS), RunScript{
		Exports: map[string]string{
			"RUN_TIME":               "",
			"RUN_TIME_FOR_SMALL_OBJ": "1800",
			"WAIT_TIME":              "60",
		},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJS() error = nil, want empty limit variable rejection")
	}
	if !strings.Contains(err.Error(), "unresolved variable RUN_TIME") {
		t.Fatalf("error = %v", err)
	}
	if got == nil || !hasDiagnosticContaining(got.Diagnostics, severityError, "unresolved variable RUN_TIME") {
		t.Fatalf("Diagnostics = %+v, want RUN_TIME error", got)
	}
}

func TestConvertJSRejectsUnboundedReadStep(t *testing.T) {
	raw := strings.Replace(maxS3SanityJS, `"time" : RUN_TIME
`, `"time" : ""
`, 1)

	_, err := ConvertJS([]byte(raw), RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJS() error = nil, want unbounded step rejection")
	}
	if !strings.Contains(err.Error(), "has no time or count limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestConvertJSRewritesInlineOutputPathToLocalBucket(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `"output" : {
          "file" : "" + MONGOOSE_DIR + "/log/MAX-W10KB/items.csv"
        }`, `"output" : {
          "path" : "/archive-bucket",
          "file" : "" + MONGOOSE_DIR + "/log/MAX-W10KB/items.csv"
        }`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "archive-bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		Bucket:        "local-bucket",
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	js := string(got.ScenarioJS)
	if strings.Contains(js, "/archive-bucket") {
		t.Fatalf("scenario retained archived output path:\n%s", js)
	}
	if !strings.Contains(js, `"path" : "/local-bucket"`) {
		t.Fatalf("scenario missing local output path:\n%s", js)
	}
}

func TestConvertJSRejectsUnsafeProcessBuilderCommand(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `sleep ${WAIT_TIME}`, `shuf /opt/mongoose/current/log/MAX-W10KB/items.csv > /opt/mongoose/current/log/MAX-W10KB/items.csv.1;`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJS() error = nil, want unsafe command rejection")
	}
	if !strings.Contains(err.Error(), "unsupported JavaScript command") {
		t.Fatalf("error = %v", err)
	}
	if got == nil || len(got.CommandOps) != 1 || got.CommandOps[0].Action != "rejected" {
		t.Fatalf("CommandOps = %+v, want rejected command", got)
	}
}

func TestConvertJSConvertsFilePrepProcessBuilderWithArchivedAbsoluteWorkDir(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `sleep ${WAIT_TIME}`, `cd /opt/mongoose/current/log/MAX-W10KB;sed '/^.\{49\}./d' items.csv > items.csv.1; sed -r '/^.{,45}$/d' items.csv.1 > items.csv;`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	if len(got.CommandOps) != 1 || got.CommandOps[0].Action != "converted" || !strings.Contains(got.CommandOps[0].Detail, "safe file command") {
		t.Fatalf("CommandOps = %+v, want converted file-prep command", got.CommandOps)
	}
	if hasDiagnosticContaining(got.Diagnostics, severityError, "unsupported JavaScript command") {
		t.Fatalf("Diagnostics = %+v, want no unsupported command error", got.Diagnostics)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		`function runReplayProcess(command, args, cwd) {`,
		`function runReplayProcessToFile(command, args, cwd, outputPath, append) {`,
		`runReplayProcessToFile("sed", ["/^.\\{49\\}./d", "items.csv"], sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create", sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv.1", false);`,
		`runReplayProcessToFile("sed", ["-r", "/^.{,45}$/d", "items.csv.1"], sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create", sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv", false);`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
}

func TestConvertJSConvertsFilePrepProcessBuilderWithArchivedAbsoluteFileArgs(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `sleep ${WAIT_TIME}`, `sed '/^.\{49\}./d' /opt/mongoose/current/log/MAX-W10KB/items.csv > /opt/mongoose/current/log/MAX-W10KB/items.csv.1; sed -r '/^.{,45}$/d' /opt/mongoose/current/log/MAX-W10KB/items.csv.1 > /opt/mongoose/current/log/MAX-W10KB/items.csv;`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	if len(got.CommandOps) != 1 || got.CommandOps[0].Action != "converted" {
		t.Fatalf("CommandOps = %+v, want converted file-prep command", got.CommandOps)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		`runReplayProcessToFile("sed", ["/^.\\{49\\}./d", sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv"], sptHomeDir, sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv.1", false);`,
		`runReplayProcessToFile("sed", ["-r", "/^.{,45}$/d", sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv.1"], sptHomeDir, sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv", false);`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
}

func TestConvertJSClassifiesUnsupportedProcessBuilder(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `sleep ${WAIT_TIME}`, `python /tmp/s3query.py ${BUCKET}`, 1))
	_, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJS() error = nil, want unsupported JS ProcessBuilder")
	}
	if got := ErrorClass(err); got != failureUnsupportedJSProcess {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureUnsupportedJSProcess, err)
	}
}

func TestConvertJSRewritesBareParentConfigAssignment(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, "var parentConfig_1", "parentConfig_1", 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	js := string(got.ScenarioJS)
	if strings.Contains(js, `"port" : 9020`) {
		t.Fatalf("scenario retained archived parent port:\n%s", js)
	}
	if !strings.Contains(js, `var parentConfig_1 = {`) {
		t.Fatalf("scenario missing sanitized parent declaration:\n%s", js)
	}
}

func TestConvertJSPreservesRepresentableParentSSLSettings(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `"net" : {
      "node" : {
        "port" : 9020
      }
    },`, `"net" : {
      "node" : {
        "port" : 9020
      },
      "ssl" : {
        "enabled" : true,
        "ciphers" : ["TLS_AES_128_GCM_SHA256"],
        "protocols" : ["TLSv1.2", "TLSv1.3"],
        "provider" : "OPENSSL",
        "jsseProvider" : "SunJSSE",
        "namedGroups" : ["x25519"],
        "pqcMode" : "hybrid"
      }
    },`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"https://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	if hasDiagnosticContaining(got.Diagnostics, severityWarning, "archived parent settings") {
		t.Fatalf("Diagnostics = %+v, want no generic parentConfig stripping warning for representable SSL", got.Diagnostics)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		`"ssl": {`,
		`"enabled": true`,
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
	if strings.Contains(js, `"port": 9020`) || strings.Contains(js, `"port" : 9020`) {
		t.Fatalf("scenario retained archived parent port:\n%s", js)
	}
}

func TestConvertJSWarnsWhenParentConfigDropsUnsupportedSSLSettings(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `"net" : {
      "node" : {
        "port" : 9020
      }
    },`, `"net" : {
      "node" : {
        "port" : 9020
      },
      "ssl" : {
        "enabled" : true,
        "trustAll" : true
      }
    },`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"https://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	if !hasDiagnosticContaining(got.Diagnostics, severityWarning, "unsupported archived ssl setting") {
		t.Fatalf("Diagnostics = %+v, want unsupported SSL warning", got.Diagnostics)
	}
	js := string(got.ScenarioJS)
	if !strings.Contains(js, `"enabled": true`) {
		t.Fatalf("scenario missing preserved ssl.enabled:\n%s", js)
	}
	if strings.Contains(js, `"trustAll"`) {
		t.Fatalf("scenario retained unsupported archived ssl setting:\n%s", js)
	}
}

func TestConvertJSRejectsUnsupportedLoadFactory(t *testing.T) {
	raw := []byte(maxS3SanityJS + `
MixedLoad
    .config({
      "load" : {
        "step" : {
          "id" : "MIXED"
        }
      }
    })
    .run();
`)

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJS() error = nil, want unsupported load factory rejection")
	}
	if !strings.Contains(err.Error(), "unsupported JavaScript load factory MixedLoad") {
		t.Fatalf("error = %v", err)
	}
	if got := ErrorClass(err); got != failureUnsupportedJSLoadFactory {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureUnsupportedJSLoadFactory, err)
	}
	if got == nil || !hasDiagnosticContaining(got.Diagnostics, severityError, "MixedLoad") {
		t.Fatalf("Diagnostics = %+v, want MixedLoad error", got)
	}
}

func TestConvertJSRejectsInlineArchivedAuth(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `"storage" : {
        "driver" : {
          "limit" : {
            "concurrency" : 70
          }
        }
      }`, `"storage" : {
        "auth" : {
          "uid" : "archived-user",
          "secret" : "archived-secret"
        },
        "driver" : {
          "limit" : {
            "concurrency" : 70
          }
        }
      }`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJS() error = nil, want inline auth rejection")
	}
	if strings.Contains(err.Error(), "archived-secret") || strings.Contains(err.Error(), "archived-user") {
		t.Fatalf("error leaked inline auth value: %v", err)
	}
	if got == nil || len(got.ScenarioJS) != 0 {
		t.Fatalf("generated = %+v, want no scenario emitted on inline auth rejection", got)
	}
}

func TestConvertJSInfersLoadReadOperationFromOpType(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `ReadLoad
    .config(parentConfig_1)`, `Load
    .config(parentConfig_1)`, 1))
	raw = []byte(strings.Replace(string(raw), `"op" : {
          "shuffle" : true,`, `"op" : {
          "type" : "read",
          "shuffle" : true,`, 1))

	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	if got.Steps[1].Operation != opTypeRead {
		t.Fatalf("Steps[1].Operation = %q, want read", got.Steps[1].Operation)
	}
	if !strings.Contains(string(got.ScenarioJS), `"id" : "replay-002-20260605.121400.000-read"`) {
		t.Fatalf("scenario missing read-suffixed canonical step ID:\n%s", string(got.ScenarioJS))
	}
}

func TestConvertJSSupportsUpdateLoadFactory(t *testing.T) {
	raw := []byte(`var parentConfig_1 = {
  "storage" : {
    "driver" : {
      "type" : "s3"
    }
  }
};

Load
    .config(parentConfig_1)
    .config({
      "item" : {
        "data" : {
          "size" : "10KB"
        },
        "output" : {
          "file" : "" + MONGOOSE_DIR + "/log/MAX-W10KB/items.csv"
        }
      },
      "load" : {
        "step" : {
          "id" : "MAX-W10KB",
          "limit" : {
            "count" : 10
          }
        }
      }
    })
    .run();

UpdateLoad
    .config(parentConfig_1)
    .config({
      "item" : {
        "data" : {
          "size" : "10KB"
        },
        "input" : {
          "file" : "" + MONGOOSE_DIR + "/log/MAX-W10KB/items.csv"
        }
      },
      "storage" : {
        "driver" : {
          "limit" : {
            "concurrency" : 64
          }
        }
      },
      "load" : {
        "step" : {
          "id" : "MAX-U10KB"
        }
      }
    })
    .run();

`)
	got, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err != nil {
		t.Fatalf("ConvertJS() error = %v", err)
	}
	if got.Steps[1].Operation != opTypeUpdate {
		t.Fatalf("Steps[1].Operation = %q, want update", got.Steps[1].Operation)
	}
	js := string(got.ScenarioJS)
	for _, want := range []string{
		`"id" : "replay-002-20260605.121400.000-update"`,
		`sptHomeDir + "/log/" + "replay-001-20260605.121400.000-create" + "/items.csv"`,
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("scenario missing %q\n%s", want, js)
		}
	}
}

func TestConvertJSRejectsUnsupportedDriver(t *testing.T) {
	raw := []byte(strings.Replace(maxS3SanityJS, `"type" : "s3"`, `"type" : "swift"`, 1))

	_, err := ConvertJS(raw, RunScript{
		Exports:        map[string]string{"RUN_TIME": "900", "RUN_TIME_FOR_SMALL_OBJ": "1800", "WAIT_TIME": "60"},
		ItemOutputPath: "bucket",
	}, Options{
		Endpoints:     []string{"http://10.0.0.1:9020"},
		BaseTimestamp: "20260605.121400.000",
	})
	if err == nil {
		t.Fatal("ConvertJS() error = nil, want unsupported driver error")
	}
	if !strings.Contains(err.Error(), "SWIFT replay is not implemented") {
		t.Fatalf("error = %v", err)
	}
}

func hasDiagnosticContaining(diagnostics []Diagnostic, severity, text string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == severity && strings.Contains(diagnostic.Message, text) {
			return true
		}
	}
	return false
}
