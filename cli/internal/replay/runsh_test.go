package replay

import (
	"strings"
	"testing"
)

func TestParseRunScriptExtractsExportsAndScenario(t *testing.T) {
	script := `#!/bin/bash
export DATA_NODE=10.0.0.1
export DATA_NODES="10.0.0.1,10.0.0.2"
export USER_ID=archived_user
export KEY="archived-sensitive-value"
export BUCKET="archive-bucket"
java -jar ${MONGOOSE_DIR}/mongoose.jar --storage-net-node-addrs=${DATA_NODES} --storage-driver-addrs=client1,client2 --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json
`
	got, err := ParseRunScript(script)
	if err != nil {
		t.Fatalf("ParseRunScript() error = %v", err)
	}
	if got.ScenarioPath != "/tmp/perf/max.s3.sanity.json" {
		t.Fatalf("ScenarioPath = %q", got.ScenarioPath)
	}
	if got.ItemOutputPath != "archive-bucket" {
		t.Fatalf("ItemOutputPath = %q", got.ItemOutputPath)
	}
	if got.StorageNodeAddrs != "10.0.0.1,10.0.0.2" {
		t.Fatalf("StorageNodeAddrs = %q", got.StorageNodeAddrs)
	}
	if got.Exports["KEY"] != "archived-sensitive-value" {
		t.Fatalf("KEY export was not parsed")
	}
}

func TestParseRunScriptClassifiesMissingScenarioReference(t *testing.T) {
	_, err := ParseRunScript(`#!/bin/bash
export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET}`)
	if err == nil {
		t.Fatal("ParseRunScript() error = nil, want missing scenario")
	}
	if got := ErrorClass(err); got != failureRunScriptMissingScenario {
		t.Fatalf("ErrorClass() = %q, want %q (err=%v)", got, failureRunScriptMissingScenario, err)
	}
}

// Replay keeps only the four flags above and the exports. Every other --storage-* or --load-* flag
// on the launch script is named once, in order, without its value, which can hold a credential or
// an endpoint.
func TestParseRunScriptNamesTheEngineFlagsItDoesNotKeep(t *testing.T) {
	script := `#!/bin/bash
export USER_ID=archived_user
export KEY="archived-sensitive-value"
export BUCKET=archive-bucket
export PROVISION="--storage-targetBucket-provision=false"
export UNUSED="--storage-net-ssl=true"
java -jar ${MONGOOSE_DIR}/mongoose.jar \
  --storage-net-node-addrs=10.0.0.1,10.0.0.2 --storage-driver-addrs=client1,client2 \
  --storage-auth-uid=${USER_ID} --storage-auth-secret=${KEY} \
  ${PROVISION} --storage-targetBucket-count=4 \
  --load-op-limit-rate 100 "--load-step-id=archived-step" --storage-auth-uid=${USER_ID} \
  --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json
`
	got, err := ParseRunScript(script)
	if err != nil {
		t.Fatalf("ParseRunScript() error = %v", err)
	}
	want := []string{
		"--storage-auth-uid", "--storage-auth-secret", "--storage-targetBucket-provision",
		"--storage-targetBucket-count", "--load-op-limit-rate", "--load-step-id",
	}
	if strings.Join(got.IgnoredFlags, " ") != strings.Join(want, " ") {
		t.Fatalf("IgnoredFlags = %q, want %q", got.IgnoredFlags, want)
	}
	if got.StorageNodeAddrs != "10.0.0.1,10.0.0.2" || got.ClientAddrs != "client1,client2" {
		t.Fatalf("the kept flags changed: nodes %q, clients %q", got.StorageNodeAddrs, got.ClientAddrs)
	}

	kept, err := ParseRunScript(`export BUCKET=archive-bucket
java -jar mongoose.jar --storage-net-node-addrs=10.0.0.1 --storage-driver-addrs=c1 --item-output-path=${BUCKET} --test-scenario-file=/tmp/s.json`)
	if err != nil {
		t.Fatalf("ParseRunScript() error = %v", err)
	}
	if kept.IgnoredFlags != nil {
		t.Fatalf("IgnoredFlags = %q, want none for a script with only the flags replay reads", kept.IgnoredFlags)
	}
}

// One warning lists every ignored flag, by name only.
func TestReplayWarningsListTheIgnoredLaunchFlagsOnce(t *testing.T) {
	artifacts := Artifacts{
		FolderURL:      "https://archive.example/run/",
		ScenarioFormat: "json",
		RunScript: RunScript{IgnoredFlags: []string{
			"--storage-auth-secret", "--storage-targetBucket-provision", "--load-op-limit-rate",
		}},
	}
	want := "replay does not keep these launch-script flags: --storage-auth-secret, " +
		"--storage-targetBucket-provision, --load-op-limit-rate"
	var found int
	for _, d := range replayWarnings(artifacts, Options{}) {
		if strings.Contains(d.Message, "launch-script flags") {
			found++
			if d.Message != want || d.Severity != severityWarning {
				t.Fatalf("diagnostic = %+v, want warning %q", d, want)
			}
		}
	}
	if found != 1 {
		t.Fatalf("found %d launch-script flag warnings, want 1", found)
	}
	for _, d := range replayWarnings(Artifacts{ScenarioFormat: "json"}, Options{}) {
		if strings.Contains(d.Message, "launch-script flags") {
			t.Fatalf("a script without ignored flags got %q", d.Message)
		}
	}
}
