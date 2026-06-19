package replay

import "testing"

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
