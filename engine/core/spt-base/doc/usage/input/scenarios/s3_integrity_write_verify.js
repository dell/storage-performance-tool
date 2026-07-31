// S3 persisted-data integrity: write, then read every successful write once.
//
// Run from a complete SPT distribution (including S3 extensions):
//   java -jar <SPT_DIR>/spt.jar \
//     --run-scenario=/path/to/s3_integrity_write_verify.js
//
// Configure with S3_ENDPOINT (hostname only), S3_PORT, S3_SSL_ENABLED,
// S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_REGION, INTEGRITY_THREADS,
// INTEGRITY_COUNT, INTEGRITY_OBJECT_SIZE, and INTEGRITY_PREFIX.

var ENDPOINT = "%{env:S3_ENDPOINT:=s3.example.com}";
var PORT = %{env:S3_PORT:=443};
var SSL_ENABLED = %{env:S3_SSL_ENABLED:=true};
var ACCESS_KEY = "%{env:S3_ACCESS_KEY:=}";
var SECRET_KEY = "%{env:S3_SECRET_KEY:=}";
var BUCKET = "%{env:S3_BUCKET:=qualification}";
var REGION = "%{env:S3_REGION:=us-east-1}";
var THREADS = %{env:INTEGRITY_THREADS:=8};
var COUNT = %{env:INTEGRITY_COUNT:=1000};
var OBJECT_SIZE = "%{env:INTEGRITY_OBJECT_SIZE:=1MiB}";
var PREFIX = "%{env:INTEGRITY_PREFIX:=spt-integrity/}";

var createStep = "direct-integrity-create";
var verifyStep = "direct-integrity-verify";
var homeDir = org.apache.logging.log4j.ThreadContext.get("home_dir");
var writtenFile = homeDir + "/log/" + createStep + "/written.csv";
var verifiedFile = homeDir + "/log/" + verifyStep + "/verified.csv";

var commonS3 = {
	"storage": {
		"driver": {"type": "s3", "limit": {"concurrency": THREADS}},
		"net": {
			"node": {"addrs": [ENDPOINT], "port": PORT},
			"ssl": {"enabled": SSL_ENABLED}
		},
		"auth": {
			"uid": ACCESS_KEY,
			"secret": SECRET_KEY,
			"version": 4,
			"region": REGION
		}
	}
};

CreateLoad
	.config(commonS3)
	.config({
		"storage": {
			"integrity": {
				"mode": "metadata",
				"algorithm": "sha256",
				"input": {"provenance": "none", "expectedProducerId": ""}
			}
		},
		"item": {
			"type": "data",
			"data": {"size": OBJECT_SIZE},
			"naming": {"prefix": PREFIX},
			"output": {"path": "/" + BUCKET, "file": writtenFile}
		},
		"load": {
			"op": {"type": "create", "limit": {"count": COUNT}},
			"step": {"id": createStep}
		},
		"output": {"metrics": {"summary": {"persist": true}}}
	})
	.run();

ReadLoad
	.config(commonS3)
	.config({
		"storage": {
			"integrity": {
				"mode": "metadata",
				"algorithm": "sha256",
				"input": {
					"provenance": "engine_step",
					"expectedProducerId": createStep
				}
			}
		},
		"item": {
			"type": "data",
			"input": {"file": writtenFile},
			"output": {"file": verifiedFile}
		},
		"load": {
			"op": {
				"type": "read",
				"limit": {"fail": {"count": 0}},
				"wait": {"finish": true}
			},
			"step": {"id": verifyStep}
		},
		"output": {"metrics": {"summary": {"persist": true}}}
	})
	.run();
