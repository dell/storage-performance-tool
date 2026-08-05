// S3 persisted-data integrity: verify a QA-maintained finite item list.
//
// Run from a complete SPT distribution (including S3 extensions):
//   java -jar <SPT_DIR>/spt.jar \
//     --run-scenario=/path/to/s3_integrity_read_verify.js
//
// INTEGRITY_ITEMS_FILE may be a legacy QA item file for current-version reads
// in S3_BUCKET, or canonical RFC 4180 CSV with header:
// bucket,key,size,version_id (whose per-row bucket is authoritative).

function envOrDefault(name, defaultValue) {
	var value = java.lang.System.getenv(name);
	return value == null || value.length() == 0 ? defaultValue : String(value);
}

var ENDPOINT = envOrDefault("S3_ENDPOINT", "s3.example.com");
var PORT = java.lang.Integer.parseInt(envOrDefault("S3_PORT", "443"));
var SSL_ENABLED = java.lang.Boolean.parseBoolean(envOrDefault("S3_SSL_ENABLED", "true"));
var ACCESS_KEY = envOrDefault("S3_ACCESS_KEY", "");
var SECRET_KEY = envOrDefault("S3_SECRET_KEY", "");
var BUCKET = envOrDefault("S3_BUCKET", "qualification");
var REGION = envOrDefault("S3_REGION", "us-east-1");
var THREADS = java.lang.Integer.parseInt(envOrDefault("INTEGRITY_THREADS", "8"));
var ITEMS_FILE = envOrDefault("INTEGRITY_ITEMS_FILE", "objects-to-verify.csv");

var verifyStep = "direct-integrity-verify";
var homeDir = org.apache.logging.log4j.ThreadContext.get("home_dir");
var verifiedFile = homeDir + "/log/" + verifyStep + "/verified.csv";

ReadLoad.config({
	"storage": {
		"driver": {"type": "s3", "limit": {"concurrency": THREADS}},
		"net": {
			"node": {"addrs": ENDPOINT, "port": PORT},
			"ssl": {"enabled": SSL_ENABLED}
		},
		"auth": {
			"uid": ACCESS_KEY,
			"secret": SECRET_KEY,
			"version": 4
		},
		"region": REGION,
		"integrity": {
			"mode": "metadata",
			"algorithm": "sha256",
			"input": {"provenance": "external", "expectedProducerId": ""}
		}
	},
	"item": {
		"type": "data",
		"input": {"path": "/" + BUCKET, "file": ITEMS_FILE},
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
}).run();
