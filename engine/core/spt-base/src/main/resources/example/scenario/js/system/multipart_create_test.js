// Multipart Upload (MPU) write scenario.
//
// This scenario uploads objects using S3 multipart upload. Each object is split
// into parts of PART_SIZE bytes and the parts are uploaded in parallel.
//
// Required variables (pass via --run-scenario=... or -D on the command line):
//   PART_SIZE        - part size, e.g. "64MB" (also acts as the MPU threshold)
//   ITEM_OUTPUT_FILE - path for the items CSV produced by this step
//   SIZE_LIMIT       - total data volume limit, e.g. "10GB"
//
// Key settings:
//   item.data.ranges.threshold  = PART_SIZE  -- enables MPU; objects > this are
//                                               split into parts of this size.
//   load.batch.size             = 1          -- REQUIRED for MPU. The default of
//                                               4096 overflows the child-op queue.
//
// The engine automatically retries individual parts up to 3 times. If a part
// still fails, the upload is aborted (AbortMultipartUpload) to clean up orphaned
// parts on the storage target.
//
// To enable per-part checksums, add to the "storage" block:
//   "checksum-enabled": true,
//   "checksum-algorithm": "crc32c"  // or md5, crc32, sha1, sha256

var cmd = new java.lang.ProcessBuilder()
	.command("sh", "-c", "rm -f ${ITEM_OUTPUT_FILE}")
	.inheritIO()
	.start();
cmd.waitFor();

Load
	.config({
	"item" : {
		"data" : {
		"ranges" : {
			"threshold" : PART_SIZE
		}
		},
		"output" : {
		"file" : "" + ITEM_OUTPUT_FILE + ""
		}
	},
	"storage" : {
		"namespace" : "ns1"
	},
	"load" : {
		"batch" : {
		"size" : 1
		},
		"step" : {
		"limit" : {
			"size" : SIZE_LIMIT
		}
		}
	}
	})
	.run();
