// Multipart Upload with per-part checksums and cleanup.
//
// This scenario demonstrates a complete MPU write workflow for JAR/Docker users:
//   1. Write large objects using multipart upload with per-part CRC32C checksums.
//   2. Optionally read the objects back (uncomment the ReadLoad section).
//   3. Delete the objects to clean up.
//
// Usage (JAR):
//   java -jar spt-base-<VERSION>.jar \
//       --storage-driver-type=s3 \
//       --storage-net-node-addrs=s3.example.com \
//       --storage-auth-uid=<ACCESS_KEY> \
//       --storage-auth-secret=<SECRET_KEY> \
//       --run-scenario=multipart_upload_with_checksums.js
//
// Usage (Docker):
//   docker run --network host ghcr.io/dell/storage-performance-tool \
//       --storage-net-node-addrs=s3.example.com \
//       --storage-auth-uid=<ACCESS_KEY> \
//       --storage-auth-secret=<SECRET_KEY> \
//       --run-scenario=multipart_upload_with_checksums.js
//
// Key configuration notes:
//   - item.data.ranges.threshold = "64MB" enables MPU with 64MB parts.
//   - load.batch.size = 1 is REQUIRED for MPU (prevents child-op queue overflow).
//   - storage.checksum-enabled = true with checksum-algorithm = "crc32c" sends a
//     per-part CRC32C checksum header on every UploadPart request.
//   - The engine retries individual parts up to 3 times and sends
//     AbortMultipartUpload if retries are exhausted.

// ---- Step 1: Multipart Upload Write ----------------------------------------

var itemsFile = "items.csv";

var writeConfig = {
    "item": {
        "data": {
            "size": "1GB",
            "ranges": {
                "threshold": "64MB"
            }
        },
        "output": {
            "file": itemsFile,
            "path": "/my-bucket"
        }
    },
    "load": {
        "batch": {
            "size": 1
        },
        "op": {
            "limit": {
                "count": 10
            }
        }
    },
    "storage": {
        "checksum-enabled": true,
        "checksum-algorithm": "crc32c"
    }
};

Load
    .config(writeConfig)
    .run();

// ---- Step 2 (optional): Read the objects back -------------------------------
// Uncomment to verify the uploaded objects can be read.
//
// ReadLoad
//     .config({
//         "item": {
//             "data": {
//                 "size": "1GB",
//                 "verify": true
//             },
//             "input": {
//                 "file": itemsFile
//             }
//         },
//         "load": {
//             "batch": {
//                 "size": 1
//             },
//             "op": {
//                 "recycle": {
//                     "mode": true
//                 }
//             },
//             "step": {
//                 "limit": {
//                     "time": "60s"
//                 }
//             }
//         }
//     })
//     .run();

// ---- Step 3: Cleanup (delete the objects) -----------------------------------

DeleteLoad
    .config({
        "item": {
            "input": {
                "file": itemsFile
            }
        },
        "load": {
            "batch": {
                "size": 4096
            }
        }
    })
    .run();
