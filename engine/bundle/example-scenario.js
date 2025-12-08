// Example Spt Scenario
// 
// This example demonstrates a simple S3 performance test scenario.
// Copy this file and modify it for your specific test requirements.
//
// To run this scenario with Docker:
//   make docker-scenario SCENARIO=example-scenario.js
//
// Or directly:
//   docker run --rm \
//     -v $(pwd):/workspace \
//     -v $(pwd)/logs:/home/spt/log \
//     ghcr.io/dell/storage-performance-tool:latest \
//     --run-scenario=/workspace/example-scenario.js

// Configuration that will be shared across all load steps
var sharedConfig = {
    "storage": {
        "driver": {
            "type": "s3",
            "limit": {
                "concurrency": 10
            }
        },
        "net": {
            "node": {
                "addrs": ["your-s3-endpoint:9000"]  // Your S3 endpoint
            }
        },
        "auth": {
            "uid": "your-access-key",      // Your access key
            "secret": "your-secret-key"    // Your secret key
        }
    },
    "item": {
        "data": {
            "size": "1MB"
        },
        "output": {
            "path": "/your-bucket/spt-test"  // Bucket and path prefix
        }
    }
};

// Step 1: Create 1000 objects
print("Starting create operation...");
Load
    .config(sharedConfig)
    .config({
        "load": {
            "op": {
                "type": "create",
                "limit": {
                    "count": 1000
                }
            }
        }
    })
    .run();

// Step 2: Read the objects we just created
print("Starting read operation...");
Load
    .config(sharedConfig)
    .config({
        "load": {
            "op": {
                "type": "read",
                "limit": {
                    "time": "30s"
                }
            }
        },
        "item": {
            "input": {
                "path": "/your-bucket/spt-test"
            }
        }
    })
    .run();

// Step 3: Clean up - delete the objects
print("Starting cleanup...");
Load
    .config(sharedConfig)
    .config({
        "load": {
            "op": {
                "type": "delete"
            }
        },
        "item": {
            "input": {
                "path": "/your-bucket/spt-test"
            }
        }
    })
    .run();

print("Scenario completed!");