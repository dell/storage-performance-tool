var sharedConcurrency = 100;
var itemDataSize = "10KB";
var itemsFile = "weighted_load_example.csv";
var itemOutputPath = "/weighted_load_example";

// declare the cleanup shell command
new java.lang.ProcessBuilder("/bin/sh", "-c", "rm -f " + itemsFile)
	.inheritIO()
	.start()
	.waitFor();

var sharedConfig = {
	"item": {
		"data": {
			"size": itemDataSize
		},
		"output": {
			"path": itemOutputPath
		}
	},
	"storage": {
		"driver": {
			"limit": {
				"concurrency": sharedConcurrency
			}
		}
	}
}

// prepare (create) the 10000 items on a storage before the test
PreconditionLoad
	.config(sharedConfig)
	.config(
		{
			"item": {
				"output": {
					"file": itemsFile,
				}
			},
			"load": {
				"op": {
					"limit": {
						"count": 10000
					}
				}
			}
		}
	)
	.run();

// declare the weighted load step instance (create weight 20, read weight 80). The recycled
// read sub-step holds the create sub-step to its share while all its items fit in the input queue
WeightedLoad
	.config(sharedConfig)
	.config(
		{
			"load": {
				"step": {
					"limit": {
						"time": "100s"
					}
				}
			}
		}
	)
	.append(
		{
			"load": {
				"op": {
					"weight": 20
				}
			}
		}
	)
	.append(
		{
			"item": {
				"input": {
					"file": itemsFile
				}
			},
			"load": {
				"op": {
					"recycle": {
						"mode": true
					},
					"weight": 80,
					"type": "read"
				}
			}
		}
	)
	.run();
