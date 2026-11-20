sharedConcurrency = 100
itemDataSize = "10KB"
itemsFile = "weighted_load_example.csv"
itemOutputPath = "/weighted_load_example"

# declare the cleanup shell command
precondition1 = Command \
	.value("rm -f " + itemsFile)

# prepare (create) the 10000 items on a storage before the test
precondition2 = PreconditionLoad \
	.config(
		{
			"item": {
				"data": {
					"size": itemDataSize
				},
				"output": {
					"file": itemsFile,
					"path": itemOutputPath
				}
			},
			"load": {
				"op": {
					"limit": {
						"count": 10000
					}
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
	)

# declare the weighted load step instance (create weight 20, read weight 80). The recycled
# read sub-step holds the create sub-step to its share while all its items fit in the input queue
weightedLoad1 = WeightedLoad \
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
	) \
	.append(
		{
			"item": {
				"data": {
					"size": itemDataSize
				},
				"output": {
					"path": itemOutputPath
				}
			},
			"load": {
				"op": {
					"weight": 20
				}
			}
		}
	) \
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
						"mode": True
					},
					"weight": 80,
					"type": "read"
				}
			}
		}
	)

# go
precondition1.run()
precondition2.run()
weightedLoad1.run()
