var sharedConfig = {
	"load": {
		"op": {
			"wait": {
				"finish": false
			}
		}
	},
	"output": {
		"metrics": {
			"average": {
				"aggregation": {
					"period": 1
				}
			}
		}
	}
}

PreconditionLoad
	.config(sharedConfig)
	.config({
	"item" : {
		"output" : {
		"file" : ITEM_LIST_FILE
		}
	}
	})
	.run();

UpdateLoad
	.config(sharedConfig)
	.config({
	"item" : {
		"data" : {
		"ranges" : {
			"random" : RANDOM_BYTE_RANGE_COUNT
		}
		},
		"input" : {
		"file" : ITEM_LIST_FILE
		}
	}
	})
	.run();
