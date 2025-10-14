var sharedConfig = {
	"load": {
		"step": {
			"limit": {
				"time": "3m"
			}
		},
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
	.config(
		{
			"item": {
				"output": {
					"path": ITEM_SRC_PATH
				}
			}
		}
	)
	.run();

Load
	.config(sharedConfig)
	.config(
		{
			"item": {
				"input": {
					"path": ITEM_SRC_PATH
				},
				"output": {
					"path": ITEM_DST_PATH
				}
			}
		}
	)
	.run();
