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

var customHttpHeadersConfig = {
	"storage" : {
		"net" : {
			"http" : {
				"headers" : {
					"x-amz-meta-var-var1":"My-Object"
				}
			}
		}
	},
	"load": {
		"op": {
			"wait": {
				"finish": false
			}
		}
	}
};

Load
	.config(sharedConfig)
	.config(customHttpHeadersConfig)
	.run()
