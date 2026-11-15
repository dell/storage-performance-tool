// This example uses current configuration names; the Mongoose 3/4 names it once used are not aliased.
Load
	.config(
		{
			"load": {
				"op": {
					"limit": {
						"count": 100
					}
				},
				"step": {
					"id": "backward-compatibility-test"
				}
			},
			"storage": {
				"auth": {
					"uid": "wuser1@sanity.local"
				},
				"driver": {
					"limit": {
						"concurrency": 10
					}
				},
				"net": {
					"node": {
						"addrs": new java.util.ArrayList(["127.0.0.1"])
					}
				}
			}
		}
	)
	.run();
