var pipelineSharedConfig = {
	"item": {
		"output": {
			"path": "endurance_0"
		}
	},
	"load": {
		"step": {
			"id": "endurance_0"
		}
	}
}

var pipelineCreateCtx = {
}

var pipelineUpdateCtx = {
	"item": {
		"data": {
			"ranges": {
				"random": 4
			}
		}
	},
	"load": {
		"op": {
			"type": "update"
		}
	}
}

var pipelineReadCtx = {
	"item": {
		"data": {
			"verify": true
		}
	},
	"load": {
		"op": {
			"type": "read"
		}
	}
}

var pipelineDeleteCtx = {
	"load": {
		"op": {
			"type": "delete"
		}
	}
}

// Each step gets its own id: the engine refuses a step id that an earlier step used in the same
// log directory. The start time keeps the ids of a rerun apart from the earlier run's.
var infiniteLoopStepIdPrefix = "endurance_1_" + new Date().getTime() + "_";

function infiniteLoopIterationLoadConfig(iteration) {
	return {
		"item": {
			"data": {
				"size": "1KB"
			},
			"output": {
				"path": "endurance_1"
			}
		},
		"load": {
			"step": {
				"id": infiniteLoopStepIdPrefix + iteration,
				"limit": {
					"time": "5s"
				}
			}
		}
	};
}

var pipelineLoad = PipelineLoad
	.config(pipelineSharedConfig)
	.append(pipelineCreateCtx)
	.append(pipelineUpdateCtx)
	.append(pipelineReadCtx)
	.append(pipelineDeleteCtx)
	.start();

for(var iteration = 0; ; iteration ++) {
	Load
		.config(infiniteLoopIterationLoadConfig(iteration))
		.run();
}
