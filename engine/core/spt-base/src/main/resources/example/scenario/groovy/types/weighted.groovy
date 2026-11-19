package example.scenario.groovy.types

final sharedConcurrency = 100
final itemDataSize = "10KB"
final itemsFile = "weighted_load_example.csv"
final itemOutputPath = "/weighted_load_example"

// prepare (create) the 10000 items on a storage before the test
final preconditionLoad1 = PreconditionLoad
	.config(
		[
			item : [
				data : [
					size : itemDataSize
				],
				output : [
					file : itemsFile,
					path : itemOutputPath
				]
			],
			load : [
				op :[
					limit : [
						count : 10000
					]
				]
			],
			storage : [
				driver : [
					limit : [
						concurrency : sharedConcurrency
					]
				]
			]
		]
	)

// declare the weighted load step instance (create weight 20, read weight 80). The recycled
// read sub-step holds the create sub-step to its share while all its items fit in the input queue
final weightedLoad1 = WeightedLoad
	.config(
		[
			load : [
				step : [
					limit : [
						time : "100s"
					]
				]
			]
		]
	)
	.append(
		[
			item : [
				data : [
					size : itemDataSize
				],
				output : [
					path : itemOutputPath
				]
			],
			load : [
				op : [
					weight : 20
				]
			]
		]
	)
	.append(
		[
			item : [
				input : [
					file : itemsFile
				]
			],
			load : [
				op : [
					recycle : [
						mode : true
					],
					weight : 80,
					type : "read"
				]
			]
		]
	)

// go
"rm -f $itemsFile".execute().waitFor()
preconditionLoad1.run()
weightedLoad1.run()
