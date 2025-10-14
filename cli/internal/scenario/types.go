package scenario

// Params holds all parameters needed to generate a Spt scenario.
type Params struct {
	WorkloadType string
	Endpoint     string
	Endpoints    []string
	AccessKey    string
	SecretKey    string
	Bucket       string
	Prefix       string
	Threads      int
	ObjectSize   string
	ObjectCount  int
	Duration     string
	AuthVersion  int
	Cleanup      bool // Automatically delete created objects after test
	KeepScenario bool // Keep the scenario file after test completes
	// Multi-endpoint controls
	SliceEndpoints bool // Partition endpoint list across nodes in distributed runs
}

// ScenarioParams is a temporary alias for Params kept for backward
// compatibility while the codebase migrates away from the stuttered name.
// TODO: remove alias after dependent code is updated.
//
//nolint:revive // compatibility alias retained during migration away from stuttered name
type ScenarioParams = Params
