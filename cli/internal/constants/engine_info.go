package constants

import "time"

// Engine Build Information protocol values and client resource bounds.
const (
	EngineVersionEndpoint         = "/version"
	EngineBuildInfoSchemaVersion  = 1
	EngineBuildInfoProduct        = "spt-engine"
	EngineBuildInfoUnknown        = "unknown"
	EngineVersionRequestAttempts  = 3
	EngineVersionResponseMaxBytes = 64 * 1024
	EngineVersionRequestTimeout   = 2 * time.Second
	EngineVersionRetryDelay       = 100 * time.Millisecond
)
