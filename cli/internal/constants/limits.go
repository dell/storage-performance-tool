/*
Copyright © 2025 Dell Technologies
*/

package constants

// Limit types reported by Spt node metrics payloads (the `limit.type` field of
// the JSON metrics API). These govern how a load step decides it is complete.
const (
	// LimitTypeNone indicates an unbounded run (no time or op-count limit).
	LimitTypeNone = "none"
	// LimitTypeTime indicates a duration-bounded run (limit.time_sec).
	LimitTypeTime = "time"
	// LimitTypeOpCount indicates an operation-count-bounded run (limit.op_count).
	LimitTypeOpCount = "op_count"
)
