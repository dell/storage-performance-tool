package constants

import "strconv"

const runClusterPrefix = "spt-run-"

// RunClusterID returns the stable cluster identity shared by one SPT run.
func RunClusterID(runID int64) string {
	return runClusterPrefix + strconv.FormatInt(runID, 10)
}
