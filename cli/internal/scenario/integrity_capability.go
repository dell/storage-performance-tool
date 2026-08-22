/*
Copyright © 2026 Dell Technologies
*/

package scenario

import "github.com/dell/storage-performance-tool/cli/internal/constants"

// RequiresIntegrityCapability reports whether the selected scenario depends on engine integrity
// configuration. Keep this broader than IsIntegrityWorkload: existing-prefix DELETE needs a schema
// gate, but must not enter verification-only planning or finalization.
func RequiresIntegrityCapability(params Params) bool {
	return IsIntegrityWorkload(params) ||
		(params.WorkloadType == WorkloadTypeDelete && params.DeleteExisting)
}

// RequiresIntegrityRuntimeIdentity reports whether every selected runtime must be proven to use the
// same immutable engine image. Existing-prefix DELETE shares the identity gate because a legacy or
// heterogeneous worker could otherwise bypass destructive-discovery guards.
func RequiresIntegrityRuntimeIdentity(params Params) bool {
	return IsIntegrityWorkload(params) ||
		(params.WorkloadType == WorkloadTypeDelete && params.DeleteExisting)
}

// IntegritySchemaPathsFor returns only the engine schema leaves required by the selected scenario.
// Returning a copy prevents callers from mutating the shared compatibility contracts.
func IntegritySchemaPathsFor(params Params) []string {
	var paths []string
	switch {
	case IsIntegrityWorkload(params):
		paths = constants.RequiredIntegritySchemaPaths
	case params.WorkloadType == WorkloadTypeDelete && params.DeleteExisting:
		paths = constants.RequiredExistingPrefixDeleteSchemaPaths
	default:
		return nil
	}
	return append([]string(nil), paths...)
}
