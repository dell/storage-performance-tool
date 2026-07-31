/*
Copyright © 2026 Dell Technologies
*/

package constants

// Engine configuration paths for the S3 persisted-data integrity contract. These are the
// dotted schema paths the CLI requires an integrity-capable engine to declare. They mirror the
// nested `storage.integrity` subtree in the engine base config schema.
const (
	IntegrityModePath              = "storage.integrity.mode"
	IntegrityAlgorithmPath         = "storage.integrity.algorithm"
	IntegrityInputProvenancePath   = "storage.integrity.input.provenance"
	IntegrityInputProducerIDPath   = "storage.integrity.input.expectedProducerId"
	IntegritySelectionMaxCountPath = "storage.integrity.selection.maxCount"
)

// RequiredIntegritySchemaPaths is the exact set an engine must declare before the CLI will submit
// a verification scenario. Presence is what matters: confuse schema leaves are type descriptors,
// so the probe never compares runtime values or infers semantics from leaf strings.
var RequiredIntegritySchemaPaths = []string{
	IntegrityModePath,
	IntegrityAlgorithmPath,
	IntegrityInputProvenancePath,
	IntegrityInputProducerIDPath,
	IntegritySelectionMaxCountPath,
}

// Integrity mode values.
const (
	IntegrityModeNone     = "none"
	IntegrityModeMetadata = "metadata"
)

// Distributed verification runtime-identity tiers. The public CLI accepts these short values;
// result evidence records the more descriptive engine-runtime tier names.
const (
	IntegrityRuntimeIdentityTierImage   = "image"
	IntegrityRuntimeIdentityTierPayload = "payload"
	IntegrityPayloadRoot                = "/opt/spt"
)

// Integrity input provenance values.
const (
	IntegrityProvenanceNone       = "none"
	IntegrityProvenanceEngineStep = "engine_step"
	IntegrityProvenanceCLIStager  = "cli_stager"
	IntegrityProvenanceExternal   = "external"
)

// IntegrityCLIStagerProducerID is the stable producer identity the CLI records when it stages an
// external `--items-file` selection and publishes the matching completion record.
const IntegrityCLIStagerProducerID = "spt-cli-items-stager-v1"

// Verification process exit codes are stable public automation contracts.
const (
	ExitCodeSuccess             = 0
	ExitCodeWorkloadFailure     = 1
	ExitCodeIntegrityCorruption = 20
)
