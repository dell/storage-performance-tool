package engineinfo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// Manifest is the CLI-owned, run-level Engine Identity Manifest.
type Manifest struct {
	SchemaVersion int                   `json:"schema_version"`
	RunID         int64                 `json:"run_id"`
	GeneratedAt   string                `json:"generated_at"`
	Consistency   ManifestConsistency   `json:"consistency"`
	Builds        []ManifestBuild       `json:"builds"`
	Participants  []ManifestParticipant `json:"participants"`
}

// ManifestConsistency records the pre-submission fleet assessment and override state.
type ManifestConsistency struct {
	Status ConsistencyStatus `json:"status"`
	Forced bool              `json:"forced"`
	Reason string            `json:"reason"`
}

// ManifestBuild is one deterministic, document-local build group.
type ManifestBuild struct {
	BuildID     string `json:"build_id"`
	Product     string `json:"product"`
	Version     string `json:"version"`
	Revision    string `json:"revision"`
	BuildTime   string `json:"build_time"`
	Development bool   `json:"development"`
	SourceDirty *bool  `json:"source_dirty"`
}

// ManifestParticipant retains safe evidence for every planned engine participant.
type ManifestParticipant struct {
	NodeID                string           `json:"node_id"`
	Role                  ParticipantRole  `json:"role"`
	CollectionStatus      CollectionStatus `json:"collection_status"`
	ReportedSchemaVersion int              `json:"reported_schema_version,omitempty"`
	BuildID               string           `json:"build_id,omitempty"`
	ConfiguredVersionHint string           `json:"configured_version_hint,omitempty"`
	Reason                string           `json:"reason,omitempty"`
}

// NewManifest converts one gate outcome into its canonical run-level representation.
// configuredVersionHints are non-authoritative and never participate in grouping or consistency.
func NewManifest(
	runID int64,
	generatedAt time.Time,
	outcome GateOutcome,
	configuredVersionHints map[string]string,
) (Manifest, error) {
	builds := make([]GroupedBuild, len(outcome.Fleet.Builds))
	copy(builds, outcome.Fleet.Builds)
	sort.Slice(builds, func(i, j int) bool {
		return buildKey(builds[i].Information) < buildKey(builds[j].Information)
	})

	manifestBuilds := make([]ManifestBuild, 0, len(builds))
	buildIDByPriorID := make(map[string]string, len(builds))
	for index, grouped := range builds {
		buildID := canonicalBuildReference(index)
		buildIDByPriorID[grouped.BuildID] = buildID
		manifestBuilds = append(manifestBuilds, ManifestBuild{
			BuildID:     buildID,
			Product:     grouped.Information.Product,
			Version:     grouped.Information.Version,
			Revision:    grouped.Information.Revision,
			BuildTime:   grouped.Information.BuildTime,
			Development: grouped.Information.Development,
			SourceDirty: cloneBool(grouped.Information.SourceDirty),
		})
	}

	participants := make([]ManifestParticipant, 0, len(outcome.Fleet.Participants))
	for _, participant := range outcome.Fleet.Participants {
		buildID := ""
		if participant.BuildID != "" {
			var ok bool
			buildID, ok = buildIDByPriorID[participant.BuildID]
			if !ok {
				return Manifest{}, fmt.Errorf("engine participant %s references unknown build %s", participant.NodeID, participant.BuildID)
			}
		}
		configuredVersionHint := ""
		if participant.CollectionStatus == StatusLegacyEndpointUnavailable {
			configuredVersionHint = safeConfiguredVersionHint(configuredVersionHints[participant.NodeID])
		}
		participants = append(participants, ManifestParticipant{
			NodeID:                participant.NodeID,
			Role:                  participant.Role,
			CollectionStatus:      participant.CollectionStatus,
			ReportedSchemaVersion: participant.ReportedSchemaVersion,
			BuildID:               buildID,
			ConfiguredVersionHint: configuredVersionHint,
			Reason:                participant.Reason,
		})
	}
	sort.Slice(participants, func(i, j int) bool {
		leftRank, _, _ := participantRolePolicy(participants[i].Role)
		rightRank, _, _ := participantRolePolicy(participants[j].Role)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return participants[i].NodeID < participants[j].NodeID
	})

	reason := outcome.Fleet.Consistency.Reason
	switch outcome.Decision {
	case GateRejectedMismatch:
		reason = "engine build identity mismatch rejected before scenario submission"
	case GateCollectionFailure:
		reason = "engine identity collection failed before scenario submission"
	}
	manifest := Manifest{
		SchemaVersion: constants.EngineInfoManifestSchemaVersion,
		RunID:         runID,
		GeneratedAt:   generatedAt.UTC().Format(time.RFC3339),
		Consistency: ManifestConsistency{
			Status: outcome.Fleet.Consistency.Status,
			Forced: outcome.Fleet.Consistency.Forced,
			Reason: reason,
		},
		Builds:       manifestBuilds,
		Participants: participants,
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func safeConfiguredVersionHint(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return ""
		}
	}
	return value
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// Validate checks schema-one referential and lifecycle invariants.
func (manifest Manifest) Validate() error {
	if err := validateManifestEnvelope(manifest); err != nil {
		return err
	}
	buildsByID, buildReferences, err := validateManifestBuilds(manifest.Builds)
	if err != nil {
		return err
	}
	participantEvidence, err := validateManifestParticipants(manifest.Participants, buildsByID, buildReferences)
	if err != nil {
		return err
	}
	if err := validateManifestTopology(participantEvidence, len(manifest.Participants)); err != nil {
		return err
	}
	if err := validateManifestBuildReferences(buildReferences); err != nil {
		return err
	}
	assessed := assessConsistency(participantEvidence.collections)
	if assessed.Status != manifest.Consistency.Status {
		return fmt.Errorf(
			"engine identity manifest consistency %q does not match participant evidence %q",
			manifest.Consistency.Status, assessed.Status,
		)
	}
	return nil
}

func validateManifestEnvelope(manifest Manifest) error {
	if manifest.SchemaVersion != constants.EngineInfoManifestSchemaVersion {
		return fmt.Errorf("unsupported engine identity manifest schema %d", manifest.SchemaVersion)
	}
	if manifest.RunID <= 0 {
		return fmt.Errorf("engine identity manifest run_id must be positive")
	}
	generatedAt, err := time.Parse(time.RFC3339, manifest.GeneratedAt)
	if err != nil || generatedAt.Location() != time.UTC {
		return fmt.Errorf("engine identity manifest generated_at must be UTC RFC 3339")
	}
	switch manifest.Consistency.Status {
	case ConsistencyConsistent, ConsistencyMismatch, ConsistencyIndeterminate:
	default:
		return fmt.Errorf("engine identity manifest consistency status is invalid")
	}
	if strings.TrimSpace(manifest.Consistency.Reason) == "" {
		return fmt.Errorf("engine identity manifest consistency reason is required")
	}
	if manifest.Consistency.Forced && manifest.Consistency.Status != ConsistencyMismatch {
		return fmt.Errorf("only a mismatched engine identity manifest may be forced")
	}
	if manifest.Builds == nil || manifest.Participants == nil {
		return fmt.Errorf("engine identity manifest builds and participants must be arrays")
	}
	return nil
}

func validateManifestBuilds(builds []ManifestBuild) (map[string]BuildInformation, map[string]int, error) {
	buildsByID := make(map[string]BuildInformation, len(builds))
	buildReferences := make(map[string]int, len(builds))
	priorBuildKey := ""
	for index, build := range builds {
		expectedID := canonicalBuildReference(index)
		if build.BuildID != expectedID {
			return nil, nil, fmt.Errorf("engine identity manifest build %d has noncanonical id %q", index+1, build.BuildID)
		}
		information := BuildInformation{
			SchemaVersion: constants.EngineBuildInfoSchemaVersion,
			Product:       build.Product,
			Version:       build.Version,
			Revision:      build.Revision,
			BuildTime:     build.BuildTime,
			Development:   build.Development,
			SourceDirty:   build.SourceDirty,
		}
		if reason := validateSchemaOne(schemaOneFromBuild(information), information.SourceDirty); reason != "" {
			return nil, nil, fmt.Errorf("engine identity manifest build %s is invalid: %s", build.BuildID, reason)
		}
		key := buildKey(information)
		if index > 0 && key <= priorBuildKey {
			return nil, nil, fmt.Errorf("engine identity manifest builds are not in canonical unique order")
		}
		priorBuildKey = key
		buildsByID[build.BuildID] = information
		buildReferences[build.BuildID] = 0
	}
	return buildsByID, buildReferences, nil
}

type manifestParticipantEvidence struct {
	collections     []CollectionResult
	entryCount      int
	standaloneCount int
}

type manifestParticipantOrder struct {
	nodeIDs       map[string]struct{}
	priorRoleRank int
	priorNodeID   string
}

func validateManifestParticipants(
	participants []ManifestParticipant,
	buildsByID map[string]BuildInformation,
	buildReferences map[string]int,
) (manifestParticipantEvidence, error) {
	if len(participants) == 0 {
		return manifestParticipantEvidence{}, fmt.Errorf("engine identity manifest must contain every planned participant")
	}
	evidence := manifestParticipantEvidence{collections: make([]CollectionResult, 0, len(participants))}
	order := manifestParticipantOrder{nodeIDs: make(map[string]struct{}, len(participants)), priorRoleRank: -1}
	for _, participant := range participants {
		topology, err := validateManifestParticipantIdentity(participant, &order)
		if err != nil {
			return manifestParticipantEvidence{}, err
		}
		switch topology {
		case topologyEntry:
			evidence.entryCount++
		case topologyStandalone:
			evidence.standaloneCount++
		}
		build, err := manifestParticipantBuild(participant, buildsByID, buildReferences)
		if err != nil {
			return manifestParticipantEvidence{}, err
		}
		collection, err := validateManifestParticipantEvidence(participant, build)
		if err != nil {
			return manifestParticipantEvidence{}, err
		}
		evidence.collections = append(evidence.collections, collection)
	}
	return evidence, nil
}

func validateManifestParticipantIdentity(
	participant ManifestParticipant,
	order *manifestParticipantOrder,
) (participantTopology, error) {
	if strings.TrimSpace(participant.NodeID) == "" {
		return 0, fmt.Errorf("engine identity manifest participant node_id is required")
	}
	roleRank, topology, ok := participantRolePolicy(participant.Role)
	if !ok {
		return 0, fmt.Errorf("engine identity manifest participant role is invalid")
	}
	host, port, err := net.SplitHostPort(participant.NodeID)
	if err != nil {
		return 0, fmt.Errorf("engine identity manifest participant node_id is invalid")
	}
	descriptor, err := NewParticipantDescriptor(&hostparse.HostInfo{Host: host}, port, participant.Role)
	if err != nil || descriptor.nodeID != participant.NodeID {
		return 0, fmt.Errorf("engine identity manifest participant node_id is not canonical")
	}
	if _, exists := order.nodeIDs[participant.NodeID]; exists {
		return 0, fmt.Errorf("engine identity manifest contains duplicate participant %q", participant.NodeID)
	}
	order.nodeIDs[participant.NodeID] = struct{}{}
	if roleRank < order.priorRoleRank || (roleRank == order.priorRoleRank && participant.NodeID <= order.priorNodeID) {
		return 0, fmt.Errorf("engine identity manifest participants are not in canonical order")
	}
	order.priorRoleRank = roleRank
	order.priorNodeID = participant.NodeID
	return topology, nil
}

func manifestParticipantBuild(
	participant ManifestParticipant,
	buildsByID map[string]BuildInformation,
	buildReferences map[string]int,
) (*BuildInformation, error) {
	var build *BuildInformation
	if participant.BuildID != "" {
		information, ok := buildsByID[participant.BuildID]
		if !ok {
			return nil, fmt.Errorf("engine identity manifest participant references unknown build %q", participant.BuildID)
		}
		cloned := cloneBuild(information)
		build = &cloned
		buildReferences[participant.BuildID]++
	}
	return build, nil
}

func validateManifestParticipantEvidence(
	participant ManifestParticipant,
	build *BuildInformation,
) (CollectionResult, error) {
	switch participant.CollectionStatus {
	case StatusCollected, StatusLegacyEndpointUnavailable, StatusUnsupportedSchema,
		StatusIncompleteBuildInfo, StatusCollectionFailed:
	default:
		return CollectionResult{}, fmt.Errorf("engine identity manifest participant collection status is invalid")
	}
	if participant.ConfiguredVersionHint != safeConfiguredVersionHint(participant.ConfiguredVersionHint) ||
		(participant.CollectionStatus != StatusLegacyEndpointUnavailable && participant.ConfiguredVersionHint != "") {
		return CollectionResult{}, fmt.Errorf("engine identity manifest participant configured version hint is invalid")
	}
	complete := false
	switch participant.CollectionStatus {
	case StatusCollected:
		if participant.ReportedSchemaVersion != constants.EngineBuildInfoSchemaVersion || build == nil || participant.Reason != "" {
			return CollectionResult{}, fmt.Errorf("collected engine identity manifest participant is incomplete")
		}
		if !completeBuildIdentity(*build) {
			return CollectionResult{}, fmt.Errorf("collected engine identity manifest participant has incomplete comparison fields")
		}
		complete = true
	case StatusIncompleteBuildInfo:
		if participant.ReportedSchemaVersion != constants.EngineBuildInfoSchemaVersion || build == nil {
			return CollectionResult{}, fmt.Errorf("incomplete engine identity manifest participant lacks its schema-one build")
		}
		if completeBuildIdentity(*build) {
			return CollectionResult{}, fmt.Errorf("incomplete engine identity manifest participant has complete comparison fields")
		}
	case StatusLegacyEndpointUnavailable:
		if participant.ReportedSchemaVersion != 0 || build != nil {
			return CollectionResult{}, fmt.Errorf("legacy engine identity manifest participant contains build evidence")
		}
	case StatusUnsupportedSchema:
		if participant.ReportedSchemaVersion <= constants.EngineBuildInfoSchemaVersion || build != nil {
			return CollectionResult{}, fmt.Errorf("unsupported engine identity manifest participant schema evidence is invalid")
		}
	case StatusCollectionFailed:
		if build != nil || participant.ReportedSchemaVersion < 0 ||
			participant.ReportedSchemaVersion > constants.EngineBuildInfoSchemaVersion {
			return CollectionResult{}, fmt.Errorf("failed engine identity manifest participant contains build evidence")
		}
	}
	if participant.CollectionStatus != StatusCollected && strings.TrimSpace(participant.Reason) == "" {
		return CollectionResult{}, fmt.Errorf("engine identity manifest participant collection reason is required")
	}
	return CollectionResult{
		Status: participant.CollectionStatus, Build: build, Complete: complete,
		ReportedSchemaVersion: participant.ReportedSchemaVersion, Reason: participant.Reason,
	}, nil
}

func validateManifestTopology(evidence manifestParticipantEvidence, participantCount int) error {
	if evidence.standaloneCount > 0 {
		if evidence.standaloneCount != 1 || participantCount != 1 {
			return fmt.Errorf("standalone engine identity manifest participant must be the only participant")
		}
	} else if evidence.entryCount != 1 {
		return fmt.Errorf("distributed engine identity manifest requires exactly one entry participant")
	}
	return nil
}

func validateManifestBuildReferences(buildReferences map[string]int) error {
	for buildID, count := range buildReferences {
		if count == 0 {
			return fmt.Errorf("engine identity manifest build %s is unreferenced", buildID)
		}
	}
	return nil
}

func completeBuildIdentity(build BuildInformation) bool {
	return build.Version != constants.EngineBuildInfoUnknown &&
		build.Revision != constants.EngineBuildInfoUnknown && build.SourceDirty != nil
}

func schemaOneFromBuild(build BuildInformation) schemaOneResponse {
	return schemaOneResponse{
		SchemaVersion: &build.SchemaVersion,
		Product:       &build.Product,
		Version:       &build.Version,
		Revision:      &build.Revision,
		BuildTime:     &build.BuildTime,
		Development:   &build.Development,
	}
}

// WriteManifestAtomic writes exactly one root manifest through an atomic rename.
func WriteManifestAtomic(root string, manifest Manifest) error {
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("results root is required")
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return fmt.Errorf("create results root: %w", err)
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode engine identity manifest: %w", err)
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(root, "."+constants.EngineInfoManifestName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary engine identity manifest: %w", err)
	}
	tmpPath := tmp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary engine identity manifest: %w", err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set engine identity manifest permissions: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temporary engine identity manifest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary engine identity manifest: %w", err)
	}
	path := filepath.Join(root, constants.EngineInfoManifestName)
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace engine identity manifest: %w", err)
	}
	removeTemp = false
	directory, err := os.Open(root) // #nosec G304 -- root is the selected results directory
	if err != nil {
		return fmt.Errorf("open results root for engine identity sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync engine identity manifest directory: %w", errors.Join(syncErr, closeErr))
	}
	return nil
}

// LoadManifest decodes and validates a stored run-level manifest.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- caller selects a local results artifact
	if err != nil {
		return nil, err
	}
	manifest := &Manifest{}
	if err := json.Unmarshal(data, manifest); err != nil {
		return nil, fmt.Errorf("decode engine identity manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return manifest, nil
}
