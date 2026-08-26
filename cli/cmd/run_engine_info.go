package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/tui"
)

var (
	evaluateRunEngineIdentityGate = engineinfo.EvaluateGate
	writeRunEngineIdentityAbort   = writeEngineIdentityAbortManifest
	runEngineIdentityNow          = time.Now
	localRunEnginePlan            = localRunEngineParticipants
	distributedRunEnginePlan      = distributedRunEngineParticipants
)

type runEngineIdentityGateOptions struct {
	force       bool
	verbose     bool
	autoResults bool
	resultsRoot string
	runID       int64
	metadata    *runMetadata
	descriptors func() ([]engineinfo.ParticipantDescriptor, error)
}

func newRunEngineIdentityPreSubmissionCheck(
	collector engineinfo.FleetCollector,
	options runEngineIdentityGateOptions,
) func(context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		descriptors, err := options.descriptors()
		if err != nil {
			return nil, fmt.Errorf("freeze engine participant plan: %w", err)
		}
		outcome, gateErr := evaluateRunEngineIdentityGate(ctx, collector, descriptors, options.force)
		if options.metadata != nil {
			options.metadata.engineIdentity = &outcome
		}
		lines := engineIdentityGateLines(outcome, options.verbose, gateErr)
		if gateErr == nil {
			return lines, nil
		}
		if options.autoResults {
			persistErr := writeRunEngineIdentityAbort(options.resultsRoot, options.runID, outcome)
			if persistErr != nil {
				gateErr = errors.Join(gateErr, fmt.Errorf("preserve engine identity rejection evidence: %w", persistErr))
			}
		}
		return lines, gateErr
	}
}

func engineIdentityGateLines(
	outcome engineinfo.GateOutcome,
	verbose bool,
	gateErr error,
) []string {
	lines := outcome.Fleet.OutputLines(verbose)
	switch {
	case outcome.Fleet.Consistency.Forced:
		lines = append(lines,
			"WARNING: ENGINE BUILD MISMATCH FORCED; performance results combine different engine builds.")
	case outcome.Decision == engineinfo.GateRejectedMismatch:
		lines = append(lines, "ERROR: Engine build mismatch rejected before scenario submission.")
	case outcome.Decision == engineinfo.GateCollectionFailure:
		lines = append(lines, "ERROR: Engine identity collection failed; scenario submission blocked.")
	case outcome.Fleet.Consistency.Status == engineinfo.ConsistencyIndeterminate:
		lines = append(lines, "WARNING: Engine identity is indeterminate: "+indeterminateParticipantSummary(outcome.Fleet))
	case gateErr != nil:
		lines = append(lines, "ERROR: Engine identity gate failed before scenario submission.")
	}
	return lines
}

func indeterminateParticipantSummary(fleet engineinfo.FleetResult) string {
	affected := make([]string, 0)
	for _, participant := range fleet.Participants {
		if participant.CollectionStatus == engineinfo.StatusCollected {
			continue
		}
		detail := participant.NodeID + "=" + string(participant.CollectionStatus)
		if participant.Reason != "" {
			detail += " (" + participant.Reason + ")"
		}
		affected = append(affected, detail)
	}
	if len(affected) == 0 {
		return fleet.Consistency.Reason
	}
	return strings.Join(affected, ", ")
}

func localRunEngineParticipants(apiPort string) ([]engineinfo.ParticipantDescriptor, error) {
	descriptor, err := engineinfo.NewParticipantDescriptor(
		&hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
		apiPort,
		engineinfo.RoleStandalone,
	)
	if err != nil {
		return nil, err
	}
	return []engineinfo.ParticipantDescriptor{descriptor}, nil
}

func distributedRunEngineParticipants(
	orchestrator *tui.MultiHostOrchestrator,
	apiPort string,
	entryOnly bool,
) ([]engineinfo.ParticipantDescriptor, error) {
	if orchestrator == nil {
		return nil, fmt.Errorf("multi-host orchestrator is required")
	}
	// Freeze the execution plan once after readiness. Collection results never
	// select or remove participants from this descriptor set.
	readyHosts := append([]*tui.HostConnection(nil), orchestrator.GetReadyHosts()...)
	lockedHosts, locked, err := orchestrator.GetExecutionParticipantHosts()
	if err != nil {
		return nil, err
	}
	if locked {
		readyHosts = lockedHosts
	}
	readyHosts = engineExecutionHosts(readyHosts, entryOnly)
	return engineParticipantsFromReadyHosts(readyHosts, apiPort)
}

func engineExecutionHosts(readyHosts []*tui.HostConnection, entryOnly bool) []*tui.HostConnection {
	// LIST currently executes only on the entry. Its other ready containers are
	// UI-visible API peers, not engine participants contributing work or
	// coordination to the run.
	if entryOnly && len(readyHosts) > 1 {
		return readyHosts[:1]
	}
	return readyHosts
}

func engineParticipantsFromReadyHosts(
	readyHosts []*tui.HostConnection,
	apiPort string,
) ([]engineinfo.ParticipantDescriptor, error) {
	if len(readyHosts) == 0 {
		return nil, fmt.Errorf("engine execution participant plan is empty")
	}
	descriptors := make([]engineinfo.ParticipantDescriptor, 0, len(readyHosts))
	for index, participant := range readyHosts {
		role := engineinfo.RoleWorker
		if index == 0 {
			role = engineinfo.RoleEntry
		}
		descriptor, err := engineinfo.NewParticipantDescriptor(participant.Info, apiPort, role)
		if err != nil {
			return nil, fmt.Errorf("participant %d: %w", index, err)
		}
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, nil
}

type engineInfoAbortManifest struct {
	SchemaVersion int                             `json:"schema_version"`
	RunID         int64                           `json:"run_id"`
	GeneratedAt   string                          `json:"generated_at"`
	Consistency   engineInfoManifestConsistency   `json:"consistency"`
	Builds        []engineInfoManifestBuild       `json:"builds"`
	Participants  []engineInfoManifestParticipant `json:"participants"`
}

type engineInfoManifestConsistency struct {
	Status engineinfo.ConsistencyStatus `json:"status"`
	Forced bool                         `json:"forced"`
	Reason string                       `json:"reason"`
}

type engineInfoManifestBuild struct {
	BuildID     string `json:"build_id"`
	Product     string `json:"product"`
	Version     string `json:"version"`
	Revision    string `json:"revision"`
	BuildTime   string `json:"build_time"`
	Development bool   `json:"development"`
	SourceDirty *bool  `json:"source_dirty"`
}

type engineInfoManifestParticipant struct {
	NodeID                string                      `json:"node_id"`
	Role                  engineinfo.ParticipantRole  `json:"role"`
	CollectionStatus      engineinfo.CollectionStatus `json:"collection_status"`
	ReportedSchemaVersion int                         `json:"reported_schema_version,omitempty"`
	BuildID               string                      `json:"build_id,omitempty"`
	Reason                string                      `json:"reason,omitempty"`
}

func writeEngineIdentityAbortManifest(
	root string,
	runID int64,
	outcome engineinfo.GateOutcome,
) error {
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("results root is required")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return fmt.Errorf("create results root: %w", err)
	}
	reason := outcome.Fleet.Consistency.Reason
	switch outcome.Decision {
	case engineinfo.GateRejectedMismatch:
		reason = "engine build identity mismatch rejected before scenario submission"
	case engineinfo.GateCollectionFailure:
		reason = "engine identity collection failed before scenario submission"
	}
	manifest := engineInfoAbortManifest{
		SchemaVersion: constants.EngineInfoManifestSchemaVersion,
		RunID:         runID,
		GeneratedAt:   runEngineIdentityNow().UTC().Format(time.RFC3339),
		Consistency: engineInfoManifestConsistency{
			Status: outcome.Fleet.Consistency.Status,
			Forced: outcome.Fleet.Consistency.Forced,
			Reason: reason,
		},
		Builds:       make([]engineInfoManifestBuild, 0, len(outcome.Fleet.Builds)),
		Participants: make([]engineInfoManifestParticipant, 0, len(outcome.Fleet.Participants)),
	}
	for _, build := range outcome.Fleet.Builds {
		manifest.Builds = append(manifest.Builds, engineInfoManifestBuild{
			BuildID: build.BuildID, Product: build.Information.Product,
			Version: build.Information.Version, Revision: build.Information.Revision,
			BuildTime: build.Information.BuildTime, Development: build.Information.Development,
			SourceDirty: build.Information.SourceDirty,
		})
	}
	for _, participant := range outcome.Fleet.Participants {
		manifest.Participants = append(manifest.Participants, engineInfoManifestParticipant{
			NodeID: participant.NodeID, Role: participant.Role,
			CollectionStatus:      participant.CollectionStatus,
			ReportedSchemaVersion: participant.ReportedSchemaVersion,
			BuildID:               participant.BuildID, Reason: participant.Reason,
		})
	}
	// The collector already returns deterministic order; sorting here protects
	// partial evidence supplied by a future alternative collector.
	sort.SliceStable(manifest.Participants, func(i, j int) bool {
		if manifest.Participants[i].Role != manifest.Participants[j].Role {
			return manifest.Participants[i].Role == engineinfo.RoleEntry
		}
		return manifest.Participants[i].NodeID < manifest.Participants[j].NodeID
	})
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode engine identity manifest: %w", err)
	}
	data = append(data, '\n')
	return writeAtomic(filepath.Join(root, constants.EngineInfoManifestName), data, 0o644)
}
