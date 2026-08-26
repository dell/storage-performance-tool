package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/tui"
)

var (
	evaluateRunEngineIdentityGate = engineinfo.EvaluateGate
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
			persistErr := persistRejectedEngineIdentity(options.metadata, options.resultsRoot, options.runID, outcome, gateErr)
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
