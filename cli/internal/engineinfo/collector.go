package engineinfo

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// ParticipantRole is the topology role assigned by the orchestration plan.
type ParticipantRole string

// Supported participant roles.
const (
	RoleEntry      ParticipantRole = "entry"
	RoleWorker     ParticipantRole = "worker"
	RoleStandalone ParticipantRole = "standalone"
)

// ParticipantDescriptor is a validated planned engine participant.
type ParticipantDescriptor struct {
	nodeID  string
	baseURL string
	role    ParticipantRole
}

// NewParticipantDescriptor derives a public node identity and endpoint from
// the plan's host and control port. SSH and original input details are ignored.
func NewParticipantDescriptor(host *hostparse.HostInfo, controlPort string, role ParticipantRole) (ParticipantDescriptor, error) {
	if host == nil {
		return ParticipantDescriptor{}, fmt.Errorf("engine participant host is required")
	}
	normalizedHost := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host.Host), "."))
	normalizedHost = strings.TrimPrefix(strings.TrimSuffix(normalizedHost, "]"), "[")
	if err := hostparse.ValidateHost(normalizedHost); err != nil {
		return ParticipantDescriptor{}, fmt.Errorf("engine participant host is invalid: %w", err)
	}
	if address := net.ParseIP(normalizedHost); address != nil {
		normalizedHost = address.String()
	}
	port, err := strconv.Atoi(strings.TrimSpace(controlPort))
	if err != nil || port < 1 || port > 65535 {
		return ParticipantDescriptor{}, fmt.Errorf("engine participant control port is invalid")
	}
	if _, _, supported := participantRolePolicy(role); !supported {
		return ParticipantDescriptor{}, fmt.Errorf("engine participant role is unsupported")
	}
	nodeID := net.JoinHostPort(normalizedHost, strconv.Itoa(port))
	return ParticipantDescriptor{
		nodeID:  nodeID,
		baseURL: "http://" + nodeID,
		role:    role,
	}, nil
}

// VersionClient is the participant-level Engine Build Information seam.
type VersionClient interface {
	Fetch(context.Context, string) (CollectionResult, error)
}

// Collector collects and assesses Engine Build Information for a planned fleet.
type Collector struct {
	client      VersionClient
	maxParallel int
}

// NewCollector returns a fleet collector using the supplied version client.
func NewCollector(client VersionClient) *Collector {
	return NewCollectorWithOptions(client, CollectorOptions{})
}

// CollectorOptions configures bounded fleet collection.
type CollectorOptions struct {
	MaxParallel int
}

// NewCollectorWithOptions returns a fleet collector with explicit bounds.
func NewCollectorWithOptions(client VersionClient, options CollectorOptions) *Collector {
	maxParallel := options.MaxParallel
	if maxParallel <= 0 {
		maxParallel = constants.EngineInfoCollectionParallelism
	}
	return &Collector{client: client, maxParallel: maxParallel}
}

// ParticipantResult is the safe collected evidence for one planned participant.
type ParticipantResult struct {
	NodeID                string
	Role                  ParticipantRole
	CollectionStatus      CollectionStatus
	ReportedSchemaVersion int
	BuildID               string
	Reason                string
	Attempts              int
}

// GroupedBuild is one document-local group of identical full build records.
type GroupedBuild struct {
	BuildID     string
	Information BuildInformation
}

// ConsistencyStatus is the fleet's Engine Identity Consistency assessment.
type ConsistencyStatus string

// Supported consistency assessments.
const (
	ConsistencyConsistent    ConsistencyStatus = "consistent"
	ConsistencyMismatch      ConsistencyStatus = "mismatch"
	ConsistencyIndeterminate ConsistencyStatus = "indeterminate"
)

// ConsistencyAssessment explains the fleet-level result.
type ConsistencyAssessment struct {
	Status ConsistencyStatus
	Reason string
}

// FleetResult is a deterministic fleet collection and assessment.
type FleetResult struct {
	Participants []ParticipantResult
	Builds       []GroupedBuild
	Consistency  ConsistencyAssessment
}

// OutputLines returns one fleet-level line plus verbose participant detail.
// Both headless and TUI adapters can consume the same already-sanitized lines.
func (result FleetResult) OutputLines(verbose bool) []string {
	lines := []string{fmt.Sprintf("Engine identity: %s (%d participants, %d build records)",
		result.Consistency.Status, len(result.Participants), len(result.Builds))}
	if !verbose {
		return lines
	}
	for _, participant := range result.Participants {
		attempts := participant.Attempts
		retries := max(0, attempts-1)
		detail := fmt.Sprintf("Engine participant %s role=%s status=%s",
			participant.NodeID, participant.Role, participant.CollectionStatus)
		if participant.ReportedSchemaVersion > 0 {
			detail += fmt.Sprintf(" schema=%d", participant.ReportedSchemaVersion)
		}
		if participant.BuildID != "" {
			detail += " build=" + participant.BuildID
		}
		if attempts > 0 {
			detail += fmt.Sprintf(" attempts=%d retries=%d", attempts, retries)
		}
		if participant.Reason != "" {
			detail += " reason=" + participant.Reason
		}
		lines = append(lines, detail)
	}
	return lines
}

// Collect queries the planned participants and returns their safe evidence.
func (c *Collector) Collect(ctx context.Context, descriptors []ParticipantDescriptor) (FleetResult, error) {
	if c == nil || c.client == nil {
		return FleetResult{}, fmt.Errorf("engine version client is required")
	}
	if err := validateTopology(descriptors); err != nil {
		return FleetResult{}, err
	}
	ordered := append([]ParticipantDescriptor(nil), descriptors...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].role != ordered[j].role {
			leftRank, _, _ := participantRolePolicy(ordered[i].role)
			rightRank, _, _ := participantRolePolicy(ordered[j].role)
			return leftRank < rightRank
		}
		return ordered[i].nodeID < ordered[j].nodeID
	})
	collections := make([]CollectionResult, len(ordered))
	attempted := make([]bool, len(ordered))
	failed := make([]bool, len(ordered))
	jobs := make(chan int, len(ordered))
	for index := range ordered {
		jobs <- index
	}
	close(jobs)
	workerCount := min(c.maxParallel, len(ordered))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				index, ok := <-jobs
				if !ok {
					return
				}
				attempted[index] = true
				collection, err := c.client.Fetch(ctx, ordered[index].baseURL)
				if collection.Status == "" {
					collection = CollectionResult{Status: StatusCollectionFailed, Reason: "engine version collection failed"}
				}
				collections[index] = collection
				failed[index] = err != nil || collection.Status == StatusCollectionFailed
			}
		}()
	}
	workers.Wait()

	failureCount := 0
	participants := make([]ParticipantResult, 0, len(ordered))
	for index, descriptor := range ordered {
		collection := collections[index]
		if !attempted[index] {
			collection = CollectionResult{Status: StatusCollectionFailed, Reason: "engine version collection canceled"}
			collections[index] = collection
			failed[index] = true
		}
		if failed[index] {
			failureCount++
		}
		participants = append(participants, ParticipantResult{
			NodeID:                descriptor.nodeID,
			Role:                  descriptor.role,
			CollectionStatus:      collection.Status,
			ReportedSchemaVersion: collection.ReportedSchemaVersion,
			Reason:                collection.Reason,
			Attempts:              collection.Attempts,
		})
	}
	builds, buildIDs := groupBuilds(collections)
	for index, collection := range collections {
		if collection.Build != nil {
			participants[index].BuildID = buildIDs[buildKey(*collection.Build)]
		}
	}
	result := FleetResult{
		Participants: participants,
		Builds:       builds,
		Consistency:  assessConsistency(collections),
	}
	if ctx.Err() != nil {
		return result, fmt.Errorf("engine build information collection canceled: %w", ctx.Err())
	}
	if failureCount > 0 {
		return result, fmt.Errorf("engine build information collection failed for %d participant(s)", failureCount)
	}
	return result, nil
}

func validateTopology(descriptors []ParticipantDescriptor) error {
	if len(descriptors) == 0 {
		return fmt.Errorf("engine participant plan is empty")
	}
	entries := 0
	standalones := 0
	nodes := make(map[string]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		if _, exists := nodes[descriptor.nodeID]; exists {
			return fmt.Errorf("engine participant plan contains duplicate node %s", descriptor.nodeID)
		}
		nodes[descriptor.nodeID] = struct{}{}
		_, topology, supported := participantRolePolicy(descriptor.role)
		if !supported {
			return fmt.Errorf("engine participant plan contains unsupported role")
		}
		switch topology {
		case topologyEntry:
			entries++
		case topologyWorker:
		case topologyStandalone:
			standalones++
		}
	}
	if standalones > 0 {
		if standalones != 1 || len(descriptors) != 1 {
			return fmt.Errorf("standalone engine participant must be the only planned participant")
		}
		return nil
	}
	if entries != 1 {
		return fmt.Errorf("distributed engine participant plan requires exactly one entry")
	}
	return nil
}

type participantTopology uint8

const (
	topologyEntry participantTopology = iota
	topologyWorker
	topologyStandalone
)

func participantRolePolicy(role ParticipantRole) (int, participantTopology, bool) {
	switch role {
	case RoleEntry:
		return 0, topologyEntry, true
	case RoleWorker:
		return 1, topologyWorker, true
	case RoleStandalone:
		return 0, topologyStandalone, true
	default:
		return 0, 0, false
	}
}

func groupBuilds(collections []CollectionResult) ([]GroupedBuild, map[string]string) {
	byKey := make(map[string]BuildInformation)
	for _, collection := range collections {
		if collection.Build != nil {
			byKey[buildKey(*collection.Build)] = cloneBuild(*collection.Build)
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	builds := make([]GroupedBuild, 0, len(keys))
	ids := make(map[string]string, len(keys))
	for index, key := range keys {
		buildID := fmt.Sprintf("build-%d", index+1)
		ids[key] = buildID
		builds = append(builds, GroupedBuild{BuildID: buildID, Information: byKey[key]})
	}
	return builds, ids
}

func cloneBuild(build BuildInformation) BuildInformation {
	if build.SourceDirty != nil {
		dirty := *build.SourceDirty
		build.SourceDirty = &dirty
	}
	return build
}

func buildKey(build BuildInformation) string {
	dirty := "null"
	if build.SourceDirty != nil {
		dirty = strconv.FormatBool(*build.SourceDirty)
	}
	return fmt.Sprintf("%d\x00%s\x00%s\x00%s\x00%s\x00%t\x00%s",
		build.SchemaVersion, build.Product, build.Version, build.Revision,
		build.BuildTime, build.Development, dirty)
}

func assessConsistency(collections []CollectionResult) ConsistencyAssessment {
	if len(collections) == 0 {
		return ConsistencyAssessment{Status: ConsistencyIndeterminate, Reason: "no engine participants were collected"}
	}
	for left := 0; left < len(collections); left++ {
		if collections[left].Build == nil {
			continue
		}
		for right := left + 1; right < len(collections); right++ {
			if collections[right].Build != nil && knownComparisonDifference(*collections[left].Build, *collections[right].Build) {
				return ConsistencyAssessment{Status: ConsistencyMismatch, Reason: "participants reported different engine build identity fields"}
			}
		}
	}
	allComplete := true
	for _, collection := range collections {
		if !collection.Complete || collection.Build == nil {
			allComplete = false
		}
	}
	if allComplete {
		return ConsistencyAssessment{
			Status: ConsistencyConsistent,
			Reason: fmt.Sprintf("all %d participants reported consistent engine build identity fields", len(collections)),
		}
	}
	return ConsistencyAssessment{Status: ConsistencyIndeterminate, Reason: "one or more participants lack complete engine build information"}
}

func knownComparisonDifference(left, right BuildInformation) bool {
	return left.Product != right.Product ||
		knownStringDifference(left.Version, right.Version) ||
		knownStringDifference(left.Revision, right.Revision) ||
		left.Development != right.Development ||
		(left.SourceDirty != nil && right.SourceDirty != nil && *left.SourceDirty != *right.SourceDirty)
}

func knownStringDifference(left, right string) bool {
	return left != constants.EngineBuildInfoUnknown && right != constants.EngineBuildInfoUnknown && left != right
}
