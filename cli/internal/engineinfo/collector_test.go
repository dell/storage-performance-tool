package engineinfo_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestCollectorUsesPlannedRoleAndSanitizedHostPortIdentity(t *testing.T) {
	host := &hostparse.HostInfo{
		User:     "secret-user",
		Host:     "Worker.EXAMPLE.",
		Original: "secret-user@Worker.EXAMPLE.",
	}
	descriptor, err := engineinfo.NewParticipantDescriptor(host, "09999", engineinfo.RoleStandalone)
	if err != nil {
		t.Fatalf("NewParticipantDescriptor() error = %v", err)
	}

	client := &capturingVersionClient{result: collectedBuild(
		"2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false))}
	result, err := engineinfo.NewCollector(client).Collect(context.Background(), []engineinfo.ParticipantDescriptor{descriptor})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(result.Participants) != 1 {
		t.Fatalf("participants = %d, want 1", len(result.Participants))
	}
	participant := result.Participants[0]
	if participant.NodeID != "worker.example:9999" {
		t.Fatalf("node ID = %q, want sanitized host and port", participant.NodeID)
	}
	if participant.Role != engineinfo.RoleStandalone {
		t.Fatalf("role = %q, want standalone from plan", participant.Role)
	}
	if client.baseURL != "http://worker.example:9999" {
		t.Fatalf("client base URL = %q, want sanitized endpoint", client.baseURL)
	}
	assertExcludes(t, client.baseURL, host.User, host.Original)
}

func TestParticipantDescriptorCanonicalizesIPv6AndRejectsUnsafeEndpointInputs(t *testing.T) {
	ipv6, err := engineinfo.NewParticipantDescriptor(&hostparse.HostInfo{
		Host: "[2001:0DB8:0:0:0:0:0:1]",
	}, "9999", engineinfo.RoleEntry)
	if err != nil {
		t.Fatalf("NewParticipantDescriptor(IPv6) error = %v", err)
	}
	result, err := engineinfo.NewCollector(staticVersionClient{result: collectedBuild(
		"2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false)),
	}).Collect(context.Background(), []engineinfo.ParticipantDescriptor{ipv6})
	if err != nil {
		t.Fatalf("Collect(IPv6) error = %v", err)
	}
	if got := result.Participants[0].NodeID; got != "[2001:db8::1]:9999" {
		t.Fatalf("IPv6 node ID = %q, want canonical host and control port", got)
	}

	tests := []struct {
		name string
		host string
		port string
		role engineinfo.ParticipantRole
	}{
		{name: "SSH user", host: "secret-user@node.example", port: "9999", role: engineinfo.RoleWorker},
		{name: "credential URL", host: "secret-user:password@node.example", port: "9999", role: engineinfo.RoleWorker},
		{name: "filesystem path", host: "/private/node", port: "9999", role: engineinfo.RoleWorker},
		{name: "port suffix", host: "node.example", port: "9999/private", role: engineinfo.RoleWorker},
		{name: "unsupported role", host: "node.example", port: "9999", role: engineinfo.ParticipantRole("container-123")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := engineinfo.NewParticipantDescriptor(&hostparse.HostInfo{Host: test.host}, test.port, test.role)
			if err == nil {
				t.Fatal("NewParticipantDescriptor() error = nil, want unsafe input rejection")
			}
		})
	}
}

func TestCollectorOrdersSupportedTopologyShapesFromPlan(t *testing.T) {
	client := staticVersionClient{result: collectedBuild(
		"2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false))}
	tests := []struct {
		name        string
		descriptors []engineinfo.ParticipantDescriptor
		want        []string
	}{
		{
			name:        "local standalone",
			descriptors: []engineinfo.ParticipantDescriptor{descriptor(t, "localhost", "9999", engineinfo.RoleStandalone)},
			want:        []string{"127.0.0.1:9999=standalone"},
		},
		{
			name:        "single remote entry",
			descriptors: []engineinfo.ParticipantDescriptor{descriptor(t, "entry.example", "9999", engineinfo.RoleEntry)},
			want:        []string{"entry.example:9999=entry"},
		},
		{
			name: "multi host",
			descriptors: []engineinfo.ParticipantDescriptor{
				descriptor(t, "worker-z.example", "9999", engineinfo.RoleWorker),
				descriptor(t, "entry.example", "9999", engineinfo.RoleEntry),
				descriptor(t, "worker-a.example", "9999", engineinfo.RoleWorker),
			},
			want: []string{
				"entry.example:9999=entry",
				"worker-a.example:9999=worker",
				"worker-z.example:9999=worker",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := engineinfo.NewCollector(client).Collect(context.Background(), test.descriptors)
			if err != nil {
				t.Fatalf("Collect() error = %v", err)
			}
			got := make([]string, 0, len(result.Participants))
			for _, participant := range result.Participants {
				got = append(got, participant.NodeID+"="+string(participant.Role))
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("participants = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCollectorRejectsInvalidPlannedTopologiesBeforeCollection(t *testing.T) {
	standalone := descriptor(t, "localhost", "9999", engineinfo.RoleStandalone)
	entry := descriptor(t, "entry.example", "9999", engineinfo.RoleEntry)
	worker := descriptor(t, "worker.example", "9999", engineinfo.RoleWorker)
	tests := []struct {
		name        string
		descriptors []engineinfo.ParticipantDescriptor
	}{
		{name: "empty plan"},
		{name: "worker without entry", descriptors: []engineinfo.ParticipantDescriptor{worker}},
		{name: "standalone mixed with distributed", descriptors: []engineinfo.ParticipantDescriptor{standalone, entry}},
		{name: "multiple entries", descriptors: []engineinfo.ParticipantDescriptor{entry, descriptor(t, "entry-2.example", "9999", engineinfo.RoleEntry)}},
		{name: "duplicate node", descriptors: []engineinfo.ParticipantDescriptor{entry, worker, worker}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := engineinfo.NewCollector(staticVersionClient{result: collectedBuild(
				"2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false)),
			}).Collect(context.Background(), test.descriptors)
			if err == nil {
				t.Fatal("Collect() error = nil, want invalid topology rejection")
			}
		})
	}
}

func TestCollectorRejectsMissingVersionClient(t *testing.T) {
	standalone := []engineinfo.ParticipantDescriptor{descriptor(t, "localhost", "9999", engineinfo.RoleStandalone)}
	for _, collector := range []*engineinfo.Collector{nil, engineinfo.NewCollector(nil)} {
		if _, err := collector.Collect(context.Background(), standalone); err == nil {
			t.Fatal("Collect() error = nil, want missing client rejection")
		}
	}
}

func TestCollectorNormalizesInvalidEmptyClientOutcome(t *testing.T) {
	result, err := engineinfo.NewCollector(staticVersionClient{}).Collect(context.Background(), []engineinfo.ParticipantDescriptor{
		descriptor(t, "localhost", "9999", engineinfo.RoleStandalone),
	})
	if err == nil {
		t.Fatal("Collect() error = nil, want invalid client outcome rejection")
	}
	if len(result.Participants) != 1 || result.Participants[0].CollectionStatus != engineinfo.StatusCollectionFailed ||
		result.Participants[0].Reason != "engine version collection failed" {
		t.Fatalf("participants = %+v, want normalized safe collection failure", result.Participants)
	}
}

func TestCollectorGroupsFullBuildRecordsAndAssessesTimestampOnlyDifferenceAsConsistent(t *testing.T) {
	descriptors := []engineinfo.ParticipantDescriptor{
		descriptor(t, "worker.example", "9999", engineinfo.RoleWorker),
		descriptor(t, "entry.example", "9999", engineinfo.RoleEntry),
		descriptor(t, "worker-2.example", "9999", engineinfo.RoleWorker),
	}
	client := mappedVersionClient{results: map[string]engineinfo.CollectionResult{
		"http://entry.example:9999":    collectedBuild("2026-08-26T13:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false)),
		"http://worker.example:9999":   collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false)),
		"http://worker-2.example:9999": collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false)),
	}}

	result, err := engineinfo.NewCollector(client).Collect(context.Background(), descriptors)
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if result.Consistency.Status != engineinfo.ConsistencyConsistent {
		t.Fatalf("consistency = %q, want consistent", result.Consistency.Status)
	}
	if len(result.Builds) != 2 {
		t.Fatalf("build groups = %d, want 2", len(result.Builds))
	}
	if result.Builds[0].BuildID != "build-1" || result.Builds[0].Information.BuildTime != "2026-08-26T12:34:56Z" ||
		result.Builds[1].BuildID != "build-2" || result.Builds[1].Information.BuildTime != "2026-08-26T13:34:56Z" {
		t.Fatalf("builds = %+v, want canonical timestamp order with document-local IDs", result.Builds)
	}
	wantParticipants := []engineinfo.ParticipantResult{
		{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-2"},
		{NodeID: "worker-2.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1"},
		{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1"},
	}
	if !reflect.DeepEqual(result.Participants, wantParticipants) {
		t.Fatalf("participants = %+v, want %+v", result.Participants, wantParticipants)
	}
}

func TestCollectorAppliesExactConsistencyComparisonAndPrecedence(t *testing.T) {
	complete := collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false))
	tests := []struct {
		name    string
		results []engineinfo.CollectionResult
		want    engineinfo.ConsistencyStatus
	}{
		{name: "single complete participant", results: []engineinfo.CollectionResult{complete}, want: engineinfo.ConsistencyConsistent},
		{name: "single incomplete participant", results: []engineinfo.CollectionResult{
			incompleteBuild("5.14.2", "unknown", false, boolPointer(false)),
		}, want: engineinfo.ConsistencyIndeterminate},
		{name: "unknown build time is informational", results: []engineinfo.CollectionResult{
			complete,
			collectedBuild("unknown", "5.14.2", testRevisionA, false, boolPointer(false)),
		}, want: engineinfo.ConsistencyConsistent},
		{name: "version difference", results: []engineinfo.CollectionResult{
			complete,
			collectedBuild("2026-08-26T12:34:56Z", "5.14.3", testRevisionA, false, boolPointer(false)),
		}, want: engineinfo.ConsistencyMismatch},
		{name: "product difference", results: []engineinfo.CollectionResult{
			complete,
			withProduct(collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false)), "other-engine"),
		}, want: engineinfo.ConsistencyMismatch},
		{name: "revision difference", results: []engineinfo.CollectionResult{
			complete,
			collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionB, false, boolPointer(false)),
		}, want: engineinfo.ConsistencyMismatch},
		{name: "development difference", results: []engineinfo.CollectionResult{
			complete,
			collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, true, boolPointer(false)),
		}, want: engineinfo.ConsistencyMismatch},
		{name: "dirty state difference", results: []engineinfo.CollectionResult{
			complete,
			collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(true)),
		}, want: engineinfo.ConsistencyMismatch},
		{name: "known mismatch takes precedence over legacy absence", results: []engineinfo.CollectionResult{
			complete,
			collectedBuild("2026-08-26T12:34:56Z", "5.14.3", testRevisionA, false, boolPointer(false)),
			{Status: engineinfo.StatusLegacyEndpointUnavailable, Reason: "engine version endpoint is unavailable on this engine"},
		}, want: engineinfo.ConsistencyMismatch},
		{name: "known revision mismatch takes precedence over unknown dirty state", results: []engineinfo.CollectionResult{
			complete,
			incompleteBuild("5.14.2", testRevisionB, false, nil),
		}, want: engineinfo.ConsistencyMismatch},
		{name: "unknown revision prevents equality proof", results: []engineinfo.CollectionResult{
			complete,
			incompleteBuild("5.14.2", "unknown", false, boolPointer(false)),
		}, want: engineinfo.ConsistencyIndeterminate},
		{name: "unknown version prevents equality proof", results: []engineinfo.CollectionResult{
			complete,
			incompleteBuild("unknown", testRevisionA, false, boolPointer(false)),
		}, want: engineinfo.ConsistencyIndeterminate},
		{name: "unknown dirty state prevents equality proof", results: []engineinfo.CollectionResult{
			complete,
			incompleteBuild("5.14.2", testRevisionA, false, nil),
		}, want: engineinfo.ConsistencyIndeterminate},
		{name: "unsupported schema is indeterminate", results: []engineinfo.CollectionResult{
			complete,
			{Status: engineinfo.StatusUnsupportedSchema, ReportedSchemaVersion: 2, Reason: "engine version schema is newer than this CLI supports"},
		}, want: engineinfo.ConsistencyIndeterminate},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			descriptors := make([]engineinfo.ParticipantDescriptor, 0, len(test.results))
			results := make(map[string]engineinfo.CollectionResult, len(test.results))
			for index, collection := range test.results {
				host := fmt.Sprintf("node-%02d.example", index)
				role := engineinfo.RoleWorker
				if index == 0 {
					role = engineinfo.RoleEntry
				}
				descriptors = append(descriptors, descriptor(t, host, "9999", role))
				results["http://"+host+":9999"] = collection
			}
			result, err := engineinfo.NewCollector(mappedVersionClient{results: results}).Collect(context.Background(), descriptors)
			if err != nil {
				t.Fatalf("Collect() error = %v", err)
			}
			if result.Consistency.Status != test.want {
				t.Fatalf("consistency = %q (%s), want %q", result.Consistency.Status, result.Consistency.Reason, test.want)
			}
		})
	}
}

func TestCollectorBoundsParallelRequestsAcrossManyWorkers(t *testing.T) {
	const (
		participantCount = 65
		parallelism      = constants.EngineInfoCollectionParallelism
	)
	transport := newBlockingVersionTransport()
	client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
		HTTPClient:      &http.Client{Transport: transport},
		RequestAttempts: 1,
		RequestTimeout:  5 * time.Second,
	})
	descriptors := plannedFleet(t, participantCount)
	done := make(chan struct{})
	var (
		result engineinfo.FleetResult
		err    error
	)
	go func() {
		result, err = engineinfo.NewCollector(client).Collect(context.Background(), descriptors)
		close(done)
	}()

	for index := 0; index < parallelism; index++ {
		select {
		case <-transport.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for bounded requests to start")
		}
	}
	select {
	case unexpected := <-transport.started:
		t.Fatalf("request %q exceeded parallelism bound %d", unexpected, parallelism)
	case <-time.After(50 * time.Millisecond):
	}
	close(transport.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("collection did not complete after releasing requests")
	}
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if got := transport.maxActive.Load(); got != parallelism {
		t.Fatalf("maximum active requests = %d, want %d", got, parallelism)
	}
	if got := transport.calls.Load(); got != participantCount {
		t.Fatalf("request calls = %d, want %d", got, participantCount)
	}
	if len(result.Participants) != participantCount {
		t.Fatalf("participants = %d, want %d", len(result.Participants), participantCount)
	}
}

func TestCollectorResultIsIndependentOfResponseCompletionOrder(t *testing.T) {
	descriptors := plannedFleet(t, 17)
	collect := func(reverse bool) engineinfo.FleetResult {
		t.Helper()
		client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
			HTTPClient:      &http.Client{Transport: delayedVersionTransport{reverse: reverse}},
			RequestAttempts: 1,
			RequestTimeout:  time.Second,
		})
		result, err := engineinfo.NewCollectorWithOptions(client, engineinfo.CollectorOptions{MaxParallel: 5}).Collect(
			context.Background(), descriptors)
		if err != nil {
			t.Fatalf("Collect(reverse=%t) error = %v", reverse, err)
		}
		return result
	}

	forward := collect(false)
	reverse := collect(true)
	if !reflect.DeepEqual(forward, reverse) {
		t.Fatalf("fleet result changed with response completion order:\nforward=%+v\nreverse=%+v", forward, reverse)
	}
	if len(forward.Builds) != 2 || forward.Consistency.Status != engineinfo.ConsistencyConsistent {
		t.Fatalf("fleet result = %+v, want two timestamp groups and consistent identity", forward)
	}
}

func TestCollectorCancellationStopsAndJoinsOutstandingRequests(t *testing.T) {
	const parallelism = 3
	transport := newBlockingVersionTransport()
	client := engineinfo.NewClientWithOptions(engineinfo.ClientOptions{
		HTTPClient:      &http.Client{Transport: transport},
		RequestAttempts: 1,
		RequestTimeout:  30 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	descriptors := plannedFleet(t, 40)
	done := make(chan struct{})
	var (
		result engineinfo.FleetResult
		err    error
	)
	go func() {
		result, err = engineinfo.NewCollectorWithOptions(client, engineinfo.CollectorOptions{MaxParallel: parallelism}).Collect(
			ctx, descriptors)
		close(done)
	}()
	for index := 0; index < parallelism; index++ {
		select {
		case <-transport.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for requests before cancellation")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled collection did not return promptly")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Collect() error = %v, want context cancellation", err)
	}
	if active := transport.active.Load(); active != 0 {
		t.Fatalf("active requests after Collect() = %d, want 0", active)
	}
	if calls := transport.calls.Load(); calls != parallelism {
		t.Fatalf("request calls after cancellation = %d, want only %d active workers", calls, parallelism)
	}
	if len(result.Participants) != 40 {
		t.Fatalf("partial participants = %d, want all 40 planned participants", len(result.Participants))
	}
	for _, participant := range result.Participants {
		if participant.CollectionStatus != engineinfo.StatusCollectionFailed {
			t.Fatalf("participant %s status = %q, want collection_failed", participant.NodeID, participant.CollectionStatus)
		}
	}
}

func TestFleetOutputKeepsSuccessfulNodeAndRetryDetailsVerboseOnly(t *testing.T) {
	entryResult := collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false))
	entryResult.Attempts = 2
	workerResult := collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false))
	workerResult.Attempts = 1
	result, err := engineinfo.NewCollector(mappedVersionClient{results: map[string]engineinfo.CollectionResult{
		"http://entry.example:9999":  entryResult,
		"http://worker.example:9999": workerResult,
	}}).Collect(context.Background(), []engineinfo.ParticipantDescriptor{
		descriptor(t, "worker.example", "9999", engineinfo.RoleWorker),
		descriptor(t, "entry.example", "9999", engineinfo.RoleEntry),
	})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	for _, mode := range []string{"headless", "full TUI"} {
		t.Run(mode, func(t *testing.T) {
			normal := result.OutputLines(false)
			if len(normal) != 1 || !strings.Contains(normal[0], "consistent") {
				t.Fatalf("normal output = %q, want one fleet-level result", normal)
			}
			assertExcludes(t, strings.Join(normal, "\n"), "entry.example", "worker.example", "attempts", "retries")

			verbose := result.OutputLines(true)
			if len(verbose) != 3 {
				t.Fatalf("verbose output lines = %d, want fleet result plus two participant details", len(verbose))
			}
			joined := strings.Join(verbose, "\n")
			for _, detail := range []string{
				"entry.example:9999", "role=entry", "status=collected", "schema=1", "build=build-1", "attempts=2", "retries=1",
				"worker.example:9999", "role=worker", "attempts=1", "retries=0",
			} {
				if !strings.Contains(joined, detail) {
					t.Fatalf("verbose output %q does not contain %q", joined, detail)
				}
			}
		})
	}
}

func TestCollectorRetainsEveryApprovedCollectionStateAndSafePartialEvidence(t *testing.T) {
	results := map[string]clientOutcome{
		"http://entry.example:9999": {
			result: collectedBuild("2026-08-26T12:34:56Z", "5.14.2", testRevisionA, false, boolPointer(false)),
		},
		"http://incomplete.example:9999": {
			result: incompleteBuild("5.14.2", "unknown", false, nil),
		},
		"http://legacy.example:9999": {
			result: engineinfo.CollectionResult{Status: engineinfo.StatusLegacyEndpointUnavailable, Reason: "engine version endpoint is unavailable on this engine", Attempts: 1},
		},
		"http://future.example:9999": {
			result: engineinfo.CollectionResult{Status: engineinfo.StatusUnsupportedSchema, ReportedSchemaVersion: 27, Reason: "engine version schema is newer than this CLI supports", Attempts: 1},
		},
		"http://failed.example:9999": {
			result: engineinfo.CollectionResult{Status: engineinfo.StatusCollectionFailed, Reason: "engine version request failed after 3 attempts", Attempts: 3},
			err:    errors.New("engine version request failed after 3 attempts"),
		},
	}
	descriptors := []engineinfo.ParticipantDescriptor{
		descriptor(t, "legacy.example", "9999", engineinfo.RoleWorker),
		descriptor(t, "future.example", "9999", engineinfo.RoleWorker),
		descriptor(t, "entry.example", "9999", engineinfo.RoleEntry),
		descriptor(t, "failed.example", "9999", engineinfo.RoleWorker),
		descriptor(t, "incomplete.example", "9999", engineinfo.RoleWorker),
	}

	result, err := engineinfo.NewCollector(outcomeVersionClient{outcomes: results}).Collect(context.Background(), descriptors)
	if err == nil {
		t.Fatal("Collect() error = nil, want hard collection failure with partial evidence")
	}
	wantStatuses := map[string]engineinfo.CollectionStatus{
		"entry.example:9999":      engineinfo.StatusCollected,
		"failed.example:9999":     engineinfo.StatusCollectionFailed,
		"future.example:9999":     engineinfo.StatusUnsupportedSchema,
		"incomplete.example:9999": engineinfo.StatusIncompleteBuildInfo,
		"legacy.example:9999":     engineinfo.StatusLegacyEndpointUnavailable,
	}
	if len(result.Participants) != len(wantStatuses) {
		t.Fatalf("participants = %d, want all %d planned participants", len(result.Participants), len(wantStatuses))
	}
	for _, participant := range result.Participants {
		if want := wantStatuses[participant.NodeID]; participant.CollectionStatus != want {
			t.Fatalf("participant %s status = %q, want %q", participant.NodeID, participant.CollectionStatus, want)
		}
		if (participant.CollectionStatus == engineinfo.StatusCollected || participant.CollectionStatus == engineinfo.StatusIncompleteBuildInfo) && participant.BuildID == "" {
			t.Fatalf("participant %s lost usable supported build evidence", participant.NodeID)
		}
	}
	if result.Consistency.Status != engineinfo.ConsistencyIndeterminate {
		t.Fatalf("consistency = %q, want indeterminate", result.Consistency.Status)
	}
	assertExcludes(t, err.Error(), "entry.example", "failed.example")

	verbose := result.OutputLines(true)
	legacyLine := outputLineForNode(t, verbose, "legacy.example:9999")
	assertExcludes(t, legacyLine, "schema=", "build=")
	futureLine := outputLineForNode(t, verbose, "future.example:9999")
	if !strings.Contains(futureLine, "schema=27") {
		t.Fatalf("future-schema output = %q, want safely reported schema", futureLine)
	}
	assertExcludes(t, futureLine, "build=")
	failedLine := outputLineForNode(t, verbose, "failed.example:9999")
	assertExcludes(t, failedLine, "schema=", "build=")
	incompleteLine := outputLineForNode(t, verbose, "incomplete.example:9999")
	if !strings.Contains(incompleteLine, "schema=1") || !strings.Contains(incompleteLine, "build=build-") {
		t.Fatalf("incomplete supported output = %q, want available schema and build reference", incompleteLine)
	}
}

type staticVersionClient struct {
	result engineinfo.CollectionResult
	err    error
}

type capturingVersionClient struct {
	result  engineinfo.CollectionResult
	baseURL string
}

func (c *capturingVersionClient) Fetch(_ context.Context, baseURL string) (engineinfo.CollectionResult, error) {
	c.baseURL = baseURL
	return c.result, nil
}

type mappedVersionClient struct {
	results map[string]engineinfo.CollectionResult
}

type clientOutcome struct {
	result engineinfo.CollectionResult
	err    error
}

type outcomeVersionClient struct {
	outcomes map[string]clientOutcome
}

func (c outcomeVersionClient) Fetch(_ context.Context, baseURL string) (engineinfo.CollectionResult, error) {
	outcome, ok := c.outcomes[baseURL]
	if !ok {
		return engineinfo.CollectionResult{}, errors.New("unexpected endpoint")
	}
	return outcome.result, outcome.err
}

func (c mappedVersionClient) Fetch(_ context.Context, baseURL string) (engineinfo.CollectionResult, error) {
	result, ok := c.results[baseURL]
	if !ok {
		return engineinfo.CollectionResult{}, errors.New("unexpected endpoint")
	}
	return result, nil
}

func (c staticVersionClient) Fetch(context.Context, string) (engineinfo.CollectionResult, error) {
	return c.result, c.err
}

const (
	testRevisionA = "0123456789abcdef0123456789abcdef01234567"
	testRevisionB = "fedcba9876543210fedcba9876543210fedcba98"
)

func collectedBuild(buildTime, version, revision string, development bool, dirty *bool) engineinfo.CollectionResult {
	return engineinfo.CollectionResult{
		Status:                engineinfo.StatusCollected,
		ReportedSchemaVersion: 1,
		Complete:              true,
		Build: &engineinfo.BuildInformation{
			SchemaVersion: 1,
			Product:       "spt-engine",
			Version:       version,
			Revision:      revision,
			BuildTime:     buildTime,
			Development:   development,
			SourceDirty:   dirty,
		},
	}
}

func incompleteBuild(version, revision string, development bool, dirty *bool) engineinfo.CollectionResult {
	result := collectedBuild("2026-08-26T12:34:56Z", version, revision, development, dirty)
	result.Status = engineinfo.StatusIncompleteBuildInfo
	result.Complete = false
	result.Reason = "engine build comparison fields are incomplete"
	return result
}

func withProduct(result engineinfo.CollectionResult, product string) engineinfo.CollectionResult {
	result.Build.Product = product
	return result
}

func boolPointer(value bool) *bool {
	return &value
}

func descriptor(t *testing.T, host, port string, role engineinfo.ParticipantRole) engineinfo.ParticipantDescriptor {
	t.Helper()
	parsed, err := hostparse.ParseSingleHost(host)
	if err != nil {
		t.Fatalf("ParseSingleHost(%q) error = %v", host, err)
	}
	descriptor, err := engineinfo.NewParticipantDescriptor(parsed, port, role)
	if err != nil {
		t.Fatalf("NewParticipantDescriptor(%q) error = %v", host, err)
	}
	return descriptor
}

func plannedFleet(t *testing.T, count int) []engineinfo.ParticipantDescriptor {
	t.Helper()
	descriptors := make([]engineinfo.ParticipantDescriptor, 0, count)
	for index := 0; index < count; index++ {
		role := engineinfo.RoleWorker
		if index == 0 {
			role = engineinfo.RoleEntry
		}
		descriptors = append(descriptors, descriptor(t, fmt.Sprintf("node-%03d.example", index), "9999", role))
	}
	return descriptors
}

type blockingVersionTransport struct {
	started   chan string
	release   chan struct{}
	active    atomic.Int32
	maxActive atomic.Int32
	calls     atomic.Int32
}

type delayedVersionTransport struct {
	reverse bool
}

func (transport delayedVersionTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var index int
	if _, err := fmt.Sscanf(request.URL.Hostname(), "node-%03d.example", &index); err != nil {
		return nil, errors.New("unexpected test host")
	}
	delay := index
	if transport.reverse {
		delay = 17 - index
	}
	timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-request.Context().Done():
		return nil, request.Context().Err()
	}
	buildTime := "2026-08-26T12:34:56Z"
	if index%2 == 1 {
		buildTime = "2026-08-26T13:34:56Z"
	}
	return schemaOneHTTPResponse(request, buildTime), nil
}

func newBlockingVersionTransport() *blockingVersionTransport {
	return &blockingVersionTransport{
		started: make(chan string, 128),
		release: make(chan struct{}),
	}
}

func (transport *blockingVersionTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.calls.Add(1)
	active := transport.active.Add(1)
	defer transport.active.Add(-1)
	for {
		maximum := transport.maxActive.Load()
		if active <= maximum || transport.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	transport.started <- request.URL.Host
	select {
	case <-transport.release:
	case <-request.Context().Done():
		return nil, request.Context().Err()
	}
	return schemaOneHTTPResponse(request, "2026-08-26T12:34:56Z"), nil
}

func schemaOneHTTPResponse(request *http.Request, buildTime string) *http.Response {
	body := fmt.Sprintf(`{
		"schema_version": 1,
		"product": "spt-engine",
		"version": "5.14.2",
		"revision": %q,
		"build_time": %q,
		"development": false,
		"source_dirty": false
	}`, testRevisionA, buildTime)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

func outputLineForNode(t *testing.T, lines []string, nodeID string) string {
	t.Helper()
	for _, line := range lines {
		if strings.Contains(line, nodeID) {
			return line
		}
	}
	t.Fatalf("output %q has no line for %s", lines, nodeID)
	return ""
}
