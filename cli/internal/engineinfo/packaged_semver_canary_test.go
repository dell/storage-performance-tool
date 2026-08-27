//go:build packaged_engine_canary

package engineinfo_test

import (
	"context"
	"net"
	"net/url"
	"os"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestPackagedEngineSemanticVersionCanary(t *testing.T) {
	baseURL := os.Getenv("SPT_TEST_PACKAGED_ENGINE_URL")
	expectedVersion := os.Getenv("SPT_TEST_PACKAGED_ENGINE_VERSION")
	if baseURL == "" || expectedVersion == "" {
		t.Fatal("packaged engine canary requires SPT_TEST_PACKAGED_ENGINE_URL and SPT_TEST_PACKAGED_ENGINE_VERSION")
	}

	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse packaged engine URL: %v", err)
	}
	host, port, err := net.SplitHostPort(parsedURL.Host)
	if err != nil {
		t.Fatalf("parse packaged engine address: %v", err)
	}
	descriptor, err := engineinfo.NewParticipantDescriptor(
		&hostparse.HostInfo{Host: host, IsLocal: true, Original: host}, port, engineinfo.RoleStandalone)
	if err != nil {
		t.Fatalf("create packaged engine descriptor: %v", err)
	}

	fleet, err := engineinfo.NewCollector(engineinfo.NewClient()).Collect(
		context.Background(), []engineinfo.ParticipantDescriptor{descriptor})
	if err != nil {
		t.Fatalf("collect packaged Engine Build Information: %v", err)
	}
	if fleet.Consistency.Status != engineinfo.ConsistencyConsistent || len(fleet.Builds) != 1 {
		t.Fatalf("packaged fleet = %+v, want one consistent build", fleet)
	}
	if got := fleet.Builds[0].Information.Version; got != expectedVersion {
		t.Fatalf("packaged engine version = %q, want exact %q", got, expectedVersion)
	}
}
