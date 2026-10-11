package scenario

import (
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
	"gopkg.in/yaml.v3"
)

func TestEndpointSelectionReachesDefaultsForEveryS3Workload(t *testing.T) {
	selection := EndpointSelection{Mode: EndpointSelectionPerRequestDNS, DNSServer: "10.0.0.53", DNSTimeoutMillis: 2000}
	for _, workloadType := range []string{workload.Write, workload.Read, workload.WriteVerify, workload.ReadVerify,
		workload.Mixed, workload.Delete, workload.List} {
		t.Run(workloadType, func(t *testing.T) {
			data, err := GenerateDefaults(Params{
				WorkloadType: workloadType, Endpoints: []string{"https://s3.example.com"}, AccessKey: "a", SecretKey: "s",
				Bucket: "b", Threads: 1, ObjectSize: "1KiB", EndpointSelection: selection,
			})
			if err != nil {
				t.Fatal(err)
			}
			var config struct {
				Storage struct {
					Net struct {
						Endpoint *EndpointConfig `yaml:"endpoint"`
					} `yaml:"net"`
				} `yaml:"storage"`
			}
			if err := yaml.Unmarshal(data, &config); err != nil {
				t.Fatal(err)
			}
			got := config.Storage.Net.Endpoint
			if got == nil || got.Selection != EndpointSelectionPerRequestDNS || got.DNS.Server != "10.0.0.53" ||
				got.DNS.TimeoutMilliSec != 2000 || got.Connect != nil {
				t.Fatalf("unexpected storage.net.endpoint %+v", got)
			}
		})
	}
}

func TestEndpointSelectionRejectsTablesWorkload(t *testing.T) {
	err := ValidateEndpointSelection(EndpointSelection{Mode: EndpointSelectionRoundRobin},
		EndpointSelectionTarget{RawEndpoints: []string{"http://10.0.0.1"}, WorkloadType: workload.Tables})
	if err == nil || !strings.Contains(err.Error(), "tables") {
		t.Fatalf("want tables rejection, got %v", err)
	}
}

func TestDurationMillisBounds(t *testing.T) {
	if ms, err := DurationMillis("dns-timeout", 1500*time.Millisecond); err != nil || ms != 1500 {
		t.Fatalf("1.5s -> %d, %v", ms, err)
	}
	for _, d := range []time.Duration{0, -time.Second, 1500 * time.Microsecond, 600 * time.Hour} {
		if _, err := DurationMillis("dns-timeout", d); err == nil {
			t.Fatalf("%s must be rejected", d)
		}
	}
}
