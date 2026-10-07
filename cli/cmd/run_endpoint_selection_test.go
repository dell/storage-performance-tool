package cmd

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"gopkg.in/yaml.v3"
)

func newEndpointSelectionCmd(t *testing.T, args ...string) (scenario.Params, error) {
	t.Helper()
	return newEndpointSelectionCmdFor(t, "write", args...)
}

func newEndpointSelectionCmdFor(t *testing.T, workloadType string, args ...string) (scenario.Params, error) {
	t.Helper()
	cmd := newRunLikeCmd()
	cmd.Flags().Bool("slice-endpoints", false, "")
	cmd.Flags().String("region", "", "")
	cmd.Flags().String("range-size", "", "")
	cmd.Flags().String("range-offset", "", "")
	cmd.Flags().String("range-align", "", "")
	registerEndpointSelectionFlags(cmd.Flags())
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatal(err)
	}
	return buildScenarioParams(workloadType, cmd)
}

func endpointYAML(t *testing.T, params scenario.Params) (map[string]interface{}, bool) {
	t.Helper()
	data, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]interface{}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	net := config["storage"].(map[string]interface{})["net"].(map[string]interface{})
	endpoint, ok := net["endpoint"].(map[string]interface{})
	return endpoint, ok
}

func TestEndpointSelectionFlagsAreRegisteredOnRun(t *testing.T) {
	for flag, def := range map[string]string{
		flagEndpointSelection:      scenario.EndpointSelectionDefault,
		flagEndpointHostname:       "",
		flagDNSServer:              "",
		flagDNSTimeout:             "0s",
		flagEndpointConnectTimeout: "0s",
	} {
		f := runCmd.Flags().Lookup(flag)
		if f == nil || f.DefValue != def {
			t.Fatalf("run must register --%s with default %q, got %v", flag, def, f)
		}
	}
}

func TestDefaultSelectionLeavesGeneratedDefaultsUnchanged(t *testing.T) {
	params, err := newEndpointSelectionCmd(t, "--endpoints", "https://s3.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, present := endpointYAML(t, params); present {
		t.Fatal("default selection must not emit storage.net.endpoint")
	}
}

func TestRoundRobinFlagsReachEngineYAML(t *testing.T) {
	params, err := newEndpointSelectionCmd(t,
		"--endpoints", "http://10.0.0.2:9020,http://10.0.0.1:9020",
		"--endpoint-selection", "round-robin",
		"--endpoint-hostname", "s3.example.com",
		"--endpoint-connect-timeout", "2500ms")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, ok := endpointYAML(t, params)
	if !ok {
		t.Fatal("round robin must emit storage.net.endpoint")
	}
	if endpoint["selection"] != "round-robin" || endpoint["hostname"] != "s3.example.com" {
		t.Fatalf("unexpected endpoint config %v", endpoint)
	}
	if endpoint["connect"].(map[string]interface{})["timeoutMilliSec"] != 2500 {
		t.Fatalf("connect timeout not emitted in ms: %v", endpoint)
	}
	if _, hasDNS := endpoint["dns"]; hasDNS {
		t.Fatalf("round robin must not emit DNS settings: %v", endpoint)
	}
}

func TestPerRequestDNSFlagsReachEngineYAML(t *testing.T) {
	params, err := newEndpointSelectionCmd(t,
		"--endpoints", "https://s3.example.com:9021",
		"--endpoint-selection", "per-request-dns",
		"--dns-server", "10.0.0.53",
		"--dns-timeout", "2s")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, ok := endpointYAML(t, params)
	if !ok {
		t.Fatal("per-request DNS must emit storage.net.endpoint")
	}
	dns := endpoint["dns"].(map[string]interface{})
	if endpoint["selection"] != "per-request-dns" || dns["server"] != "10.0.0.53" || dns["timeoutMilliSec"] != 2000 {
		t.Fatalf("unexpected endpoint config %v", endpoint)
	}
}

func TestEndpointSelectionRejectsInvalidCombinations(t *testing.T) {
	rr := []string{"--endpoint-selection", "round-robin"}
	dns := []string{"--endpoint-selection", "per-request-dns"}
	cases := []struct {
		name     string
		args     []string
		want     string
		workload string
	}{
		{"unknown mode", []string{"--endpoints", "http://10.0.0.1", "--endpoint-selection", "random"}, "must be default, round-robin or per-request-dns", ""},
		{"hostname with default mode", []string{"--endpoints", "http://s3.example.com", "--endpoint-hostname", "s3.example.com"}, "--endpoint-hostname requires", ""},
		{"explicit default-valued timeout with default mode", []string{"--endpoints", "http://s3.example.com", "--dns-timeout", "5s"}, "--dns-timeout requires", ""},
		{"round robin hostname endpoint", append([]string{"--endpoints", "http://s3.example.com"}, rr...), "requires IPv4 endpoint addresses", ""},
		{"round robin duplicate before deduplication", append([]string{"--endpoints", "http://10.0.0.1:9020,http://10.0.0.1:9020"}, rr...), "duplicate round-robin endpoint", ""},
		{"round robin duplicate via default port", append([]string{"--endpoints", "http://10.0.0.1,http://10.0.0.1:80"}, rr...), "duplicate round-robin endpoint", ""},
		{"round robin with dns server", append([]string{"--endpoints", "http://10.0.0.1", "--dns-server", "10.0.0.53"}, rr...), "--dns-server and --dns-timeout require", ""},
		{"round robin ip hostname", append([]string{"--endpoints", "http://10.0.0.1", "--endpoint-hostname", "10.0.0.9"}, rr...), "not an IP address", ""},
		{"round robin with slicing", append([]string{"--endpoints", "http://10.0.0.1", "--slice-endpoints"}, rr...), "--slice-endpoints", ""},
		{"round robin with aws driver", append([]string{"--endpoints", "http://10.0.0.1", "--s3-driver", "aws"}, rr...), "default Netty S3 driver", ""},
		{"round robin with partial reads", append([]string{"--endpoints", "http://10.0.0.1", "--range-size", "4KiB"}, rr...), "partial-object reads", "read"},
		{"dns with two endpoints", append([]string{"--endpoints", "http://a.example.com,http://b.example.com"}, dns...), "exactly one endpoint hostname", ""},
		{"dns with ip endpoint", append([]string{"--endpoints", "http://10.0.0.1"}, dns...), "not an IP address", ""},
		{"dns with endpoint hostname", append([]string{"--endpoints", "http://s3.example.com", "--endpoint-hostname", "s3.example.com"}, dns...), "applies only to --endpoint-selection round-robin", ""},
		{"dns with hostname server", append([]string{"--endpoints", "http://s3.example.com", "--dns-server", "dns.example.com"}, dns...), "must be an IPv4 address", ""},
		{"dns with bad server port", append([]string{"--endpoints", "http://s3.example.com", "--dns-server", "10.0.0.53:0"}, dns...), "invalid port", ""},
		{"fractional milliseconds", append([]string{"--endpoints", "http://s3.example.com", "--dns-timeout", "1500us"}, dns...), "whole number of milliseconds", ""},
		{"zero duration", append([]string{"--endpoints", "http://s3.example.com", "--endpoint-connect-timeout", "0s"}, dns...), "must be positive", ""},
		{"overflowing duration", append([]string{"--endpoints", "http://s3.example.com", "--dns-timeout", "600h"}, dns...), "at most", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workloadType := tc.workload
			if workloadType == "" {
				workloadType = "write"
			}
			_, err := newEndpointSelectionCmdFor(t, workloadType, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestRoundRobinKeepsDistinctPortsOnOneAddress(t *testing.T) {
	params, err := newEndpointSelectionCmd(t,
		"--endpoints", "http://10.0.0.1:9020,http://10.0.0.1:9021",
		"--endpoint-selection", "round-robin")
	if err != nil {
		t.Fatal(err)
	}
	data, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "10.0.0.1:9020") || !strings.Contains(string(data), "10.0.0.1:9021") {
		t.Fatalf("per-entry ports must be preserved:\n%s", data)
	}
}
