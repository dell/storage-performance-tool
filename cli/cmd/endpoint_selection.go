package cmd

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// endpointSelectionAuxFlags apply only to a non-default --endpoint-selection.
var endpointSelectionAuxFlags = []string{flagEndpointHostname, flagDNSServer, flagDNSTimeout, flagEndpointConnectTimeout}

// registerEndpointSelectionFlags adds the endpoint-selection flags shared by run and replay.
func registerEndpointSelectionFlags(flags *pflag.FlagSet) {
	flags.String(flagEndpointSelection, scenario.EndpointSelectionDefault,
		"S3 connect-destination selection: default, round-robin (rotate over the --endpoints IPv4 addresses per request) "+
			"or per-request-dns (resolve the single --endpoints hostname for every request); Netty S3 only")
	flags.String(flagEndpointHostname, "",
		"Round robin: logical hostname for HTTP Host, request signing and TLS SNI (default: the selected address)")
	flags.String(flagDNSServer, "",
		"Per-request DNS: IPv4[:port] DNS server queried without fallback (default: the worker's host DNS configuration)")
	flags.Duration(flagDNSTimeout, 0, "Per-request DNS: total deadline of one lookup (default 5s)")
	flags.Duration(flagEndpointConnectTimeout, 0, "Round robin and per-request DNS: TCP connect deadline (default 30s)")
}

// readEndpointSelectionFlags reads the endpoint-selection flags as given. The mode is empty when
// --endpoint-selection is not given, and durations are zero unless given, so callers can tell
// explicit settings from defaults.
func readEndpointSelectionFlags(cmd *cobra.Command) (scenario.EndpointSelection, error) {
	flags := cmd.Flags()
	if flags.Lookup(flagEndpointSelection) == nil {
		return scenario.EndpointSelection{}, nil
	}
	var sel scenario.EndpointSelection
	if flags.Changed(flagEndpointSelection) {
		mode, _ := flags.GetString(flagEndpointSelection)
		sel.Mode = strings.TrimSpace(mode)
	}
	hostname, _ := flags.GetString(flagEndpointHostname)
	sel.Hostname = strings.TrimSpace(hostname)
	server, _ := flags.GetString(flagDNSServer)
	sel.DNSServer = strings.TrimSpace(server)
	for _, duration := range []struct {
		flag   string
		millis *int
	}{
		{flagDNSTimeout, &sel.DNSTimeoutMillis},
		{flagEndpointConnectTimeout, &sel.ConnectTimeoutMillis},
	} {
		if !flags.Changed(duration.flag) {
			continue
		}
		value, _ := flags.GetDuration(duration.flag)
		millis, err := scenario.DurationMillis(duration.flag, value)
		if err != nil {
			return sel, err
		}
		*duration.millis = millis
	}
	return sel, nil
}

// endpointSelectionFromFlags reads the endpoint-selection flags for spt run. Auxiliary flags given
// with the default mode are rejected even when they equal their defaults, because they would have
// no effect. Replay instead merges the flags with archived settings and validates the result.
func endpointSelectionFromFlags(cmd *cobra.Command) (scenario.EndpointSelection, error) {
	sel, err := readEndpointSelectionFlags(cmd)
	if err != nil || sel.Active() {
		return sel, err
	}
	for _, flag := range endpointSelectionAuxFlags {
		if cmd.Flags().Changed(flag) {
			return sel, fmt.Errorf("--%s requires --%s %s or %s", flag, flagEndpointSelection,
				scenario.EndpointSelectionRoundRobin, scenario.EndpointSelectionPerRequestDNS)
		}
	}
	return sel, nil
}

// rawEndpointFlags returns the endpoint URLs as given, before any deduplication, so that
// round-robin duplicates can be reported instead of silently removed.
func rawEndpointFlags(cmd *cobra.Command) []string {
	var raw []string
	if flag := cmd.Flags().Lookup("endpoint"); flag != nil {
		if endpoint, _ := cmd.Flags().GetString("endpoint"); strings.TrimSpace(endpoint) != "" {
			raw = append(raw, strings.TrimSpace(endpoint))
		}
	}
	if flag := cmd.Flags().Lookup("endpoints"); flag != nil {
		endpoints, _ := cmd.Flags().GetStringSlice("endpoints")
		for _, endpoint := range endpoints {
			if strings.TrimSpace(endpoint) != "" {
				raw = append(raw, strings.TrimSpace(endpoint))
			}
		}
	}
	return raw
}
