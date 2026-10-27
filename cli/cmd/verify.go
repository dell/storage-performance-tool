/*
Copyright © 2025 Dell Technologies
*/

package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/verification"
	"github.com/spf13/cobra"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify distributed testing infrastructure readiness",
	Long: `Validates that nodes are properly configured for distributed Spt testing.

If no --test-hosts is specified, verifies localhost (127.0.0.1) by default.

Tests performed for each node:
  - SSH connectivity (for remote nodes)
  - Docker daemon availability
  - Spt container startup in node mode
  - Port accessibility (RMI and REST)
  - REST API endpoints functionality
  - Clean container shutdown

Each node also lists its NICs, from the SPT 5.17 or later image. With
--interfaces, verify plans each node's NIC set for the S3 endpoints, as a run
would: the NICs used and why the others are not, the Paths of each pairing
group, the permits per NIC at --threads, and the failures and warnings. A node
whose plan has a failure fails verification.

After verification, no containers will be left running.

Examples:
  spt verify                                           # Verify localhost (127.0.0.1)
  spt verify --test-hosts "node1,node2,node3"         # Verify multiple nodes
  spt verify --test-hosts "root@server1,root@server2" --min-hosts 1
  spt verify --test-hosts "node1,node2" --interfaces auto -e http://192.0.2.101:9000,http://192.0.2.102:9000 --threads 64`,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		// Ensure .env and logger are initialized at the subcommand level.
		_ = config.LoadDotEnv()
		if err := initializeLogger(); err != nil { // idempotent
			return err
		}
		return applyEnvDefaultsToVerifyFlags(cmd)
	},
	RunE: runVerify,
}

func init() {
	verifyCmd.Flags().String("test-hosts", "", "Comma-separated list of hosts to verify (defaults to localhost)")
	// Removed MarkFlagRequired to allow localhost default
	verifyCmd.Flags().Int("min-hosts", 0, "Minimum hosts that must pass (0 = all)")
	verifyCmd.Flags().String("network-mode", "host", "Docker network mode: 'host' (default, required for RMI) or 'bridge'")
	verifyCmd.Flags().Int("api-port", 9999, "Spt API port to verify")
	verifyCmd.Flags().Int("rmi-port-start", 40000, "Starting port for RMI range")
	verifyCmd.Flags().Int("rmi-port-count", 10, "Number of RMI ports to verify")
	verifyCmd.Flags().Bool("force-cleanup", false, "Automatically clean up conflicting containers without prompting")
	verifyCmd.Flags().Bool("use-rdma", false, "Include RDMA hardware and configuration checks")
	verifyCmd.Flags().String(flagInterfaces, "", "Plan each node's NIC set: auto, or interface names, IPv4 addresses or RDMA devices (comma-separated)")
	verifyCmd.Flags().StringSliceP("endpoints", "e", []string{}, "S3 endpoint URLs to plan Paths for with --interfaces (comma-separated or repeatable)")
	verifyCmd.Flags().Bool("slice-endpoints", false, "Plan each node's slice of the endpoints, as a distributed run with --slice-endpoints")
	verifyCmd.Flags().IntP("threads", "t", 1, "Concurrency limit whose per-NIC permit shares --interfaces shows")
}

func runVerify(cmd *cobra.Command, _ []string) error {
	// Parse configuration
	hostString, _ := cmd.Flags().GetString("test-hosts")
	// Robust fallback: if flag not provided and HOSTS is set in env/.env, use it.
	if strings.TrimSpace(hostString) == "" {
		if envHosts := strings.TrimSpace(os.Getenv("HOSTS")); envHosts != "" {
			hostString = envHosts
		}
	}
	minHosts, _ := cmd.Flags().GetInt("min-hosts")
	networkMode, _ := cmd.Flags().GetString("network-mode")
	apiPort, _ := cmd.Flags().GetInt("api-port")
	rmiPortStart, _ := cmd.Flags().GetInt("rmi-port-start")
	rmiPortCount, _ := cmd.Flags().GetInt("rmi-port-count")
	forceCleanup, _ := cmd.Flags().GetBool("force-cleanup")
	useRdma, _ := cmd.Flags().GetBool("use-rdma")
	interfacesValue, _ := cmd.Flags().GetString(flagInterfaces)
	sliceEndpoints, _ := cmd.Flags().GetBool("slice-endpoints")
	threads, _ := cmd.Flags().GetInt("threads")

	logging.LogInfo("verify", "starting verification",
		"hosts", hostString,
		"min_hosts", minHosts,
		"network_mode", networkMode,
		"api_port", apiPort,
		"rmi_port_range", fmt.Sprintf("%d-%d", rmiPortStart, rmiPortStart+rmiPortCount-1),
		"force_cleanup", forceCleanup,
		"use_rdma", useRdma,
		"interfaces", interfacesValue)

	// Parse hosts
	hosts, err := hostparse.ParseTestHosts(hostString)
	if err != nil {
		return fmt.Errorf("invalid host specification: %w", err)
	}

	// Default min-hosts to all if not specified
	if minHosts == 0 {
		minHosts = len(hosts)
	}

	// Validate configuration
	if minHosts > len(hosts) {
		return fmt.Errorf("min-hosts (%d) cannot be greater than total hosts (%d)", minHosts, len(hosts))
	}

	if networkMode != constants.BridgeNetworkMode && networkMode != constants.HostNetworkMode {
		return fmt.Errorf("network-mode must be '%s' or '%s', got: %s", constants.BridgeNetworkMode, constants.HostNetworkMode, networkMode)
	}

	interfaces, endpoints, err := verifyNicPlanInputs(cmd, interfacesValue, networkMode)
	if err != nil {
		return err
	}

	// Warn if using bridge mode with multiple hosts (distributed testing)
	if networkMode == constants.BridgeNetworkMode && len(hosts) > 1 {
		fmt.Println("⚠️  WARNING: Bridge networking may not work for distributed testing.")
		fmt.Println("   Java RMI requires host networking for inter-node communication.")
		fmt.Println("   Consider using --network-mode host (the default).")
		fmt.Println()
	}

	// Display initial startup message
	printStartupMessage(hosts)
	fmt.Println()

	if useRdma {
		_ = os.Setenv(constants.EnvRdmaEnabled, "true")
	}

	// Create and run verifier
	config := verification.Config{
		NetworkMode:  networkMode,
		APIPort:      apiPort,
		RMIPortStart: rmiPortStart,
		RMIPortCount: rmiPortCount,
		MinHosts:     minHosts,
		ForceCleanup: forceCleanup,
		ShowProgress: len(hosts) == 1, // Show detailed progress for single host verification
		UseRdma:      useRdma,

		Interfaces:     interfaces,
		Endpoints:      endpoints,
		SliceEndpoints: sliceEndpoints,
		Threads:        threads,
	}

	verifier := verification.NewVerifier(hosts, config)
	report := verifier.VerifyAll()

	// Display results
	report.Print()

	// Return error if not ready
	if !report.Ready {
		return fmt.Errorf("verification failed")
	}

	return nil
}

func init() {
	rootCmd.AddCommand(verifyCmd)
}

// verifyNicPlanInputs returns the --interfaces entries and, with them, the S3 endpoints as the
// engine's drivers name them. Without --interfaces, verify lists NICs only and reads no endpoints.
func verifyNicPlanInputs(cmd *cobra.Command, value, networkMode string) ([]string, []string, error) {
	interfaces, err := nicplan.ParseInterfaces(value)
	if err != nil || len(interfaces) == 0 {
		return nil, nil, err
	}
	if networkMode == constants.BridgeNetworkMode {
		return nil, nil, fmt.Errorf("--interfaces needs --network-mode %s: a bridge-network container sees only its own interface", constants.HostNetworkMode)
	}
	urls := collectNormalizedEndpoints(cmd)
	if len(urls) == 0 {
		return nil, nil, fmt.Errorf("--interfaces needs the S3 endpoints: pass --endpoints or set %s", constants.EnvS3Endpoints)
	}
	nodes, err := scenario.ParseNodeEndpoints(urls)
	if err != nil {
		return nil, nil, err
	}
	endpoints := nodes.DriverEndpoints(nodes.Addrs)
	if err := nicplan.ValidateEndpoints(endpoints); err != nil {
		return nil, nil, err
	}
	return interfaces, endpoints, nil
}

func printStartupMessage(hosts []*hostparse.HostInfo) {
	if len(hosts) == 1 {
		h := hosts[0]
		if h.IsLocal {
			fmt.Printf("Starting verification of localhost (%s)...\n", h.Host)
		} else {
			fmt.Printf("Starting verification of %s...\n", h.Host)
		}
		return
	}
	fmt.Printf("Starting parallel verification of %d hosts...\n", len(hosts))
	for _, h := range hosts {
		if h.IsLocal {
			fmt.Printf("  - localhost (%s)\n", h.Host)
		} else {
			fmt.Printf("  - %s\n", h.Host)
		}
	}
}
