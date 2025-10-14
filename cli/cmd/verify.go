/*
Copyright © 2025 Dell Technologies
*/

package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
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

After verification, no containers will be left running.

Examples:
  spt verify                                           # Verify localhost (127.0.0.1)
  spt verify --test-hosts "node1,node2,node3"         # Verify multiple nodes
  spt verify --test-hosts "root@server1,root@server2" --min-hosts 1`,
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
	verifyCmd.Flags().String("network-mode", "bridge", "Docker network mode (bridge/host)")
	verifyCmd.Flags().Int("api-port", 9999, "Spt API port to verify")
	verifyCmd.Flags().Int("rmi-port-start", 40000, "Starting port for RMI range")
	verifyCmd.Flags().Int("rmi-port-count", 10, "Number of RMI ports to verify")
	verifyCmd.Flags().Bool("force-cleanup", false, "Automatically clean up conflicting containers without prompting")
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

	logging.LogInfo("verify", "starting verification",
		"hosts", hostString,
		"min_hosts", minHosts,
		"network_mode", networkMode,
		"api_port", apiPort,
		"rmi_port_range", fmt.Sprintf("%d-%d", rmiPortStart, rmiPortStart+rmiPortCount-1),
		"force_cleanup", forceCleanup)

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

	if networkMode != "bridge" && networkMode != "host" {
		return fmt.Errorf("network-mode must be 'bridge' or 'host', got: %s", networkMode)
	}

	// Display initial startup message
	printStartupMessage(hosts)
	fmt.Println()

	// Create and run verifier
	config := verification.Config{
		NetworkMode:  networkMode,
		APIPort:      apiPort,
		RMIPortStart: rmiPortStart,
		RMIPortCount: rmiPortCount,
		MinHosts:     minHosts,
		ForceCleanup: forceCleanup,
		ShowProgress: len(hosts) == 1, // Show detailed progress for single host verification
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
