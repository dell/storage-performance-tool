/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"fmt"
	"os"
	"strings"
)

// ANSI color codes for terminal output
const (
	ansiYellow = "\033[1;33m"
	ansiRed    = "\033[0;31m"
	ansiReset  = "\033[0m"
)

// colorEnabled returns true if stdout is a terminal and NO_COLOR is not set.
func colorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// assess determines if the verification passed overall
func (r *Report) assess() {
	passedCount := 0
	failedNodes := []string{}

	for host, result := range r.Results {
		if result.Overall.Passed {
			passedCount++
		} else {
			failedNodes = append(failedNodes,
				fmt.Sprintf("%s (%s)", host, result.Overall.Message))
		}
	}

	r.Ready = passedCount >= r.Config.MinHosts

	if !r.Ready {
		r.Summary = fmt.Sprintf("%d/%d nodes passed (required: %d). Failed: %s",
			passedCount, len(r.Results), r.Config.MinHosts,
			strings.Join(failedNodes, ", "))
	} else if passedCount < len(r.Results) {
		r.Summary = fmt.Sprintf("%d/%d nodes passed (minimum threshold met)",
			passedCount, len(r.Results))
	} else {
		r.Summary = "All nodes passed verification"
	}
}

// Print displays the verification report in a user-friendly format
func (r *Report) Print() {
	fmt.Println("═══ Spt Distributed Testing Verification ═══")
	fmt.Printf("Nodes tested: %d\n", len(r.Results))
	fmt.Printf("Time: %.1fs\n\n", r.Duration.Seconds())

	color := colorEnabled()

	// Display each node's results
	for host, result := range r.Results {
		r.printNodeResult(host, result, color)
	}

	// Summary
	fmt.Println("═══ Summary ═══")
	if r.Ready {
		fmt.Printf("✅ READY: %s\n", r.Summary)
		fmt.Println("You can run distributed tests with these nodes.")
	} else {
		fmt.Printf("❌ NOT READY: %s\n", r.Summary)
		fmt.Println("Please address the issues above before running tests.")
	}
}

// printNodeResult displays the results for a single node
func (r *Report) printNodeResult(host string, result *NodeResult, color bool) {
	fmt.Printf("Node: %s\n", host)

	// Show each check with appropriate symbol
	r.printCheck("SSH Connectivity", result.SSHConnectivity, color)
	if result.ClockSkew.Message != "" {
		r.printCheck("Clock Sync", result.ClockSkew, color)
	}
	r.printCheck("Docker Available", result.DockerAvailable, color)
	r.printCheck("Docker Image Pull", result.DockerImagePull, color)
	r.printCheck("Container Start", result.ContainerStart, color)
	if result.RdmaDevice.Message != "" {
		r.printCheck("RDMA Device", result.RdmaDevice, color)
	}
	if result.RdmaDriver.Message != "" {
		r.printCheck("RDMA Driver", result.RdmaDriver, color)
	}
	if result.RdmaReadiness.Message != "" {
		r.printCheck("RDMA Readiness", result.RdmaReadiness, color)
	}
	r.printCheck("Ports Accessible", result.PortsAccessible, color)
	r.printCheck("Metrics Endpoint", result.MetricsEndpoint, color)
	r.printCheck("Control Endpoint", result.ControlEndpoint, color)
	r.printCheck("Container Cleanup", result.ContainerCleanup, color)

	// Overall status
	if result.Overall.Passed {
		fmt.Printf("  Overall: ✅ %s\n", result.Overall.Message)
	} else {
		fmt.Printf("  Overall: ❌ %s\n", result.Overall.Message)
	}
	fmt.Println()
}

// printCheck displays a single check result
func (r *Report) printCheck(name string, check Check, color bool) {
	symbol := "✓"
	status := "PASS"
	colorStart, colorEnd := "", ""

	if !check.Passed {
		if check.Warning {
			symbol = "⚠"
			status = "WARN"
			if color {
				colorStart = ansiYellow
				colorEnd = ansiReset
			}
		} else {
			symbol = "✗"
			status = "FAIL"
			if color {
				colorStart = ansiRed
				colorEnd = ansiReset
			}
		}
	}

	message := check.Message
	if check.Duration > 0 {
		message = fmt.Sprintf("%s (%.1fs)", message, check.Duration.Seconds())
	}

	fmt.Printf("  %s%s %-20s %s - %s%s\n", colorStart, symbol, name+":", status, message, colorEnd)

	// Display detailed command output for failed operations (not warnings)
	if !check.Passed && !check.Warning && check.CommandResult != nil {
		fmt.Println("    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Printf("    Failed Command: %s\n", check.CommandResult.Command)

		if check.CommandResult.Stdout != "" {
			fmt.Println("    Standard Output:")
			// Indent each line of stdout
			lines := strings.Split(check.CommandResult.Stdout, "\n")
			for _, line := range lines {
				if line != "" {
					fmt.Printf("    │ %s\n", line)
				}
			}
		}

		if check.CommandResult.Stderr != "" {
			fmt.Println("    Error Output:")
			// Indent each line of stderr
			lines := strings.Split(check.CommandResult.Stderr, "\n")
			for _, line := range lines {
				if line != "" {
					fmt.Printf("    │ %s\n", line)
				}
			}
		}

		fmt.Println("    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	}
}
