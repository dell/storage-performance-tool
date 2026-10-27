package verification

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
)

// stepListNics lists the host's NICs in the verify container and, with --interfaces, checks
// the host's NIC plan. Without --interfaces the listing is information only.
func (v *Verifier) stepListNics(host *hostparse.HostInfo, result *NodeResult) {
	planning := len(v.config.Interfaces) > 0
	if v.config.ShowProgress {
		if planning {
			fmt.Print("  ⏳ Planning NICs in container...")
		} else {
			fmt.Print("  ⏳ Listing NICs in container...")
		}
	}
	listing, check := v.listNics(host, result.containerID)
	result.NICs = listing
	if planning {
		result.NicPlan = check
	}
	if v.config.ShowProgress {
		switch {
		case check.Passed:
			fmt.Println(" ✅")
		case check.Warning || !planning:
			printProgressWarning(check.Message)
		default:
			fmt.Println(" ❌")
		}
	}
}

// listNics runs the image's NIC planner on the host, given the host's slice of the endpoints.
func (v *Verifier) listNics(host *hostparse.HostInfo, containerID string) (*NicListing, Check) {
	start := v.timeProvider.Now()
	if containerID == "" {
		message := "No container to list NICs"
		return &NicListing{Note: message}, Check{Passed: false, Message: message, Duration: v.timeProvider.Since(start)}
	}

	planning := len(v.config.Interfaces) > 0
	options := nicplan.RenderOptions{
		Bridge:  v.config.NetworkMode == constants.BridgeNetworkMode,
		Threads: v.config.Threads,
	}
	var endpoints []string
	if planning {
		endpoints = v.config.Endpoints
		if v.config.SliceEndpoints {
			slice, sliceCount := v.sliceOf(host)
			endpoints = nicplan.SliceEndpoints(endpoints, sliceCount, slice)
			options.Slice, options.SliceCount = slice, sliceCount
		}
		options.Endpoints = endpoints
	}

	output, err := v.runNicPlan(host, containerID, endpoints)
	if err != nil {
		logging.LogWarn("verifier", "NIC listing failed", "host", host.Host, "error", err.Error())
		return &NicListing{Note: err.Error()}, Check{Passed: false, Message: err.Error(), Duration: v.timeProvider.Since(start), Error: err}
	}

	listing := &NicListing{Output: output, Options: options}
	if !planning {
		return listing, Check{Passed: true, Message: fmt.Sprintf("%d interfaces listed", len(output.Inventory.Nics)), Duration: v.timeProvider.Since(start)}
	}
	listing.Options.PolicyRules, listing.Options.PolicyRulesUnread = v.policyRules(host)

	plan := output.Plan
	check := Check{Passed: true, Message: nicplan.Summary(plan), Duration: v.timeProvider.Since(start)}
	switch {
	case !plan.OK || len(plan.Failures) > 0:
		check.Passed = false
	case len(plan.Warnings) > 0:
		check.Passed = false
		check.Warning = true
	}
	return listing, check
}

// runNicPlan runs the image command in the container; with --interfaces, its output must have a plan.
func (v *Verifier) runNicPlan(host *hostparse.HostInfo, containerID string, endpoints []string) (*nicplan.Output, error) {
	ctx, cancel := context.WithTimeout(context.Background(), constants.NicPlanTimeoutSecs*time.Second)
	defer cancel()
	command := append([]string{constants.DockerCommand, dockerExecCommand, containerID},
		nicplan.Args(v.config.Interfaces, endpoints, v.config.UseRdma)...)
	stdout, stderr, err := v.cmdExecutor.ExecuteCommand(ctx, host, command)
	switch {
	case err != nil && nicplan.CommandMissing(stderr):
		return nil, nicplan.ErrImagePredatesNicPlan
	case err != nil:
		return nil, fmt.Errorf("spt-nic-plan failed: %w%s", err, firstLine(stderr))
	}
	output, err := nicplan.Parse([]byte(stdout))
	if err == nil && len(v.config.Interfaces) > 0 && output.Plan == nil {
		return nil, errors.New("spt-nic-plan returned no plan")
	}
	return output, err
}

// sliceOf returns the host's slice and the slice count: run order, the entry node first.
func (v *Verifier) sliceOf(host *hostparse.HostInfo) (int, int) {
	for i, h := range v.hosts {
		if h == host {
			return i, len(v.hosts)
		}
	}
	return 0, len(v.hosts)
}

// policyRules lists the host's routing policy rules other than the defaults (item 6b). The
// image has no ip command, so it runs on the host, whose network the container shares.
func (v *Verifier) policyRules(host *hostparse.HostInfo) ([]string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stdout, _, err := v.cmdExecutor.ExecuteCommand(ctx, host, nicplan.PolicyRulesCommand)
	if err != nil {
		logging.LogDebug("verifier", "cannot list policy routing rules", "host", host.Host, "error", err.Error())
		return nil, true
	}
	return nicplan.NonDefaultPolicyRules(stdout), false
}

// printNics writes the host's NIC listing, or the note that replaces it.
func printNics(listing *NicListing) {
	if listing == nil {
		return
	}
	if listing.Output == nil {
		fmt.Printf("  NICs: not listed: %s\n", listing.Note)
		return
	}
	nicplan.Render(os.Stdout, listing.Output, listing.Options)
}

func firstLine(stderr string) string {
	for _, line := range strings.Split(stderr, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return ": " + line
		}
	}
	return ""
}
