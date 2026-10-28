package nicplan

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
)

const (
	blockIndent = "  "
	rowIndent   = "    "
	none        = "-"
	unknown     = "?"
)

// RenderOptions carry what verify knows beside the command's output.
type RenderOptions struct {
	// Bridge marks a container on a bridge network, which sees only its own interfaces.
	Bridge bool
	// Endpoints are the endpoints given to the planner, after slicing.
	Endpoints []string
	// Slice is the host's slice; SliceCount, when above 1, says the endpoints were sliced.
	Slice, SliceCount int
	// Threads is the concurrency limit P that the permit shares divide.
	Threads int
	// PolicyRules are the host's routing policy rules other than the defaults.
	PolicyRules []string
	// PolicyRulesUnread says the rules could not be listed.
	PolicyRulesUnread bool
}

// Render writes the inventory table and, when the output has one, the plan.
func Render(w io.Writer, out *Output, opts RenderOptions) {
	engine := out.Engine.Version
	if engine == "" {
		engine = "unknown version"
	}
	_, _ = fmt.Fprintf(w, "%sNICs (engine %s):\n", blockIndent, engine)
	if opts.Bridge {
		_, _ = fmt.Fprintf(w, "%sBridge network: verify sees only the container's interfaces, not the host's.\n", rowIndent)
	}
	renderInventory(w, out.Inventory)
	if out.Plan != nil {
		renderPlan(w, out.Plan, opts)
	}
}

func renderInventory(w io.Writer, inventory Inventory) {
	devices := make(map[string]RdmaDevice, len(inventory.RdmaDevices))
	probed := true
	for _, device := range inventory.RdmaDevices {
		devices[device.Name] = device
		if device.DcSupported == nil || device.ActiveMTU == nil {
			probed = false
		}
	}
	if len(inventory.Nics) == 0 {
		_, _ = fmt.Fprintf(w, "%s(no interfaces)\n", rowIndent)
		return
	}
	sharing := sharedPorts(inventory.Nics)

	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, rowIndent+"INTERFACE\tSTATE\tADDRESS\tSPEED\tMTU\tNUMA\tRDMA\tPORT\tGID\tDC\tROCE MTU\tSHARES PORT WITH")
	for _, nic := range inventory.Nics {
		_, _ = fmt.Fprintln(table, rowIndent+strings.Join(inventoryRow(nic, devices, sharing[nic.Name]), "\t"))
	}
	_ = table.Flush()
	if !probed {
		_, _ = fmt.Fprintf(w, "%sDC support and the RoCE MTU are read with --use-rdma.\n", rowIndent)
	}
}

// inventoryRow is one NIC's cells of the inventory table.
func inventoryRow(nic Nic, devices map[string]RdmaDevice, sharing []string) []string {
	state := "down"
	if nic.Up {
		state = "up"
	}
	device, port, gid, dc, roceMTU := none, none, none, none, none
	if nic.RdmaDevice != "" {
		device = nic.RdmaDevice
		if ports, indexes := roceV2Gids(nic); len(ports) > 0 {
			port, gid = strings.Join(ports, ","), strings.Join(indexes, ",")
		}
		if d, ok := devices[nic.RdmaDevice]; ok {
			dc, roceMTU = yesNo(d.DcSupported), count(d.ActiveMTU)
		}
	}
	return []string{nic.Name, state, orNone(strings.Join(nic.Addresses, ",")), speed(nic.SpeedMbps), count(nic.MTU),
		count(nic.NumaNode), device, port, gid, dc, roceMTU, orNone(strings.Join(sharing, ","))}
}

func renderPlan(w io.Writer, plan *Plan, opts RenderOptions) {
	renderPlanHeader(w, plan, opts)
	renderUse(w, plan, opts.Threads)
	for i, group := range plan.Groups {
		_, _ = fmt.Fprintf(w, "%sGroup %d: %s; %s, %s\n", rowIndent, i+1, strings.Join(group.Nics, ", "),
			plural(len(group.Endpoints), "endpoint"), plural(len(group.Paths), "Path"))
		for _, path := range group.Paths {
			_, _ = fmt.Fprintf(w, "%s  %s → %s\n", rowIndent, path.Nic, path.Endpoint)
		}
	}
	for _, failure := range plan.Failures {
		_, _ = fmt.Fprintf(w, "%s✗ %s\n", rowIndent, describeFinding(failure))
	}
	for _, warning := range plan.Warnings {
		_, _ = fmt.Fprintf(w, "%s⚠ %s\n", rowIndent, describeFinding(warning))
	}
	renderPolicyRules(w, opts)
}

func renderPlanHeader(w io.Writer, plan *Plan, opts RenderOptions) {
	status := "ok"
	if n := len(plan.Failures); n > 0 {
		status = plural(n, "failure")
	}
	mode := "--interfaces " + strings.Join(plan.Interfaces, ",")
	if plan.Rdma {
		mode += ", RDMA"
	}
	_, _ = fmt.Fprintf(w, "%sNIC plan (%s): %s\n", blockIndent, mode, status)

	endpoints := "Endpoints"
	if opts.SliceCount > 1 {
		endpoints = fmt.Sprintf("Endpoints (slice %d of %d, %s)", opts.Slice+1, opts.SliceCount, sliceRole(opts.Slice))
	}
	_, _ = fmt.Fprintf(w, "%s%s: %s\n", rowIndent, endpoints, orNone(strings.Join(opts.Endpoints, ", ")))
	_, _ = fmt.Fprintf(w, "%sNIC set: %s\n", rowIndent, orNone(strings.Join(plan.NicSet, ", ")))
}

// renderUse writes the permit shares at threads and each interface's part in the plan.
func renderUse(w io.Writer, plan *Plan, threads int) {
	permits := PermitShares(threads, len(plan.NicSet))
	if len(plan.NicSet) > 0 {
		_, _ = fmt.Fprintf(w, "%sPermits at --threads %d: %s\n", rowIndent, threads, describePermits(permits, len(plan.NicSet)))
	}
	share := make(map[string]int, len(permits.PerNic))
	for i, permit := range permits.PerNic {
		share[plan.NicSet[i]] = permit
	}

	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, nic := range plan.Nics {
		_, _ = fmt.Fprintf(table, "%s%s\t%s\n", rowIndent, nic.Name, describeUse(nic, share, len(plan.Endpoints)))
	}
	_ = table.Flush()
}

func renderPolicyRules(w io.Writer, opts RenderOptions) {
	switch {
	case opts.PolicyRulesUnread:
		_, _ = fmt.Fprintf(w, "%sPolicy routing: not checked; `%s` failed on the host.\n", rowIndent, strings.Join(PolicyRulesCommand, " "))
	case len(opts.PolicyRules) > 0:
		_, _ = fmt.Fprintf(w, "%sPolicy routing: %s besides the defaults. NIC choice follows connected subnets and the main table only:\n",
			rowIndent, plural(len(opts.PolicyRules), "rule"))
		for _, rule := range opts.PolicyRules {
			_, _ = fmt.Fprintf(w, "%s  %s\n", rowIndent, rule)
		}
	}
}

// Summary is the one-line result for the verify check.
func Summary(plan *Plan) string {
	if n := len(plan.Failures); n > 0 {
		return fmt.Sprintf("%s: %s", plural(n, "failure"), describeFinding(plan.Failures[0]))
	}
	paths := 0
	for _, group := range plan.Groups {
		paths += len(group.Paths)
	}
	summary := fmt.Sprintf("%s: %s, %s", plan.Mode, plural(len(plan.NicSet), "NIC"), plural(paths, "Path"))
	if n := len(plan.Warnings); n > 0 {
		summary += ", " + plural(n, "warning")
	}
	return summary
}

func describePermits(permits Permits, nics int) string {
	switch {
	case permits.Limit <= 0:
		return "unlimited, shared by the NICs in rotation"
	case permits.Shared:
		return fmt.Sprintf("%s shared by %d NICs in rotation, fewer than one per NIC", plural(permits.Limit, "permit"), nics)
	case permits.Limit%nics == 0:
		return fmt.Sprintf("%d per NIC", permits.Limit/nics)
	default:
		return fmt.Sprintf("%d or %d per NIC, the first %d NICs taking one more", permits.Limit/nics, permits.Limit/nics+1, permits.Limit%nics)
	}
}

// describeUse says whether a NIC is used, from which source address and RDMA GID; share holds
// the permits of each NIC when the limit is divided, and is empty when the NICs share it.
func describeUse(nic PlannedNic, share map[string]int, endpoints int) string {
	if !nic.Used {
		return describeUnused(nic.Reason)
	}
	use := "used"
	if nic.Address != "" {
		use += " from " + nic.Address
		if nic.GidIndex != nil {
			use += fmt.Sprintf(" (GID %d)", *nic.GidIndex)
		}
	}
	use += ": " + plural(nic.Paths, "Path")
	if permits, ok := share[nic.Name]; ok {
		use += ", " + plural(permits, "permit")
	}
	switch {
	case len(nic.Serves) > 1 && len(nic.Serves) == endpoints:
		use += fmt.Sprintf("; serves all %d endpoints", endpoints)
	case len(nic.Serves) > 0:
		use += "; serves " + strings.Join(nic.Serves, ", ")
	}
	return use
}

func describeUnused(reason *Reason) string {
	if reason == nil {
		return "not used"
	}
	text := reason.Text
	if text == "" {
		text = reason.Code
	}
	if reason.With != "" {
		text += " with " + reason.With
	}
	return "not used: " + text
}

func describeFinding(finding Finding) string {
	text := finding.Text
	if text == "" {
		text = finding.Code
	}
	if finding.Fix != "" {
		text += " (fix: " + finding.Fix + ")"
	}
	return text
}

// sharedPorts maps each NIC to the other NICs on its physical port.
func sharedPorts(nics []Nic) map[string][]string {
	byPort := make(map[string][]string)
	for _, nic := range nics {
		if nic.Port != "" {
			byPort[nic.Port] = append(byPort[nic.Port], nic.Name)
		}
	}
	sharing := make(map[string][]string)
	for _, names := range byPort {
		if len(names) < 2 {
			continue
		}
		for _, name := range names {
			for _, other := range names {
				if other != name {
					sharing[name] = append(sharing[name], other)
				}
			}
		}
	}
	return sharing
}

// roceV2Gids are the ports of the NIC's RoCE v2 GIDs and, for each of its addresses in order,
// the index of its RoCE v2 GID, on port 1 when it has one there, or none. With no RoCE v2 GID
// the result is empty. The planner's chosen address and GID are the plan's, not these.
func roceV2Gids(nic Nic) ([]string, []string) {
	var ports, indexes []string
	found := false
	for _, address := range nic.Addresses {
		gid, ok := roceV2Gid(nic.Gids, strings.SplitN(address, "/", 2)[0])
		if !ok {
			indexes = append(indexes, none)
			continue
		}
		found = true
		indexes = append(indexes, strconv.Itoa(gid.Index))
		if port := strconv.Itoa(gid.Port); !slices.Contains(ports, port) {
			ports = append(ports, port)
		}
	}
	if !found {
		return nil, nil
	}
	return ports, indexes
}

// roceV2Gid is the RoCE v2 GID of address, on port 1 when it has one there.
func roceV2Gid(gids []Gid, address string) (Gid, bool) {
	var found *Gid
	for i := range gids {
		gid := gids[i]
		if !gid.RoceV2 || gid.Address != address {
			continue
		}
		if gid.Port == 1 {
			return gid, true
		}
		if found == nil {
			found = &gids[i]
		}
	}
	if found != nil {
		return *found, true
	}
	return Gid{}, false
}

func sliceRole(slice int) string {
	if slice == 0 {
		return "entry node"
	}
	return "worker"
}

func speed(mbps *int) string {
	switch {
	case mbps == nil:
		return unknown
	case *mbps >= 1000 && *mbps%1000 == 0:
		return fmt.Sprintf("%dG", *mbps/1000)
	case *mbps >= 1000:
		return fmt.Sprintf("%.1fG", float64(*mbps)/1000)
	default:
		return fmt.Sprintf("%dM", *mbps)
	}
}

func count(value *int) string {
	if value == nil {
		return unknown
	}
	return strconv.Itoa(*value)
}

func yesNo(value *bool) string {
	switch {
	case value == nil:
		return unknown
	case *value:
		return "yes"
	default:
		return "no"
	}
}

func orNone(value string) string {
	if value == "" {
		return none
	}
	return value
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
