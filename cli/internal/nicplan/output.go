// Package nicplan reads the engine image's NIC inventory and plan, the spt.nic-plan output of
// /opt/spt/spt-nic-plan, and renders it for spt verify. The engine owns the planner and its
// codes; this package keys on codes only to count findings, and shows unknown ones verbatim.
package nicplan

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// Format is the output's format name.
	Format = "spt.nic-plan"
	// Version is the newest output version this CLI reads. Additions keep it; a removed or
	// renamed field, or a changed meaning, increments it.
	Version = 1
)

// ErrImagePredatesNicPlan reports an engine image without the NIC inventory command.
var ErrImagePredatesNicPlan = errors.New("this image predates NIC inventory; use an SPT 5.17 or later image")

// Output is one host's spt.nic-plan document.
type Output struct {
	Format    string    `json:"format"`
	Version   int       `json:"version"`
	Engine    Engine    `json:"engine"`
	Host      string    `json:"host"`
	Inventory Inventory `json:"inventory"`
	// Plan is present only when the command was given --interfaces.
	Plan *Plan `json:"plan"`
}

// Engine is the engine build that produced the output.
type Engine struct {
	Version  string `json:"version"`
	Revision string `json:"revision"`
}

// Inventory lists the host's interfaces and RDMA devices.
type Inventory struct {
	Nics        []Nic        `json:"nics"`
	RdmaDevices []RdmaDevice `json:"rdmaDevices"`
	Sysctls     Sysctls      `json:"sysctls"`
}

// Nic is one interface as the inventory reads it. Null numbers are unknown.
type Nic struct {
	Name           string   `json:"name"`
	Kind           string   `json:"kind"`
	Up             bool     `json:"up"`
	Addresses      []string `json:"addresses"`
	SpeedMbps      *int     `json:"speedMbps"`
	MTU            *int     `json:"mtu"`
	NumaNode       *int     `json:"numaNode"`
	Port           string   `json:"port"`
	LowerDevices   []string `json:"lowerDevices"`
	BondMaster     string   `json:"bondMaster"`
	BondHashPolicy string   `json:"bondHashPolicy"`
	RdmaDevice     string   `json:"rdmaDevice"`
	Gids           []Gid    `json:"gids"`
	Sysctls        Sysctls  `json:"sysctls"`
}

// Gid is one IPv4 GID of an RDMA port.
type Gid struct {
	Device  string `json:"device"`
	Port    int    `json:"port"`
	Index   int    `json:"index"`
	Address string `json:"address"`
	RoceV2  bool   `json:"roceV2"`
}

// RdmaDevice is one RDMA device. DC support and the active MTU are known only with --rdma.
type RdmaDevice struct {
	Name        string     `json:"name"`
	Driver      string     `json:"driver"`
	DcSupported *bool      `json:"dcSupported"`
	ActiveMTU   *int       `json:"activeMtu"`
	Ports       []RdmaPort `json:"ports"`
}

// RdmaPort is one port of an RDMA device.
type RdmaPort struct {
	Number    int    `json:"number"`
	State     string `json:"state"`
	LinkLayer string `json:"linkLayer"`
}

// Sysctls are the ARP and reverse-path settings, for net.ipv4.conf.all or one interface.
type Sysctls struct {
	ArpIgnore   *int `json:"arpIgnore"`
	ArpAnnounce *int `json:"arpAnnounce"`
	RpFilter    *int `json:"rpFilter"`
}

// Plan is the planner's result for the given NIC set and endpoints.
type Plan struct {
	Interfaces []string          `json:"interfaces"`
	Mode       string            `json:"mode"`
	Rdma       bool              `json:"rdma"`
	OK         bool              `json:"ok"`
	NicSet     []string          `json:"nicSet"`
	Nics       []PlannedNic      `json:"nics"`
	Endpoints  []PlannedEndpoint `json:"endpoints"`
	Ports      []SharedPort      `json:"ports"`
	Groups     []Group           `json:"groups"`
	Failures   []Finding         `json:"failures"`
	Warnings   []Finding         `json:"warnings"`
}

// PlannedNic is one interface's part in the plan.
type PlannedNic struct {
	Name     string   `json:"name"`
	Used     bool     `json:"used"`
	Address  string   `json:"address"`
	GidIndex *int     `json:"gidIndex"`
	Serves   []string `json:"serves"`
	Paths    int      `json:"paths"`
	// Reason says why an interface is unused.
	Reason *Reason `json:"reason"`
}

// Reason is why an interface is unused; With names the NIC it shares a port with.
type Reason struct {
	Code string `json:"code"`
	Text string `json:"text"`
	With string `json:"with"`
}

// PlannedEndpoint is one endpoint the plan was given.
type PlannedEndpoint struct {
	Endpoint      string      `json:"endpoint"`
	Address       string      `json:"address"`
	Candidates    []string    `json:"candidates"`
	TierSpeedMbps *int        `json:"tierSpeedMbps"`
	Kept          []string    `json:"kept"`
	Reductions    []Reduction `json:"reductions"`
}

// Reduction is a candidate NIC dropped for one endpoint.
type Reduction struct {
	Nic  string `json:"nic"`
	Code string `json:"code"`
	With string `json:"with"`
}

// SharedPort is a physical port that backs more than one interface.
type SharedPort struct {
	ID   string   `json:"id"`
	Nics []string `json:"nics"`
}

// Group is a pairing group: endpoints served by the same chosen NICs, and their Paths.
type Group struct {
	Nics      []string `json:"nics"`
	Endpoints []string `json:"endpoints"`
	Paths     []Path   `json:"paths"`
}

// Path is a NIC paired with one endpoint it serves.
type Path struct {
	Nic      string `json:"nic"`
	Endpoint string `json:"endpoint"`
}

// Finding is a failure or a warning, with the NIC and endpoint it concerns and its fix.
type Finding struct {
	Code     string `json:"code"`
	Text     string `json:"text"`
	Nic      string `json:"nic"`
	Endpoint string `json:"endpoint"`
	Fix      string `json:"fix"`
}

// Parse reads the command's stdout. It rejects another format and a newer version.
func Parse(stdout []byte) (*Output, error) {
	var out Output
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(stdout))), &out); err != nil {
		return nil, fmt.Errorf("unreadable %s output: %w", Format, err)
	}
	if out.Format != Format {
		return nil, fmt.Errorf("unexpected output format %q; expected %q", out.Format, Format)
	}
	if out.Version > Version {
		return nil, fmt.Errorf("image reports nic-plan version %d; this CLI reads %d; update the CLI", out.Version, Version)
	}
	if out.Version < 1 {
		return nil, fmt.Errorf("unexpected nic-plan version %d", out.Version)
	}
	return &out, nil
}

// CommandMissing reports whether a failed docker exec of the command means the image lacks it.
func CommandMissing(stderr string) bool {
	text := strings.ToLower(stderr)
	return strings.Contains(text, "spt-nic-plan") &&
		(strings.Contains(text, "no such file or directory") || strings.Contains(text, "executable file not found"))
}
