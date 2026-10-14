// Package driverstats defines the driver statistics carried by engine metrics rows.
package driverstats

import (
	"fmt"
	"strings"
)

// Stats is the driver_stats object of a metrics row. Counter and gauge names belong to the
// storage driver: the CLI keeps them as reported, including names it does not know.
type Stats struct {
	// Driver names the storage driver that reported the statistics.
	Driver   string           `json:"driver,omitempty"`
	Counters map[string]int64 `json:"counters,omitempty"`
	Gauges   map[string]int64 `json:"gauges,omitempty"`
	// Reporting is the number of contributors summed into a fleet total. A node row has none.
	Reporting int `json:"reporting,omitempty"`
	// Partial marks a fleet total that does not cover every expected contributor alike.
	Partial bool `json:"partial,omitempty"`
	// Drivers lists the driver names seen when contributors disagree. Such a row has no totals.
	Drivers []string `json:"drivers,omitempty"`
}

// Counted reports whether a counter is above zero: the driver did work. A gauge is a level and
// does not count. The engine keeps the row of a finished step whose driver did work.
func (s *Stats) Counted() bool {
	if s == nil {
		return false
	}
	for _, count := range s.Counters {
		if count > 0 {
			return true
		}
	}
	return false
}

// ValidateTerminal reports why the statistics of a terminal row must not be stored.
// fleetContributors is the number of contributors a fleet total must cover; zero validates a
// node row, which carries the statistics of one driver and no contributor count.
func ValidateTerminal(stats *Stats, fleetContributors int) error {
	if stats == nil {
		return fmt.Errorf("driver statistics are missing")
	}
	if len(stats.Drivers) > 0 {
		return fmt.Errorf("contributors report different drivers (%s)", strings.Join(stats.Drivers, ", "))
	}
	if stats.Partial {
		return fmt.Errorf("statistics are partial (%d contributors reporting)", stats.Reporting)
	}
	if strings.TrimSpace(stats.Driver) == "" {
		return fmt.Errorf("driver name is missing")
	}
	if fleetContributors > 0 && stats.Reporting != fleetContributors {
		return fmt.Errorf("fleet total covers %d of %d contributors", stats.Reporting, fleetContributors)
	}
	return nil
}
