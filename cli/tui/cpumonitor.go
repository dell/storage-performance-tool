/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/shirou/gopsutil/v3/cpu"
)

// CPUMonitor provides cross-platform CPU usage monitoring
type CPUMonitor struct {
	interval   time.Duration
	currentCPU float64
	mu         sync.RWMutex
	stopCh     chan struct{}
	started    bool
}

// NewCPUMonitor creates a new CPU monitor with the specified sampling interval
func NewCPUMonitor(interval time.Duration) *CPUMonitor {
	if interval <= 0 {
		interval = time.Second // Default to 1 second
	}

	return &CPUMonitor{
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins CPU monitoring in a background goroutine
func (c *CPUMonitor) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil // Already started
	}

	// Test if CPU monitoring is available on this platform
	_, err := cpu.Percent(0, false)
	if err != nil {
		logging.LogError("cpumonitor", "CPU monitoring not available on this platform", err)
		// Still mark as started but CPU will remain 0%
		c.started = true
		return nil
	}

	c.started = true
	go c.monitorLoop()

	logging.LogInfo("cpumonitor", "CPU monitoring started",
		"interval", c.interval)

	return nil
}

// Stop stops the CPU monitoring goroutine
func (c *CPUMonitor) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return
	}

	close(c.stopCh)
	c.started = false

	logging.LogInfo("cpumonitor", "CPU monitoring stopped")
}

// GetCPUPercent returns the current CPU usage percentage (0-100)
// Returns 0 if monitoring is not available or not started
func (c *CPUMonitor) GetCPUPercent() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentCPU
}

// IsStarted returns true if the CPU monitor is currently running
func (c *CPUMonitor) IsStarted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started
}

// monitorLoop runs the CPU monitoring in a background goroutine
func (c *CPUMonitor) monitorLoop() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Initial reading to "prime" the CPU monitoring
	// The first call to cpu.Percent() may return 0, so we discard it
	_, _ = cpu.Percent(0, false)

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.updateCPU()
		}
	}
}

// updateCPU fetches the current CPU usage and updates the stored value
func (c *CPUMonitor) updateCPU() {
	// Get CPU percentage averaged across all cores
	// interval=0 means non-blocking, use cached value from previous call
	percentages, err := cpu.Percent(0, false)
	if err != nil {
		logging.LogError("cpumonitor", "Failed to get CPU percentage", err)
		return
	}

	if len(percentages) == 0 {
		return
	}

	// Use the first value which represents overall CPU usage
	newCPU := percentages[0]

	// Sanity check - CPU percentage should be 0-100
	if newCPU < 0 {
		newCPU = 0
	} else if newCPU > 100 {
		newCPU = 100
	}

	c.mu.Lock()
	c.currentCPU = newCPU
	c.mu.Unlock()

	logging.LogDebug("cpumonitor", "CPU usage updated",
		"percent", newCPU)
}
