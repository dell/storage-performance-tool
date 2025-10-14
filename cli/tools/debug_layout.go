/*
Copyright © 2025 Dell Technologies
*/

// Debug utility for testing TUI layouts
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dell/storage-performance-tool/cli/tui"
)

func main() {
	var (
		width    = flag.Int("width", 120, "Terminal width")
		height   = flag.Int("height", 40, "Terminal height")
		output   = flag.String("output", "layout_debug.txt", "Output file")
		withData = flag.Bool("data", true, "Include sample data")
		calcOnly = flag.Bool("calc", false, "Show only calculations, no rendering")
	)
	flag.Parse()

	fmt.Printf("TUI Layout Debugger\n")
	fmt.Printf("===================\n")
	fmt.Printf("Terminal: %dx%d\n", *width, *height)
	fmt.Printf("Output: %s\n", *output)
	fmt.Printf("With Data: %v\n", *withData)
	fmt.Printf("-------------------\n\n")

	if *calcOnly {
		// Show just the calculations
		tui.PrintLayoutCalculations(*width, *height)
		return
	}

	// Generate full layout debug
	err := tui.RenderSampleTUIToFile(*output, *width, *height, *withData)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Also print calculations to console
	tui.PrintLayoutCalculations(*width, *height)
}
