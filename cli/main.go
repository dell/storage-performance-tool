/*
Copyright © 2025 Dell Technologies
*/

// Package main is the entry point for the spt application.
// spt is a CLI wrapper for the Spt benchmarking tool,
// providing an intuitive interface for S3-compatible storage performance testing.
package main

import "github.com/dell/storage-performance-tool/cli/cmd"

func main() {
	cmd.Execute()
}
