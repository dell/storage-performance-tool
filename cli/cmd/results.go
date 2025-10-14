/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
// Package cmd implements the command-line interface for spt.
package cmd

import (
	"fmt"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/spf13/cobra"
)

// resultsCmd represents the results command
var resultsCmd = &cobra.Command{
	Use:   "results",
	Short: "Manages and inspects past benchmark results.",
	Long: `The 'results' command group is for listing, inspecting, and managing past benchmark results.
This command is intended for future scope to provide insights into previous Spt runs.`,
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		// Load .env so any environment-based defaults are in place.
		_ = config.LoadDotEnv()
		return nil
	},
	Run: func(_ *cobra.Command, _ []string) {
		fmt.Println("results called")
		// TODO: Implement results management logic here
	},
}

func init() {
	rootCmd.AddCommand(resultsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// resultsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which only run when this command
	// is called directly, e.g.:
	// resultsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
