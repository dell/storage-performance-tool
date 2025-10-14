/*
Copyright © 2025 Dell Technologies
*/

// Package cmd implements the command-line interface for spt.
// It provides the root command and all subcommands for the application.
package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/spf13/cobra"
)

// isTesting is a global variable to indicate if the application is running in test mode.
// This is used to prevent the TUI from starting during unit tests.
var isTesting bool

// Logging configuration variables
var (
	Logger    *slog.Logger
	debug     bool
	logLevel  string
	logFile   string
	logAppend bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "spt",
	Short: "A CLI wrapper for the Spt benchmarking tool for S3-compatible storage.",
	Long: `spt is a command-line interface (CLI) wrapper designed to simplify the use of the Spt benchmarking tool for S3-compatible storage.

It provides a user-friendly interface to execute various benchmark tests (e.g., write, read, mixed, delete) against S3 endpoints, manage Spt Docker images, and inspect test results.`,
	SilenceUsage: true, // Don't auto-print usage on runtime errors
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		// Load .env files before logger so flags/env fallbacks can read them.
		// Local .env overrides $HOME/.env; neither overrides existing OS env.
		// Errors are non-fatal; they will be logged after logger init.
		res := config.LoadDotEnv()

		// Initialize logger after env is available
		if err := initializeLogger(); err != nil {
			return err
		}

		// Log .env loading summary (no values)
		if len(res.LoadedFiles) > 0 {
			logging.GetLogger().Info("loaded .env", "files", res.LoadedFiles)
		}
		for _, e := range res.Errors {
			logging.GetLogger().Warn(".env parse warning", "error", e.Error())
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if !isTesting && len(args) == 0 && cmd.Flags().ArgsLenAtDash() == -1 {
			if err := tui.StartTUI(); err != nil {
				cmd.PrintErrf("Error starting TUI: %v\n", err)
				os.Exit(1)
			}
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// initializeLogger sets up the slog logger based on command-line flags
func initializeLogger() error {
	// If debug flag is set, override log level
	if debug {
		logLevel = "debug"
	}

	// Parse log level
	var level slog.Level
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return fmt.Errorf("invalid log level: %s", logLevel)
	}

	// Open log file
	flags := os.O_CREATE | os.O_WRONLY
	if logAppend {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	// #nosec G304: Accept user-specified log file path for CLI logging.
	file, err := os.OpenFile(logFile, flags, 0600)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// Create handler with options
	opts := &slog.HandlerOptions{
		Level: level,
	}

	// Create text handler for file output
	handler := slog.NewTextHandler(file, opts)

	// Create logger
	Logger = slog.New(handler)

	// Set the logger in the logging package
	logging.SetLogger(Logger)

	// Log initial message
	Logger.Info("spt starting",
		"version", "1.0.0",
		"log_level", logLevel,
		"log_file", logFile,
		"log_append", logAppend)

	return nil
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// Logging flags
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Run in debug mode (alias for --log-level debug)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logFile, "log-file", "spt.log", "Log file path")
	rootCmd.PersistentFlags().BoolVar(&logAppend, "log-append", false, "Append to existing log file (default is to create new)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
