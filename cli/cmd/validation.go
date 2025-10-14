/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
// Package cmd implements validation helpers for CLI flags.
package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// ValidateWorkloadType checks if the provided workload type is valid
func ValidateWorkloadType(workloadType string) error {
	validTypes := map[string]bool{
		WorkloadTypeWrite:  true,
		WorkloadTypeRead:   true,
		WorkloadTypeMixed:  true,
		WorkloadTypeDelete: true,
		WorkloadTypeList:   true,
		WorkloadTypeMock:   true,
	}

	if !validTypes[workloadType] {
		return fmt.Errorf(ErrInvalidWorkloadType, workloadType)
	}
	return nil
}

// ValidateS3Flags ensures all required S3 flags are present for non-mock workloads
func ValidateS3Flags(cmd *cobra.Command, workloadType string) error {
	// Mock mode doesn't require S3 flags
	if workloadType == WorkloadTypeMock {
		return nil
	}

	endpoints := collectNormalizedEndpoints(cmd)
	if len(endpoints) == 0 {
		return fmt.Errorf(ErrMissingEndpoint, workloadType)
	}

	accessKey, _ := cmd.Flags().GetString("access-key")
	if accessKey == "" {
		return fmt.Errorf(ErrMissingAccessKey, workloadType)
	}

	secretKey, _ := cmd.Flags().GetString("secret-key")
	if secretKey == "" {
		return fmt.Errorf(ErrMissingSecretKey, workloadType)
	}

	bucket, _ := cmd.Flags().GetString("bucket")
	if bucket == "" {
		return fmt.Errorf(ErrMissingBucket, workloadType)
	}

	return nil
}

// ValidateDurationOrCount ensures duration and object-count are not both specified
func ValidateDurationOrCount(cmd *cobra.Command) error {
	objectCount, _ := cmd.Flags().GetInt("object-count")
	duration, _ := cmd.Flags().GetString("duration")

	// Only error if BOTH are specified - it's OK if neither is specified (will default to 100 objects)
	if objectCount != 0 && duration != "" {
		return errors.New("cannot specify both --object-count and --duration")
	}
	return nil
}

// ValidateRunCommand performs all validation for the run command
func ValidateRunCommand(cmd *cobra.Command, args []string) error {
	workloadType := args[0]

	// Validate S3 flags
	if err := ValidateS3Flags(cmd, workloadType); err != nil {
		// For validation errors, show usage to help the user
		cmd.SilenceUsage = false
		return err
	}

	if err := validateAuthVersion(cmd, workloadType); err != nil {
		cmd.SilenceUsage = false
		return err
	}

	if workloadType == WorkloadTypeList {
		if err := validateListFlagCompatibility(cmd); err != nil {
			cmd.SilenceUsage = false
			return err
		}
	}

	// Validate duration vs object-count
	if err := ValidateDurationOrCount(cmd); err != nil {
		cmd.SilenceUsage = false
		return err
	}
	return nil
}

func validateListFlagCompatibility(cmd *cobra.Command) error {
	cleanup, _ := cmd.Flags().GetBool("cleanup")
	if cleanup {
		return fmt.Errorf(ErrFlagNotSupported, "--cleanup", WorkloadTypeList)
	}

	if createPrefixFlag := cmd.Flags().Lookup("create-prefix"); createPrefixFlag != nil {
		createPrefix, _ := cmd.Flags().GetBool("create-prefix")
		if createPrefix {
			return fmt.Errorf(ErrFlagNotSupported, "--create-prefix", WorkloadTypeList)
		}
	}

	return nil
}

func validateAuthVersion(cmd *cobra.Command, workloadType string) error {
	if workloadType == WorkloadTypeMock {
		return nil
	}

	authVersion, _ := cmd.Flags().GetInt("auth-version")
	if authVersion == 0 {
		authVersion = 4
	}
	if authVersion != 2 && authVersion != 4 {
		return fmt.Errorf(ErrInvalidAuthVersion, authVersion)
	}
	return nil
}

func collectNormalizedEndpoints(cmd *cobra.Command) []string {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	var endpoints []string
	if f := cmd.Flags().Lookup("endpoints"); f != nil {
		endpoints, _ = cmd.Flags().GetStringSlice("endpoints")
	}

	combined := make([]string, 0, len(endpoints)+1)
	seen := make(map[string]struct{}, len(endpoints)+1)
	appendEndpoint := func(val string) {
		trimmed := strings.TrimSpace(val)
		if trimmed == "" {
			return
		}
		if _, ok := seen[trimmed]; ok {
			return
		}
		seen[trimmed] = struct{}{}
		combined = append(combined, trimmed)
	}

	appendEndpoint(endpoint)
	for _, ep := range endpoints {
		appendEndpoint(ep)
	}

	return combined
}
