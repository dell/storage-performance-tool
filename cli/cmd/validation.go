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

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
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
		WorkloadTypeTables: true,
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

	// Tables workload uses --table-bucket instead of --bucket
	if workloadType != WorkloadTypeTables {
		bucket, _ := cmd.Flags().GetString("bucket")
		if bucket == "" {
			return fmt.Errorf(ErrMissingBucket, workloadType)
		}
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

// ValidateMixedFlags validates flags specific to the mixed workload.
func ValidateMixedFlags(cmd *cobra.Command) error {
	get, _ := cmd.Flags().GetInt("get-distrib")
	put, _ := cmd.Flags().GetInt("put-distrib")
	del, _ := cmd.Flags().GetInt("delete-distrib")
	stat, _ := cmd.Flags().GetInt("stat-distrib")
	readItems, _ := cmd.Flags().GetString("read-items-file")
	deleteItems, _ := cmd.Flags().GetString("delete-items-file")
	duration, _ := cmd.Flags().GetString("duration")
	seedObjects, _ := cmd.Flags().GetInt("seed-objects")
	cleanup, _ := cmd.Flags().GetBool("cleanup")

	// Reject negative values before any sum check
	for name, v := range map[string]int{"get": get, "put": put, "delete": del, "stat": stat} {
		if v < 0 {
			return fmt.Errorf("negative distribution value for --%s-distrib (%d)", name, v)
		}
	}

	// Distribution must sum to exactly 100
	sum := get + put + del + stat
	if sum != 100 {
		return fmt.Errorf("distribution percentages sum to %d, must equal 100 (got: --get-distrib %d --put-distrib %d --delete-distrib %d --stat-distrib %d)",
			sum, get, put, del, stat)
	}

	// At least two non-zero distributions required
	nonZero := 0
	for _, v := range []int{get, put, del, stat} {
		if v > 0 {
			nonZero++
		}
	}
	if nonZero < 2 {
		return errors.New("at least two operation types must have a non-zero distribution")
	}

	// delete-distrib <= put-distrib unless an external delete seed file is provided
	if del > put && deleteItems == "" {
		return fmt.Errorf("delete-distrib (%d) exceeds put-distrib (%d): PUT must sustain the DELETE queue (use --delete-items-file to relax this constraint)", del, put)
	}

	// Duration is required — mixed workloads are time-based only
	if strings.TrimSpace(duration) == "" {
		return errors.New("--duration is required for mixed workload (e.g. --duration 2m)")
	}

	// Seed objects required unless an external read items file is provided
	if readItems == "" && seedObjects <= 0 {
		return errors.New("--seed-objects must be > 0 when --read-items-file is not provided")
	}

	// --cleanup is incompatible with external item files (we didn't create them)
	if cleanup && (readItems != "" || deleteItems != "") {
		return errors.New("--cleanup cannot be used with --read-items-file or --delete-items-file: spt did not create those objects")
	}

	return nil
}

func validateReadShuffleFlags(cmd *cobra.Command, workloadType string) error {
	shuffleChanged := cmd.Flags().Changed(flagReadShuffle)
	batchChanged := cmd.Flags().Changed(flagReadShuffleBatchSize)

	if workloadType != WorkloadTypeRead {
		if shuffleChanged {
			return fmt.Errorf(ErrFlagNotSupported, "--"+flagReadShuffle, workloadType)
		}
		if batchChanged {
			return fmt.Errorf(ErrFlagNotSupported, "--"+flagReadShuffleBatchSize, workloadType)
		}
		return nil
	}

	shuffleEnabled, _ := cmd.Flags().GetBool(flagReadShuffle)
	batchSize, _ := cmd.Flags().GetInt(flagReadShuffleBatchSize)

	if batchChanged && !shuffleEnabled {
		return errors.New(ErrReadShuffleBatchRequiresToggle)
	}
	if batchChanged && batchSize <= 0 {
		return errors.New(ErrReadShuffleBatchPositive)
	}
	if batchChanged && batchSize > constants.ReadShuffleMaxBatchSize {
		return fmt.Errorf(ErrReadShuffleBatchTooLarge, constants.ReadShuffleMaxBatchSize)
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

	if workloadType == WorkloadTypeMixed {
		if err := ValidateMixedFlags(cmd); err != nil {
			cmd.SilenceUsage = false
			return err
		}
	}

	if err := validateReadShuffleFlags(cmd, workloadType); err != nil {
		cmd.SilenceUsage = false
		return err
	}

	// Validate duration vs object-count
	if err := ValidateDurationOrCount(cmd); err != nil {
		cmd.SilenceUsage = false
		return err
	}

	// Validate part-size if specified
	if err := validatePartSize(cmd); err != nil {
		cmd.SilenceUsage = false
		return err
	}

	if err := validateObjectDataFlags(cmd); err != nil {
		cmd.SilenceUsage = false
		return err
	}

	return nil
}

func validateObjectDataFlags(cmd *cobra.Command) error {
	compressibility, _ := cmd.Flags().GetFloat64("object-data-compressibility")
	if compressibility < 0.0 || compressibility > 100.0 {
		return fmt.Errorf("--object-data-compressibility must be in range [0, 100]")
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

// validatePartSize checks that --part-size, when provided, is a valid parseable
// size and is smaller than --object-size (multipart with a single part is pointless).
// We intentionally do NOT enforce a minimum part size here — different S3-compatible
// storage systems have varying constraints, so we let the storage system reject
// invalid values at runtime rather than blocking up front.
func validatePartSize(cmd *cobra.Command) error {
	partSize, _ := cmd.Flags().GetString("part-size")

	mpuObjects, _ := cmd.Flags().GetInt("mpu-concurrent-objects")
	if mpuObjects < 0 {
		return fmt.Errorf("--mpu-concurrent-objects must be >= 0")
	}
	if mpuObjects > 0 && partSize == "" {
		return fmt.Errorf("--mpu-concurrent-objects can only be used when --part-size is set")
	}

	mpuParts, _ := cmd.Flags().GetInt("mpu-concurrent-parts")
	if mpuParts < 0 {
		return fmt.Errorf("--mpu-concurrent-parts must be >= 0")
	}
	if mpuParts > 0 && partSize == "" {
		return fmt.Errorf("--mpu-concurrent-parts can only be used when --part-size is set")
	}

	if partSize == "" {
		return nil
	}

	partBytes, err := sizeparse.Parse(partSize)
	if err != nil {
		return fmt.Errorf("invalid --part-size value %q: %w", partSize, err)
	}
	if partBytes <= 0 {
		return fmt.Errorf("--part-size must be a positive size (got %s)", partSize)
	}

	// If object size is set, part size must be smaller (otherwise MPU is pointless)
	objectSize, _ := cmd.Flags().GetString("object-size")
	if objectSize != "" {
		objBytes, err := sizeparse.Parse(objectSize)
		if err == nil && objBytes > 0 && partBytes >= objBytes {
			return fmt.Errorf("--part-size (%s) must be smaller than --object-size (%s)", partSize, objectSize)
		}
	}

	return nil
}
