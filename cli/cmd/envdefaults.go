package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"github.com/spf13/cobra"
)

// applyEnvDefaultsToRunFlags injects environment-provided defaults into run flags
// when the user did not explicitly set them. This allows .env/OS env to satisfy
// required S3 settings and optional multi-host inputs.
//
// Env variables honored:
//
//	S3_ENDPOINTS (CSV) or S3_ENDPOINT (single)
//	S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
//	HOSTS (equivalent to --test-hosts)
func applyEnvDefaultsToRunFlags(cmd *cobra.Command) error {
	// Helper to set simple string flags if not changed
	setIf := func(flag, envKey string) error {
		if cmd.Flags().Lookup(flag) == nil || cmd.Flags().Changed(flag) {
			return nil
		}
		if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
			return cmd.Flags().Set(flag, v)
		}
		return nil
	}

	// Prefer multi-endpoints if provided in env
	if f := cmd.Flags().Lookup("endpoints"); f != nil && !cmd.Flags().Changed("endpoints") {
		if !cmd.Flags().Changed("endpoint") {
			if v := strings.TrimSpace(os.Getenv("S3_ENDPOINTS")); v != "" {
				// Cobra accepts CSV for StringSlice Set
				if err := cmd.Flags().Set("endpoints", v); err != nil {
					return err
				}
			} else if v := strings.TrimSpace(os.Getenv("S3_ENDPOINT")); v != "" {
				if err := cmd.Flags().Set("endpoints", v); err != nil {
					return err
				}
			}
		}
	}

	// S3 auth/bucket
	_ = setIf("access-key", "S3_ACCESS_KEY")
	_ = setIf("secret-key", "S3_SECRET_KEY")
	_ = setIf("bucket", "S3_BUCKET")

	if f := cmd.Flags().Lookup("auth-version"); f != nil && !cmd.Flags().Changed("auth-version") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvS3AuthVersion)); v != "" {
			if _, err := strconv.Atoi(v); err != nil {
				return fmt.Errorf("invalid S3_AUTH_VERSION value %q: %w", v, err)
			}
			if err := cmd.Flags().Set("auth-version", v); err != nil {
				return err
			}
		}
	}

	// Multi-host settings
	_ = setIf("test-hosts", "HOSTS")

	// Image pull behavior
	if f := cmd.Flags().Lookup(flagSkipImagePull); f != nil && !cmd.Flags().Changed(flagSkipImagePull) {
		if v := strings.TrimSpace(os.Getenv(constants.EnvSkipImagePull)); v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				if err := cmd.Flags().Set(flagSkipImagePull, strconv.FormatBool(b)); err != nil {
					return err
				}
			}
		}
	}

	// Concurrency (THREADS): allow .env/OS to set default when --threads not provided
	if f := cmd.Flags().Lookup("threads"); f != nil && !cmd.Flags().Changed("threads") {
		if v := strings.TrimSpace(os.Getenv("THREADS")); v != "" {
			if _, err := strconv.Atoi(v); err != nil {
				return fmt.Errorf("invalid THREADS value %q: %w", v, err)
			}
			if err := cmd.Flags().Set("threads", v); err != nil {
				return err
			}
		}
	}

	// RDMA opt-in: --use-rdma flag or SPT_RDMA env var
	if f := cmd.Flags().Lookup("use-rdma"); f != nil && !cmd.Flags().Changed("use-rdma") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvRdmaEnabled)); v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				_ = cmd.Flags().Set("use-rdma", strconv.FormatBool(b))
			}
		}
	}

	// S3 driver selection: SPT_S3_DRIVER env var (values: default, netty, aws, rdma)
	_ = setIf("s3-driver", constants.EnvS3Driver)

	// Multipart upload part size (accepts humanized sizes like "64MB")
	if f := cmd.Flags().Lookup("part-size"); f != nil && !cmd.Flags().Changed("part-size") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvPartSize)); v != "" {
			if _, err := sizeparse.Parse(v); err != nil {
				return fmt.Errorf("invalid %s value %q: %w", constants.EnvPartSize, v, err)
			}
			_ = cmd.Flags().Set("part-size", v)
		}
	}

	// Engine VT parallelism: --service-threads or SPT_SERVICE_THREADS
	if f := cmd.Flags().Lookup("service-threads"); f != nil && !cmd.Flags().Changed("service-threads") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvServiceThreads)); v != "" {
			if _, err := strconv.Atoi(v); err != nil {
				return fmt.Errorf("invalid %s value %q: %w", constants.EnvServiceThreads, v, err)
			}
			if err := cmd.Flags().Set("service-threads", v); err != nil {
				return err
			}
		}
	}

	// RDMA string settings
	_ = setIf("rdma-local-ip", constants.EnvRdmaLocalIP)
	_ = setIf("rdma-device", constants.EnvRdmaDevice)
	_ = setIf("rdma-log-level", constants.EnvRdmaLogLevel)

	// RDMA threshold (accepts humanized sizes like "1MB" or plain bytes "1048576")
	if f := cmd.Flags().Lookup("rdma-threshold"); f != nil && !cmd.Flags().Changed("rdma-threshold") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvRdmaThreshold)); v != "" {
			if _, err := sizeparse.Parse(v); err != nil {
				return fmt.Errorf("invalid %s value %q: %w", constants.EnvRdmaThreshold, v, err)
			}
			_ = cmd.Flags().Set("rdma-threshold", v)
		}
	}
	// RDMA timeout (milliseconds, integer only)
	if f := cmd.Flags().Lookup("rdma-timeout-ms"); f != nil && !cmd.Flags().Changed("rdma-timeout-ms") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvRdmaTimeout)); v != "" {
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				return fmt.Errorf("invalid %s value %q: %w", constants.EnvRdmaTimeout, v, err)
			}
			_ = cmd.Flags().Set("rdma-timeout-ms", v)
		}
	}

	// RDMA bool setting
	if f := cmd.Flags().Lookup("rdma-fallback"); f != nil && !cmd.Flags().Changed("rdma-fallback") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvRdmaFallback)); v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				_ = cmd.Flags().Set("rdma-fallback", strconv.FormatBool(b))
			}
		}
	}

	// Checksum algorithm: SPT_CHECKSUM env var (values: crc32, crc32c, sha1, sha256)
	_ = setIf("checksum", constants.EnvChecksum)

	// Object data shaping
	if f := cmd.Flags().Lookup("object-data-compressibility"); f != nil && !cmd.Flags().Changed("object-data-compressibility") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvObjectDataCompressibility)); v != "" {
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return fmt.Errorf("invalid %s value %q: %w", constants.EnvObjectDataCompressibility, v, err)
			}
			if err := cmd.Flags().Set("object-data-compressibility", v); err != nil {
				return err
			}
		}
	}
	if f := cmd.Flags().Lookup("object-data-dedupable"); f != nil && !cmd.Flags().Changed("object-data-dedupable") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvObjectDataDedupable)); v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				if err := cmd.Flags().Set("object-data-dedupable", strconv.FormatBool(b)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// applyEnvDefaultsToVerifyFlags injects HOSTS env into verify flags when not provided.
func applyEnvDefaultsToVerifyFlags(cmd *cobra.Command) error {
	if !cmd.Flags().Changed("test-hosts") {
		if v := strings.TrimSpace(os.Getenv("HOSTS")); v != "" {
			if err := cmd.Flags().Set("test-hosts", v); err != nil {
				return err
			}
		}
	}

	// RDMA opt-in for verify
	if f := cmd.Flags().Lookup("use-rdma"); f != nil && !cmd.Flags().Changed("use-rdma") {
		if v := strings.TrimSpace(os.Getenv(constants.EnvRdmaEnabled)); v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				_ = cmd.Flags().Set("use-rdma", strconv.FormatBool(b))
			}
		}
	}

	return nil
}
