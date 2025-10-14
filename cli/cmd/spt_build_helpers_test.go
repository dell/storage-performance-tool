package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/spf13/cobra"
)

// buildSptCommand builds the Spt command-line arguments based on spt flags.
// Test-only helper: used by buildspt_test.
func buildSptCommand(workloadType string, cmd *cobra.Command) ([]string, error) {
	if workloadType == WorkloadTypeMock {
		return buildMockCommand(cmd)
	}
	if workloadType != WorkloadTypeWrite {
		return nil, fmt.Errorf(ErrWorkloadNotImplemented, workloadType)
	}
	return buildS3Command(cmd)
}

// buildMockCommand builds command arguments for mock workload (test-only helper).
func buildMockCommand(cmd *cobra.Command) ([]string, error) {
	args := []string{SptStorageDriverType + "=" + SptStorageDriverTypeMock}

	threads, _ := cmd.Flags().GetInt("threads")
	if cmd.Flags().Changed("threads") {
		args = append(args, fmt.Sprintf("%s=%d", SptStorageDriverLimitConc, threads))
	}

	if cmd.Flags().Changed("duration") {
		duration, _ := cmd.Flags().GetString("duration")
		args = append(args, SptLoadStepLimitTime+"="+duration)
	} else if cmd.Flags().Changed("object-count") {
		objectCount, _ := cmd.Flags().GetInt("object-count")
		args = append(args, fmt.Sprintf("%s=%d", SptLoadOpLimitCount, objectCount))
	}

	if cmd.Flags().Changed("object-size") {
		objectSize, _ := cmd.Flags().GetString("object-size")
		args = append(args, SptItemDataSize+"="+objectSize)
	}

	return args, nil
}

// buildS3Command builds command arguments for S3 workloads (test-only helper).
func buildS3Command(cmd *cobra.Command) ([]string, error) {
	args := []string{SptStorageDriverType + "=" + SptStorageDriverTypeS3}

	endpoint, _ := cmd.Flags().GetString("endpoint")
	parsedURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf(ErrInvalidEndpointURL, err)
	}

	hostname := parsedURL.Hostname()
	if hostname == "" {
		return nil, errors.New(ErrMissingHostname)
	}
	args = append(args, SptStorageNetNodeAddrs+"="+hostname)

	port := parsedURL.Port()
	if port == "" {
		if parsedURL.Scheme == SchemeHTTPS {
			port = constants.DefaultHTTPSPort
		} else {
			port = constants.DefaultHTTPPort
		}
	}
	args = append(args, SptStorageNetNodePort+"="+port)

	if parsedURL.Scheme == SchemeHTTPS {
		args = append(args, SptStorageNetSSLEnabled+"=true")
	}

	accessKey, _ := cmd.Flags().GetString("access-key")
	args = append(args, SptStorageAuthUID+"="+accessKey)

	secretKey, _ := cmd.Flags().GetString("secret-key")
	args = append(args, SptStorageAuthSecret+"="+secretKey)

	bucket, _ := cmd.Flags().GetString("bucket")
	bucketPath := "/" + strings.TrimPrefix(bucket, "/")
	args = append(args, SptItemOutputPath+"="+bucketPath)

	args = append(args, SptLoadOpType+"="+SptLoadOpTypeCreate)

	threads, _ := cmd.Flags().GetInt("threads")
	args = append(args, fmt.Sprintf("%s=%d", SptStorageDriverLimitConc, threads))

	objectSize, _ := cmd.Flags().GetString("object-size")
	if objectSize != "" {
		args = append(args, SptItemDataSize+"="+objectSize)
	}

	objectCount, _ := cmd.Flags().GetInt("object-count")
	duration, _ := cmd.Flags().GetString("duration")
	if objectCount > 0 {
		args = append(args, fmt.Sprintf("%s=%d", SptLoadOpLimitCount, objectCount))
	} else if duration != "" {
		args = append(args, SptLoadStepLimitTime+"="+duration)
	}

	return args, nil
}
