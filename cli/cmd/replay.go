package cmd

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/replay"
	"github.com/spf13/cobra"
)

var replayCmd = &cobra.Command{
	Use:          "replay",
	Short:        "Import archived run artifacts and generate an equivalent replay workload.",
	SilenceUsage: true,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		_ = config.LoadDotEnv()
		if err := initializeLogger(); err != nil {
			return err
		}
		return applyEnvDefaultsToRunFlags(cmd)
	},
	RunE: runReplay,
}

func runReplay(cmd *cobra.Command, _ []string) error {
	generateOnly, _ := cmd.Flags().GetBool("generate-only")
	if !generateOnly {
		return fmt.Errorf("replay execution is not implemented yet; rerun with --generate-only to import and inspect generated artifacts")
	}

	sourceURL, _ := cmd.Flags().GetString("from")
	if strings.TrimSpace(sourceURL) == "" {
		return fmt.Errorf("--from URL is required")
	}
	endpoints, _ := cmd.Flags().GetStringSlice("endpoints")
	if endpoint, _ := cmd.Flags().GetString("endpoint"); strings.TrimSpace(endpoint) != "" && len(endpoints) == 0 {
		endpoints = []string{strings.TrimSpace(endpoint)}
	}
	accessKey, _ := cmd.Flags().GetString("access-key")
	secretKey, _ := cmd.Flags().GetString("secret-key")
	bucket, _ := cmd.Flags().GetString("bucket")
	authVersion, _ := cmd.Flags().GetInt("auth-version")
	testHosts, _ := cmd.Flags().GetString("test-hosts")
	label, _ := cmd.Flags().GetString("label")
	s3Driver, _ := cmd.Flags().GetString("s3-driver")

	generated, err := replay.Generate(cmd.Context(), replay.Options{
		SourceURL:   sourceURL,
		Endpoints:   endpoints,
		AccessKey:   accessKey,
		SecretKey:   secretKey,
		Bucket:      bucket,
		AuthVersion: authVersion,
		TestHosts:   testHosts,
		Label:       label,
		S3Driver:    s3Driver,
	})
	if err != nil {
		return err
	}

	outputDir, _ := cmd.Flags().GetString("output-dir")
	paths, err := replay.WriteGenerated(generated, outputDir)
	if err != nil {
		return fmt.Errorf("write generated replay artifacts: %w", err)
	}

	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintln(out, generated.Preflight)
	_, _ = fmt.Fprintln(out, "Generated files")
	_, _ = fmt.Fprintf(out, "  Directory: %s\n", paths.Dir)
	_, _ = fmt.Fprintf(out, "  Scenario: %s\n", paths.Scenario)
	_, _ = fmt.Fprintf(out, "  Defaults: %s\n", paths.Defaults)
	_, _ = fmt.Fprintf(out, "  Metadata: %s\n", paths.Metadata)
	return nil
}

func init() {
	rootCmd.AddCommand(replayCmd)

	replayCmd.Flags().String("from", "", "HTTP folder URL containing archived replay artifacts")
	replayCmd.Flags().Bool("generate-only", false, "Generate replay scenario/defaults/metadata without launching Spt")
	replayCmd.Flags().StringP("output-dir", "O", "", "Directory for generated replay artifacts (default: private temp directory)")
	replayCmd.Flags().StringSliceP("endpoints", "e", []string{}, "One or more local S3 endpoint URLs (comma-separated or repeatable)")
	replayCmd.Flags().String("endpoint", "", "Deprecated: alias for --endpoints (single value)")
	_ = replayCmd.Flags().MarkHidden("endpoint")
	replayCmd.Flags().StringP("access-key", "a", "", "The local S3 access key credential")
	replayCmd.Flags().StringP("secret-key", "s", "", "The local S3 secret key credential")
	replayCmd.Flags().StringP("bucket", "b", "", "The local bucket override (default: S3_BUCKET or archived bucket)")
	replayCmd.Flags().Int("auth-version", 4, "S3 authentication signature version (2 or 4; default 4)")
	replayCmd.Flags().String("test-hosts", "127.0.0.1", "Comma-separated local test host list for replay variable remapping")
	replayCmd.Flags().String("label", "replay", "Label prefix for generated canonical step IDs")
	replayCmd.Flags().String("s3-driver", "default", "S3 driver selection: default, netty, aws, or rdma")
}
