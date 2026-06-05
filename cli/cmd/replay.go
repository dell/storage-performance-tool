package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/replay"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
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
	out := cmd.OutOrStdout()

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

	if generateOnly {
		outputDir, _ := cmd.Flags().GetString("output-dir")
		paths, err := replay.WriteGenerated(generated, outputDir)
		if err != nil {
			return fmt.Errorf("write generated replay artifacts: %w", err)
		}
		printReplayArtifacts(out, generated, paths)
		return nil
	}

	hostInfos, err := hostparse.ParseTestHosts(testHosts)
	if err != nil {
		return fmt.Errorf("invalid test hosts: %w", err)
	}
	if len(hostInfos) > 1 {
		return fmt.Errorf("multi-host replay execution is not implemented yet; set --test-hosts to a single host or use --generate-only")
	}

	resultsOpts := buildResultsOptions(cmd)
	runToken := time.Now().UTC().Format("20060102.150405.000")
	plannedResultsRoot := filepath.Join(resultsOpts.ResultsDir, fmt.Sprintf("%s-%s", resultsOpts.Label, runToken))
	outputDir, _ := cmd.Flags().GetString("output-dir")
	generatedOutputDir := strings.TrimSpace(outputDir)
	if generatedOutputDir == "" {
		generatedOutputDir = plannedResultsRoot
	}
	paths, err := replay.WriteGenerated(generated, generatedOutputDir)
	if err != nil {
		return fmt.Errorf("write generated replay artifacts: %w", err)
	}
	printReplayArtifacts(out, generated, paths)

	skipImagePull, _ := cmd.Flags().GetBool(flagSkipImagePull)
	if skipImagePull {
		_ = os.Setenv(constants.EnvSkipImagePull, "true")
		_, _ = fmt.Fprintln(out, "Skipping Docker image pull; using locally cached image.")
	} else {
		_ = os.Unsetenv(constants.EnvSkipImagePull)
	}
	if sptImageFlag, _ := cmd.Flags().GetString(flagSptImage); strings.TrimSpace(sptImageFlag) != "" {
		_ = os.Setenv(constants.EnvSptImage, strings.TrimSpace(sptImageFlag))
	}

	params := generated.Params
	minimalTUI, _ := cmd.Flags().GetBool("minimal")
	params.MinimalTUI = minimalTUI

	switch params.S3Driver {
	case scenario.S3DriverRdma:
		return fmt.Errorf("RDMA replay launch is not implemented yet")
	case scenario.S3DriverAws:
		_, _ = fmt.Fprintln(out, "Using AWS SDK S3 driver (s3-aws).")
	}

	if err := checkPortConflicts(cmd); err != nil {
		return err
	}

	headlessMode := shouldRunHeadless(cmd)
	if !headlessMode {
		if err := confirmReplayLaunch(out, os.OpenFile); err != nil {
			return err
		}
	}

	sptImage := constants.EffectiveSptImage()
	apiPort := getAPIPort(cmd)
	autoTerminate, _ := cmd.Flags().GetInt("auto-terminate-seconds")
	tracePath, _ := cmd.Flags().GetString("trace-file")
	traceAppend, _ := cmd.Flags().GetBool("trace-append")
	traceOpts, err := prepareTraceOptions(tracePath, traceAppend, resultsOpts.AutoResults, plannedResultsRoot, runToken, out)
	if err != nil {
		return err
	}
	metadata := buildRunMetadata(runMetadataInput{
		WorkloadType:         "replay",
		Params:               params,
		ScenarioPath:         paths.Scenario,
		ResultsOptions:       resultsOpts,
		HostInfos:            hostInfos,
		TestHostsRaw:         testHosts,
		MinHosts:             len(hostInfos),
		APIPort:              apiPort,
		BaseURL:              fmt.Sprintf("http://localhost:%s", apiPort),
		ExpectedStepIDs:      replayStepIDs(generated),
		SptImage:             sptImage,
		Command:              cmd,
		AutoTerminateSeconds: autoTerminate,
	})
	metadata.TraceFile = traceOpts.Path
	metadata.TraceAuto = traceOpts.Auto
	if plannedResultsRoot != "" {
		metadata.ResultsRoot = plannedResultsRoot
	}

	_, _ = fmt.Fprintf(out, "Launching replay against %d configured endpoint(s)...\n", len(params.Endpoints))
	_, _ = fmt.Fprintf(out, "Container: %s\n", sptImage)

	var autoResultsDone chan struct{}
	if resultsOpts.AutoResults {
		autoResultsDone = startAutoResults(metadata.BaseURL, resultsOpts.Label, resultsOpts.ResultsDir, replayStepIDs(generated), resultsOpts.Debug, hostInfos, apiPort, resultsOpts.ShutdownOnComplete, resultsOpts.ShutdownLingerSec, paths.Scenario, metadata, out, out, traceOpts.Path)
	}
	waitForAutoResults := func() {
		if autoResultsDone == nil {
			return
		}
		select {
		case <-autoResultsDone:
		case <-time.After(2 * time.Minute):
			logging.GetLogger().Warn("Timed out waiting for replay auto-results")
		}
	}
	finalizeTraceArtifact := func() {
		if err := appendTraceToResultsManifest(plannedResultsRoot, traceOpts.Path); err != nil {
			logging.GetLogger().Debug("Failed to finalize replay trace artifact",
				"results_root", plannedResultsRoot,
				"trace_file", traceOpts.Path,
				"error", err.Error())
		}
	}

	if headlessMode {
		verbose, _ := cmd.Flags().GetBool("verbose")
		options := buildHeadlessOptions(traceOpts, verbose, apiPort, autoTerminate, false, replayStepIDs(generated))
		if autoTerminate > 0 {
			_, _ = fmt.Fprintf(out, "Auto-terminate: will stop after %d seconds\n", autoTerminate)
		}
		err := headless.StartHeadlessModeWithScenarioContentAndParams(sptImage, paths.Scenario, params, options, generated.ScenarioJS, generated.DefaultsYAML)
		waitForAutoResults()
		finalizeTraceArtifact()
		return err
	}

	_, _ = fmt.Fprintln(out, "Starting TUI...")
	if autoTerminate > 0 {
		_, _ = fmt.Fprintf(out, "Auto-terminate: will stop after %d seconds\n", autoTerminate)
		err := tui.StartTUIWithScenarioContentAndParamsTimeoutWithTrace(sptImage, paths.Scenario, params, apiPort, autoTerminate, nil, traceOpts.Path, traceOpts.Append, generated.ScenarioJS, generated.DefaultsYAML)
		waitForAutoResults()
		finalizeTraceArtifact()
		return err
	}
	err = tui.StartTUIWithScenarioContentAndParamsWithTrace(sptImage, paths.Scenario, params, apiPort, nil, traceOpts.Path, traceOpts.Append, generated.ScenarioJS, generated.DefaultsYAML)
	waitForAutoResults()
	finalizeTraceArtifact()
	return err
}

func printReplayArtifacts(out io.Writer, generated *replay.Generated, paths replay.OutputPaths) {
	_, _ = fmt.Fprintln(out, generated.Preflight)
	_, _ = fmt.Fprintln(out, "Generated files")
	_, _ = fmt.Fprintf(out, "  Directory: %s\n", paths.Dir)
	_, _ = fmt.Fprintf(out, "  Scenario: %s\n", paths.Scenario)
	_, _ = fmt.Fprintf(out, "  Defaults: %s\n", paths.Defaults)
	_, _ = fmt.Fprintf(out, "  Metadata: %s\n", paths.Metadata)
}

func replayStepIDs(g *replay.Generated) []string {
	ids := make([]string, 0, len(g.Steps))
	for _, step := range g.Steps {
		if step.StepID != "" {
			ids = append(ids, step.StepID)
		}
	}
	return ids
}

type ttyOpenFunc func(name string, flag int, perm os.FileMode) (*os.File, error)

func confirmReplayLaunch(out io.Writer, openTTY ttyOpenFunc) error {
	_, _ = fmt.Fprintln(out, "Replay is ready. Press Enter to start, or q/Esc then Enter to abort.")
	tty, err := openTTY("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("replay confirmation requires an interactive terminal; rerun with --headless or --generate-only: %w", err)
	}
	defer func() { _ = tty.Close() }()

	input, readErr := bufio.NewReader(tty).ReadString('\n')
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fmt.Errorf("read replay confirmation: %w", readErr)
	}
	answer := strings.TrimSpace(input)
	switch answer {
	case "":
		return nil
	case "q", "Q", "\x1b":
		return fmt.Errorf("replay aborted by user")
	default:
		return fmt.Errorf("replay aborted; press Enter to start or q/Esc to abort")
	}
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
	replayCmd.Flags().Bool("headless", false, "Force headless (non-interactive) mode")
	replayCmd.Flags().Bool("minimal", false, "Start TUI with only the live stats panel visible")
	replayCmd.Flags().Int("auto-terminate-seconds", 0, "Automatically terminate runs after N seconds (0 = unlimited)")
	replayCmd.Flags().Bool("force", false, "Automatically resolve port conflicts without user interaction")
	replayCmd.Flags().String("api-port", "", "Spt API port (defaults to 9999)")
	replayCmd.Flags().Bool(flagSkipImagePull, false, "Use the locally cached Docker image without pulling the latest tag (env: SPT_SKIP_IMAGE_PULL)")
	replayCmd.Flags().String(flagSptImage, "", "Override the engine image ref (env: SPT_IMAGE)")
	replayCmd.Flags().String("trace-file", "", "Save all output to specified trace file")
	replayCmd.Flags().Bool("trace-append", false, "Append to existing trace file (default: overwrite)")
	replayCmd.Flags().Bool("verbose", false, "Show detailed Docker API calls and debug information")
	replayCmd.Flags().Bool("auto-results", true, "Automatically retrieve results artifacts at end of replay")
	replayCmd.Flags().String("results-dir", "./results", "Directory to write retrieved replay results artifacts")
	replayCmd.Flags().Bool("auto-results-debug", false, "Enable verbose debug logs for replay auto-results completion detection")
	replayCmd.Flags().Bool("shutdown-on-complete", true, "After fetching results, request /shutdown on Spt")
	replayCmd.Flags().Int("shutdown-linger", 5, "Seconds to wait for /status linger after /shutdown")
}
