package cmd

import (
	"bufio"
	"context"
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

var (
	newReplaySingleHostOrchestrator = tui.NewMultiHostOrchestratorWithRMI
	newReplayMultiHostOrchestrator  = tui.NewMultiHostOrchestratorWithRMI
	connectReplayOrchestrator       = func(ctx context.Context, orchestrator *tui.MultiHostOrchestrator) error {
		return orchestrator.ConnectHosts(ctx)
	}
)

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
		if generated != nil && strings.TrimSpace(generated.Preflight) != "" {
			_, _ = fmt.Fprintln(out, generated.Preflight)
		}
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
	minHosts, _ := cmd.Flags().GetInt("min-hosts")
	if minHosts == 0 {
		minHosts = len(hostInfos)
	}
	if minHosts < 1 {
		return fmt.Errorf("min-hosts must be at least 1")
	}
	if minHosts > len(hostInfos) {
		return fmt.Errorf("min-hosts (%d) cannot exceed total hosts (%d)", minHosts, len(hostInfos))
	}
	attachExisting, _ := cmd.Flags().GetBool(flagAttachExistingWorkers)
	if attachExisting && len(hostInfos) < 2 {
		return fmt.Errorf("--%s requires at least two hosts (entry + worker)", flagAttachExistingWorkers)
	}
	remoteSingleHost := len(hostInfos) == 1 && !hostInfos[0].IsLocal
	multiHostReplay := len(hostInfos) > 1
	orchestratedReplay := remoteSingleHost || multiHostReplay

	resultsOpts := buildResultsOptions(cmd)
	runToken := time.Now().UTC().Format("20060102.150405.000")
	plannedResultsRoot := filepath.Join(resultsOpts.ResultsDir, fmt.Sprintf("%s-%s", resultsOpts.Label, runToken))
	outputDir, _ := cmd.Flags().GetString("output-dir")
	generatedOutputDir := strings.TrimSpace(outputDir)
	includeDefaults := generatedOutputDir != ""
	if generatedOutputDir == "" {
		generatedOutputDir = plannedResultsRoot
	}
	paths, err := replay.WriteGeneratedWithOptions(generated, replay.WriteGeneratedOptions{
		OutputDir:       generatedOutputDir,
		IncludeDefaults: includeDefaults,
	})
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

	if !orchestratedReplay {
		if err := checkPortConflicts(cmd); err != nil {
			return err
		}
	}

	sptImage := constants.EffectiveSptImage()
	apiPort := getAPIPort(cmd)
	networkMode, _ := cmd.Flags().GetString("network-mode")
	rmiPortStart, _ := cmd.Flags().GetInt("rmi-port-start")
	rmiPortCount, _ := cmd.Flags().GetInt("rmi-port-count")
	var replayOrchestrator *tui.MultiHostOrchestrator
	if orchestratedReplay {
		if multiHostReplay {
			if networkMode == constants.BridgeNetworkMode {
				_, _ = fmt.Fprintln(out, "WARNING: Bridge networking may not work for distributed replay; Java RMI requires host networking for inter-node communication.")
			}
			_, _ = fmt.Fprintf(out, "Multi-host replay: %d hosts, minimum required: %d\n", len(hostInfos), minHosts)
			if attachExisting {
				_, _ = fmt.Fprintln(out, "Attach mode enabled: expecting worker nodes prestarted with --run-node; replay will launch the entry node.")
			}
			replayOrchestrator = newReplayMultiHostOrchestrator(hostInfos, minHosts, tui.RMIConfig{
				NetworkMode: networkMode,
				PortStart:   rmiPortStart,
				PortCount:   rmiPortCount,
			})
			replayOrchestrator.SetAttachExistingWorkers(attachExisting)
		} else {
			_, _ = fmt.Fprintf(out, "Replay host: %s\n", hostInfos[0].Original)
			replayOrchestrator = newReplaySingleHostOrchestrator(hostInfos, 1, tui.RMIConfig{
				NetworkMode: networkMode,
				PortStart:   rmiPortStart,
				PortCount:   rmiPortCount,
			})
		}
		replayOrchestrator.SetNotifier(func(msg string) {
			_, _ = fmt.Fprintln(out, msg)
		})
		replayOrchestrator.SetImage(sptImage)
		replayOrchestrator.SetAPIPort(apiPort)
		forceMode, _ := cmd.Flags().GetBool("force")
		replayOrchestrator.SetForceCleanup(forceMode)
		if err := connectReplayOrchestrator(context.Background(), replayOrchestrator); err != nil {
			return fmt.Errorf("failed to connect to replay host(s): %w", err)
		}
	}

	headlessMode := shouldRunHeadless(cmd)
	if !headlessMode {
		if err := confirmReplayLaunch(out, os.OpenFile); err != nil {
			return err
		}
	}

	autoTerminate, _ := cmd.Flags().GetInt("auto-terminate-seconds")
	tracePath, _ := cmd.Flags().GetString("trace-file")
	traceAppend, _ := cmd.Flags().GetBool("trace-append")
	traceOpts, err := prepareTraceOptions(tracePath, traceAppend, resultsOpts.AutoResults, plannedResultsRoot, runToken, out)
	if err != nil {
		return err
	}
	baseURL := replayBaseURL(apiPort, hostInfos)
	metadata := buildRunMetadata(runMetadataInput{
		WorkloadType:         "replay",
		Params:               params,
		ScenarioPath:         paths.Scenario,
		ResultsOptions:       resultsOpts,
		HostInfos:            hostInfos,
		TestHostsRaw:         testHosts,
		MinHosts:             minHosts,
		AttachExisting:       attachExisting,
		NetworkMode:          networkMode,
		RMIPortStart:         rmiPortStart,
		RMIPortCount:         rmiPortCount,
		APIPort:              apiPort,
		BaseURL:              baseURL,
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
	startReplayAutoResults := func() {
		if autoResultsDone != nil || !resultsOpts.AutoResults {
			return
		}
		autoResultsDone = startAutoResults(metadata.BaseURL, resultsOpts.Label, resultsOpts.ResultsDir, replayStepIDs(generated), resultsOpts.Debug, hostInfos, apiPort, resultsOpts.ShutdownOnComplete, resultsOpts.ShutdownLingerSec, paths.Scenario, metadata, out, out, traceOpts.Path)
	}
	waitForAutoResults := func() bool {
		if autoResultsDone == nil {
			return true
		}
		select {
		case <-autoResultsDone:
			return true
		case <-time.After(2 * time.Minute):
			logging.GetLogger().Warn("Timed out waiting for replay auto-results")
			return false
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

	if orchestratedReplay {
		startReplayAutoResults()
		if headlessMode {
			verbose, _ := cmd.Flags().GetBool("verbose")
			delegateShutdownToAutoResults := resultsOpts.AutoResults && resultsOpts.ShutdownOnComplete
			options := buildHeadlessOptions(traceOpts, verbose, apiPort, autoTerminate, delegateShutdownToAutoResults, replayStepIDs(generated))
			if autoTerminate > 0 {
				_, _ = fmt.Fprintf(out, "Auto-terminate: will stop after %d seconds\n", autoTerminate)
			}
			err := headless.StartHeadlessModeWithOrchestratorContent(replayOrchestrator, sptImage, paths.Scenario, params, options, generated.ScenarioJS, generated.DefaultsYAML)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				finalizeTraceArtifact()
				return err
			}
			autoResultsOK := waitForAutoResults()
			if delegateShutdownToAutoResults {
				if !autoResultsOK {
					logging.GetLogger().Warn("Timed out waiting for replay auto-results; forcing remote container cleanup")
				}
				if stopErr := cleanupReplayContainers(replayOrchestrator, 30*time.Second); stopErr != nil {
					logging.GetLogger().Error("Failed to clean up replay containers", "error", stopErr.Error())
				}
			}
			finalizeTraceArtifact()
			return err
		}

		if multiHostReplay {
			_, _ = fmt.Fprintln(out, "Starting multi-host replay TUI...")
		} else {
			_, _ = fmt.Fprintln(out, "Starting remote-host TUI...")
		}
		if autoTerminate > 0 {
			_, _ = fmt.Fprintf(out, "Auto-terminate: will stop after %d seconds\n", autoTerminate)
			err := tui.StartTUIWithMultiHostOrchestratorContentTimeoutWithTrace(replayOrchestrator, sptImage, paths.Scenario, params, autoTerminate, nil, traceOpts.Path, traceOpts.Append, generated.ScenarioJS, generated.DefaultsYAML)
			waitForAutoResults()
			finalizeTraceArtifact()
			return err
		}
		err = tui.StartTUIWithMultiHostOrchestratorContentWithTrace(replayOrchestrator, sptImage, paths.Scenario, params, nil, traceOpts.Path, traceOpts.Append, generated.ScenarioJS, generated.DefaultsYAML)
		waitForAutoResults()
		finalizeTraceArtifact()
		return err
	}

	startReplayAutoResults()
	if headlessMode {
		verbose, _ := cmd.Flags().GetBool("verbose")
		options := buildHeadlessOptions(traceOpts, verbose, apiPort, autoTerminate, false, replayStepIDs(generated))
		options.NetworkMode = networkMode
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
		err := tui.StartTUIWithScenarioContentAndParamsTimeoutWithTrace(sptImage, paths.Scenario, params, apiPort, networkMode, autoTerminate, nil, traceOpts.Path, traceOpts.Append, generated.ScenarioJS, generated.DefaultsYAML)
		waitForAutoResults()
		finalizeTraceArtifact()
		return err
	}
	err = tui.StartTUIWithScenarioContentAndParamsWithTrace(sptImage, paths.Scenario, params, apiPort, networkMode, nil, traceOpts.Path, traceOpts.Append, generated.ScenarioJS, generated.DefaultsYAML)
	waitForAutoResults()
	finalizeTraceArtifact()
	return err
}

type replayContainerCleaner interface {
	StopAllContainers(context.Context) error
}

func cleanupReplayContainers(cleaner replayContainerCleaner, timeout time.Duration) error {
	if cleaner == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return cleaner.StopAllContainers(ctx)
}

func replayBaseURL(apiPort string, hosts []*hostparse.HostInfo) string {
	if len(hosts) > 1 && hosts[0] != nil {
		return hosts[0].GetAPIURL(apiPort)
	}
	if len(hosts) == 1 && hosts[0] != nil && !hosts[0].IsLocal {
		return hosts[0].GetAPIURL(apiPort)
	}
	return fmt.Sprintf("http://localhost:%s", apiPort)
}

func printReplayArtifacts(out io.Writer, generated *replay.Generated, paths replay.OutputPaths) {
	_, _ = fmt.Fprintln(out, generated.Preflight)
	_, _ = fmt.Fprintln(out, "Generated files")
	_, _ = fmt.Fprintf(out, "  Directory: %s\n", paths.Dir)
	_, _ = fmt.Fprintf(out, "  Scenario: %s\n", paths.Scenario)
	if paths.Defaults == "" {
		_, _ = fmt.Fprintln(out, "  Defaults: in memory only")
	} else {
		_, _ = fmt.Fprintf(out, "  Defaults: %s\n", paths.Defaults)
	}
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
	replayCmd.Flags().Int("min-hosts", 0, "Minimum number of replay hosts that must connect (default: all hosts)")
	replayCmd.Flags().Bool(flagAttachExistingWorkers, false, "Attach to prestarted worker nodes; replay still launches the entry node")
	replayCmd.Flags().String("network-mode", "host", "Docker network mode: 'host' (default, required for RMI) or 'bridge'")
	replayCmd.Flags().Int("rmi-port-start", 40000, "Starting port for RMI range")
	replayCmd.Flags().Int("rmi-port-count", 10, "Number of RMI ports to verify")
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
