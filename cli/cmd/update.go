package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/buildinfo"
	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	updater "github.com/dell/storage-performance-tool/cli/internal/update"
	"github.com/spf13/cobra"
)

const defaultUpdateTimeout = 30 * time.Second

type updateOptions struct {
	check   bool
	pre     bool
	yes     bool
	timeout time.Duration
	output  string
	token   string
}

var (
	updateCurrentVersion  = func() string { return buildinfo.Version }
	updateExecutable      = os.Executable
	updateRuntimeGOOS     = func() string { return runtime.GOOS }
	updateRuntimeGOARCH   = func() string { return runtime.GOARCH }
	updateNewGitHubClient = func(timeout time.Duration, token string) updater.GitHubClient {
		client := updater.NewGitHubClient(timeout, token)
		client.UserAgent = "spt/" + buildinfo.Version
		return client
	}
	updateVerifyReplaceAccess = updater.VerifyReplaceAccess
	updateIsTerminalInput     = isTerminalInput
)

func newUpdateCommand() *cobra.Command {
	opts := &updateOptions{timeout: defaultUpdateTimeout}
	cmd := &cobra.Command{
		Use:           "update",
		Short:         "Check for and install SPT CLI updates",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return updatePreRun(cmd)
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUpdateCommand(cmd, opts)
		},
	}
	cmd.Flags().BoolVar(&opts.check, "check", false, "Check whether an update is available without downloading or writing files")
	cmd.Flags().BoolVar(&opts.pre, "pre", false, "Consider prerelease tags")
	cmd.Flags().BoolVarP(&opts.yes, "yes", "y", false, "Skip confirmation prompts")
	cmd.Flags().DurationVar(&opts.timeout, "timeout", defaultUpdateTimeout, "Network timeout")
	cmd.Flags().StringVar(&opts.output, "output", "", "Write the release binary to this path instead of replacing the running binary")
	cmd.Flags().StringVar(&opts.token, "token", "", "GitHub token for API rate limits; prefer GITHUB_TOKEN or SPT_GITHUB_TOKEN")
	return cmd
}

func updatePreRun(cmd *cobra.Command) error {
	config.LoadDotEnv()
	if inheritedFlagChanged(cmd, "log-file") {
		if err := initializeLogger(); err != nil {
			return updateUserError(cmd, "%v", err)
		}
	}
	return nil
}

func inheritedFlagChanged(cmd *cobra.Command, name string) bool {
	if f := cmd.Flags().Lookup(name); f != nil && f.Changed {
		return true
	}
	if f := cmd.InheritedFlags().Lookup(name); f != nil && f.Changed {
		return true
	}
	return false
}

func runUpdateCommand(cmd *cobra.Command, opts *updateOptions) error {
	if opts.check && opts.output != "" {
		return updateUserError(cmd, "--check cannot be combined with --output")
	}
	if opts.check && opts.yes {
		return updateUserError(cmd, "--check cannot be combined with --yes")
	}
	if opts.timeout <= 0 {
		return updateUserError(cmd, "--timeout must be positive")
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
	defer cancel()

	token := resolveUpdateToken(opts.token)
	client := updateNewGitHubClient(opts.timeout, token)
	releases, err := client.ListReleases(ctx)
	if err != nil {
		return updateUserError(cmd, "failed to list GitHub releases: %v", err)
	}
	channel := updater.ChannelStable
	if opts.pre {
		channel = updater.ChannelPrerelease
	}
	latest, err := updater.SelectLatestRelease(releases, channel)
	if err != nil {
		return updateUserError(cmd, "failed to select latest release: %v", err)
	}
	latestVersion, err := latest.Version()
	if err != nil {
		return updateUserError(cmd, "failed to parse latest release tag %q: %v", latest.TagName, err)
	}
	assetName, err := updater.AssetNameForPlatform(latestVersion.String(), updateRuntimeGOOS(), updateRuntimeGOARCH())
	if err != nil {
		return updateUserError(cmd, "%v", err)
	}
	current := updateCurrentVersion()
	currentVersion, currentVersionErr := updater.ParseVersion(current)
	available := currentVersionErr == nil && updater.Available(currentVersion, latestVersion)
	if opts.check {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "current=%s latest=%s available=%t\n", current, latestVersion.String(), available)
		if available {
			return &ExitCodeError{Code: 10}
		}
		return nil
	}

	target := ""
	if opts.output == "" {
		if currentVersionErr == nil && !available {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "spt is already up to date: current=%s latest=%s\n", current, latestVersion.String())
			return nil
		}
		if updateRuntimeGOOS() == "windows" {
			return updateUserError(cmd, "running-binary self-update is not enabled on Windows yet; use --output <path> to download the verified release binary")
		}
		if ok, reason := updater.CanSelfUpdateCurrentBuild(current); !ok {
			return updateUserError(cmd, "cannot self-update this build (%s): %s", current, reason)
		}
		exe, err := updateExecutable()
		if err != nil {
			return updateUserError(cmd, "failed to resolve current executable: %v", err)
		}
		target, err = updater.ResolveExecutableTarget(exe)
		if err != nil {
			return updateUserError(cmd, "failed to resolve executable target %s: %v", exe, err)
		}
		if err := updateVerifyReplaceAccess(target); err != nil {
			return updateUserError(cmd, "cannot replace %s before downloading update: %v. Re-run with sufficient privileges (for example sudo) or use --output <path> to write the release binary elsewhere", target, err)
		}
		warnImageOverride(cmd)
		if err := confirmUpdateIfNeeded(cmd, opts, latestVersion.String(), target, latest.HTMLURL, current); err != nil {
			return err
		}
	} else if currentVersionErr == nil && !available {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "spt is already up to date: current=%s latest=%s\n", current, latestVersion.String())
		return nil
	}

	asset, err := updater.FindAsset(latest.Assets, assetName)
	if err != nil {
		return updateUserError(cmd, "%v", err)
	}
	checksumAsset, err := updater.FindAsset(latest.Assets, updater.ChecksumAssetName)
	if err != nil {
		return updateUserError(cmd, "%v", err)
	}

	assetBytes, err := client.DownloadAsset(ctx, asset)
	if err != nil {
		return updateUserError(cmd, "failed to download %s: %v", asset.Name, err)
	}
	checksumBytes, err := client.DownloadAsset(ctx, checksumAsset)
	if err != nil {
		return updateUserError(cmd, "failed to download %s: %v", checksumAsset.Name, err)
	}
	wantChecksum, err := updater.FindChecksum(checksumBytes, asset.Name)
	if err != nil {
		return updateUserError(cmd, "%v", err)
	}
	if err := updater.VerifyChecksum(assetBytes, wantChecksum); err != nil {
		return updateUserError(cmd, "%v", err)
	}
	binary, err := updater.ExtractBinary(asset.Name, assetBytes)
	if err != nil {
		return updateUserError(cmd, "failed to extract %s: %v", asset.Name, err)
	}
	if opts.output != "" {
		if err := updater.WriteFileAtomic(opts.output, binary, 0o755); err != nil {
			return updateUserError(cmd, "failed to write %s: %v", opts.output, err)
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Wrote spt %s to %s\n", latestVersion.String(), opts.output)
		return nil
	}
	if err := updater.ReplaceExecutable(target, binary); err != nil {
		return updateUserError(cmd, "failed to replace %s: %v", target, err)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Update complete: installed spt %s at %s. Re-run spt to use the new version.\n", latestVersion.String(), target)
	return nil
}

func resolveUpdateToken(flagToken string) string {
	if strings.TrimSpace(flagToken) != "" {
		return strings.TrimSpace(flagToken)
	}
	if v := strings.TrimSpace(os.Getenv(constants.EnvSptGitHubToken)); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
}

func warnImageOverride(cmd *cobra.Command) {
	if image := strings.TrimSpace(os.Getenv(constants.EnvSptImage)); image != "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s is set; engine runs will continue using %s until the override is changed or removed\n", constants.EnvSptImage, image)
	}
}

func confirmUpdateIfNeeded(cmd *cobra.Command, opts *updateOptions, latest, target, releaseURL, current string) error {
	if opts.yes {
		return nil
	}
	if !updateIsTerminalInput(cmd.InOrStdin()) {
		return updateUserError(cmd, "non-interactive update requires --yes")
	}
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Update spt from %s to %s?\ntarget: %s\nrelease: %s\nProceed? [y/N] ", current, latest, target, releaseURL)
	answer, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return updateUserError(cmd, "failed to read confirmation: %v", err)
	}
	answer = strings.TrimSpace(answer)
	if !strings.EqualFold(answer, "y") && !strings.EqualFold(answer, "yes") {
		return updateUserError(cmd, "update cancelled")
	}
	return nil
}

func isTerminalInput(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

func updateUserError(cmd *cobra.Command, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), msg)
	return errors.New(msg)
}

func init() {
	rootCmd.AddCommand(newUpdateCommand())
}
