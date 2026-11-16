/*
Copyright © 2025 Dell Technologies
*/

package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// The skip spares only the pull of a present image: every engine launch route still pulls a
// missing non-dev image, as cli/README.md documents. The message and the flag help must say so.
func TestSkipImagePullMessageSaysAMissingImageIsStillPulled(t *testing.T) {
	const want = "Skipping the engine image pull when the image is present; a missing non-dev engine image is still pulled before the engine starts."
	if skipImagePullMessage != want {
		t.Fatalf("skipImagePullMessage = %q, want %q", skipImagePullMessage, want)
	}
	for _, stale := range []string{"locally cached", "without pulling"} {
		if strings.Contains(skipImagePullMessage, stale) || strings.Contains(skipImagePullFlagHelp, stale) {
			t.Fatalf("skip-image-pull text still claims %q", stale)
		}
	}
	if !strings.Contains(skipImagePullFlagHelp, "a missing non-dev image is still pulled") {
		t.Fatalf("skipImagePullFlagHelp = %q, want it to say a missing non-dev image is still pulled", skipImagePullFlagHelp)
	}
}

func TestSkipImagePullFlagHelpIsSharedByRunAndReplay(t *testing.T) {
	for name, cmd := range map[string]*cobra.Command{"run": runCmd, "replay": replayCmd} {
		flag := cmd.Flags().Lookup(flagSkipImagePull)
		if flag == nil {
			t.Fatalf("spt %s has no --%s flag", name, flagSkipImagePull)
		}
		if flag.Usage != skipImagePullFlagHelp {
			t.Fatalf("spt %s --%s help = %q, want %q", name, flagSkipImagePull, flag.Usage, skipImagePullFlagHelp)
		}
	}
}
