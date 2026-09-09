package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestRunAndReplayForceHelpDescribesNarrowEngineMismatchOverride(t *testing.T) {
	for name, command := range map[string]*cobra.Command{
		"run":    runCmd,
		"replay": replayCmd,
	} {
		help := command.Flags().FlagUsages()
		for _, want := range []string{"engine build mismatches", "invalid build information", "collection failures"} {
			if !strings.Contains(help, want) {
				t.Errorf("%s help missing %q:\n%s", name, want, help)
			}
		}
	}
}

func TestEngineBuildInformationDocumentationCoversPublicContract(t *testing.T) {
	docPath := filepath.Join("..", "docs", "ENGINE_BUILD_INFO.md")
	data, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(data)
	for _, want := range []string{
		"Engine Build Information", "META-INF/spt-build-info.properties", "GET /version",
		"engine.build.json", "engine.info.json", "spt_run_params.json", "index.json",
		"consistent", "mismatch", "indeterminate", "--force", "spt replay",
		"legacy_endpoint_unavailable", "unsupported_schema", "incomplete_build_info",
		"collection_failed", "enabled by default", "credentials", "environment variables",
		"explicitly reports `unknown`", "`version` or `revision`", "`null` for `source_dirty`",
		"omits a required field",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("documentation missing %q", want)
		}
	}
	for _, forbidden := range []string{"user@node", "/home/", `"access_key":`, `"secret_key":`, `"container_id":`} {
		if strings.Contains(strings.ToLower(doc), forbidden) {
			t.Errorf("documentation contains private or credential-bearing text %q", forbidden)
		}
	}
	readme, err := os.ReadFile(filepath.Join("..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), "docs/ENGINE_BUILD_INFO.md") {
		t.Fatal("CLI README does not link Engine Build Information documentation")
	}
	rootReadme, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rootReadme), "cli/docs/ENGINE_BUILD_INFO.md") {
		t.Fatal("root README does not link Engine Build Information documentation")
	}
	engineReadme, err := os.ReadFile(filepath.Join("..", "..", "engine", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(engineReadme), "../cli/docs/ENGINE_BUILD_INFO.md") {
		t.Fatal("engine README does not link Engine Build Information documentation")
	}
}
