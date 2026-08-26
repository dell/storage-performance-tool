package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"gopkg.in/yaml.v3"
)

func persistRejectedEngineIdentity(
	metadata *runMetadata,
	root string,
	runID int64,
	outcome engineinfo.GateOutcome,
	gateErr error,
) error {
	if metadata == nil {
		return fmt.Errorf("run metadata is required to preserve engine identity rejection evidence")
	}
	metadata.runID = runID
	metadata.ScenarioParams.RunID = runID
	metadata.engineIdentity = &outcome
	metadata.engineIdentityError = errorText(gateErr)
	metadata.Lifecycle = &runLifecycleMetadata{
		Workload: lifecyclePhaseMetadata{
			Completed: true,
			State:     "rejected",
			Error:     metadata.engineIdentityError,
		},
	}
	return writeRunMetadata(metadata, root)
}

func persistEngineIdentity(metadata *runMetadata, root string) error {
	if metadata == nil || metadata.engineIdentity == nil {
		return nil
	}
	hints := configuredVersionHints(root, metadata.engineIdentity.Fleet.Participants)
	manifest, err := writeEngineIdentityManifest(root, metadata.runID, *metadata.engineIdentity, hints)
	if err != nil {
		return err
	}
	metadata.EngineInfoFile = constants.EngineInfoManifestName
	metadata.EngineConsistency = manifest.Consistency.Status
	return indexEngineIdentityManifest(root)
}

func writeEngineIdentityManifest(
	root string,
	runID int64,
	outcome engineinfo.GateOutcome,
	hints map[string]string,
) (engineinfo.Manifest, error) {
	manifest, err := engineinfo.NewManifest(runID, runEngineIdentityNow(), outcome, hints)
	if err != nil {
		return engineinfo.Manifest{}, fmt.Errorf("create engine identity manifest: %w", err)
	}
	if err := engineinfo.WriteManifestAtomic(root, manifest); err != nil {
		return engineinfo.Manifest{}, err
	}
	return manifest, nil
}

func configuredVersionHints(root string, participants []engineinfo.ParticipantResult) map[string]string {
	target := ""
	for _, participant := range participants {
		if participant.CollectionStatus != engineinfo.StatusLegacyEndpointUnavailable {
			continue
		}
		if participant.Role == engineinfo.RoleStandalone || participant.Role == engineinfo.RoleEntry {
			target = participant.NodeID
			break
		}
	}
	if target == "" {
		return nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	versions := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "."+constants.ResultsArtifactSuffixConfig) {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(root, entry.Name())) // #nosec G304 -- entry is from the results root
		if readErr != nil {
			continue
		}
		var config struct {
			Run struct {
				Version string `yaml:"version"`
			} `yaml:"run"`
		}
		if yaml.Unmarshal(data, &config) != nil {
			continue
		}
		version := strings.TrimSpace(config.Run.Version)
		if version != "" {
			versions[version] = struct{}{}
		}
	}
	if len(versions) != 1 {
		return nil
	}
	for version := range versions {
		return map[string]string{target: version}
	}
	return nil
}

func indexEngineIdentityManifest(root string) error {
	path := filepath.Join(root, constants.EngineInfoManifestName)
	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat engine identity manifest: %w", err)
	}
	manifestPath := filepath.Join(root, constants.ResultsManifestFileName)
	manifest := &results.Manifest{}
	data, err := os.ReadFile(manifestPath) // #nosec G304 -- path is under the selected results root
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read results manifest: %w", err)
	}
	if err == nil {
		if decodeErr := json.Unmarshal(data, manifest); decodeErr != nil {
			return fmt.Errorf("decode results manifest: %w", decodeErr)
		}
	}
	if manifest.OutputDir == "" {
		manifest.OutputDir = root
	}
	if manifest.GeneratedAt.IsZero() {
		manifest.GeneratedAt = runEngineIdentityNow().UTC()
	}
	status := results.FileStatus{
		Name:        constants.EngineInfoManifestName,
		Size:        stat.Size(),
		Status:      "ok",
		Modified:    stat.ModTime().UTC().Format(time.RFC3339),
		ContentType: "application/json",
	}
	replaced := false
	for index := range manifest.RunFiles {
		if manifest.RunFiles[index].Name == status.Name {
			manifest.RunFiles[index] = status
			replaced = true
			break
		}
	}
	if !replaced {
		manifest.RunFiles = append(manifest.RunFiles, status)
	}
	sort.Slice(manifest.RunFiles, func(i, j int) bool {
		return manifest.RunFiles[i].Name < manifest.RunFiles[j].Name
	})
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode results manifest: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := writeAtomic(manifestPath, encoded, 0o644); err != nil {
		return fmt.Errorf("write results manifest: %w", err)
	}
	return nil
}

func persistRunMetadataAndIdentity(metadata *runMetadata, root string) error {
	identityErr := persistEngineIdentity(metadata, root)
	metadataErr := writeRunMetadataFile(metadata, root)
	return errors.Join(identityErr, metadataErr)
}
