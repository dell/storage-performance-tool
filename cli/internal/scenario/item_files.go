package scenario

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/integrity"
)

const containerItemFilesDir = "/spt-input/items"

// FileMount describes a read-only host file bind-mounted into the SPT container.
type FileMount struct {
	HostPath      string
	ContainerPath string
}

// PrepareExternalItemFiles rewrites user-provided item CSV paths to container paths.
func PrepareExternalItemFiles(params Params) (Params, error) {
	if len(params.ItemFileMounts) > 0 || !hasExternalItemFiles(params) {
		return params, nil
	}

	var mounts []FileMount
	var err error
	if params.ItemsFile != "" {
		if params.WorkloadType == WorkloadTypeReadVerify {
			stagingDir, manifest, marker, stageErr := integrity.StageInputManifest(params.ItemsFile, params.RunID)
			if stageErr != nil {
				return params, fmt.Errorf("--items-file: %w", stageErr)
			}
			params.ItemStagingDirs = append(params.ItemStagingDirs, stagingDir)
			params.ItemsFile = containerItemFilesDir + "/" + integrity.VerifyInputName
			mounts = append(mounts,
				FileMount{HostPath: manifest, ContainerPath: params.ItemsFile},
				FileMount{HostPath: marker, ContainerPath: containerItemFilesDir + "/" + integrity.VerifyInputCompletionName})
		} else {
			params.ItemsFile, mounts, err = prepareItemFileMount(params.ItemsFile, "read-items.csv", mounts)
			if err != nil {
				return params, fmt.Errorf("--items-file: %w", err)
			}
		}
	}
	if params.ReadItemsFile != "" {
		params.ReadItemsFile, mounts, err = prepareItemFileMount(params.ReadItemsFile, "mixed-read-items.csv", mounts)
		if err != nil {
			return params, fmt.Errorf("--read-items-file: %w", err)
		}
	}
	if params.DeleteItemsFile != "" {
		params.DeleteItemsFile, mounts, err = prepareItemFileMount(params.DeleteItemsFile, "mixed-delete-items.csv", mounts)
		if err != nil {
			return params, fmt.Errorf("--delete-items-file: %w", err)
		}
	}
	params.ItemFileMounts = mounts
	return params, nil
}

func hasExternalItemFiles(params Params) bool {
	return params.ItemsFile != "" || params.ReadItemsFile != "" || params.DeleteItemsFile != ""
}

func prepareItemFileMount(hostPath, containerName string, mounts []FileMount) (string, []FileMount, error) {
	if strings.HasPrefix(hostPath, containerItemFilesDir+"/") {
		return hostPath, mounts, nil
	}
	absPath, err := filepath.Abs(hostPath)
	if err != nil {
		return "", nil, fmt.Errorf("resolve path %q: %w", hostPath, err)
	}
	info, err := os.Stat(absPath)
	if err != nil {
		return "", nil, fmt.Errorf("stat %q: %w", hostPath, err)
	}
	if info.IsDir() {
		return "", nil, fmt.Errorf("%q is a directory, expected an items CSV file", hostPath)
	}
	containerPath := containerItemFilesDir + "/" + containerName
	mounts = append(mounts, FileMount{HostPath: absPath, ContainerPath: containerPath})
	return containerPath, mounts, nil
}

// CleanupPreparedItemFiles removes only private staging directories created by this package.
func CleanupPreparedItemFiles(params Params) error {
	var cleanupErrs []error
	for _, dir := range params.ItemStagingDirs {
		if strings.HasPrefix(filepath.Base(dir), "spt-integrity-input-") {
			if err := os.RemoveAll(dir); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove prepared item staging %q: %w", dir, err))
			}
		}
	}
	return errors.Join(cleanupErrs...)
}
