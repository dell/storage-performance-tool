package scenario

import (
	"context"
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
		switch params.WorkloadType {
		case WorkloadTypeDelete:
			staged, stageErr := integrity.StageDeleteInputManifest(
				params.ItemsFile, params.RunID, params.ObjectCount, params.Bucket)
			if stageErr != nil {
				return params, fmt.Errorf("--items-file: %w", stageErr)
			}
			if staged.MultipleBuckets && params.DeleteBatchSize > MinDeleteBatchSize {
				cleanupErr := os.RemoveAll(staged.StagingDir)
				return params, errors.Join(
					fmt.Errorf("--items-file is a multi-bucket manifest; use --delete-batch-size=1"),
					cleanupErr,
				)
			}
			params.ItemStagingDirs = append(params.ItemStagingDirs, staged.StagingDir)
			params.ItemsFile = containerItemFilesDir + "/" + integrity.VerifyInputName
			params.SelectionSourceCount = staged.Completion.SourceRecordCount
			params.SelectionUniqueCount = staged.Completion.UniqueRecordCount
			params.SelectionSelectedCount = staged.Completion.SelectedRecordCount
			params.SelectionSHA256 = staged.Completion.ManifestSHA256
			params.SelectionOrder = SelectionOrderCanonical
			mounts = append(mounts,
				FileMount{HostPath: staged.ManifestPath, ContainerPath: params.ItemsFile},
				FileMount{HostPath: staged.CompletionPath, ContainerPath: containerItemFilesDir + "/" + integrity.VerifyInputCompletionName})
		case WorkloadTypeReadVerify:
			stagingDir, manifest, marker, stageErr := integrity.StageInputManifest(params.ItemsFile, params.RunID)
			if stageErr != nil {
				return params, fmt.Errorf("--items-file: %w", stageErr)
			}
			params.ItemStagingDirs = append(params.ItemStagingDirs, stagingDir)
			params.ItemsFile = containerItemFilesDir + "/" + integrity.VerifyInputName
			mounts = append(mounts,
				FileMount{HostPath: manifest, ContainerPath: params.ItemsFile},
				FileMount{HostPath: marker, ContainerPath: containerItemFilesDir + "/" + integrity.VerifyInputCompletionName})
		default:
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
func CleanupPreparedItemFiles(ctx context.Context, params Params) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("prepared item cleanup canceled: %w", err)
	}
	var cleanupErrs []error
	for _, dir := range params.ItemStagingDirs {
		if err := ctx.Err(); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("prepared item cleanup canceled: %w", err))
			break
		}
		if strings.HasPrefix(filepath.Base(dir), "spt-integrity-input-") {
			if err := os.RemoveAll(dir); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove prepared item staging %q: %w", dir, err))
			}
		}
	}
	return errors.Join(cleanupErrs...)
}
