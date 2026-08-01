package integrity

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Crash-durable publication requires same-directory rename semantics and a filesystem that honors
// fsync for regular files and directories. Calls fail closed when the host filesystem cannot
// provide those primitives; network and userspace filesystems must document equivalent persistence
// semantics before their output is treated as crash durable.

type durablePublicationOperations interface {
	syncFile(path string) error
	rename(source, destination string) error
	syncDirectory(path string) error
}

type osDurablePublicationOperations struct{}

const durableArtifactFileMode os.FileMode = 0o600

func (osDurablePublicationOperations) syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0) // #nosec G304 -- private publication path
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	return errors.Join(syncErr, file.Close())
}

func (osDurablePublicationOperations) rename(source, destination string) error {
	return os.Rename(source, destination)
}

func (osDurablePublicationOperations) syncDirectory(path string) error {
	directory, err := os.Open(path) // #nosec G304 -- internally resolved publication directory
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	return errors.Join(syncErr, directory.Close())
}

var durableOSOperations durablePublicationOperations = osDurablePublicationOperations{}

func durableRenameWithOperations(source, destination string, operations durablePublicationOperations) error {
	destinationDirectory, err := durablePublicationDirectory(source, destination)
	if err != nil {
		return err
	}
	if err := operations.syncFile(source); err != nil {
		return fmt.Errorf("synchronize publication file %s: %w", filepath.Base(source), err)
	}
	return renameSyncedFileToDirectoryWithOperations(
		source, destination, destinationDirectory, operations,
	)
}

func durableRenameNoReplace(source, destination string) error {
	destinationDirectory, err := durablePublicationDirectory(source, destination)
	if err != nil {
		return err
	}
	if err = durableOSOperations.syncFile(source); err != nil {
		return fmt.Errorf("synchronize publication file %s: %w", filepath.Base(source), err)
	}
	if err = renameNoReplace(source, destination); err != nil {
		return fmt.Errorf("atomically publish %s without replacement: %w", filepath.Base(destination), err)
	}
	if err = durableOSOperations.syncDirectory(destinationDirectory); err != nil {
		return fmt.Errorf(
			"published %s but failed to synchronize its directory; durable state is indeterminate: %w",
			filepath.Base(destination), err,
		)
	}
	return nil
}

// durableRenameNoReplaceOrMatch publishes a deterministic non-pair artifact. A prior destination
// is accepted only when its bytes exactly match the newly derived staging file; the existing file
// and directory are then resynchronized so a retry can resolve an earlier indeterminate result.
func durableRenameNoReplaceOrMatch(source, destination string) error {
	destinationDirectory, err := durablePublicationDirectory(source, destination)
	if err != nil {
		return err
	}
	if err = durableOSOperations.syncFile(source); err != nil {
		return fmt.Errorf("synchronize publication file %s: %w", filepath.Base(source), err)
	}
	if err = renameNoReplace(source, destination); err == nil {
		if err = durableOSOperations.syncDirectory(destinationDirectory); err != nil {
			return fmt.Errorf(
				"published %s but failed to synchronize its directory; durable state is indeterminate: %w",
				filepath.Base(destination), err,
			)
		}
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("atomically publish %s without replacement: %w", filepath.Base(destination), err)
	}

	equal, compareErr := filesEqual(source, destination)
	if compareErr != nil {
		return fmt.Errorf("compare existing deterministic artifact %s: %w", filepath.Base(destination), compareErr)
	}
	if !equal {
		return fmt.Errorf("existing deterministic artifact %s differs from current derivation: %w",
			filepath.Base(destination), os.ErrExist)
	}
	if err = durableOSOperations.syncFile(destination); err != nil {
		return fmt.Errorf("synchronize existing deterministic artifact %s: %w", filepath.Base(destination), err)
	}
	if err = os.Remove(source); err != nil {
		return fmt.Errorf("remove matching publication staging file %s: %w", filepath.Base(source), err)
	}
	if err = durableOSOperations.syncDirectory(destinationDirectory); err != nil {
		return fmt.Errorf("synchronize recovered deterministic artifact directory for %s: %w",
			filepath.Base(destination), err)
	}
	return nil
}

const deterministicArtifactCompareBufferSize = 64 * 1024

func filesEqual(leftPath, rightPath string) (bool, error) {
	leftInfo, err := os.Stat(leftPath)
	if err != nil {
		return false, err
	}
	rightInfo, err := os.Stat(rightPath)
	if err != nil {
		return false, err
	}
	if leftInfo.Size() != rightInfo.Size() {
		return false, nil
	}
	left, err := os.Open(leftPath) // #nosec G304 -- internally resolved staging path
	if err != nil {
		return false, err
	}
	defer func() { _ = left.Close() }()
	right, err := os.Open(rightPath) // #nosec G304 -- internally resolved canonical path
	if err != nil {
		return false, err
	}
	defer func() { _ = right.Close() }()
	leftBuffer := make([]byte, deterministicArtifactCompareBufferSize)
	rightBuffer := make([]byte, deterministicArtifactCompareBufferSize)
	for {
		leftCount, leftErr := io.ReadFull(left, leftBuffer)
		rightCount, rightErr := io.ReadFull(right, rightBuffer)
		if leftCount != rightCount || !bytes.Equal(leftBuffer[:leftCount], rightBuffer[:rightCount]) {
			return false, nil
		}
		if errors.Is(leftErr, io.EOF) && errors.Is(rightErr, io.EOF) {
			return true, nil
		}
		if errors.Is(leftErr, io.ErrUnexpectedEOF) && errors.Is(rightErr, io.ErrUnexpectedEOF) {
			return true, nil
		}
		if leftErr != nil || rightErr != nil {
			return false, errors.Join(leftErr, rightErr)
		}
	}
}

func durablePublicationDirectory(source, destination string) (string, error) {
	sourceDirectory, err := filepath.Abs(filepath.Dir(source))
	if err != nil {
		return "", fmt.Errorf("resolve publication source directory: %w", err)
	}
	destinationDirectory, err := filepath.Abs(filepath.Dir(destination))
	if err != nil {
		return "", fmt.Errorf("resolve publication destination directory: %w", err)
	}
	if sourceDirectory != destinationDirectory {
		return "", fmt.Errorf(
			"crash-durable publication requires source and destination in one directory: %s -> %s",
			source, destination,
		)
	}
	return destinationDirectory, nil
}

func renameSyncedFileToDirectoryWithOperations(
	source string,
	destination string,
	destinationDirectory string,
	operations durablePublicationOperations,
) error {
	var err error
	if err = operations.rename(source, destination); err != nil {
		return fmt.Errorf("atomically publish %s: %w", filepath.Base(destination), err)
	}
	if err = operations.syncDirectory(destinationDirectory); err != nil {
		return fmt.Errorf(
			"published %s but failed to synchronize its directory; durable state is indeterminate: %w",
			filepath.Base(destination), err,
		)
	}
	return nil
}

func closeAndPublishTempFile(
	tempFile *os.File,
	tempPath string,
	destination string,
) error {
	return closeAndPublishTempFileWithOperations(
		tempFile, tempPath, destination, durableOSOperations,
	)
}

func closeAndPublishTempFileWithOperations(
	tempFile *os.File,
	tempPath string,
	destination string,
	operations durablePublicationOperations,
) error {
	if err := tempFile.Chmod(durableArtifactFileMode); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("set publication permissions: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close publication staging file: %w", err)
	}
	return durableRenameWithOperations(tempPath, destination, operations)
}

func closeAndPublishTempFileNoReplace(
	tempFile *os.File,
	tempPath string,
	destination string,
) error {
	if err := tempFile.Chmod(durableArtifactFileMode); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("set publication permissions: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close publication staging file: %w", err)
	}
	return durableRenameNoReplace(tempPath, destination)
}

func closeAndPublishTempFileNoReplaceOrMatch(
	tempFile *os.File,
	tempPath string,
	destination string,
) error {
	if err := tempFile.Chmod(durableArtifactFileMode); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("set publication permissions: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close publication staging file: %w", err)
	}
	return durableRenameNoReplaceOrMatch(tempPath, destination)
}

func writeFileDurableAtomicWithOperations(
	destination string,
	data []byte,
	pattern string,
	operations durablePublicationOperations,
) error {
	tempFile, err := os.CreateTemp(filepath.Dir(destination), pattern)
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err = tempFile.Write(data); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err = closeAndPublishTempFileWithOperations(
		tempFile, tempPath, destination, operations,
	); err != nil {
		return err
	}
	committed = true
	return nil
}
