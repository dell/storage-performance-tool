package integrity

import (
	"errors"
	"fmt"
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
