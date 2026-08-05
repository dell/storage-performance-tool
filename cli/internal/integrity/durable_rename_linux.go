//go:build linux

package integrity

import (
	"errors"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// renameNoReplace atomically publishes one fully written same-directory file without allowing an
// existing canonical name to be replaced between validation and publication.
func renameNoReplace(source, destination string) error {
	err := unix.Renameat2(
		unix.AT_FDCWD,
		source,
		unix.AT_FDCWD,
		destination,
		unix.RENAME_NOREPLACE,
	)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
		return unsupportedNoReplaceProviderError(destination, err)
	}
	return err
}

func unsupportedNoReplaceProviderError(destination string, cause error) error {
	return fmt.Errorf(
		"filesystem/provider for results directory %q does not support renameat2(RENAME_NOREPLACE), required by the crash-durable integrity publication contract: %w",
		filepath.Dir(destination), cause,
	)
}
