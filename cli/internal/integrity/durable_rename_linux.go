//go:build linux

package integrity

import "golang.org/x/sys/unix"

// renameNoReplace atomically publishes one fully written same-directory file without allowing an
// existing canonical name to be replaced between validation and publication.
func renameNoReplace(source, destination string) error {
	return unix.Renameat2(
		unix.AT_FDCWD,
		source,
		unix.AT_FDCWD,
		destination,
		unix.RENAME_NOREPLACE,
	)
}
