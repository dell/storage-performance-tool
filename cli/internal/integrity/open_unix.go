//go:build unix

package integrity

import (
	"os"
	"syscall"
)

// openWithoutWaiting opens path for reading without waiting for a writer if it is a named pipe.
func openWithoutWaiting(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0) // #nosec G304 -- explicit user-selected local input file
}

// StatItemsFile is os.Stat: on unix its result already holds the file's device and inode, so
// os.SameFile compares identities without opening the path again.
func StatItemsFile(path string) (os.FileInfo, error) {
	return os.Stat(path)
}
