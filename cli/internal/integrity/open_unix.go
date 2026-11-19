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
