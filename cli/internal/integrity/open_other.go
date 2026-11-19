//go:build !unix

package integrity

import "os"

// openWithoutWaiting opens path for reading. Without unix named pipes there is no writer to wait for.
func openWithoutWaiting(path string) (*os.File, error) {
	return os.Open(path) // #nosec G304 -- explicit user-selected local input file
}
