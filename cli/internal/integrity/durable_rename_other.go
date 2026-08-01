//go:build !linux && !windows

package integrity

import "os"

// A same-filesystem hard link makes the destination visible atomically and fails if it already
// exists. Removing the private source name afterward is equivalent to a no-replace rename for the
// fully synchronized regular files published by this package. A crash between the calls may leave
// the private staging name behind, but never exposes partial content or replaces canonical data.
func renameNoReplace(source, destination string) error {
	if err := os.Link(source, destination); err != nil {
		return err
	}
	if err := os.Remove(source); err != nil {
		return err
	}
	return nil
}
