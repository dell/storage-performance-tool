// Package terminal reports whether the CLI's input comes from a terminal.
package terminal

import (
	"os"

	"github.com/mattn/go-isatty"
)

// IsTerminal reports whether fd is an open terminal. It is false for
// /dev/null, which is a character device but not a terminal, and for a pipe,
// a regular file or a closed descriptor.
func IsTerminal(fd uintptr) bool {
	return isatty.IsTerminal(fd)
}

// StdinIsTerminal reports whether standard input is a terminal, so that a
// prompt can be answered.
func StdinIsTerminal() bool {
	return IsTerminal(os.Stdin.Fd())
}
