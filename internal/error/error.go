package error

import (
	"errors"
	"strings"

	"github.com/mattn/go-sqlite3"
)

type Severity int

const (
	Recoverable Severity = iota
	Fatal
)

func Classify(err error) Severity {
	switch {
	case errors.Is(err, sqlite3.ErrConstraint):
		return Recoverable

	case strings.Contains(err.Error(), "syntax error"):
		return Fatal

	case strings.Contains(err.Error(), "disk I/O error"):
		return Fatal

	case strings.Contains(err.Error(), "database disk image is malformed"):
		return Fatal

	default:
		return Recoverable
	}
}
