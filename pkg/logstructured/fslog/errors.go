package fslog

import "errors"

var (
	ErrNotImplemented = errors.New("filesystem backend is not implemented yet")
	ErrLocked         = errors.New("filesystem backend directory is already locked")
	ErrWriteConflict  = errors.New("filesystem backend write conflict")
)
