package fslog

import "errors"

var (
	// ErrNotImplemented 只是保留给未来未落地路径使用；
	// 当前第一版正常运行时不应由已实现路径返回它。
	ErrNotImplemented = errors.New("filesystem backend is not implemented yet")
	// ErrLocked 表示同一个数据目录已经被其他进程持有独占锁。
	ErrLocked = errors.New("filesystem backend directory is already locked")
	// ErrWriteConflict 表示 create/update/delete 的前置条件与当前 key 状态不匹配。
	ErrWriteConflict = errors.New("filesystem backend write conflict")
)
