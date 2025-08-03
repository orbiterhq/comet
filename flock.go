package comet

import (
	"os"
	"syscall"
	"time"
)

// IndexLock provides file-based locking for index operations in multi-process mode
type IndexLock struct {
	file     *os.File
	filePath string
}

// NewIndexLock creates a new index lock for the given shard directory
func NewIndexLock(shardDir string) *IndexLock {
	lockPath := shardDir + "/index.lock"
	return &IndexLock{
		filePath: lockPath,
	}
}

// Lock acquires an exclusive lock on the index file with timeout
func (l *IndexLock) Lock(timeout time.Duration) error {
	// Open or create the lock file
	file, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Try to acquire exclusive lock with timeout
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			l.file = file
			return nil
		}

		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			file.Close()
			return err
		}

		// Wait a bit before retrying
		time.Sleep(1 * time.Millisecond)
	}

	file.Close()
	return syscall.ETIMEDOUT
}

// Unlock releases the lock
func (l *IndexLock) Unlock() error {
	if l.file == nil {
		return nil
	}

	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil

	if err != nil {
		return err
	}
	return closeErr
}

// TryLock attempts to acquire the lock without blocking
func (l *IndexLock) TryLock() error {
	// Open or create the lock file
	file, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		file.Close()
		return err
	}

	l.file = file
	return nil
}
