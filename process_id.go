package comet

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

var (
	processIDMutex sync.Mutex
	processIDCache = make(map[string]int)
)

// GetProcessID returns a unique process ID (0 to N-1) for multi-process deployments.
// It uses a shared memory file to coordinate process ID assignment across multiple
// processes that may start in any order.
//
// This is useful for deployments where you don't have explicit control over process
// startup order, such as:
// - systemd with multiple service instances
// - Container orchestration (Kubernetes, Docker Swarm)
// - Process managers (PM2, Supervisor)
// - Manual process spawning
//
// The function will return:
// - 0 to (NumCPU-1): Successfully acquired process ID
// - -1: Failed to acquire a process ID (all slots taken or error)
//
// Example usage:
//
//	processID := comet.GetProcessID()
//	if processID < 0 {
//	    log.Fatal("Failed to acquire process ID")
//	}
//	config := comet.MultiProcessConfig(processID, runtime.NumCPU())
//	client, err := comet.NewClient(dataDir, config)
func GetProcessID(shmFile ...string) int {
	shmFile_ := filepath.Join(os.TempDir(), "comet-worker-slots")
	if len(shmFile) > 0 {
		shmFile_ = shmFile[0]
	}
	return GetProcessIDWithFile(shmFile_)
}

// GetProcessIDWithFile is like GetProcessID but allows specifying a custom shared memory file.
// This is useful when running multiple independent Comet deployments on the same machine.
func GetProcessIDWithFile(shmFile string) int {
	processIDMutex.Lock()
	defer processIDMutex.Unlock()

	// Check cache first
	if result, exists := processIDCache[shmFile]; exists {
		return result
	}

	// Not cached, acquire a new process ID
	result := doGetProcessID(shmFile)
	// Only cache successful acquisitions
	if result >= 0 {
		processIDCache[shmFile] = result
	}
	return result
}

func doGetProcessID(shmFile string) int {
	if shmFile == "" {
		shmFile = filepath.Join(os.TempDir(), "comet-worker-slots")
	}
	maxWorkers := runtime.NumCPU()
	slotSize := 8 // 8 bytes for PID (uint32) + 4 bytes padding

	file, err := os.OpenFile(shmFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return -1
	}
	defer file.Close()

	// Only truncate if file is too small
	if stat, _ := file.Stat(); stat.Size() < int64(maxWorkers*slotSize) {
		file.Truncate(int64(maxWorkers * slotSize))
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, maxWorkers*slotSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return -1
	}
	defer syscall.Munmap(data)

	myPID := uint32(os.Getpid())

	// Try to acquire a slot
	for i := range maxWorkers {
		offset := i * slotSize
		pidPtr := (*uint32)(unsafe.Pointer(&data[offset]))

		// Try to claim an empty slot
		if atomic.CompareAndSwapUint32(pidPtr, 0, myPID) {
			return i
		}

		// Check if existing PID is still alive
		existingPID := atomic.LoadUint32(pidPtr)
		if existingPID != 0 && !isProcessAlive(int(existingPID)) {
			// Process is dead, try to claim its slot
			if atomic.CompareAndSwapUint32(pidPtr, existingPID, myPID) {
				return i
			}
		}

		// If this is our PID, we already have this slot
		if existingPID == myPID {
			return i
		}
	}

	return -1
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}

	// Check if process exists and we can signal it
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}

	// ESRCH means "no such process"
	if errno, ok := err.(syscall.Errno); ok && errno == syscall.ESRCH {
		return false
	}

	// EPERM means process exists but we can't signal it
	// Treat as alive to be safe
	return true
}

// ReleaseProcessID releases the process ID when shutting down gracefully.
// This is optional but helps with faster slot reuse.
func ReleaseProcessID(shmFile_ ...string) {
	shmFile := filepath.Join(os.TempDir(), "comet-worker-slots")
	if len(shmFile_) > 0 {
		shmFile = shmFile_[0]
	}
	processIDMutex.Lock()
	defer processIDMutex.Unlock()

	processIDResult, exists := processIDCache[shmFile]
	if !exists || processIDResult < 0 {
		return // No slot to release
	}

	maxWorkers := runtime.NumCPU()
	slotSize := 8

	file, err := os.OpenFile(shmFile, os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	data, err := syscall.Mmap(int(file.Fd()), 0, maxWorkers*slotSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return
	}
	defer syscall.Munmap(data)

	myPID := uint32(os.Getpid())
	offset := processIDResult * slotSize
	pidPtr := (*uint32)(unsafe.Pointer(&data[offset]))

	// Only clear if it's still our PID
	if atomic.CompareAndSwapUint32(pidPtr, myPID, 0) {
		// Remove from cache after successful release
		delete(processIDCache, shmFile)
	}
}
