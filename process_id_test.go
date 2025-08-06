package comet

import (
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"unsafe"
)

func TestGetProcessID_SingleProcess(t *testing.T) {
	// Use a unique file for this test
	shmFile := t.TempDir() + "/worker-slots-single"

	processID := GetProcessID(shmFile)
	if processID != 0 {
		t.Errorf("Expected first process to get ID 0, got %d", processID)
	}

	// Calling again should return the same ID (cached)
	processID2 := GetProcessID(shmFile)
	if processID2 != 0 {
		t.Errorf("Expected same process to get same ID 0, got %d", processID2)
	}
}

func TestGetProcessID_SharedMemoryFile(t *testing.T) {
	// Test that the shared memory file is created correctly
	shmFile := t.TempDir() + "/worker-slots-shm"

	// File shouldn't exist initially
	if _, err := os.Stat(shmFile); err == nil {
		t.Error("Shared memory file should not exist initially")
	}

	processID := GetProcessID(shmFile)
	if processID < 0 {
		t.Errorf("Failed to create shared memory file and acquire process ID")
	}

	// File should exist now
	if _, err := os.Stat(shmFile); err != nil {
		t.Errorf("Shared memory file should exist after GetProcessID: %v", err)
	}

	// File should have correct size
	stat, err := os.Stat(shmFile)
	if err != nil {
		t.Fatalf("Failed to stat shared memory file: %v", err)
	}

	expectedSize := int64(runtime.NumCPU() * 8) // 8 bytes per slot
	if stat.Size() != expectedSize {
		t.Errorf("Expected file size %d, got %d", expectedSize, stat.Size())
	}

	// Check that our PID is in the file
	file, err := os.OpenFile(shmFile, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open shared memory file: %v", err)
	}
	defer file.Close()

	data, err := syscall.Mmap(int(file.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("Failed to mmap file: %v", err)
	}
	defer syscall.Munmap(data)

	// Check slot 0 should have our PID
	myPID := uint32(os.Getpid())
	slot0PID := *(*uint32)(unsafe.Pointer(&data[0]))

	if slot0PID != myPID {
		t.Errorf("Expected slot 0 to have PID %d, got %d", myPID, slot0PID)
	}
}

func TestGetProcessID_Integration(t *testing.T) {
	// Test the full integration with MultiProcessConfig
	shmFile := t.TempDir() + "/worker-slots-integration"

	processID := GetProcessID(shmFile)
	if processID < 0 {
		t.Fatal("Failed to acquire process ID")
	}

	// Should be able to create a MultiProcessConfig
	processCount := runtime.NumCPU()
	config := DeprecatedMultiProcessConfig(processID, processCount)

	// Validate the config
	if config.Concurrency.ProcessID != processID {
		t.Errorf("Expected ProcessID %d, got %d", processID, config.Concurrency.ProcessID)
	}

	if config.Concurrency.ProcessCount != processCount {
		t.Errorf("Expected ProcessCount %d, got %d", processCount, config.Concurrency.ProcessCount)
	}

	// Should be valid - test by trying to create a client
	tempDir := t.TempDir()
	client, err := NewClient(tempDir, config)
	if err != nil {
		t.Errorf("Config validation failed: %v", err)
	} else {
		client.Close()
	}
}

func TestGetProcessID_DefaultFile(t *testing.T) {
	// Test using the default shared memory file
	// Note: This test might interfere with other processes using the default file
	// In practice, you'd want isolation, but this tests the default behavior

	// Clean up any existing default file for this test
	defaultFile := filepath.Join(os.TempDir(), "comet-worker-slots")
	os.Remove(defaultFile)
	defer os.Remove(defaultFile)

	processID := GetProcessID()
	if processID < 0 {
		t.Error("Failed to acquire process ID with default file")
	}

	// Should be able to release it
	ReleaseProcessID(defaultFile)
}

func TestGetProcessID_ReleaseAndReacquire(t *testing.T) {
	shmFile := t.TempDir() + "/worker-slots-release"

	// Get a process ID
	processID := GetProcessID(shmFile)
	if processID < 0 {
		t.Fatal("Failed to acquire process ID")
	}

	// Release it
	ReleaseProcessID(shmFile)

	// The same slot should be available for reuse
	// Note: In a real scenario, this would be a different process
	processID2 := GetProcessID(shmFile)
	if processID2 != processID {
		t.Errorf("Expected to reuse slot %d, got %d", processID, processID2)
	}
}

func TestIsProcessAlive(t *testing.T) {
	// Test with current process (should be alive)
	myPID := os.Getpid()
	if !isProcessAlive(myPID) {
		t.Error("Current process should be reported as alive")
	}

	// Test with invalid PIDs
	if isProcessAlive(0) {
		t.Error("PID 0 should not be reported as alive")
	}

	if isProcessAlive(-1) {
		t.Error("Negative PID should not be reported as alive")
	}

	// Test with a likely non-existent PID
	// Use a very high PID that's unlikely to exist
	if isProcessAlive(999999) {
		t.Log("PID 999999 reported as alive - this might be expected on some systems")
	}
}

// Benchmark the process ID acquisition performance
func BenchmarkGetProcessID(b *testing.B) {
	shmFile := b.TempDir() + "/worker-slots-bench"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processID := GetProcessID(shmFile)
		if processID < 0 {
			b.Fatal("Failed to acquire process ID")
		}
		// Note: In real usage, each process would only call this once
		// This benchmark tests the raw performance of the allocation mechanism
	}
}
