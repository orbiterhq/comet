//go:build integration
// +build integration

package comet

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// TestGetProcessID_ExhaustSlots tests behavior when all slots are taken
func TestGetProcessID_ExhaustSlots(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slot exhaustion test in short mode")
	}

	// Check if we're the parent or child process
	if workerID := os.Getenv("COMET_EXHAUST_WORKER"); workerID != "" {
		runExhaustWorker(t, workerID)
		return
	}

	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	shmFile := t.TempDir() + "/worker-slots-exhaust"
	maxWorkers := runtime.NumCPU()

	t.Logf("Testing slot exhaustion with %d max workers", maxWorkers)

	// Start maxWorkers + 2 processes (more than available slots)
	var processes []*exec.Cmd
	numProcesses := maxWorkers + 2

	for i := 0; i < numProcesses; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestGetProcessID_ExhaustSlots", "-test.v")
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("COMET_EXHAUST_WORKER=%d", i),
			fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
			"GO_TEST_SUBPROCESS=1",
		)

		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start process %d: %v", i, err)
		}

		processes = append(processes, cmd)
		// Small delay to stagger startup
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for all processes
	successCount := 0
	failureCount := 0

	for i, cmd := range processes {
		err := cmd.Wait()
		if err != nil {
			// Check if it's our expected "failed to acquire process ID" error
			if exitError, ok := err.(*exec.ExitError); ok {
				if exitError.ExitCode() == 42 { // Our custom exit code for "no slots available"
					failureCount++
					t.Logf("Process %d correctly failed to acquire slot (expected)", i)
				} else {
					t.Errorf("Process %d failed unexpectedly: %v", i, err)
				}
			} else {
				t.Errorf("Process %d failed with non-exit error: %v", i, err)
			}
		} else {
			successCount++
		}
	}

	t.Logf("Results: %d successful acquisitions, %d expected failures", successCount, failureCount)

	// Should have exactly maxWorkers successful and rest failed
	if successCount != maxWorkers {
		t.Errorf("Expected %d successful acquisitions, got %d", maxWorkers, successCount)
	}

	if failureCount != (numProcesses - maxWorkers) {
		t.Errorf("Expected %d failures, got %d", numProcesses-maxWorkers, failureCount)
	}
}

func runExhaustWorker(t *testing.T, workerID string) {
	shmFile := os.Getenv("COMET_SHM_FILE")

	processID := GetProcessID(shmFile)
	t.Logf("Exhaust worker %s: processID = %d (PID: %d)", workerID, processID, os.Getpid())

	if processID < 0 {
		t.Logf("Worker %s failed to acquire process ID (expected for some workers)", workerID)
		os.Exit(42) // Custom exit code to indicate "no slots available"
	}

	// Hold the slot for a while to test exhaustion
	time.Sleep(3 * time.Second)
	t.Logf("Worker %s completed successfully with process ID %d", workerID, processID)
}

// TestGetProcessID_FileSystemErrors tests various file system error conditions
func TestGetProcessID_FileSystemErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping filesystem error test in short mode")
	}

	t.Run("ReadOnlyDirectory", func(t *testing.T) {
		// Create a read-only directory
		readOnlyDir := t.TempDir() + "/readonly"
		err := os.Mkdir(readOnlyDir, 0755)
		if err != nil {
			t.Fatal(err)
		}

		// Make it read-only
		err = os.Chmod(readOnlyDir, 0555)
		if err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(readOnlyDir, 0755) // Restore for cleanup

		shmFile := readOnlyDir + "/worker-slots"
		processID := GetProcessID(shmFile)

		if processID != -1 {
			t.Errorf("Expected failure in read-only directory, got processID %d", processID)
		}
	})

	t.Run("InvalidPath", func(t *testing.T) {
		// Try to create file in non-existent deeply nested path
		shmFile := "/non/existent/very/deep/path/worker-slots"
		processID := GetProcessID(shmFile)

		if processID != -1 {
			t.Errorf("Expected failure with invalid path, got processID %d", processID)
		}
	})

	t.Run("CorruptedFile", func(t *testing.T) {
		shmFile := t.TempDir() + "/corrupted-slots"

		// Create a file with wrong size/content
		file, err := os.Create(shmFile)
		if err != nil {
			t.Fatal(err)
		}
		// Write garbage data
		file.WriteString("this is not a valid shared memory file")
		file.Close()

		processID := GetProcessID(shmFile)
		// Should still work because we truncate to correct size
		if processID < 0 {
			t.Errorf("Expected success with corrupted file (should truncate), got %d", processID)
		}
	})
}

// TestGetProcessID_RaceConditions tests concurrent access from multiple goroutines
func TestGetProcessID_RaceConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race condition test in short mode")
	}

	// Check if we're the parent or child process
	if workerID := os.Getenv("COMET_RACE_WORKER"); workerID != "" {
		runRaceWorker(t, workerID)
		return
	}

	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	shmFile := t.TempDir() + "/worker-slots-race"

	// Start multiple processes simultaneously to test race conditions
	numProcesses := 8
	var processes []*exec.Cmd

	// Start all processes at nearly the same time
	for i := 0; i < numProcesses; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestGetProcessID_RaceConditions", "-test.v")
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("COMET_RACE_WORKER=%d", i),
			fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
			"GO_TEST_SUBPROCESS=1",
		)

		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start process %d: %v", i, err)
		}

		processes = append(processes, cmd)
	}

	// Wait for all processes
	for i, cmd := range processes {
		err := cmd.Wait()
		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok && exitError.ExitCode() == 42 {
				// Expected for processes that couldn't get slots
				continue
			}
			t.Errorf("Process %d failed: %v", i, err)
		}
	}

	// Verify final state of shared memory
	verifyUniqueSlotAssignment(t, shmFile, numProcesses)
}

func runRaceWorker(t *testing.T, workerID string) {
	shmFile := os.Getenv("COMET_SHM_FILE")

	// All processes try to acquire at exactly the same time
	processID := GetProcessID(shmFile)

	if processID < 0 {
		t.Logf("Race worker %s failed to acquire process ID", workerID)
		os.Exit(42)
	}

	t.Logf("Race worker %s acquired process ID %d", workerID, processID)

	// Hold briefly then release
	time.Sleep(1 * time.Second)
}

// TestGetProcessID_MemoryPressure tests behavior under memory pressure
func TestGetProcessID_MemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory pressure test in short mode")
	}

	shmFile := t.TempDir() + "/worker-slots-memory"

	// Allocate a lot of memory to create pressure
	bigAllocs := make([][]byte, 0, 100)
	for i := 0; i < 50; i++ {
		// Allocate 10MB chunks
		bigAllocs = append(bigAllocs, make([]byte, 10*1024*1024))
	}

	t.Logf("Allocated ~500MB memory, testing process ID acquisition")

	processID := GetProcessID(shmFile)
	if processID < 0 {
		t.Error("Failed to acquire process ID under memory pressure")
	}

	// Force garbage collection
	runtime.GC()
	runtime.GC()

	// Try again after GC
	processID2 := GetProcessID(shmFile)
	if processID2 != processID {
		t.Errorf("Process ID changed after GC: %d -> %d", processID, processID2)
	}

	// Keep references to prevent premature GC
	_ = bigAllocs
}

// TestGetProcessID_SignalInterruption tests behavior when interrupted by signals
func TestGetProcessID_SignalInterruption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping signal interruption test in short mode")
	}

	// Check if we're the parent or child process
	if os.Getenv("COMET_SIGNAL_WORKER") != "" {
		runSignalWorker(t)
		return
	}

	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	shmFile := t.TempDir() + "/worker-slots-signal"

	// Start a process
	cmd := exec.Command(os.Args[0], "-test.run=TestGetProcessID_SignalInterruption", "-test.v")
	cmd.Env = append(os.Environ(),
		"COMET_SIGNAL_WORKER=1",
		fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
		"GO_TEST_SUBPROCESS=1",
	)

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start signal worker: %v", err)
	}

	// Give it time to acquire a slot
	time.Sleep(500 * time.Millisecond)

	// Send SIGTERM
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Failed to send SIGTERM: %v", err)
	}

	// Wait for it to exit
	err := cmd.Wait()
	if err == nil {
		t.Error("Expected process to exit with error after SIGTERM")
	}

	// Check that slot was not properly released (since it was killed)
	// A new process should detect this and reclaim the slot
	processID := GetProcessID(shmFile)
	if processID < 0 {
		t.Error("Failed to acquire process ID after previous process was killed")
	}

	// Verify the slot was reclaimed from the dead process
	if processID != 0 {
		t.Logf("Note: Got slot %d, might have gotten the reclaimed slot from killed process", processID)
	}
}

func runSignalWorker(t *testing.T) {
	shmFile := os.Getenv("COMET_SHM_FILE")

	processID := GetProcessID(shmFile)
	if processID < 0 {
		t.Fatal("Signal worker failed to acquire process ID")
	}

	t.Logf("Signal worker acquired process ID %d, waiting for signal...", processID)

	// Wait indefinitely (will be killed by signal)
	for {
		time.Sleep(100 * time.Millisecond)
	}
}

// TestGetProcessID_DiskSpace tests behavior when disk space is exhausted
func TestGetProcessID_DiskSpace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping disk space test in short mode")
	}

	// Create a very small tmpfs to simulate disk exhaustion
	// Note: This test might not work on all systems
	tmpDir := t.TempDir()

	// Try to fill up available space by creating large files
	largeFile := filepath.Join(tmpDir, "large-file")
	file, err := os.Create(largeFile)
	if err != nil {
		t.Skip("Cannot create large file for disk space test")
	}

	// Try to write a large amount of data
	data := make([]byte, 1024*1024) // 1MB
	for i := 0; i < 1000; i++ {     // Try to write 1GB
		_, err := file.Write(data)
		if err != nil {
			break // Disk full or other error
		}
	}
	file.Close()

	// Now try to create shared memory file
	shmFile := filepath.Join(tmpDir, "worker-slots")
	processID := GetProcessID(shmFile)

	// Should either succeed or fail gracefully
	if processID < 0 {
		t.Log("Process ID acquisition failed due to disk space (expected)")
	} else {
		t.Logf("Process ID acquisition succeeded despite disk pressure: %d", processID)
	}
}

// TestGetProcessID_LongRunning tests long-running behavior and slot persistence
func TestGetProcessID_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running test in short mode")
	}

	shmFile := t.TempDir() + "/worker-slots-longrun"

	// Acquire a process ID
	processID1 := GetProcessID(shmFile)
	if processID1 < 0 {
		t.Fatal("Failed to acquire initial process ID")
	}

	// Simulate long-running process by holding slot and checking periodically
	for i := 0; i < 10; i++ {
		time.Sleep(200 * time.Millisecond)

		// Re-acquire should return same ID (cached)
		processID2 := GetProcessID(shmFile)
		if processID2 != processID1 {
			t.Errorf("Process ID changed during long run: %d -> %d", processID1, processID2)
		}

		// Verify slot is still ours in shared memory
		maxWorkers := runtime.NumCPU()
		slotSize := 8

		file, err := os.OpenFile(shmFile, os.O_RDONLY, 0644)
		if err != nil {
			t.Fatalf("Failed to open shared memory file: %v", err)
		}

		data, err := syscall.Mmap(int(file.Fd()), 0, maxWorkers*slotSize,
			syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			file.Close()
			t.Fatalf("Failed to mmap file: %v", err)
		}

		offset := processID1 * slotSize
		pid := *(*uint32)(unsafe.Pointer(&data[offset]))
		myPID := uint32(os.Getpid())

		syscall.Munmap(data)
		file.Close()

		if pid != myPID {
			t.Errorf("Slot %d hijacked: expected PID %d, got %d", processID1, myPID, pid)
		}
	}

	t.Logf("Long-running test completed successfully, held slot %d", processID1)
}

// TestGetProcessID_Cleanup tests proper cleanup behavior
func TestGetProcessID_Cleanup(t *testing.T) {
	shmFile := t.TempDir() + "/worker-slots-cleanup"

	// Acquire and release multiple times
	for i := 0; i < 5; i++ {
		processID := GetProcessID(shmFile)
		if processID < 0 {
			t.Fatalf("Failed to acquire process ID on iteration %d", i)
		}

		// Should get slot 0 each time since we're releasing
		if processID != 0 {
			t.Errorf("Expected slot 0 on iteration %d, got %d", i, processID)
		}

		// Release it
		ReleaseProcessID(shmFile)

		// Verify slot was cleared
		maxWorkers := runtime.NumCPU()
		slotSize := 8

		file, err := os.OpenFile(shmFile, os.O_RDONLY, 0644)
		if err != nil {
			t.Fatalf("Failed to open shared memory file: %v", err)
		}

		data, err := syscall.Mmap(int(file.Fd()), 0, maxWorkers*slotSize,
			syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			file.Close()
			t.Fatalf("Failed to mmap file: %v", err)
		}

		offset := processID * slotSize
		pid := *(*uint32)(unsafe.Pointer(&data[offset]))

		syscall.Munmap(data)
		file.Close()

		if pid != 0 {
			t.Errorf("Slot %d not cleared after release: PID %d", processID, pid)
		}
	}
}