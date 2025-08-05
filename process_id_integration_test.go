//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// TestGetProcessID_MultiProcess tests actual multi-process coordination
func TestGetProcessID_MultiProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping process ID test in short mode")
	}

	// Check if we're the parent or child process
	if workerID := os.Getenv("COMET_PROCESSID_WORKER"); workerID != "" {
		// We're a child process - run the worker
		runProcessIDWorker(t, workerID)
		return
	}

	// Safety check - don't spawn if we're already in a subprocess
	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	shmFile := t.TempDir() + "/worker-slots-integration"
	numProcesses := 4

	t.Logf("Starting process ID test with %d processes, shared memory file: %s", numProcesses, shmFile)

	// Start multiple processes
	var processes []*exec.Cmd
	var wg sync.WaitGroup

	for i := 0; i < numProcesses; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestGetProcessID_MultiProcess", "-test.v")
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("COMET_PROCESSID_WORKER=%d", i),
			fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
			"GO_TEST_SUBPROCESS=1",
		)

		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start process %d: %v", i, err)
		}

		processes = append(processes, cmd)
	}

	// Wait for all processes with timeout
	for i, cmd := range processes {
		wg.Add(1)
		go func(processIndex int, process *exec.Cmd) {
			defer wg.Done()

			done := make(chan error, 1)
			go func() {
				done <- process.Wait()
			}()

			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Process %d failed: %v", processIndex, err)
				}
			case <-time.After(30 * time.Second):
				t.Errorf("Process %d timed out", processIndex)
				process.Process.Kill()
			}
		}(i, cmd)
	}

	wg.Wait()

	// Verify that processes acquired unique IDs by checking the shared memory file
	verifyUniqueSlotAssignment(t, shmFile, numProcesses)
}

func runProcessIDWorker(t *testing.T, workerIDStr string) {
	workerID, _ := strconv.Atoi(workerIDStr)
	shmFile := os.Getenv("COMET_SHM_FILE")

	t.Logf("Worker %d starting (PID: %d)", workerID, os.Getpid())

	// Get process ID
	processID := GetProcessID(shmFile)
	t.Logf("Worker %d acquired process ID: %d", workerID, processID)

	if processID < 0 {
		t.Fatalf("Worker %d failed to acquire process ID", workerID)
	}

	// Test that we can create a Comet client with this process ID
	config := DeprecatedMultiProcessConfig(processID, runtime.NumCPU())

	tempDir := t.TempDir()
	client, err := NewClient(tempDir, config)
	if err != nil {
		t.Fatalf("Worker %d failed to create Comet client: %v", workerID, err)
	}
	defer client.Close()

	// Test basic operations
	ctx := context.Background()
	streamName := fmt.Sprintf("test:worker%d:shard:%04d", workerID, processID)

	// Write some data
	data := [][]byte{[]byte(fmt.Sprintf("Hello from worker %d", workerID))}
	ids, err := client.Append(ctx, streamName, data)
	if err != nil {
		t.Fatalf("Worker %d failed to write data: %v", workerID, err)
	}

	if len(ids) != 1 {
		t.Fatalf("Worker %d expected 1 ID, got %d", workerID, len(ids))
	}

	// Read it back
	length, err := client.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("Worker %d failed to get length: %v", workerID, err)
	}

	if length != 1 {
		t.Fatalf("Worker %d expected length 1, got %d", workerID, length)
	}

	// Hold the slot briefly to test concurrent access
	time.Sleep(200 * time.Millisecond)

	t.Logf("Worker %d completed successfully with process ID %d", workerID, processID)
}

func verifyUniqueSlotAssignment(t *testing.T, shmFile string, expectedProcesses int) {
	maxWorkers := runtime.NumCPU()
	slotSize := 8

	file, err := os.OpenFile(shmFile, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open shared memory file: %v", err)
	}
	defer file.Close()

	data, err := syscall.Mmap(int(file.Fd()), 0, maxWorkers*slotSize,
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("Failed to mmap file: %v", err)
	}
	defer syscall.Munmap(data)

	// Count occupied slots and verify uniqueness
	occupiedSlots := 0
	seenPIDs := make(map[uint32]int)

	for i := 0; i < maxWorkers; i++ {
		offset := i * slotSize
		pid := *(*uint32)(unsafe.Pointer(&data[offset]))
		if pid != 0 {
			occupiedSlots++
			seenPIDs[pid] = i
			t.Logf("Slot %d: PID %d", i, pid)
		}
	}

	if occupiedSlots == 0 {
		t.Error("No processes acquired slots")
	}

	// Each PID should only appear once
	if len(seenPIDs) != occupiedSlots {
		t.Errorf("Expected %d unique PIDs, got %d", occupiedSlots, len(seenPIDs))
	}

	t.Logf("Successfully assigned %d unique process slots out of %d processes", occupiedSlots, expectedProcesses)
}

// TestGetProcessID_ProcessRestart tests process restart scenarios
func TestGetProcessID_ProcessRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping process restart test in short mode")
	}

	// Check if we're the parent or child process
	if phase := os.Getenv("COMET_RESTART_PHASE"); phase != "" {
		runRestartWorker(t, phase)
		return
	}

	// Safety check
	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	shmFile := t.TempDir() + "/worker-slots-restart"
	t.Logf("Testing process restart with shared memory file: %s", shmFile)

	// Start first process
	cmd1 := exec.Command(os.Args[0], "-test.run=TestGetProcessID_ProcessRestart", "-test.v")
	cmd1.Env = append(os.Environ(),
		"COMET_RESTART_PHASE=1",
		fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
		"GO_TEST_SUBPROCESS=1",
	)

	if err := cmd1.Start(); err != nil {
		t.Fatalf("Failed to start first process: %v", err)
	}

	// Wait for it to complete
	if err := cmd1.Wait(); err != nil {
		t.Fatalf("First process failed: %v", err)
	}

	// Start second process (simulating restart)
	cmd2 := exec.Command(os.Args[0], "-test.run=TestGetProcessID_ProcessRestart", "-test.v")
	cmd2.Env = append(os.Environ(),
		"COMET_RESTART_PHASE=2",
		fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
		"GO_TEST_SUBPROCESS=1",
	)

	if err := cmd2.Start(); err != nil {
		t.Fatalf("Failed to start second process: %v", err)
	}

	// Wait for it to complete
	if err := cmd2.Wait(); err != nil {
		t.Fatalf("Second process failed: %v", err)
	}

	t.Log("Process restart test completed successfully")
}

func runRestartWorker(t *testing.T, phase string) {
	shmFile := os.Getenv("COMET_SHM_FILE")

	// Get process ID
	processID := GetProcessID(shmFile)
	t.Logf("Phase %s acquired process ID: %d (PID: %d)", phase, processID, os.Getpid())

	if processID < 0 {
		t.Fatalf("Failed to acquire process ID in phase %s", phase)
	}

	// Create Comet client
	config := DeprecatedMultiProcessConfig(processID, runtime.NumCPU())
	tempDir := t.TempDir()
	client, err := NewClient(tempDir, config)
	if err != nil {
		t.Fatalf("Failed to create Comet client in phase %s: %v", phase, err)
	}
	defer client.Close()

	// Test operations
	ctx := context.Background()
	streamName := fmt.Sprintf("test:phase%s:shard:%04d", phase, processID)

	data := [][]byte{[]byte(fmt.Sprintf("Data from phase %s", phase))}
	_, err = client.Append(ctx, streamName, data)
	if err != nil {
		t.Fatalf("Failed to write data in phase %s: %v", phase, err)
	}

	// Explicitly release the process ID before exiting (simulate graceful shutdown)
	if phase == "1" {
		ReleaseProcessID(shmFile)
		t.Logf("Phase %s released process ID %d", phase, processID)
	}

	t.Logf("Phase %s completed with process ID %d", phase, processID)
}

// TestGetProcessID_FailureRecovery tests recovery from process failures
func TestGetProcessID_FailureRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping failure recovery test in short mode")
	}

	// Check if we're the parent or child process
	if workerType := os.Getenv("COMET_FAILURE_WORKER"); workerType != "" {
		runFailureWorker(t, workerType)
		return
	}

	// Safety check
	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	shmFile := t.TempDir() + "/worker-slots-failure"
	t.Logf("Testing failure recovery with shared memory file: %s", shmFile)

	// Start a process that will be killed
	cmd1 := exec.Command(os.Args[0], "-test.run=TestGetProcessID_FailureRecovery", "-test.v")
	cmd1.Env = append(os.Environ(),
		"COMET_FAILURE_WORKER=victim",
		fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
		"GO_TEST_SUBPROCESS=1",
	)

	if err := cmd1.Start(); err != nil {
		t.Fatalf("Failed to start victim process: %v", err)
	}

	// Give it time to acquire a slot
	time.Sleep(200 * time.Millisecond)

	// Kill the process without cleanup
	if err := cmd1.Process.Kill(); err != nil {
		t.Fatalf("Failed to kill victim process: %v", err)
	}

	cmd1.Wait() // Clean up zombie

	// Start a recovery process that should detect the dead process and reuse its slot
	cmd2 := exec.Command(os.Args[0], "-test.run=TestGetProcessID_FailureRecovery", "-test.v")
	cmd2.Env = append(os.Environ(),
		"COMET_FAILURE_WORKER=recovery",
		fmt.Sprintf("COMET_SHM_FILE=%s", shmFile),
		"GO_TEST_SUBPROCESS=1",
	)

	if err := cmd2.Start(); err != nil {
		t.Fatalf("Failed to start recovery process: %v", err)
	}

	if err := cmd2.Wait(); err != nil {
		t.Fatalf("Recovery process failed: %v", err)
	}

	t.Log("Failure recovery test completed successfully")
}

func runFailureWorker(t *testing.T, workerType string) {
	shmFile := os.Getenv("COMET_SHM_FILE")

	processID := GetProcessID(shmFile)
	t.Logf("%s worker acquired process ID: %d (PID: %d)", workerType, processID, os.Getpid())

	if processID < 0 {
		t.Fatalf("%s worker failed to acquire process ID", workerType)
	}

	if workerType == "victim" {
		// This process will be killed, so just hold the slot
		t.Logf("Victim worker holding slot %d", processID)
		for {
			time.Sleep(100 * time.Millisecond)
		}
	} else if workerType == "recovery" {
		// This process should successfully reuse the dead process's slot
		t.Logf("Recovery worker successfully acquired slot %d", processID)

		// Test that we can use it
		config := DeprecatedMultiProcessConfig(processID, runtime.NumCPU())
		tempDir := t.TempDir()
		client, err := NewClient(tempDir, config)
		if err != nil {
			t.Fatalf("Recovery worker failed to create Comet client: %v", err)
		}
		defer client.Close()

		ctx := context.Background()
		streamName := fmt.Sprintf("test:recovery:shard:%04d", processID)

		data := [][]byte{[]byte("Recovery test data")}
		_, err = client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("Recovery worker failed to write data: %v", err)
		}

		t.Log("Recovery worker completed successfully")
	}
}
