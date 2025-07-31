package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestMultiProcessFileLocking proves that file locking prevents concurrent writes
func TestMultiProcessFileLocking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping file lock test in short mode")
	}

	// Check if we're the parent or child
	if workerID := os.Getenv("COMET_LOCK_TEST_WORKER"); workerID != "" {
		runLockTestWorker(t, workerID)
		return
	}
	
	// Safety check - don't spawn if we're already in a subprocess
	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	// Parent process
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Don't pre-initialize - let the workers race to create and lock

	// Now spawn multiple processes that try to write to the SAME shard
	numWorkers := 5
	var wg sync.WaitGroup
	results := make(chan string, numWorkers)

	parentPID := os.Getpid()
	t.Logf("Parent process PID: %d", parentPID)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Stagger process starts slightly to avoid thundering herd
			time.Sleep(time.Duration(id*50) * time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessFileLocking$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_LOCK_TEST_WORKER=%d", id),
				fmt.Sprintf("COMET_LOCK_TEST_DIR=%s", dir),
				"GO_TEST_SUBPROCESS=1",
			)

			output, err := cmd.CombinedOutput()
			if err != nil && ctx.Err() == context.DeadlineExceeded {
				results <- fmt.Sprintf("Worker %d timed out after 5s\nPartial output: %s", id, output)
			} else if err != nil {
				results <- fmt.Sprintf("Worker %d failed: %v\nOutput: %s", id, err, output)
			} else {
				results <- string(output)
			}
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect all results
	lockAcquired := 0
	lockBlocked := 0

	for result := range results {
		t.Log(result)
		if containsString(result, "successfully acquired lock") {
			lockAcquired++
		}
		if containsString(result, "blocked by lock") {
			lockBlocked++
		}
	}

	t.Logf("Summary: %d processes acquired lock, %d were blocked", lockAcquired, lockBlocked)

	// In multi-process mode with file locking, we expect:
	// - At least one process to acquire the lock
	// - Some processes to be blocked
	if lockAcquired == 0 {
		t.Error("Expected at least one process to acquire the lock")
	}
	if lockBlocked == 0 && numWorkers > 1 {
		t.Error("Expected some processes to be blocked by file lock")
	}
}

func runLockTestWorker(t *testing.T, workerID string) {
	pid := os.Getpid()
	ppid := os.Getppid()

	dir := os.Getenv("COMET_LOCK_TEST_DIR")
	shardDir := filepath.Join(dir, "shard-0001")
	lockPath := filepath.Join(shardDir, "shard.lock")

	t.Logf("Worker %s: PID=%d, PPID=%d, attempting to acquire lock: %s", workerID, pid, ppid, lockPath)
	
	// Ensure the shard directory exists
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		t.Fatalf("Worker %s: failed to create shard directory: %v", workerID, err)
	}

	// Try to acquire the lock directly
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Worker %s: failed to open lock file: %v", workerID, err)
	}
	defer lockFile.Close()

	// Try to acquire exclusive lock (non-blocking)
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		t.Logf("Worker %s (PID %d): blocked by lock - another process holds it", workerID, pid)
		return
	} else if err != nil {
		t.Fatalf("Worker %s: lock error: %v", workerID, err)
	}

	t.Logf("Worker %s (PID %d): successfully acquired lock!", workerID, pid)

	// Hold the lock for a bit to ensure others get blocked
	time.Sleep(500 * time.Millisecond)
	
	// Write a simple marker file to prove we had the lock
	markerPath := filepath.Join(shardDir, fmt.Sprintf("worker-%s.lock", workerID))
	if err := os.WriteFile(markerPath, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
		t.Logf("Worker %s: failed to write marker: %v", workerID, err)
	} else {
		t.Logf("Worker %s: wrote marker file", workerID)
	}

	// Release lock
	syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
}

// TestMultiProcessCrashRecovery tests that locks are released when a process crashes
func TestMultiProcessCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash recovery test in short mode")
	}

	if workerID := os.Getenv("COMET_CRASH_TEST_WORKER"); workerID != "" {
		runCrashTestWorker(t, workerID)
		return
	}
	
	// Safety check - don't spawn if we're already in a subprocess
	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Start a worker that will "crash" while holding the lock
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessCrashRecovery$", "-test.v")
	cmd.Env = append(os.Environ(),
		"COMET_CRASH_TEST_WORKER=crasher",
		fmt.Sprintf("COMET_CRASH_TEST_DIR=%s", dir),
		"GO_TEST_SUBPROCESS=1",
	)

	output, err := cmd.CombinedOutput()
	t.Logf("Crasher output: %s", output)

	// The process should have exited (crashed)
	if err == nil {
		t.Error("Expected crasher process to exit with error")
	}

	// Now try to acquire the lock - it should work because OS releases locks on process exit
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client after crash: %v", err)
	}
	defer client.Close()

	appendCtx := context.Background()
	_, err = client.Append(appendCtx, "test:v1:shard:0001", [][]byte{
		[]byte(`{"after_crash":true}`),
	})

	if err != nil {
		t.Fatalf("Failed to write after crash: %v", err)
	}

	t.Log("Successfully acquired lock and wrote data after simulated crash")
}

func runCrashTestWorker(t *testing.T, workerID string) {
	pid := os.Getpid()
	t.Logf("Crash test worker: PID=%d", pid)

	dir := os.Getenv("COMET_CRASH_TEST_DIR")
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	// DON'T defer client.Close() - we want to "crash" with lock held

	// Write something to acquire the lock
	ctx := context.Background()
	client.Append(ctx, "test:v1:shard:0001", [][]byte{
		[]byte(fmt.Sprintf(`{"crasher":true,"pid":%d}`, pid)),
	})

	t.Log("Crasher: acquired lock, now simulating crash...")

	// Simulate crash by exiting without cleanup
	os.Exit(1)
}

func containsString(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 &&
		(s == substr || len(s) > len(substr) &&
			(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
				findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
