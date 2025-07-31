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

	// Parent process
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Create a client to initialize the shard
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write one entry to create the shard
	ctx := context.Background()
	client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte(`{"init":true}`)})
	client.Close()

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

			cmd := exec.Command(executable, "-test.run", "TestMultiProcessFileLocking", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_LOCK_TEST_WORKER=%d", id),
				fmt.Sprintf("COMET_LOCK_TEST_DIR=%s", dir),
			)

			output, err := cmd.CombinedOutput()
			if err != nil {
				results <- fmt.Sprintf("Worker %d failed: %v", id, err)
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

	// In multi-process mode with file locking, we expect some processes to be blocked
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

	// Now try to write through the client
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Worker %s: failed to create client: %v", workerID, err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{
		[]byte(fmt.Sprintf(`{"worker":"%s","pid":%d}`, workerID, pid)),
	})

	if err != nil {
		t.Logf("Worker %s: write failed: %v", workerID, err)
	} else {
		t.Logf("Worker %s: write succeeded", workerID)
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

	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Start a worker that will "crash" while holding the lock
	cmd := exec.Command(executable, "-test.run", "TestMultiProcessCrashRecovery", "-test.v")
	cmd.Env = append(os.Environ(),
		"COMET_CRASH_TEST_WORKER=crasher",
		fmt.Sprintf("COMET_CRASH_TEST_DIR=%s", dir),
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

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{
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
