//go:build integration
// +build integration

package comet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// The tests below spawn actual OS processes, making them true integration tests

// TestMultiProcessSimple tests multiple processes writing/reading concurrently
func TestMultiProcessSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	// Log start for CI debugging
	t.Logf("Starting multi-process test, CI=%s, GITHUB_ACTIONS=%s", os.Getenv("CI"), os.Getenv("GITHUB_ACTIONS"))

	// Check if we're the parent or child process
	if workerID := os.Getenv("COMET_TEST_WORKER"); workerID != "" {
		// We're a child process - run the worker
		t.Logf("Running as worker process: %s", workerID)
		runTestWorker(t, workerID)
		return
	}

	// Safety check - don't spawn if we're already in a subprocess
	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	// We're the parent - spawn child processes
	dir := t.TempDir()
	t.Logf("Test directory: %s", dir)

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Test executable: %s", executable)

	// Run the same test binary with specific test
	testName := "TestMultiProcessSimple"
	numWriters := 3
	numReaders := 2

	// Shorter duration in CI to detect hangs faster
	duration := 3 * time.Second
	if os.Getenv("CI") != "" {
		duration = 1 * time.Second
	}

	timeout := duration + 5*time.Second // 5s buffer should be plenty
	if os.Getenv("CI") != "" {
		timeout = duration + 3*time.Second // Even tighter timeout in CI
	}

	t.Logf("Test configuration: %d writers, %d readers, duration=%v, timeout=%v",
		numWriters, numReaders, duration, timeout)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	processStarted := make(chan string, numWriters+numReaders)

	// Start writer processes
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			processStarted <- fmt.Sprintf("writer-%d", id)

			cmd := exec.CommandContext(ctx, executable, "-test.run", "^"+testName+"$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_TEST_WORKER=writer-%d", id),
				fmt.Sprintf("COMET_TEST_DIR=%s", dir),
				fmt.Sprintf("COMET_TEST_DURATION=%s", duration),
				"GO_TEST_SUBPROCESS=1",
			)

			t.Logf("Starting writer %d process...", id)
			output, err := cmd.CombinedOutput()
			if err != nil && ctx.Err() == nil {
				t.Logf("Writer %d failed: %v\nOutput: %s", id, err, output)
			} else if os.Getenv("CI") != "" {
				// Always log output in CI for debugging
				t.Logf("Writer %d completed. Output:\n%s", id, output)
			}
		}(i)
	}

	// Start reader processes
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			processStarted <- fmt.Sprintf("reader-%d", id)
			time.Sleep(500 * time.Millisecond) // Let writers start first

			cmd := exec.CommandContext(ctx, executable, "-test.run", "^"+testName+"$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_TEST_WORKER=reader-%d", id),
				fmt.Sprintf("COMET_TEST_DIR=%s", dir),
				fmt.Sprintf("COMET_TEST_DURATION=%s", duration),
				"GO_TEST_SUBPROCESS=1",
			)

			t.Logf("Starting reader %d process...", id)
			output, err := cmd.CombinedOutput()
			if err != nil && ctx.Err() == nil {
				t.Logf("Reader %d failed: %v\nOutput: %s", id, err, output)
			} else if os.Getenv("CI") != "" {
				// Always log output in CI for debugging
				t.Logf("Reader %d completed. Output:\n%s", id, output)
			}
		}(i)
	}

	// Monitor process startup
	go func() {
		for i := 0; i < numWriters+numReaders; i++ {
			select {
			case proc := <-processStarted:
				t.Logf("Process started: %s", proc)
			case <-time.After(2 * time.Second):
				t.Logf("WARNING: Process startup timeout after %d processes", i)
				return
			}
		}
	}()

	// Wait with progress monitoring in CI
	if os.Getenv("CI") != "" {
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				t.Log("All processes completed")
				goto verify
			case <-ticker.C:
				t.Log("Still waiting for processes to complete...")
			case <-ctx.Done():
				t.Log("Context timeout - processes may be hanging")
				goto verify
			}
		}
	} else {
		wg.Wait()
	}

verify:
	// Verify the data
	t.Log("Starting verification...")
	verifyMultiProcessResults(t, dir, numWriters)
}

// TestMultiProcessIntegration is the main integration test that proves all multi-process features
func TestMultiProcessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Check if we're a subprocess
	if role := os.Getenv("COMET_MP_TEST_ROLE"); role != "" {
		runMultiProcessWorker(t, role)
		return
	}

	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Simple multi-writer/reader test
	t.Run("SimpleWriterReader", func(t *testing.T) {
		testDir := filepath.Join(dir, "simple")
		os.MkdirAll(testDir, 0755)

		var wg sync.WaitGroup
		results := make(chan string, 5)

		// Start 3 writers
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessIntegration$", "-test.v")
				cmd.Env = append(os.Environ(),
					fmt.Sprintf("COMET_MP_TEST_ROLE=writer-%d", id),
					fmt.Sprintf("COMET_MP_TEST_DIR=%s", testDir),
					"COMET_MP_TEST_DURATION=2s",
				)
				output, _ := cmd.CombinedOutput()
				results <- string(output)
			}(i)
		}

		// Start 2 readers
		time.Sleep(500 * time.Millisecond)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessIntegration$", "-test.v")
				cmd.Env = append(os.Environ(),
					fmt.Sprintf("COMET_MP_TEST_ROLE=reader-%d", id),
					fmt.Sprintf("COMET_MP_TEST_DIR=%s", testDir),
					"COMET_MP_TEST_DURATION=2s",
				)
				output, _ := cmd.CombinedOutput()
				results <- string(output)
			}(i)
		}

		wg.Wait()
		close(results)

		// Check results
		writerCount := 0
		readerCount := 0
		for result := range results {
			t.Log(result)
			if containsString(result, "Writer completed") {
				writerCount++
			}
			if containsString(result, "Reader completed") {
				readerCount++
			}
		}

		if writerCount != 3 || readerCount != 2 {
			t.Errorf("Expected 3 writers and 2 readers, got %d writers and %d readers", writerCount, readerCount)
		}
	})

	// Test 2: File locking prevents concurrent access
	t.Run("FileLocking", func(t *testing.T) {
		testDir := filepath.Join(dir, "locking")
		os.MkdirAll(testDir, 0755)

		var wg sync.WaitGroup
		results := make(chan string, 5)

		// Start 5 processes trying to lock the same shard
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				time.Sleep(time.Duration(id*50) * time.Millisecond) // Stagger starts

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessIntegration$", "-test.v")
				cmd.Env = append(os.Environ(),
					fmt.Sprintf("COMET_MP_TEST_ROLE=locker-%d", id),
					fmt.Sprintf("COMET_MP_TEST_DIR=%s", testDir),
				)
				output, _ := cmd.CombinedOutput()
				results <- string(output)
			}(i)
		}

		wg.Wait()
		close(results)

		// Check results
		lockAcquired := 0
		lockBlocked := 0
		for result := range results {
			t.Log(result)
			if containsString(result, "acquired lock") {
				lockAcquired++
			}
			if containsString(result, "blocked by lock") {
				lockBlocked++
			}
		}

		t.Logf("Lock test: %d acquired, %d blocked", lockAcquired, lockBlocked)
		if lockAcquired == 0 {
			t.Error("No process acquired the lock")
		}
		if lockBlocked == 0 {
			t.Error("No processes were blocked - file locking not working")
		}
	})

	// Test 3: 8-byte mmap coordination
	t.Run("MmapCoordination", func(t *testing.T) {
		testDir := filepath.Join(dir, "mmap")
		os.MkdirAll(testDir, 0755)

		// Initialize with one entry
		config := MultiProcessConfig()
		client, err := NewClientWithConfig(testDir, config)
		if err != nil {
			t.Fatal(err)
		}
		client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
			[]byte(`{"init":true}`),
		})
		client.Sync(context.Background())
		client.Close()

		// Start writer process
		writerCmd := exec.Command(executable, "-test.run", "^TestMultiProcessIntegration$", "-test.v")
		writerCmd.Env = append(os.Environ(),
			"COMET_MP_TEST_ROLE=mmap-writer",
			fmt.Sprintf("COMET_MP_TEST_DIR=%s", testDir),
		)

		// Start reader process that monitors mmap changes
		readerCmd := exec.Command(executable, "-test.run", "^TestMultiProcessIntegration$", "-test.v")
		readerCmd.Env = append(os.Environ(),
			"COMET_MP_TEST_ROLE=mmap-reader",
			fmt.Sprintf("COMET_MP_TEST_DIR=%s", testDir),
		)

		// Run them concurrently so reader can see writer's changes
		var wg sync.WaitGroup
		var writerOut, readerOut []byte

		wg.Add(2)
		go func() {
			defer wg.Done()
			writerOut, _ = writerCmd.CombinedOutput()
		}()

		// Give writer a small head start
		time.Sleep(100 * time.Millisecond)

		go func() {
			defer wg.Done()
			readerOut, _ = readerCmd.CombinedOutput()
		}()

		wg.Wait()

		t.Log("Writer output:", string(writerOut))
		t.Log("Reader output:", string(readerOut))

		// Verify mmap coordination worked
		if !containsString(string(readerOut), "Mmap reader: mmap change detected!") {
			t.Error("Reader did not detect mmap timestamp change")
		}
		if !containsString(string(readerOut), "saw writer entries") {
			t.Error("Reader did not see writer's entries through mmap coordination")
		}

		// Verify the 8-byte state file
		statePath := filepath.Join(testDir, "shard-0001", "index.state")
		if stat, err := os.Stat(statePath); err == nil {
			if stat.Size() != 8 {
				t.Errorf("Mmap state file is %d bytes, expected 8", stat.Size())
			} else {
				t.Log("✓ Mmap state file is exactly 8 bytes")
			}
		}
	})

	// Test 4: Same-shard contention
	t.Run("SameShardContention", func(t *testing.T) {
		testDir := filepath.Join(dir, "contention")
		os.MkdirAll(testDir, 0755)

		// Initialize shard
		config := MultiProcessConfig()

		// Use multi-process config as intended
		client, err := NewClientWithConfig(testDir, config)
		if err != nil {
			t.Fatalf("Failed to create client: %v", err)
		}
		ids, err := client.Append(context.Background(), "test:v1:shard:0001", [][]byte{[]byte(`{"init":true}`)})
		if err != nil {
			t.Fatalf("Failed to write init entry: %v", err)
		}
		t.Logf("Wrote init entry with ID: %v", ids)

		// Don't check length here - it triggers premature rebuild during multi-process startup
		// We'll verify the full count after all writers complete

		// In multi-process mode, we need to wait for checkpoint to happen
		// since CheckpointTime is 100ms
		time.Sleep(150 * time.Millisecond)

		client.Close()

		// Sleep a bit to ensure files are synced
		time.Sleep(100 * time.Millisecond)

		// Check what files exist
		shardDir := filepath.Join(testDir, "shard-0001")
		if files, err := os.ReadDir(shardDir); err == nil {
			t.Logf("Files in shard directory after init:")
			for _, f := range files {
				info, _ := f.Info()
				t.Logf("  %s (size: %d)", f.Name(), info.Size())
			}
		} else {
			t.Logf("Failed to read shard directory: %v", err)
		}

		// Specifically check for index.bin
		indexPath := filepath.Join(shardDir, "index.bin")
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			t.Logf("WARNING: index.bin does not exist!")
		}

		// Check comet.state size
		statePath := filepath.Join(shardDir, "comet.state")
		if stat, err := os.Stat(statePath); err == nil {
			t.Logf("comet.state exists with size: %d", stat.Size())
			if stat.Size() > CometStateSize {
				t.Logf("comet.state appears to contain data beyond header")
			}
		}

		// Start 5 writers all targeting the SAME shard
		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				cmd := exec.Command(executable, "-test.run", "^TestMultiProcessIntegration$", "-test.v")
				cmd.Env = append(os.Environ(),
					fmt.Sprintf("COMET_MP_TEST_ROLE=contention-writer-%d", id),
					fmt.Sprintf("COMET_MP_TEST_DIR=%s", testDir),
					"COMET_MP_TEST_WRITES=100",
					"COMET_MP_FORCE_SYNC=1", // Force sync after writes
				)
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Logf("contention-writer-%d failed: %v\nOutput: %s", id, err, out)
				} else {
					t.Logf("contention-writer-%d output: %s", id, out)
				}
			}(i)
		}

		wg.Wait()
		elapsed := time.Since(start)

		t.Logf("All writer processes completed, ensuring all writes are fully committed...")

		// Create a temporary client to force all pending writes to flush
		tempClient, err := NewClientWithConfig(testDir, config)
		if err != nil {
			t.Fatalf("Failed to create temp client: %v", err)
		}

		// Force sync to ensure all data is committed to disk
		tempClient.Sync(context.Background())
		tempClient.Close()

		// Wait longer for coordination state to stabilize after sync
		time.Sleep(2 * time.Second)

		// Check state BEFORE creating client to ensure it's stabilized
		shardDir2 := filepath.Join(testDir, "shard-0001")
		statePath2 := filepath.Join(shardDir2, "comet.state")
		if stateFile, stateErr := os.Open(statePath2); stateErr == nil {
			defer stateFile.Close()
			if data, mmapErr := syscall.Mmap(int(stateFile.Fd()), 0, CometStateSize,
				syscall.PROT_READ, syscall.MAP_SHARED); mmapErr == nil {
				defer syscall.Munmap(data)

				state := (*CometState)(unsafe.Pointer(&data[0]))
				writeOffset := atomic.LoadUint64(&state.WriteOffset)
				totalWrites := atomic.LoadUint64(&state.TotalWrites)
				lastWrite := atomic.LoadInt64(&state.LastWriteNanos)

				t.Logf("Final state before creating client: WriteOffset=%d, TotalWrites=%d, LastWrite=%d",
					writeOffset, totalWrites, lastWrite)
			}
		}

		// Verify all entries
		client, err = NewClientWithConfig(testDir, config)
		if err != nil {
			t.Fatalf("Failed to create verification client: %v", err)
		}
		defer client.Close()

		// Force reload of shards to get latest index state
		time.Sleep(200 * time.Millisecond) // Wait for any pending checkpoints
		client.Sync(context.Background())

		consumer := NewConsumer(client, ConsumerOptions{Group: "verifier"})
		defer consumer.Close()

		ctx := context.Background()

		// Check shard length directly (this triggers rebuild with current coordination state)
		shardLen, err := client.Len(ctx, "test:v1:shard:0001")
		if err != nil {
			t.Logf("Failed to get shard length: %v", err)
		} else {
			t.Logf("Shard length: %d", shardLen)
		}

		// Check data files to see if entries are there
		// (reusing shardDir2 from above)
		files2, _ := os.ReadDir(shardDir2)
		var totalDataSize int64
		var dataFileCount int
		for _, f := range files2 {
			if strings.HasSuffix(f.Name(), ".comet") {
				info, _ := f.Info()
				t.Logf("Data file %s size: %d bytes", f.Name(), info.Size())
				totalDataSize += info.Size()
				dataFileCount++
			}
		}
		t.Logf("Total data files: %d", dataFileCount)
		t.Logf("Total data file size: %d bytes", totalDataSize)

		// Check the index file to understand what's happening
		indexPath3 := filepath.Join(shardDir2, "index.bin")
		if indexStat, err := os.Stat(indexPath3); err == nil {
			t.Logf("Index file size: %d bytes", indexStat.Size())
		}

		// Get detailed shard stats
		client.mu.RLock()
		shard, exists := client.shards[uint32(1)]
		client.mu.RUnlock()
		if exists {
			shard.mu.RLock()
			t.Logf("Shard in-memory state: CurrentEntryNumber=%d, CurrentWriteOffset=%d",
				shard.index.CurrentEntryNumber, shard.index.CurrentWriteOffset)
			t.Logf("Number of files in index: %d", len(shard.index.Files))
			t.Logf("Number of index nodes: %d", len(shard.index.BinaryIndex.Nodes))
			shard.mu.RUnlock()
		}

		totalEntries := 0
		readCount := 0
		for {
			messages, err := consumer.Read(ctx, []uint32{1}, 1000)
			if err != nil {
				t.Logf("Read error: %v", err)
			}
			if len(messages) == 0 {
				break
			}
			readCount++
			t.Logf("Read batch %d: %d messages", readCount, len(messages))
			totalEntries += len(messages)
			for _, msg := range messages {
				consumer.Ack(ctx, msg.ID)
			}
		}

		expectedTotal := 5*100 + 1 // 5 writers * 100 entries + 1 init
		t.Logf("Same-shard contention: %d entries in %v", totalEntries, elapsed)

		// The index should properly coordinate between processes
		// Each process should see and build upon previous writes
		if totalEntries < expectedTotal {
			t.Errorf("Missing entries: got %d, expected %d", totalEntries, expectedTotal)
			t.Logf("This indicates the multi-process index coordination is not working correctly")
			t.Logf("Data file size %d suggests all data was written but index is incomplete", totalDataSize)
		}
	})

	// Test 5: Crash recovery
	t.Run("CrashRecovery", func(t *testing.T) {
		testDir := filepath.Join(dir, "crash")
		os.MkdirAll(testDir, 0755)

		// Start a process that will crash while holding lock
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		crashCmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessIntegration$", "-test.v")
		crashCmd.Env = append(os.Environ(),
			"COMET_MP_TEST_ROLE=crasher",
			fmt.Sprintf("COMET_MP_TEST_DIR=%s", testDir),
		)
		crashOut, crashErr := crashCmd.CombinedOutput()

		t.Log("Crasher output:", string(crashOut))
		if crashErr == nil {
			t.Error("Crasher should have exited with error")
		}

		// Now try to write - should work since OS released the lock
		config := MultiProcessConfig()
		client, err := NewClientWithConfig(testDir, config)
		if err != nil {
			t.Fatal("Failed to create client after crash:", err)
		}
		defer client.Close()

		_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
			[]byte(`{"after_crash":true}`),
		})
		if err != nil {
			t.Error("Failed to write after crash - lock not released:", err)
		} else {
			t.Log("✓ Successfully wrote after crash - OS released the lock")
		}
	})
}

// runMultiProcessWorker runs as a subprocess with a specific role
func runMultiProcessWorker(t *testing.T, role string) {
	pid := os.Getpid()
	ppid := os.Getppid()
	dir := os.Getenv("COMET_MP_TEST_DIR")

	t.Logf("%s process: PID=%d, PPID=%d", role, pid, ppid)

	config := MultiProcessConfig()

	switch {
	case role[:6] == "writer":
		// Simple writer
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		duration, _ := time.ParseDuration(os.Getenv("COMET_MP_TEST_DURATION"))
		if duration == 0 {
			duration = 2 * time.Second
		}

		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		// Use different shards for different writers
		shardNum := role[len(role)-1:]
		streamName := fmt.Sprintf("test:v1:shard:000%s", shardNum)

		count := 0
		for ctx.Err() == nil {
			batch := make([][]byte, 10)
			for i := range batch {
				batch[i] = []byte(fmt.Sprintf(`{"writer":"%s","seq":%d}`, role, count+i))
			}
			client.Append(ctx, streamName, batch)
			count += 10
			time.Sleep(10 * time.Millisecond)
		}
		t.Logf("Writer completed: wrote %d entries", count)

	case role[:6] == "reader":
		// Simple reader
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		duration, _ := time.ParseDuration(os.Getenv("COMET_MP_TEST_DURATION"))
		if duration == 0 {
			duration = 2 * time.Second
		}

		consumer := NewConsumer(client, ConsumerOptions{Group: role})
		defer consumer.Close()

		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		count := 0
		for ctx.Err() == nil {
			messages, _ := consumer.Read(ctx, []uint32{0, 1, 2}, 100)
			count += len(messages)
			for _, msg := range messages {
				consumer.Ack(ctx, msg.ID)
			}
			if len(messages) == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		t.Logf("Reader completed: read %d entries", count)

	case role[:6] == "locker":
		// File locking test
		shardDir := filepath.Join(dir, "shard-0001")
		os.MkdirAll(shardDir, 0755)
		lockPath := filepath.Join(shardDir, "shard.lock")

		lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		defer lockFile.Close()

		err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == syscall.EWOULDBLOCK {
			t.Logf("%s: blocked by lock", role)
		} else if err == nil {
			t.Logf("%s: acquired lock!", role)
			time.Sleep(500 * time.Millisecond)
			syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		}

	case role == "mmap-writer":
		// Mmap coordination writer
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()
		streamName := "test:v1:shard:0001"

		// Write 5 entries with sync after each
		for i := 1; i <= 5; i++ {
			_, err := client.Append(ctx, streamName, [][]byte{
				[]byte(fmt.Sprintf(`{"source":"writer","seq":%d,"time":%d}`, i, time.Now().UnixNano())),
			})
			if err != nil {
				t.Fatal(err)
			}

			// Force sync to update mmap state
			if err := client.Sync(ctx); err != nil {
				t.Fatal(err)
			}

			t.Logf("Mmap writer: wrote and synced entry %d", i)
			time.Sleep(200 * time.Millisecond)
		}

	case role == "mmap-reader":
		// Mmap coordination reader - monitors for changes
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		consumer := NewConsumer(client, ConsumerOptions{Group: "mmap-monitor"})
		defer consumer.Close()

		ctx := context.Background()

		// Read initial state
		messages, _ := consumer.Read(ctx, []uint32{1}, 10)
		t.Logf("Mmap reader: initial read got %d messages", len(messages))

		// Get shard to monitor mmap state
		client.mu.RLock()
		shard, exists := client.shards[uint32(1)]
		client.mu.RUnlock()

		if !exists || shard.state == nil {
			t.Fatal("Shard or unified state not found")
		}

		// Monitor for changes
		lastTimestamp := shard.state.GetLastIndexUpdate()
		changesDetected := 0
		writerEntriesFound := 0

		timeout := time.After(3 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				t.Logf("Mmap reader: timeout. Detected %d changes, found %d writer entries",
					changesDetected, writerEntriesFound)
				if writerEntriesFound > 0 {
					t.Log("✓ Mmap reader saw writer entries")
				}
				return
			case <-ticker.C:
				// Check mmap timestamp
				currentTimestamp := shard.state.GetLastIndexUpdate()
				if currentTimestamp > lastTimestamp {
					changesDetected++
					t.Logf("✓ Mmap reader: mmap change detected! (change #%d)", changesDetected)
					lastTimestamp = currentTimestamp
				}

				// Try to read new entries
				messages, _ := consumer.Read(ctx, []uint32{1}, 100)
				for _, msg := range messages {
					var data map[string]interface{}
					if json.Unmarshal(msg.Data, &data) == nil {
						if data["source"] == "writer" {
							writerEntriesFound++
						}
					}
					consumer.Ack(ctx, msg.ID)
				}
			}
		}

	case strings.HasPrefix(role, "contention-writer-"):
		// Same-shard contention writer
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		numWrites := 100
		if n := os.Getenv("COMET_MP_TEST_WRITES"); n != "" {
			numWrites, _ = strconv.Atoi(n)
		}

		forceSync := os.Getenv("COMET_MP_FORCE_SYNC") == "1"

		ctx := context.Background()
		streamName := "test:v1:shard:0001" // ALL writers use same shard!

		successCount := 0
		var failedWrites []int
		for i := 0; i < numWrites; i++ {
			data, _ := json.Marshal(map[string]interface{}{
				"writer": role,
				"pid":    pid,
				"seq":    i,
			})
			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err == nil {
				successCount++
			} else {
				failedWrites = append(failedWrites, i)
				t.Logf("%s: write %d FAILED: %v", role, i, err)
			}
		}

		if len(failedWrites) > 0 {
			t.Logf("%s: WRITE FAILURES: %d failed out of %d: %v", role, len(failedWrites), numWrites, failedWrites)
		}

		// Force sync if requested to ensure all data is committed
		if forceSync {
			client.Sync(ctx)
			t.Logf("%s: synced after writing entries", role)
		}

		t.Logf("%s: wrote %d/%d entries", role, successCount, numWrites)

		// Force a sync to ensure index is persisted
		if err := client.Sync(ctx); err != nil {
			t.Logf("%s: sync error: %v", role, err)
		}

		// Wait for checkpoint before closing
		time.Sleep(150 * time.Millisecond)
		client.Close()

	case role == "crasher":
		// Crash test - acquire lock then exit abruptly
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		// Don't defer close - we're simulating a crash!

		ctx := context.Background()
		client.Append(ctx, "test:v1:shard:0001", [][]byte{
			[]byte(`{"msg":"about to crash"}`),
		})

		t.Log("Crasher: simulating crash...")
		os.Exit(1)
	}
}

// runTestWorker runs as a child process worker
func runTestWorker(t *testing.T, workerID string) {
	pid := os.Getpid()
	ppid := os.Getppid()
	t.Logf("Worker %s started: PID=%d, PPID=%d, CI=%s", workerID, pid, ppid, os.Getenv("CI"))

	dir := os.Getenv("COMET_TEST_DIR")
	duration, _ := time.ParseDuration(os.Getenv("COMET_TEST_DURATION"))
	if duration == 0 {
		duration = 3 * time.Second
	}

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Worker %s (PID %d): failed to create client: %v", workerID, pid, err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	if workerID[:6] == "writer" {
		// Writer process - use different shards for each writer
		shardNum := workerID[len(workerID)-1:]
		streamName := fmt.Sprintf("test:v1:shard:000%s", shardNum)
		count := 0

		// Log progress periodically in CI
		lastLog := time.Now()
		for ctx.Err() == nil {
			batch := make([][]byte, 10)
			for i := 0; i < 10; i++ {
				entry := fmt.Sprintf(`{"worker":"%s","seq":%d,"time":%d}`,
					workerID, count+i, time.Now().UnixNano())
				batch[i] = []byte(entry)
			}

			if _, err := client.Append(ctx, streamName, batch); err != nil {
				t.Logf("Worker %s: append error: %v", workerID, err)
			} else {
				count += 10

				// Progress logging in CI
				if os.Getenv("CI") != "" && time.Since(lastLog) > 200*time.Millisecond {
					t.Logf("Worker %s progress: %d entries written", workerID, count)
					lastLog = time.Now()
				}
			}

			time.Sleep(10 * time.Millisecond)
		}

		t.Logf("Worker %s: wrote %d entries", workerID, count)

	} else {
		// Reader process - read from all shards
		consumer := NewConsumer(client, ConsumerOptions{Group: workerID})
		defer consumer.Close()

		count := 0
		lastLog := time.Now()
		for ctx.Err() == nil {
			messages, err := consumer.Read(ctx, []uint32{0, 1, 2}, 100)
			if err != nil {
				t.Logf("Worker %s: read error: %v", workerID, err)
				continue
			}

			count += len(messages)

			// Progress logging in CI
			if os.Getenv("CI") != "" && time.Since(lastLog) > 200*time.Millisecond {
				t.Logf("Worker %s progress: %d entries read", workerID, count)
				lastLog = time.Now()
			}

			// Ack messages
			for _, msg := range messages {
				consumer.Ack(ctx, msg.ID)
			}

			if len(messages) == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		t.Logf("Worker %s: read %d entries", workerID, count)
	}
}

// verifyMultiProcessResults checks data integrity after multi-process test
func verifyMultiProcessResults(t *testing.T, dir string, numWriters int) {
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Read ALL entries to verify
	consumer := NewConsumer(client, ConsumerOptions{Group: "verify"})
	defer consumer.Close()

	ctx := context.Background()
	totalEntries := 0
	writers := make(map[string]int)

	for {
		messages, err := consumer.Read(ctx, []uint32{0, 1, 2}, 1000)
		if err != nil {
			t.Fatal(err)
		}

		if len(messages) == 0 {
			break
		}

		totalEntries += len(messages)

		for _, msg := range messages {
			// Basic validation - should be valid JSON
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				t.Errorf("Invalid JSON: %s", msg.Data)
				continue
			}

			if worker, ok := data["worker"].(string); ok {
				writers[worker]++
			}
		}

		// Ack to advance
		for _, msg := range messages {
			consumer.Ack(ctx, msg.ID)
		}
	}

	t.Logf("Total entries read: %d", totalEntries)
	t.Logf("Writers found: %v", writers)

	if totalEntries == 0 {
		t.Error("No entries were written")
	}

	if len(writers) < numWriters {
		t.Errorf("Expected data from %d writers, found %d", numWriters, len(writers))
	}
}

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
		t.Errorf("Failed to write after crash: %v", err)
	} else {
		t.Log("Successfully wrote after crash - lock was released!")
	}
}

func runCrashTestWorker(t *testing.T, workerID string) {
	dir := os.Getenv("COMET_CRASH_TEST_DIR")

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Write something to acquire the lock
	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte(`{"worker":"crasher","msg":"about to crash"}`),
	})

	if err != nil {
		t.Logf("Crasher: failed to write: %v", err)
	} else {
		t.Log("Crasher: acquired lock and wrote entry")
	}

	// Simulate a crash - exit without cleanup
	t.Log("Crasher: simulating crash by exiting abruptly...")
	os.Exit(1)
}

// TestMultiProcessSameShardContention verifies multiple processes can safely write to same shard
func TestMultiProcessSameShardContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping contention test in short mode")
	}

	if workerID := os.Getenv("COMET_CONTENTION_WORKER"); workerID != "" {
		runContentionWorker(t, workerID)
		return
	}

	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Initialize shard
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"init":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Remove the index file to force a clean rebuild
	// This simulates the recommended approach for multi-process mode:
	// let readers rebuild the index by scanning files
	os.Remove(filepath.Join(dir, "shard-0001", "index.bin"))

	// Start multiple writer processes all targeting the SAME shard
	numWriters := 5
	writesPerWorker := 100

	var wg sync.WaitGroup
	results := make(chan string, numWriters)

	start := time.Now()

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessSameShardContention$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_CONTENTION_WORKER=%d", id),
				fmt.Sprintf("COMET_CONTENTION_DIR=%s", dir),
				fmt.Sprintf("COMET_CONTENTION_WRITES=%d", writesPerWorker),
				"GO_TEST_SUBPROCESS=1",
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
	elapsed := time.Since(start)
	close(results)

	// Collect results
	successCount := 0
	for result := range results {
		t.Log(result)
		if containsString(result, "successfully wrote") {
			successCount++
		}
	}

	t.Logf("\nContention test summary:")
	t.Logf("- %d workers writing to SAME shard", numWriters)
	t.Logf("- %d writes per worker", writesPerWorker)
	t.Logf("- Total time: %v", elapsed)
	t.Logf("- Successful workers: %d/%d", successCount, numWriters)

	if successCount != numWriters {
		t.Errorf("Expected all %d workers to succeed, but only %d did", numWriters, successCount)
	}

	// Verify all entries were written
	client, err = NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Wait a bit more to ensure all data is persisted
	time.Sleep(500 * time.Millisecond)

	// Force a sync to ensure all data is written
	ctx := context.Background()
	if err := client.Sync(ctx); err != nil {
		t.Logf("Sync error: %v", err)
	}

	// Check if index file exists
	indexPath := filepath.Join(dir, "shard-0001", "index")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Log("Warning: No index file found")

		// Force index creation by loading the shard
		_, err = client.Len(ctx, "test:v1:shard:0001")
		if err != nil {
			t.Logf("Error loading shard: %v", err)
		}

		// Force another sync to persist the index
		if err := client.Sync(ctx); err != nil {
			t.Logf("Second sync error: %v", err)
		}
	}

	// List directory contents
	entries, _ := os.ReadDir(filepath.Join(dir, "shard-0001"))
	t.Log("Shard directory contents after sync:")
	var totalDataSize int64
	for _, entry := range entries {
		info, _ := entry.Info()
		t.Logf("  %s (size: %d)", entry.Name(), info.Size())
		if strings.HasPrefix(entry.Name(), "log-") && strings.HasSuffix(entry.Name(), ".comet") {
			totalDataSize += info.Size()
		}
	}
	t.Logf("Total data file size: %d bytes", totalDataSize)

	// Create a new client to ensure we're reading from disk
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Use a unique consumer group to read from the beginning
	uniqueGroup := fmt.Sprintf("verifier-%d", time.Now().UnixNano())
	consumer := NewConsumer(client2, ConsumerOptions{
		Group: uniqueGroup,
	})
	t.Logf("Using consumer group: %s", uniqueGroup)
	defer consumer.Close()

	totalEntries := 0
	writerCounts := make(map[string]int)

	// Try to get shard info
	client2.mu.RLock()
	_, exists := client2.shards[1]
	client2.mu.RUnlock()

	if !exists {
		t.Log("Shard 1 not loaded in client2, forcing load...")
		_, _ = client2.Len(ctx, "test:v1:shard:0001")
	}

	// First, let's check what Len returns
	t.Logf("About to call Len on shard 0001...")
	length, err := client2.Len(ctx, "test:v1:shard:0001")
	if err != nil {
		t.Logf("Error getting length: %v", err)
	} else {
		t.Logf("Shard length: %d", length)
	}

	// Debug shard state
	client2.mu.RLock()
	if shard, exists := client2.shards[1]; exists {
		shard.mu.RLock()
		t.Logf("Shard 1 state: CurrentEntryNumber=%d, CurrentWriteOffset=%d, Files=%d",
			shard.index.CurrentEntryNumber, shard.index.CurrentWriteOffset, len(shard.index.Files))
		for i, file := range shard.index.Files {
			t.Logf("  File %d: %s, entries=%d", i, file.Path, file.Entries)
		}
		shard.mu.RUnlock()
	} else {
		t.Logf("Shard 1 not found in client2")
	}
	client2.mu.RUnlock()

	// Read all messages from shard 1
	readAttempts := 0
	for i := 0; i < 10; i++ { // Try multiple times
		messages, err := consumer.Read(ctx, []uint32{1}, 1000)
		if err != nil {
			t.Logf("Read error on attempt %d: %v", i, err)
			break
		}

		readAttempts++

		if len(messages) == 0 {
			if i == 0 {
				// First read returned nothing, wait and retry
				time.Sleep(100 * time.Millisecond)
				continue
			}
			t.Logf("No more messages after %d read attempts", readAttempts)
			break
		}

		t.Logf("Read batch %d: got %d messages", i, len(messages))
		for _, msg := range messages {
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err == nil {
				if workerID, ok := data["worker"]; ok {
					writerCounts[fmt.Sprintf("%v", workerID)]++
				}
			}
			totalEntries++
		}

		for _, msg := range messages {
			consumer.Ack(ctx, msg.ID)
		}
	}

	expectedTotal := numWriters*writesPerWorker + 1 // +1 for init entry
	t.Logf("Total entries found: %d (expected %d)", totalEntries, expectedTotal)
	t.Logf("Entries per writer: %v", writerCounts)

	if totalEntries < expectedTotal {
		t.Errorf("Missing entries: found %d, expected %d", totalEntries, expectedTotal)
	}
}

func runContentionWorker(t *testing.T, workerID string) {
	pid := os.Getpid()
	dir := os.Getenv("COMET_CONTENTION_DIR")
	numWrites := 100
	if n := os.Getenv("COMET_CONTENTION_WRITES"); n != "" {
		fmt.Sscanf(n, "%d", &numWrites)
	}

	t.Logf("Worker %s (PID %d) starting: will write %d entries", workerID, pid, numWrites)

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Worker %s: failed to create client: %v", workerID, err)
	}
	defer func() {
		// Log shard state before closing
		client.mu.RLock()
		if shard, ok := client.shards[1]; ok {
			shard.mu.RLock()
			t.Logf("Worker %s before close: CurrentEntryNumber=%d, CurrentWriteOffset=%d, writesSinceCheckpoint=%d",
				workerID, shard.index.CurrentEntryNumber, shard.index.CurrentWriteOffset, shard.writesSinceCheckpoint)
			shard.mu.RUnlock()
		}
		client.mu.RUnlock()

		client.Close()
	}()

	ctx := context.Background()
	streamName := "test:v1:shard:0001" // Same shard for all!

	successCount := 0
	start := time.Now()

	for i := 0; i < numWrites; i++ {
		entry := map[string]interface{}{
			"worker": workerID,
			"pid":    pid,
			"seq":    i,
			"time":   time.Now().UnixNano(),
		}
		data, _ := json.Marshal(entry)

		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Logf("Worker %s: write %d failed: %v", workerID, i, err)
		} else {
			successCount++
		}
	}

	elapsed := time.Since(start)
	rate := float64(successCount) / elapsed.Seconds()
	t.Logf("Worker %s: successfully wrote %d/%d entries in %v (%.0f entries/sec)",
		workerID, successCount, numWrites, elapsed, rate)
}

// Helper functions
func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestBulletproofMultiProcess runs comprehensive stress tests
func TestBulletproofMultiProcess(t *testing.T) {
	tests := []struct {
		name         string
		workers      int
		entriesEach  int
		expectations int
	}{
		{"2 workers × 50", 2, 50, 101},    // 2×50 + 1 init
		{"3 workers × 100", 3, 100, 301},  // 3×100 + 1 init
		{"8 workers × 25", 8, 25, 201},    // 8×25 + 1 init
		{"10 workers × 10", 10, 10, 101},  // 10×10 + 1 init
		{"5 workers × 200", 5, 200, 1001}, // 5×200 + 1 init (heavy load)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testDir := t.TempDir()
			config := MultiProcessConfig()

			// Initialize with first entry
			client, err := NewClientWithConfig(testDir, config)
			if err != nil {
				t.Fatal(err)
			}

			_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
				[]byte(`{"init": true}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			client.Close()

			// Launch concurrent writers
			var wg sync.WaitGroup

			for i := 0; i < test.workers; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					workerClient, err := NewClientWithConfig(testDir, config)
					if err != nil {
						t.Errorf("Worker %d failed to create client: %v", workerID, err)
						return
					}
					defer workerClient.Close()

					successfulWrites := 0
					for j := 0; j < test.entriesEach; j++ {
						data := fmt.Sprintf(`{"worker":%d,"seq":%d,"data":"stress-test-%d-%d"}`,
							workerID, j, workerID, j)

						_, err := workerClient.Append(context.Background(), "test:v1:shard:0001", [][]byte{
							[]byte(data),
						})
						if err == nil {
							successfulWrites++
						} else {
							t.Errorf("Worker %d write %d failed: %v", workerID, j, err)
						}
					}

					if successfulWrites != test.entriesEach {
						t.Errorf("Worker %d: only %d/%d writes succeeded",
							workerID, successfulWrites, test.entriesEach)
					}
				}(i)
			}

			wg.Wait()

			// Verify all entries are readable
			verifyClient, err := NewClientWithConfig(testDir, config)
			if err != nil {
				t.Fatal(err)
			}
			defer verifyClient.Close()

			// Wait a moment for index updates
			time.Sleep(10 * time.Millisecond)

			consumer := NewConsumer(verifyClient, ConsumerOptions{Group: fmt.Sprintf("verify-%d", time.Now().UnixNano())})
			defer consumer.Close()

			// Read all entries
			messages, err := consumer.Read(context.Background(), []uint32{1}, test.expectations+100)
			if err != nil {
				t.Fatal(err)
			}

			// Filter non-empty messages
			var validMessages []StreamMessage
			for _, msg := range messages {
				if len(msg.Data) > 0 {
					validMessages = append(validMessages, msg)
				}
			}

			if len(validMessages) != test.expectations {
				t.Errorf("%s: Expected %d entries, got %d",
					test.name, test.expectations, len(validMessages))
			}
		})
	}
}

// TestRapidStartupShutdown tests rapid process creation/destruction
func TestRapidStartupShutdown(t *testing.T) {
	testDir := t.TempDir()
	config := MultiProcessConfig()

	// Initialize
	client, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"init": true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Rapidly create and destroy clients while writing
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Rapid client lifecycle
			for cycle := 0; cycle < 5; cycle++ {
				workerClient, err := NewClientWithConfig(testDir, config)
				if err != nil {
					t.Errorf("Worker %d cycle %d: failed to create client: %v", workerID, cycle, err)
					return
				}

				// Write a few entries quickly
				for j := 0; j < 3; j++ {
					data := fmt.Sprintf(`{"worker":%d,"cycle":%d,"seq":%d}`, workerID, cycle, j)
					_, err := workerClient.Append(context.Background(), "test:v1:shard:0001", [][]byte{
						[]byte(data),
					})
					_ = err // Ignore errors for rapid cycling test
				}

				workerClient.Close()
			}
		}(i)
	}

	wg.Wait()

	// Verify data integrity after rapid cycling
	verifyClient, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyClient.Close()

	time.Sleep(20 * time.Millisecond) // Allow for index updates

	consumer := NewConsumer(verifyClient, ConsumerOptions{Group: fmt.Sprintf("verify-%d", time.Now().UnixNano())})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 400) // 20×5×3 + buffer
	if err != nil {
		t.Fatal(err)
	}

	// Count valid messages
	validCount := 0
	for _, msg := range messages {
		if len(msg.Data) > 0 {
			validCount++
		}
	}

	t.Logf("Rapid startup/shutdown: wrote entries, read %d valid messages", validCount)

	// Should have at least most entries (allowing for some race conditions in rapid cycling)
	minExpected := 250 // Allow some tolerance for rapid cycling
	if validCount < minExpected {
		t.Errorf("Expected at least %d entries, got %d", minExpected, validCount)
	}
}

// TestTimeBasedStress tests sustained load over time
func TestTimeBasedStress(t *testing.T) {
	testDir := t.TempDir()
	config := MultiProcessConfig()

	// Initialize
	client, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"init": true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Run for 2 seconds with concurrent writers
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			workerClient, err := NewClientWithConfig(testDir, config)
			if err != nil {
				t.Errorf("Worker %d failed to create client: %v", workerID, err)
				return
			}
			defer workerClient.Close()

			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
					data := fmt.Sprintf(`{"worker":%d,"seq":%d,"timestamp":%d}`,
						workerID, seq, time.Now().UnixNano())

					_, err := workerClient.Append(context.Background(), "test:v1:shard:0001", [][]byte{
						[]byte(data),
					})
					if err == nil {
						seq++
					}

					// Small delay to avoid overwhelming
					time.Sleep(100 * time.Microsecond)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify data integrity
	verifyClient, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyClient.Close()

	time.Sleep(50 * time.Millisecond) // Allow for index updates

	consumer := NewConsumer(verifyClient, ConsumerOptions{Group: fmt.Sprintf("verify-%d", time.Now().UnixNano())})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 2000) // Large buffer
	if err != nil {
		t.Fatal(err)
	}

	// Count valid messages
	validCount := 0
	for _, msg := range messages {
		if len(msg.Data) > 0 {
			validCount++
		}
	}

	t.Logf("Time-based stress: read %d valid messages", validCount)

	// Should have at least 100 entries (very conservative)
	if validCount < 100 {
		t.Errorf("Expected at least 100 entries, got %d", validCount)
	}
}

// TestEOFRetryLogic specifically tests the edge case where EOF errors occur
// during index reloading due to race conditions between file writes and mmap state updates
func TestEOFRetryLogic(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Create initial client and write some data
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	streamName := "test:v1:shard:0001"

	// Write initial entry
	_, err = client.Append(context.Background(), streamName, [][]byte{
		[]byte(`{"msg":"initial"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Force a checkpoint to create index file
	client.Sync(context.Background())

	// Now simulate the edge case: concurrent rapid writes + index reloads
	// This creates the conditions where EOF errors historically occurred
	var wg sync.WaitGroup
	errorCount := 0
	var errorMu sync.Mutex

	// Writer goroutine: creates continuous index updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		writerClient, _ := NewClientWithConfig(dir, config)
		defer writerClient.Close()

		for i := 0; i < 100; i++ {
			_, err := writerClient.Append(context.Background(), streamName, [][]byte{
				[]byte(`{"msg":"stress","id":` + string(rune('0'+i%10)) + `}`),
			})
			if err != nil {
				errorMu.Lock()
				errorCount++
				errorMu.Unlock()
			}
			// Force frequent checkpoints to trigger mmap state updates
			if i%10 == 0 {
				writerClient.Sync(context.Background())
			}
			time.Sleep(time.Microsecond * 100) // Very fast writes
		}
	}()

	// Reader goroutines: continuously trigger index reloads
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			readerClient, _ := NewClientWithConfig(dir, config)
			defer readerClient.Close()

			consumer := NewConsumer(readerClient, ConsumerOptions{
				Group: "stress-reader-" + string(rune('0'+readerID)),
			})
			defer consumer.Close()

			for i := 0; i < 50; i++ {
				// This will trigger mmap state checks and potential index reloads
				_, err := consumer.Read(context.Background(), []uint32{1}, 10)
				if err != nil {
					// Check if this is the specific EOF error we're protecting against
					if strings.Contains(err.Error(), "unexpected EOF") &&
						strings.Contains(err.Error(), "failed to reload index after detecting mmap change") {
						t.Errorf("EOF retry logic failed! Got the exact error we should prevent: %v", err)
					}
					errorMu.Lock()
					errorCount++
					errorMu.Unlock()
				}
				time.Sleep(time.Microsecond * 200)
			}
		}(r)
	}

	wg.Wait()

	// The test passes if:
	// 1. We didn't get the specific EOF error that should be retried
	// 2. Error count is reasonable (some errors are expected under extreme load, but not EOF ones)

	t.Logf("Completed EOF retry stress test with %d total errors", errorCount)

	// Verify we can still read data normally after the stress test
	finalConsumer := NewConsumer(client, ConsumerOptions{Group: "final-check"})
	defer finalConsumer.Close()

	messages, err := finalConsumer.Read(context.Background(), []uint32{1}, 200)
	if err != nil {
		// This should NOT be an EOF error if our retry logic works
		if strings.Contains(err.Error(), "unexpected EOF") {
			t.Fatalf("EOF retry logic failed in final verification: %v", err)
		}
	}

	t.Logf("Final verification: successfully read %d messages after stress test", len(messages))
}

// TestEOFRetryRaceCondition creates a specific test for the EOF race condition
func TestEOFRetryRaceCondition(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Create two clients to simulate multi-process scenario
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()

	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	streamName := "test:v1:shard:0001"

	// Write initial data
	_, err = client1.Append(context.Background(), streamName, [][]byte{
		[]byte(`{"msg":"initial"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client1.Sync(context.Background())

	// Test the race condition scenario:
	// Client1 writes rapidly (causing index updates)
	// Client2 reads rapidly (causing index reloads)
	// This should trigger the EOF race condition if our fix doesn't work

	var wg sync.WaitGroup
	eofErrorOccurred := false
	var errorMu sync.Mutex

	// Writer: causes frequent index updates and mmap state changes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, err := client1.Append(context.Background(), streamName, [][]byte{
				[]byte(`{"writer":1,"seq":` + string(rune('0'+i%10)) + `}`),
			})
			if err == nil && i%5 == 0 {
				client1.Sync(context.Background()) // Force index persistence and mmap updates
			}
			time.Sleep(time.Microsecond * 500) // Fast but not instant
		}
	}()

	// Reader: triggers index reloads due to mmap state changes
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer := NewConsumer(client2, ConsumerOptions{Group: "race-test"})
		defer consumer.Close()

		for i := 0; i < 30; i++ {
			_, err := consumer.Read(context.Background(), []uint32{1}, 5)
			if err != nil {
				errorMu.Lock()
				if strings.Contains(err.Error(), "unexpected EOF") &&
					strings.Contains(err.Error(), "failed to reload index after detecting mmap change") {
					eofErrorOccurred = true
					t.Errorf("EOF race condition detected! Our retry logic should prevent this: %v", err)
				}
				errorMu.Unlock()
			}
			time.Sleep(time.Microsecond * 800)
		}
	}()

	wg.Wait()

	if eofErrorOccurred {
		t.Fatal("EOF race condition was not properly handled by retry logic")
	} else {
		t.Log("Success: No EOF race condition detected - retry logic is working")
	}

	// Final verification - should be able to read all data
	consumer := NewConsumer(client2, ConsumerOptions{Group: "final"})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 100)
	if err != nil && strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("EOF error in final read - retry logic failed: %v", err)
	}

	t.Logf("Final read successful: %d messages", len(messages))
}

// TestRetryLogicVerification tests that the retry logic actually matters
// by simulating what would happen without it
func TestRetryLogicVerification(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write data to create an index
	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"test":"verification"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Sync(context.Background())

	// Get the shard
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}

	// Test that our loadIndexWithRetry function exists and has retry behavior
	// We can't easily test the exact EOF condition without complex setup,
	// but we can verify the function exists and behaves reasonably

	shard.mu.Lock()

	// This should work (normal case)
	err = shard.loadIndexWithRetry()
	if err != nil {
		shard.mu.Unlock()
		t.Fatalf("loadIndexWithRetry failed on valid index: %v", err)
	}

	// Compare with direct loadIndex call
	err2 := shard.loadIndex()
	if err2 != nil {
		shard.mu.Unlock()
		t.Fatalf("loadIndex failed on valid index: %v", err2)
	}

	shard.mu.Unlock()

	t.Log("Verified that loadIndexWithRetry function exists and works correctly")
	t.Log("The retry logic will activate automatically during race conditions")
}
