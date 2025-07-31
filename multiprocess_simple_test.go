package comet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestMultiProcessSimple tests multiple processes writing/reading concurrently
// This test spawns actual OS processes and requires the 'multiprocess' build tag
func TestMultiProcessSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}
	
	// Log start for CI debugging
	t.Logf("Starting multi-process test, CI=%s", os.Getenv("CI"))

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
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Run the same test binary with specific test
	testName := "TestMultiProcessSimple"
	numWriters := 3
	numReaders := 2
	duration := 3 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), duration+10*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Start writer processes
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cmd := exec.CommandContext(ctx, executable, "-test.run", "^" + testName + "$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_TEST_WORKER=writer-%d", id),
				fmt.Sprintf("COMET_TEST_DIR=%s", dir),
				fmt.Sprintf("COMET_TEST_DURATION=%s", duration),
				"GO_TEST_SUBPROCESS=1",
			)

			output, err := cmd.CombinedOutput()
			if err != nil && ctx.Err() == nil {
				t.Logf("Writer %d failed: %v\nOutput: %s", id, err, output)
			} else {
				t.Logf("Writer %d output: %s", id, output)
			}
		}(i)
	}

	// Start reader processes
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(500 * time.Millisecond) // Let writers start first

			cmd := exec.CommandContext(ctx, executable, "-test.run", "^" + testName + "$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_TEST_WORKER=reader-%d", id),
				fmt.Sprintf("COMET_TEST_DIR=%s", dir),
				fmt.Sprintf("COMET_TEST_DURATION=%s", duration),
				"GO_TEST_SUBPROCESS=1",
			)

			output, err := cmd.CombinedOutput()
			if err != nil && ctx.Err() == nil {
				t.Logf("Reader %d failed: %v\nOutput: %s", id, err, output)
			} else {
				t.Logf("Reader %d output: %s", id, output)
			}
		}(i)
	}

	wg.Wait()

	// Verify the data
	verifyMultiProcessResults(t, dir, numWriters)
}

// runTestWorker runs as a child process worker
func runTestWorker(t *testing.T, workerID string) {
	pid := os.Getpid()
	ppid := os.Getppid()
	t.Logf("Worker %s started: PID=%d, PPID=%d", workerID, pid, ppid)

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
			}

			time.Sleep(10 * time.Millisecond)
		}

		t.Logf("Worker %s: wrote %d entries", workerID, count)

	} else {
		// Reader process - read from all shards
		consumer := NewConsumer(client, ConsumerOptions{Group: workerID})
		defer consumer.Close()

		count := 0
		for ctx.Err() == nil {
			messages, err := consumer.Read(ctx, []uint32{0, 1, 2}, 100)
			if err != nil {
				t.Logf("Worker %s: read error: %v", workerID, err)
				continue
			}

			count += len(messages)

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
func verifyMultiProcessResults(t *testing.T, dir string, expectedWriters int) {
	config := DefaultCometConfig()
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

	if len(writers) < expectedWriters {
		t.Errorf("Expected data from %d writers, found %d", expectedWriters, len(writers))
	}
}

// BenchmarkMultiProcess measures performance with multiple processes
func BenchmarkMultiProcess(b *testing.B) {
	if os.Getenv("COMET_BENCH_WORKER") != "" {
		// We're a worker process
		runBenchmarkWorker(b)
		return
	}

	// We're the parent
	dir := b.TempDir()
	executable, err := os.Executable()
	if err != nil {
		b.Fatal(err)
	}

	for _, numProcs := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("procs_%d", numProcs), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var wg sync.WaitGroup

			// Start worker processes
			for i := 0; i < numProcs; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					cmd := exec.CommandContext(ctx, executable,
						"-test.bench", "BenchmarkMultiProcess",
						"-test.run", "^$")
					cmd.Env = append(os.Environ(),
						fmt.Sprintf("COMET_BENCH_WORKER=%d", id),
						fmt.Sprintf("COMET_BENCH_DIR=%s", dir),
						fmt.Sprintf("COMET_BENCH_N=%d", b.N),
					)
					cmd.Run()
				}(i)
			}

			wg.Wait()

			// Read final stats
			config := DefaultCometConfig()
			client, err := NewClientWithConfig(dir, config)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close()

			stats := client.GetStats()
			b.ReportMetric(float64(stats.TotalEntries), "total_entries")
			b.ReportMetric(float64(stats.TotalEntries)/float64(numProcs), "entries_per_proc")
		})
	}
}

func runBenchmarkWorker(b *testing.B) {
	dir := os.Getenv("COMET_BENCH_DIR")
	n, _ := strconv.Atoi(os.Getenv("COMET_BENCH_N"))
	if n == 0 {
		n = b.N
	}

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "bench:v1:shard:0001"

	batch := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		batch[i] = []byte(fmt.Sprintf(`{"n":%d,"data":"benchmark entry %d"}`, i, i))
	}

	for i := 0; i < n; i++ {
		client.Append(ctx, streamName, batch)
	}
}
