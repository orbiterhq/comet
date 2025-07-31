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
	"sync"
	"testing"
	"time"
)

// TestMultiProcessIntegration tests true multi-process coordination
func TestMultiProcessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dir := t.TempDir()

	// Build a test binary that we can spawn multiple times
	testBinary := filepath.Join(dir, "comet_worker")
	cmd := exec.Command("go", "build", "-o", testBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Skipf("failed to build test worker: %v", err)
	}

	// Test scenarios
	scenarios := []struct {
		name        string
		numWriters  int
		numReaders  int
		duration    time.Duration
		expectError bool
	}{
		{"single_writer_single_reader", 1, 1, 2 * time.Second, false},
		{"multi_writer_single_reader", 3, 1, 2 * time.Second, false},
		{"single_writer_multi_reader", 1, 3, 2 * time.Second, false},
		{"multi_writer_multi_reader", 3, 3, 2 * time.Second, false},
		{"stress_test", 10, 5, 5 * time.Second, false},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			testDir := filepath.Join(dir, sc.name)
			if err := os.MkdirAll(testDir, 0755); err != nil {
				t.Fatalf("failed to create test dir: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), sc.duration+10*time.Second)
			defer cancel()

			var wg sync.WaitGroup
			errors := make(chan error, sc.numWriters+sc.numReaders)

			// Start writers
			for i := 0; i < sc.numWriters; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					cmd := exec.CommandContext(ctx, testBinary,
						"--mode", "writer",
						"--dir", testDir,
						"--id", fmt.Sprintf("writer-%d", id),
						"--duration", sc.duration.String(),
					)
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr
					if err := cmd.Run(); err != nil && ctx.Err() == nil {
						errors <- fmt.Errorf("writer %d failed: %w", id, err)
					}
				}(i)
			}

			// Start readers
			for i := 0; i < sc.numReaders; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					// Give writers a head start
					time.Sleep(100 * time.Millisecond)

					cmd := exec.CommandContext(ctx, testBinary,
						"--mode", "reader",
						"--dir", testDir,
						"--id", fmt.Sprintf("reader-%d", id),
						"--duration", sc.duration.String(),
					)
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr
					if err := cmd.Run(); err != nil && ctx.Err() == nil {
						errors <- fmt.Errorf("reader %d failed: %w", id, err)
					}
				}(i)
			}

			// Wait for completion
			wg.Wait()
			close(errors)

			// Check for errors
			var errs []error
			for err := range errors {
				errs = append(errs, err)
			}

			if len(errs) > 0 && !sc.expectError {
				t.Errorf("unexpected errors: %v", errs)
			}

			// Verify data integrity
			verifyMultiProcessData(t, testDir, sc.numWriters)
		})
	}
}

// verifyMultiProcessData checks that all written data can be read correctly
func verifyMultiProcessData(t *testing.T, dir string, numWriters int) {
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create verification client: %v", err)
	}
	defer client.Close()

	consumer := NewConsumer(client, ConsumerOptions{
		Group: "verification",
	})
	defer consumer.Close()

	ctx := context.Background()

	// Count entries from each writer
	writerCounts := make(map[string]int)
	totalEntries := 0

	// Read all entries
	for {
		messages, err := consumer.Read(ctx, []uint32{1}, 1000)
		if err != nil {
			t.Fatalf("failed to read for verification: %v", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			totalEntries++
			// Parse writer ID from message
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err == nil {
				if writerID, ok := data["writer_id"].(string); ok {
					writerCounts[writerID]++
				}
			}
		}

		// Ack messages
		for _, msg := range messages {
			consumer.Ack(ctx, msg.ID)
		}
	}

	t.Logf("Total entries read: %d", totalEntries)
	t.Logf("Writer counts: %+v", writerCounts)

	// Verify we got data from all writers
	if len(writerCounts) != numWriters {
		t.Errorf("expected data from %d writers, got %d", numWriters, len(writerCounts))
	}

	// Verify reasonable distribution
	for writerID, count := range writerCounts {
		if count < 10 {
			t.Errorf("writer %s only wrote %d entries, seems too low", writerID, count)
		}
	}
}

// TestMultiProcessBenchmark runs benchmarks with real processes
func TestMultiProcessBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}

	dir := t.TempDir()

	// Build test binary
	testBinary := filepath.Join(dir, "comet_bench")
	cmd := exec.Command("go", "build", "-o", testBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Skipf("failed to build test worker: %v", err)
	}

	// Run benchmark with multiple processes
	numProcesses := []int{1, 2, 4, 8}
	duration := 10 * time.Second

	for _, n := range numProcesses {
		t.Run(fmt.Sprintf("processes_%d", n), func(t *testing.T) {
			testDir := filepath.Join(dir, fmt.Sprintf("bench_%d", n))
			if err := os.MkdirAll(testDir, 0755); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), duration+5*time.Second)
			defer cancel()

			start := time.Now()

			// Start writer processes
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					cmd := exec.CommandContext(ctx, testBinary,
						"--mode", "benchmark",
						"--dir", testDir,
						"--id", fmt.Sprintf("bench-%d", id),
						"--duration", duration.String(),
					)
					cmd.Run()
				}(i)
			}

			wg.Wait()
			elapsed := time.Since(start)

			// Count total entries written
			config := MultiProcessConfig()
			client, err := NewClientWithConfig(testDir, config)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			stats := client.GetStats()
			entriesPerSecond := float64(stats.TotalEntries) / elapsed.Seconds()

			t.Logf("Processes: %d, Total entries: %d, Rate: %.0f entries/sec, Per process: %.0f entries/sec",
				n, stats.TotalEntries, entriesPerSecond, entriesPerSecond/float64(n))
		})
	}
}
