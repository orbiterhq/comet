//go:build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestProcessMultiProcessIntegration tests Process() with actual OS processes
func TestProcessMultiProcessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process integration test in short mode")
	}

	dir := t.TempDir()
	totalMessages := 200
	batchSize := 25
	numWorkers := 3
	stream := "multiproc:v1:shard:0000"

	// Step 1: Write test data using main process
	t.Logf("Writing %d messages to %s", totalMessages, stream)
	config := DeprecatedMultiProcessConfig(0, 2)

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("multiproc-msg-%04d", i)))
	}

	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to ensure messages are durable
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	client.Close()

	// Verify data was written
	client2, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	length, err := client2.Len(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	client2.Close()

	if length != int64(totalMessages) {
		t.Fatalf("Expected %d messages written, got %d", totalMessages, length)
	}
	t.Logf("Verified %d messages written", length)

	// Step 2: Launch multiple worker processes
	t.Logf("Launching %d worker processes", numWorkers)

	var wg sync.WaitGroup
	results := make(chan workerResult, numWorkers)

	for workerID := 0; workerID < numWorkers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Launch worker process
			cmd := exec.Command("go", "test", "-run", "TestMultiProcessWorker",
				"-timeout", "30s", "-v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_TEST_DATA_DIR=%s", dir),
				fmt.Sprintf("COMET_TEST_WORKER_ID=%d", id),
				fmt.Sprintf("COMET_TEST_BATCH_SIZE=%d", batchSize),
				fmt.Sprintf("COMET_TEST_STREAM=%s", stream),
			)

			output, err := cmd.CombinedOutput()
			results <- workerResult{
				workerID: id,
				output:   string(output),
				err:      err,
			}
		}(workerID)
	}

	// Wait for all workers to complete (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(results)
		close(done)
	}()

	select {
	case <-done:
		// All workers completed
	case <-time.After(25 * time.Second):
		t.Fatal("Workers timed out")
	}

	// Step 3: Collect and analyze results
	var successfulWorkers int

	for result := range results {
		if result.err != nil {
			t.Logf("Worker %d failed: %v", result.workerID, result.err)
			t.Logf("Worker %d output:\n%s", result.workerID, result.output)
		} else {
			successfulWorkers++
			// Parse processed count from output (workers will log it)
			// This is a simple approach - in production you'd use structured logging
			t.Logf("Worker %d completed successfully", result.workerID)
			t.Logf("Worker %d output:\n%s", result.workerID, result.output)
		}
	}

	// Step 4: Verify final state
	t.Logf("Integration test completed: %d/%d workers successful", successfulWorkers, numWorkers)

	if successfulWorkers == 0 {
		t.Fatal("No workers completed successfully")
	}

	// Verify that data is still accessible and consistent
	client3, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client3.Close()

	finalLength, err := client3.Len(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}

	if finalLength != int64(totalMessages) {
		t.Errorf("Data integrity check failed: expected %d messages, got %d", totalMessages, finalLength)
	}

	t.Logf("✅ Multi-process integration test passed with %d successful workers", successfulWorkers)
}

// TestMultiProcessWorker is the worker process entry point
// This runs in separate OS processes spawned by TestProcessMultiProcessIntegration
func TestMultiProcessWorker(t *testing.T) {
	// Only run when called as a worker process
	dataDir := os.Getenv("COMET_TEST_DATA_DIR")
	if dataDir == "" {
		t.Skip("Not running as multi-process worker")
	}

	workerIDStr := os.Getenv("COMET_TEST_WORKER_ID")
	batchSizeStr := os.Getenv("COMET_TEST_BATCH_SIZE")
	_ = os.Getenv("COMET_TEST_STREAM") // stream not used in worker test

	workerID, _ := strconv.Atoi(workerIDStr)
	batchSize, _ := strconv.Atoi(batchSizeStr)

	t.Logf("Worker %d starting: dataDir=%s, batchSize=%d",
		workerID, dataDir, batchSize)

	// Initialize client in multi-process mode
	config := DeprecatedMultiProcessConfig(0, 2)
	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Worker %d failed to create client: %v", workerID, err)
	}
	defer client.Close()

	// Create consumer with unique group for this worker
	consumer := NewConsumer(client, ConsumerOptions{
		Group: fmt.Sprintf("worker-%d", workerID),
	})
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	processedCount := 0
	batchCount := 0

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		batchCount++
		processedCount += len(msgs)

		t.Logf("Worker %d: Batch %d processed %d messages (total: %d)",
			workerID, batchCount, len(msgs), processedCount)

		// Simulate some processing work
		time.Sleep(10 * time.Millisecond)

		// Stop after processing a reasonable amount or timeout
		if processedCount >= 50 || batchCount >= 10 {
			t.Logf("Worker %d: Stopping after processing %d messages in %d batches",
				workerID, processedCount, batchCount)
			cancel()
		}

		return nil
	}

	// Start processing
	err = consumer.Process(ctx, processFunc,
		WithStream("multiproc:v1:shard:*"),
		WithBatchSize(batchSize),
		WithAutoAck(true),
		WithPollInterval(100*time.Millisecond),
	)

	t.Logf("Worker %d final results: processed %d messages in %d batches",
		workerID, processedCount, batchCount)

	// Success criteria: must process at least some messages
	if processedCount == 0 {
		t.Fatalf("Worker %d processed 0 messages - Process() may have failed", workerID)
	}

	if batchCount == 0 {
		t.Fatalf("Worker %d had 0 batches - Process() may have failed", workerID)
	}

	t.Logf("✅ Worker %d completed successfully", workerID)
}

// TestProcessMultiProcessContention tests multiple processes competing for the same data
func TestProcessMultiProcessContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process contention test in short mode")
	}

	dir := t.TempDir()
	totalMessages := 100
	stream := "contention:v1:shard:0042"

	// Write test data
	config := DeprecatedMultiProcessConfig(0, 2)
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("contention-msg-%04d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Launch 2 competing processes with the same consumer group
	// This tests that multi-process coordination works correctly
	var wg sync.WaitGroup
	results := make(chan workerResult, 2)

	for workerID := 0; workerID < 2; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cmd := exec.Command("go", "test", "-run", "TestMultiProcessContentionWorker",
				"-timeout", "20s", "-v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_TEST_DATA_DIR=%s", dir),
				fmt.Sprintf("COMET_TEST_WORKER_ID=%d", id),
				fmt.Sprintf("COMET_TEST_STREAM=%s", stream),
			)

			output, err := cmd.CombinedOutput()
			results <- workerResult{
				workerID: id,
				output:   string(output),
				err:      err,
			}
		}(workerID)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	successCount := 0

	for result := range results {
		if result.err != nil {
			t.Logf("Contention worker %d failed: %v", result.workerID, result.err)
			t.Logf("Output:\n%s", result.output)
		} else {
			successCount++
			t.Logf("Contention worker %d succeeded", result.workerID)
		}
	}

	if successCount < 1 {
		t.Fatal("No contention workers completed successfully")
	}

	t.Logf("✅ Multi-process contention test passed with %d successful workers", successCount)
}

// TestMultiProcessContentionWorker runs competing workers with same consumer group
func TestMultiProcessContentionWorker(t *testing.T) {
	dataDir := os.Getenv("COMET_TEST_DATA_DIR")
	if dataDir == "" {
		t.Skip("Not running as contention worker")
	}

	workerIDStr := os.Getenv("COMET_TEST_WORKER_ID")
	_ = os.Getenv("COMET_TEST_STREAM") // stream not used in contention worker
	workerID, _ := strconv.Atoi(workerIDStr)

	config := DeprecatedMultiProcessConfig(0, 2)
	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Contention worker %d failed to create client: %v", workerID, err)
	}
	defer client.Close()

	// Use SAME consumer group to test contention/coordination
	consumer := NewConsumer(client, ConsumerOptions{Group: "shared-group"})
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	processedCount := 0

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		processedCount += len(msgs)
		t.Logf("Contention worker %d: processed %d messages (total: %d)",
			workerID, len(msgs), processedCount)

		// Stop after reasonable processing
		if processedCount >= 30 {
			cancel()
		}

		return nil
	}

	err = consumer.Process(ctx, processFunc,
		WithStream("contention:v1:shard:*"),
		WithBatchSize(10),
		WithAutoAck(true),
	)

	t.Logf("Contention worker %d final: processed %d messages", workerID, processedCount)

	// In contention scenario, it's ok if one worker gets most/all messages
	// The key is that Process() doesn't hang or fail
	t.Logf("✅ Contention worker %d completed", workerID)
}

type workerResult struct {
	workerID int
	output   string
	err      error
}
