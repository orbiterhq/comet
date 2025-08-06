package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestProcessMultiProcessMode tests Process() in multi-process mode
func TestProcessMultiProcessMode(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2) // Use multi-process config

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0166" // Use higher shard number like user had

	// Write multiple batches worth of data
	batchSize := 25
	totalMessages := 100

	t.Logf("Writing %d messages to %s in multi-process mode", totalMessages, stream)

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("multi-process-msg-%d", i)))
	}

	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to ensure data is visible
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Process with wildcards (like user was doing)
	consumer := NewConsumer(client, ConsumerOptions{Group: "multi-test"})
	defer consumer.Close()

	var processedCount int64
	var batchCount int64

	processCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		currentBatch := atomic.AddInt64(&batchCount, 1)
		processed := atomic.AddInt64(&processedCount, int64(len(msgs)))

		t.Logf("Multi-process batch %d: processed %d messages (total: %d/%d)",
			currentBatch, len(msgs), processed, totalMessages)

		if int(processed) >= totalMessages {
			t.Logf("All multi-process messages processed, canceling")
			cancel()
		}

		return nil
	}

	consumer.Process(processCtx, processFunc,
		WithStream("test:v1:shard:*"), // Wildcard pattern
		WithBatchSize(batchSize),
		WithAutoAck(true),
	)

	finalProcessed := atomic.LoadInt64(&processedCount)
	finalBatches := atomic.LoadInt64(&batchCount)

	t.Logf("Multi-process final: processed %d/%d messages in %d batches",
		finalProcessed, totalMessages, finalBatches)

	if finalProcessed < int64(totalMessages) {
		t.Errorf("MULTI-PROCESS BUG: Expected %d messages, got %d", totalMessages, finalProcessed)
	}
}

// TestProcessWithErrors tests Process() behavior with processing errors
func TestProcessWithErrors(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write messages
	totalMessages := 75
	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("error-test-%d", i)))
	}

	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "error-test"})
	defer consumer.Close()

	var processedCount int64
	var errorCount int64

	processCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		processed := atomic.AddInt64(&processedCount, int64(len(msgs)))

		// Simulate error on first batch only
		if processed <= 25 {
			atomic.AddInt64(&errorCount, 1)
			t.Logf("Simulating error on batch with %d messages (total processed: %d)", len(msgs), processed)
			return fmt.Errorf("simulated error")
		}

		t.Logf("Successfully processed batch with %d messages (total: %d)", len(msgs), processed)

		if int(processed) >= totalMessages {
			cancel()
		}

		return nil
	}

	consumer.Process(processCtx, processFunc,
		WithStream("test:v1:shard:*"),
		WithBatchSize(25),
		WithMaxRetries(2), // Allow retries
		WithAutoAck(true),
	)

	finalProcessed := atomic.LoadInt64(&processedCount)
	finalErrors := atomic.LoadInt64(&errorCount)

	t.Logf("Error test final: processed %d/%d messages, %d errors",
		finalProcessed, totalMessages, finalErrors)

	// Should process all messages even with initial errors (due to retries)
	if finalProcessed < int64(totalMessages) {
		t.Errorf("ERROR HANDLING BUG: Expected %d messages, got %d", totalMessages, finalProcessed)
	}
}

// TestProcessLongRunning tests Process() over longer time periods
func TestProcessLongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long running test")
	}

	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write initial batch
	initialMessages := 50
	var messages [][]byte
	for i := 0; i < initialMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("long-running-%d", i)))
	}

	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to ensure data is visible to consumers
	err = client.Sync(ctx)
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "long-test"})
	defer consumer.Close()

	var processedCount int64
	var wg sync.WaitGroup

	// Run for longer period
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		processed := atomic.AddInt64(&processedCount, int64(len(msgs)))
		t.Logf("Long-running: processed batch of %d (total: %d)", len(msgs), processed)

		// After processing initial messages, add more messages to test continuous processing
		if processed == int64(initialMessages) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(1 * time.Second)
				additionalMessages := 25
				var newMessages [][]byte
				for i := 0; i < additionalMessages; i++ {
					newMessages = append(newMessages, []byte(fmt.Sprintf("additional-%d", i)))
				}
				client.Append(context.Background(), stream, newMessages)
				client.Sync(context.Background()) // Ensure additional messages are visible
				t.Logf("Added %d additional messages", additionalMessages)
			}()
		}

		// Cancel after processing both batches
		if processed >= int64(initialMessages+25) {
			t.Logf("Processed all messages including additional ones")
			cancel()
		}

		return nil
	}

	consumer.Process(processCtx, processFunc,
		WithStream("test:v1:shard:*"),
		WithBatchSize(10),
		WithAutoAck(true),
		WithPollInterval(500*time.Millisecond), // Poll every 500ms for new data
	)

	// Wait for any background goroutines to complete
	wg.Wait()

	finalProcessed := atomic.LoadInt64(&processedCount)
	expectedTotal := int64(initialMessages + 25)

	t.Logf("Long-running final: processed %d/%d messages", finalProcessed, expectedTotal)

	if finalProcessed < expectedTotal {
		t.Errorf("LONG-RUNNING BUG: Expected %d messages, got %d", expectedTotal, finalProcessed)
	}
}
