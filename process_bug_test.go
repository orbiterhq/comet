package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestProcessContinuousBatching reproduces the bug where Process() stops after one batch
func TestProcessContinuousBatching(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0001"

	// Write MORE messages than one batch size
	batchSize := 50
	totalMessages := 150 // 3 batches worth

	t.Logf("Writing %d messages to %s", totalMessages, stream)

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("message-%d", i)))
	}

	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	// Verify messages were written
	length, err := client.Len(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Stream length: %d entries", length)
	if length != int64(totalMessages) {
		t.Fatalf("Expected %d messages, got %d", totalMessages, length)
	}

	// Now test Process() - it should read ALL messages in multiple batches
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	var processedCount int64
	var batchCount int64

	// Create a context that will cancel after reasonable time
	processCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		currentBatch := atomic.AddInt64(&batchCount, 1)
		processed := atomic.AddInt64(&processedCount, int64(len(msgs)))

		t.Logf("Batch %d: processed %d messages (total: %d/%d)",
			currentBatch, len(msgs), processed, totalMessages)

		// If we've processed all messages, cancel to exit Process()
		if int(processed) >= totalMessages {
			t.Logf("All messages processed, canceling context")
			cancel()
		}

		return nil
	}

	// Start processing with small batch size to force multiple batches
	consumer.Process(processCtx, processFunc,
		WithStream("test:v1:shard:*"),
		WithBatchSize(batchSize),
		WithAutoAck(true),
	)

	// Check results
	finalProcessed := atomic.LoadInt64(&processedCount)
	finalBatches := atomic.LoadInt64(&batchCount)

	t.Logf("Final results: processed %d/%d messages in %d batches",
		finalProcessed, totalMessages, finalBatches)

	// The bug: if Process() stops after one batch, we'll see:
	// - processedCount = batchSize (50) instead of totalMessages (150)
	// - batchCount = 1 instead of 3

	if finalProcessed < int64(totalMessages) {
		t.Errorf("BUG REPRODUCED: Process() stopped early! Expected %d messages, got %d",
			totalMessages, finalProcessed)
		t.Errorf("Expected ~%d batches, got %d", (totalMessages+batchSize-1)/batchSize, finalBatches)

		// Check consumer lag to see if messages are still available
		lag, err := consumer.GetLag(ctx, 1)
		if err == nil {
			t.Errorf("Consumer lag: %d (should be 0 if all processed)", lag)
		}
	} else {
		t.Logf("SUCCESS: Process() continued until all messages were processed")
	}
}

// TestProcessOffsetPersistence tests if ACK properly updates consumer offset
func TestProcessOffsetPersistence(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0001"

	// Write some messages
	messages := [][]byte{
		[]byte("msg1"), []byte("msg2"), []byte("msg3"), []byte("msg4"), []byte("msg5"),
	}

	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "offset-test"})
	defer consumer.Close()

	// Read first batch manually and check offset
	t.Log("=== Manual Read/ACK Test ===")
	batch1, err := consumer.Read(ctx, []uint32{1}, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Read batch 1: %d messages", len(batch1))

	// Check lag before ACK
	lagBefore, _ := consumer.GetLag(ctx, 1)
	t.Logf("Lag before ACK: %d", lagBefore)

	// ACK the messages
	for _, msg := range batch1 {
		err = consumer.Ack(ctx, msg.ID)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check lag after ACK
	lagAfter, _ := consumer.GetLag(ctx, 1)
	t.Logf("Lag after ACK: %d", lagAfter)

	if lagAfter >= lagBefore {
		t.Errorf("ACK didn't reduce lag: before=%d, after=%d", lagBefore, lagAfter)
	}

	// Read next batch - should get different messages
	batch2, err := consumer.Read(ctx, []uint32{1}, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Read batch 2: %d messages", len(batch2))

	if len(batch2) == 0 {
		t.Error("Second read returned 0 messages - offset might not be persisted")
	}

	// Verify we got different messages
	if len(batch1) > 0 && len(batch2) > 0 {
		if batch1[0].ID.EntryNumber >= batch2[0].ID.EntryNumber {
			t.Errorf("Second batch didn't advance: batch1[0]=%d, batch2[0]=%d",
				batch1[0].ID.EntryNumber, batch2[0].ID.EntryNumber)
		}
	}
}
