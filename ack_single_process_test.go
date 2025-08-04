//go:build integration

package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSingleProcessACKPersistenceIsolated tests ACK persistence in single-process mode
func TestSingleProcessACKPersistenceIsolated(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig() // Single-process mode
	stream := "single:v1:shard:0000"
	totalMessages := 100

	// Enable debug logging
	SetDebug(true)
	defer SetDebug(false)

	// Step 1: Write test messages
	t.Logf("=== STEP 1: Writing %d messages ===", totalMessages)
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("msg-%04d", i)))
	}

	ctx := context.Background()
	_, err = client1.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client1.Close()
	t.Logf("✓ Wrote %d messages", totalMessages)

	// Step 2: First consumer reads and ACKs some messages
	t.Logf("\n=== STEP 2: First consumer session (single-process) ===")
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	consumer1 := NewConsumer(client2, ConsumerOptions{
		Group: "test-group",
	})

	messagesRead := 0

	// Process with auto-ACK
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		messagesRead += len(msgs)
		t.Logf("Consumer1: Read batch of %d messages (total: %d)", len(msgs), messagesRead)

		// Log first and last message in batch
		if len(msgs) > 0 {
			t.Logf("  First: %s, Last: %s", string(msgs[0].Data), string(msgs[len(msgs)-1].Data))
		}

		// Stop after reading 50 messages
		if messagesRead >= 50 {
			t.Logf("Consumer1: Stopping after %d messages", messagesRead)
			cancel2() // Cancel the context to stop processing
		}
		return nil
	}
	err = consumer1.Process(ctx2, processFunc,
		WithStream("single:v1:shard:*"),
		WithBatchSize(10),
		WithAutoAck(true),
		WithPollInterval(50*time.Millisecond),
	)
	cancel2()

	t.Logf("Consumer1: Process returned: %v", err)
	t.Logf("Consumer1: Read %d messages total", messagesRead)

	// Get the shard to check offset
	shard, err := client2.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	shard.mu.RLock()
	offset1 := shard.index.ConsumerOffsets["test-group"]
	shard.mu.RUnlock()
	t.Logf("Consumer1: Offset after processing: %d", offset1)

	// Close consumer and client
	t.Logf("Consumer1: Closing...")
	consumer1.Close()
	client2.Close()

	// Wait a bit to ensure everything is flushed
	time.Sleep(100 * time.Millisecond)

	// Step 3: New consumer should start from where first one left off
	t.Logf("\n=== STEP 3: Second consumer session (single-process) ===")
	client3, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Check what offset is persisted
	shard2, err := client3.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	shard2.mu.RLock()
	offset2 := shard2.index.ConsumerOffsets["test-group"]
	shard2.mu.RUnlock()
	t.Logf("Consumer2: Initial offset from disk: %d", offset2)

	consumer2 := NewConsumer(client3, ConsumerOptions{
		Group: "test-group",
	})

	messagesRead2 := 0
	duplicates := 0
	seenMessages := make(map[string]bool)

	// Track what we saw in first consumer
	for i := 0; i < messagesRead; i++ {
		seenMessages[fmt.Sprintf("msg-%04d", i)] = true
	}

	processFunc2 := func(ctx context.Context, msgs []StreamMessage) error {
		messagesRead2 += len(msgs)
		t.Logf("Consumer2: Read batch of %d messages (total: %d)", len(msgs), messagesRead2)

		// Check for duplicates
		for _, msg := range msgs {
			msgStr := string(msg.Data)
			if seenMessages[msgStr] {
				duplicates++
				t.Logf("  DUPLICATE: %s", msgStr)
			}
		}

		// Log first and last message in batch
		if len(msgs) > 0 {
			t.Logf("  First: %s, Last: %s", string(msgs[0].Data), string(msgs[len(msgs)-1].Data))
		}

		return nil
	}

	// Process remaining messages
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	err = consumer2.Process(ctx3, processFunc2,
		WithStream("single:v1:shard:*"),
		WithBatchSize(10),
		WithAutoAck(true),
		WithPollInterval(50*time.Millisecond),
	)
	cancel3()

	t.Logf("Consumer2: Process returned: %v", err)
	t.Logf("Consumer2: Read %d messages", messagesRead2)
	t.Logf("Consumer2: Found %d duplicates", duplicates)

	consumer2.Close()
	client3.Close()

	// Verify results
	t.Logf("\n=== RESULTS (SINGLE-PROCESS MODE) ===")
	t.Logf("Total messages written: %d", totalMessages)
	t.Logf("Consumer1 read: %d", messagesRead)
	t.Logf("Consumer2 read: %d", messagesRead2)
	t.Logf("Total read: %d", messagesRead+messagesRead2)
	t.Logf("Duplicates: %d", duplicates)

	if duplicates > 0 {
		t.Errorf("FAIL: Found %d duplicate messages in single-process mode!", duplicates)
	}

	totalRead := messagesRead + messagesRead2 - duplicates
	if totalRead < totalMessages {
		t.Errorf("FAIL: Only read %d unique messages out of %d", totalRead, totalMessages)
	}
}
