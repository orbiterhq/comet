package comet

import (
	"context"
	"testing"
	"time"
)

// TestConsumerMemoryOffset tests that consumer memory offset tracking works correctly
func TestConsumerMemoryOffset(t *testing.T) {
	dataDir := t.TempDir()

	// Create client with short flush interval
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 // 50ms flush interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create a consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-mem-offset"})
	defer consumer.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"
	shardID := uint32(0)

	// Write and read initial messages
	t.Log("=== Phase 1: Initial messages ===")
	initialMessages := [][]byte{
		[]byte("msg-1"),
		[]byte("msg-2"),
		[]byte("msg-3"),
	}

	if _, err := client.Append(ctx, stream, initialMessages); err != nil {
		t.Fatalf("Failed to append initial messages: %v", err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Read initial messages
	messages, err := consumer.Read(ctx, []uint32{shardID}, 10)
	if err != nil {
		t.Fatalf("Failed to read initial messages: %v", err)
	}
	t.Logf("Read %d initial messages", len(messages))

	// Check consumer state before ACK
	shard := client.getShard(shardID)
	shard.mu.RLock()
	offsetBeforeAck := shard.index.ConsumerOffsets[consumer.group]
	currentEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	consumer.memOffsetsMu.RLock()
	memOffsetBeforeAck, hasMemOffsetBefore := consumer.memOffsets[shardID]
	consumer.memOffsetsMu.RUnlock()

	t.Logf("Before ACK: shard has %d entries, consumer offset: %d, mem offset: %v/%d",
		currentEntries, offsetBeforeAck, hasMemOffsetBefore, memOffsetBeforeAck)

	// ACK messages
	messageIDs := make([]MessageID, len(messages))
	for i, msg := range messages {
		messageIDs[i] = msg.ID
	}
	if err := consumer.Ack(ctx, messageIDs...); err != nil {
		t.Fatalf("Failed to ACK: %v", err)
	}

	// Check consumer state after ACK
	shard.mu.RLock()
	offsetAfterAck := shard.index.ConsumerOffsets[consumer.group]
	shard.mu.RUnlock()

	consumer.memOffsetsMu.RLock()
	memOffsetAfterAck, hasMemOffsetAfter := consumer.memOffsets[shardID]
	consumer.memOffsetsMu.RUnlock()

	t.Logf("After ACK: consumer offset: %d, mem offset: %v/%d",
		offsetAfterAck, hasMemOffsetAfter, memOffsetAfterAck)

	// Write more messages
	t.Log("\n=== Phase 2: New messages ===")
	newMessages := [][]byte{
		[]byte("new-msg-1"),
		[]byte("new-msg-2"),
	}

	if _, err := client.Append(ctx, stream, newMessages); err != nil {
		t.Fatalf("Failed to append new messages: %v", err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Check shard state after new writes
	shard.mu.RLock()
	entriesAfterWrite := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("After new writes: shard has %d entries", entriesAfterWrite)

	// Try to read new messages
	t.Log("\n=== Phase 3: Reading new messages ===")
	for attempt := 1; attempt <= 5; attempt++ {
		messages, err = consumer.Read(ctx, []uint32{shardID}, 10)
		if err != nil {
			t.Fatalf("Failed to read on attempt %d: %v", attempt, err)
		}

		// Debug info
		shard.mu.RLock()
		currentOffset := shard.index.ConsumerOffsets[consumer.group]
		currentEntries := shard.index.CurrentEntryNumber
		shard.mu.RUnlock()

		consumer.memOffsetsMu.RLock()
		currentMemOffset, hasCurrentMemOffset := consumer.memOffsets[shardID]
		consumer.memOffsetsMu.RUnlock()

		t.Logf("Attempt %d: Read %d messages, shard entries: %d, consumer offset: %d, mem offset: %v/%d",
			attempt, len(messages), currentEntries, currentOffset, hasCurrentMemOffset, currentMemOffset)

		if len(messages) > 0 {
			t.Log("Successfully read new messages!")
			for i, msg := range messages {
				t.Logf("  Message %d: %s", i, string(msg.Data))
			}
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if len(messages) == 0 {
		t.Fatal("Failed to read new messages after multiple attempts")
	}
}

// TestConsumerReadDebug tests consumer read with detailed debugging
func TestConsumerReadDebug(t *testing.T) {
	dataDir := t.TempDir()

	// Create client with short flush interval
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 // 50ms flush interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create a consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-debug"})
	defer consumer.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"
	shardID := uint32(0)

	// Write one message
	if _, err := client.Append(ctx, stream, [][]byte{[]byte("test")}); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Get shard state
	shard := client.getShard(shardID)
	shard.mu.RLock()
	entries := shard.index.CurrentEntryNumber
	files := len(shard.index.Files)
	shard.mu.RUnlock()

	t.Logf("Shard state: %d entries in %d files", entries, files)

	// Try to read
	messages, err := consumer.Read(ctx, []uint32{shardID}, 10)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	t.Logf("Read %d messages", len(messages))

	if len(messages) == 0 && entries > 0 {
		t.Error("Consumer failed to read existing entries")

		// Let's manually check what readFromShard would do
		t.Log("\n=== Manual readFromShard simulation ===")

		consumer.memOffsetsMu.RLock()
		memOffset, hasMemOffset := consumer.memOffsets[shardID]
		consumer.memOffsetsMu.RUnlock()

		shard.mu.RLock()
		persistedOffset, exists := shard.index.ConsumerOffsets[consumer.group]
		endEntryNum := shard.index.CurrentEntryNumber
		shard.mu.RUnlock()

		startEntryNum := persistedOffset
		if hasMemOffset && memOffset > startEntryNum {
			startEntryNum = memOffset
		}

		t.Logf("Persisted offset: %d (exists: %v)", persistedOffset, exists)
		t.Logf("Memory offset: %d (exists: %v)", memOffset, hasMemOffset)
		t.Logf("Start entry: %d, End entry: %d", startEntryNum, endEntryNum)
		t.Logf("Should read: %v (start < end)", startEntryNum < endEntryNum)
	}
}
