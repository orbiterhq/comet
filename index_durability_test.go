package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestIndexOnlyTracksDurableState verifies that the index only reflects what has been explicitly synced
// Note: This test demonstrates that on clean shutdown (even without explicit sync),
// buffered data is flushed to disk. Only a hard crash (kill -9, power loss) would lose unflushed data.
func TestIndexOnlyTracksDurableState(t *testing.T) {
	dataDir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 1 * time.Second // Don't auto-flush

	// Create client and write some data
	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	stream := "durability:v1:shard:0000"
	shardID := uint32(0)
	ctx := context.Background()

	// Write and flush first batch
	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("flushed-%d", i)
		_, err := client.Append(ctx, stream, [][]byte{[]byte(msg)})
		if err != nil {
			t.Fatalf("Failed to write message %d: %v", i, err)
		}
	}

	// Sync to ensure these are on disk
	if err := client.Sync(ctx); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Get the shard and check index state after flush
	shard := client.shards[shardID]
	if shard == nil {
		t.Fatal("Shard not found")
	}

	shard.mu.RLock()
	entriesAfterFlush := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("After flush: CurrentEntryNumber = %d", entriesAfterFlush)
	if entriesAfterFlush != 5 {
		t.Errorf("Expected 5 entries after flush, got %d", entriesAfterFlush)
	}

	// Write more entries but DON'T flush
	for i := 5; i < 10; i++ {
		msg := fmt.Sprintf("unflushed-%d", i)
		_, err := client.Append(ctx, stream, [][]byte{[]byte(msg)})
		if err != nil {
			t.Fatalf("Failed to write message %d: %v", i, err)
		}
	}

	// Check index state - it should NOT show unflushed entries
	shard.mu.RLock()
	entriesBeforeClose := shard.index.CurrentEntryNumber
	nextEntryNumber := shard.nextEntryNumber
	shard.mu.RUnlock()

	t.Logf("Before close (unflushed): CurrentEntryNumber = %d, nextEntryNumber = %d", entriesBeforeClose, nextEntryNumber)

	// EXPECTATION: Index should still show 5, not 10
	// nextEntryNumber tracks all writes (including pending), CurrentEntryNumber tracks durable state
	if entriesBeforeClose != 5 {
		t.Errorf("Index incorrectly shows unflushed entries: expected 5, got %d", entriesBeforeClose)
	}

	if nextEntryNumber != 10 {
		t.Errorf("nextEntryNumber should track all writes: expected 10, got %d", nextEntryNumber)
	}

	// Close the client - this will flush buffers as part of graceful shutdown
	client.Close()

	// Reopen and verify state
	client2, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to reopen client: %v", err)
	}
	defer client2.Close()

	// Try to get shard without writing (it should exist from recovery)
	client2.getOrCreateShard(shardID)
	shard2 := client2.shards[shardID]
	if shard2 == nil {
		t.Fatal("Shard should exist after recovery")
	}

	shard2.mu.RLock()
	entriesAfterReopen := shard2.index.CurrentEntryNumber
	shard2.mu.RUnlock()

	t.Logf("After reopen: CurrentEntryNumber = %d", entriesAfterReopen)

	// After close and reopen, we should see all data since Close() flushes buffers
	// Close() performs graceful shutdown which makes all pending writes durable
	if entriesAfterReopen != 10 {
		t.Errorf("After clean shutdown and recovery, expected 10 entries (all flushed), got %d", entriesAfterReopen)
	}

	// Verify consumer can read all entries
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{shardID}, 20)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	// Now the index shows all entries since Close() flushed everything
	// And we can read all 10 messages (5 flushed + 5 unflushed that were flushed during Close)
	if int64(len(messages)) != entriesAfterReopen {
		t.Errorf("Mismatch: index shows %d entries but %d messages are readable", entriesAfterReopen, len(messages))
	}

	// Verify we got all messages (both originally flushed and those flushed during Close)
	for i, msg := range messages {
		var expected string
		if i < 5 {
			expected = fmt.Sprintf("flushed-%d", i)
		} else {
			expected = fmt.Sprintf("unflushed-%d", i)
		}
		if string(msg.Data) != expected {
			t.Errorf("Message %d: expected %q, got %q", i, expected, string(msg.Data))
		}
	}

	// Write new entries - should continue from entry 10
	newEntryIDs, err := client2.Append(ctx, stream, [][]byte{[]byte("new-entry")})
	if err != nil {
		t.Fatalf("Failed to write new entry: %v", err)
	}

	// EXPECTATION: New entry should be #10 (continuing from all recovered entries)
	if len(newEntryIDs) != 1 || newEntryIDs[0].EntryNumber != 10 {
		t.Errorf("Expected new entry number to be 10, got %v", newEntryIDs)
	}
}

// TestConsumerNeverSeesUnflushedData verifies consumers only read durable data
func TestConsumerNeverSeesUnflushedData(t *testing.T) {
	dataDir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 1 * time.Second // Don't auto-flush

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stream := "consumer-safety:v1:shard:0000"
	shardID := uint32(0)
	ctx := context.Background()

	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	// Write and flush first batch
	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("batch1-%d", i)
		client.Append(ctx, stream, [][]byte{[]byte(msg)})
	}
	client.Sync(ctx)

	// Read first batch
	messages, err := consumer.Read(ctx, []uint32{shardID}, 10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if len(messages) != 5 {
		t.Errorf("Expected 5 messages, got %d", len(messages))
	}

	// ACK them
	for _, msg := range messages {
		consumer.Ack(ctx, msg.ID)
	}

	// Write second batch but don't flush
	for i := 5; i < 10; i++ {
		msg := fmt.Sprintf("batch2-%d", i)
		client.Append(ctx, stream, [][]byte{[]byte(msg)})
	}

	// Try to read - should get 0 messages (nothing new on disk)
	messages, err = consumer.Read(ctx, []uint32{shardID}, 10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	// EXPECTATION: No messages because nothing new is flushed
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages (nothing flushed), got %d", len(messages))
	}

	// Now flush
	client.Sync(ctx)

	// Now we should be able to read the second batch
	messages, err = consumer.Read(ctx, []uint32{shardID}, 10)
	if err != nil {
		t.Fatalf("Failed to read after flush: %v", err)
	}

	// EXPECTATION: Now we get the 5 messages that were flushed
	if len(messages) != 5 {
		t.Errorf("Expected 5 messages after flush, got %d", len(messages))
	}

	// Verify they're from batch 2
	for i, msg := range messages {
		expected := fmt.Sprintf("batch2-%d", i+5)
		if string(msg.Data) != expected {
			t.Errorf("Message %d: expected %q, got %q", i, expected, string(msg.Data))
		}
	}
}

// TestAppendReturnsPendingEntryNumbers verifies Append returns correct entry numbers even before flush
func TestAppendReturnsPendingEntryNumbers(t *testing.T) {
	dataDir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 1 * time.Second // Don't auto-flush

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stream := "entry-numbers:v1:shard:0000"
	ctx := context.Background()

	// Track returned entry numbers
	entryNumbers := make([]int64, 0)

	// Write entries without flushing
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("msg-%d", i)
		entryIDs, err := client.Append(ctx, stream, [][]byte{[]byte(msg)})
		if err != nil {
			t.Fatalf("Failed to write message %d: %v", i, err)
		}
		if len(entryIDs) != 1 {
			t.Fatalf("Expected 1 entry ID, got %d", len(entryIDs))
		}
		entryNumbers = append(entryNumbers, entryIDs[0].EntryNumber)
		t.Logf("Append %d returned entry number: %d", i, entryIDs[0].EntryNumber)
	}

	// EXPECTATION: Entry numbers should be sequential starting from 0
	for i, entryNum := range entryNumbers {
		if entryNum != int64(i) {
			t.Errorf("Expected entry number %d, got %d", i, entryNum)
		}
	}

	// After crash and recovery, new writes should continue from last persisted
	// Close() will flush pending writes, so all 10 entries will be persisted
	client.Close()

	client2, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to reopen client: %v", err)
	}
	defer client2.Close()

	// Write new entry
	entryIDs, err := client2.Append(ctx, stream, [][]byte{[]byte("after-crash")})
	if err != nil {
		t.Fatalf("Failed to write after crash: %v", err)
	}

	// EXPECTATION: Should start from 10 since Close() flushes all pending data
	if len(entryIDs) != 1 || entryIDs[0].EntryNumber != 10 {
		t.Errorf("Expected entry number 10 after crash recovery, got %v", entryIDs)
	}
}
