package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestMultiShardIndexPersistence verifies index persistence across multiple shards
func TestMultiShardIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultCometConfig()
	cfg.Storage.FlushInterval = 100 * time.Millisecond // Fast flush for testing
	ctx := context.Background()

	// Step 1: Create writer and write to 4 shards
	writer, err := NewClient(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write 10 messages to each of 4 shards
	for shardID := 0; shardID < 4; shardID++ {
		streamName := fmt.Sprintf("test:v1:shard:%04d", shardID)
		for i := 0; i < 10; i++ {
			_, err := writer.Append(ctx, streamName, [][]byte{[]byte(fmt.Sprintf("shard-%d-msg-%d", shardID, i))})
			if err != nil {
				t.Fatalf("Failed to write to shard %d: %v", shardID, err)
			}
		}
	}

	// Wait for flush and sync all shards
	time.Sleep(200 * time.Millisecond)
	if err := writer.Sync(ctx); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Check state before close
	t.Logf("=== BEFORE CLOSE ===")
	for shardID := 0; shardID < 4; shardID++ {
		shard := writer.shards[uint32(shardID)]
		if shard == nil {
			t.Errorf("Shard %d not found", shardID)
			continue
		}
		t.Logf("Shard %d: nextEntryNumber=%d, index.CurrentEntryNumber=%d",
			shardID, shard.nextEntryNumber, shard.index.CurrentEntryNumber)
	}

	// Close the writer
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Step 2: Create consumer and check what it sees
	t.Logf("\n=== AFTER REOPEN ===")
	reader, err := NewClient(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	consumer := NewConsumer(reader, ConsumerOptions{Group: "test"})

	// Check each shard individually
	totalMessages := 0
	for shardID := 0; shardID < 4; shardID++ {
		// Force shard creation if needed
		shard, err := reader.getOrCreateShard(uint32(shardID))
		if err != nil {
			t.Errorf("Failed to get shard %d: %v", shardID, err)
			continue
		}

		// Force index reload
		shard.mu.Lock()
		if err := shard.loadIndex(); err != nil {
			shard.mu.Unlock()
			t.Errorf("Failed to load index for shard %d: %v", shardID, err)
			continue
		}
		entries := shard.index.CurrentEntryNumber
		offset := shard.index.ConsumerOffsets["test"]
		shard.mu.Unlock()

		t.Logf("Shard %d: entries=%d, offset=%d", shardID, entries, offset)

		// Try to read messages from this shard
		messages, err := consumer.Read(ctx, []uint32{uint32(shardID)}, 100)
		if err != nil {
			t.Errorf("Failed to read from shard %d: %v", shardID, err)
		} else {
			t.Logf("Shard %d: read %d messages", shardID, len(messages))
			totalMessages += len(messages)
		}
	}

	// Verify we can read all 40 messages
	if totalMessages != 40 {
		t.Errorf("Expected to read 40 messages total, got %d", totalMessages)
	}
}
