package comet

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestIndexPersistenceAfterClose proves that index files don't reflect true state after Close()
func TestIndexPersistenceAfterClose(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultCometConfig()
	cfg.Storage.FlushInterval = 100 // Fast flush for testing
	ctx := context.Background()

	// Step 1: Create writer and write messages
	writer, err := NewClient(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write 10 messages
	for i := 0; i < 10; i++ {
		_, err := writer.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("message")})
		if err != nil {
			t.Fatalf("Failed to write message %d: %v", i, err)
		}
	}

	// Get the shard to check its state
	shard := writer.shards[0]
	if shard == nil {
		t.Fatal("Shard 0 not found")
	}

	t.Logf("Before close - In memory: nextEntryNumber=%d, index.CurrentEntryNumber=%d",
		shard.nextEntryNumber, shard.index.CurrentEntryNumber)
		
	// Wait for flush to happen
	time.Sleep(150 * time.Millisecond)
	
	t.Logf("After flush - In memory: nextEntryNumber=%d, index.CurrentEntryNumber=%d",
		shard.nextEntryNumber, shard.index.CurrentEntryNumber)

	// Step 2: Close the writer (should persist everything)
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Step 3: Check if index file exists
	indexPath := filepath.Join(dir, "shard-0000", "index.bin")
	stat, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("Index file doesn't exist after Close(): %v", err)
	}
	t.Logf("Index file exists: %s (size=%d bytes)", indexPath, stat.Size())

	// Step 4: Create a new client and check what it sees
	reader, err := NewClient(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Get the shard from the reader
	readerShard, err := reader.getOrCreateShard(0)
	if err != nil {
		t.Fatalf("Failed to get shard: %v", err)
	}

	t.Logf("After reopen - From disk: index.CurrentEntryNumber=%d", 
		readerShard.index.CurrentEntryNumber)

	// Step 5: Verify the index shows the correct state
	if readerShard.index.CurrentEntryNumber != 10 {
		t.Errorf("Index doesn't reflect true state: expected 10 entries, got %d",
			readerShard.index.CurrentEntryNumber)
	}

	// Also check if we can read the messages
	consumer := NewConsumer(reader, ConsumerOptions{Group: "test"})
	messages, err := consumer.Read(ctx, []uint32{0}, 100)
	if err != nil {
		t.Fatalf("Failed to read messages: %v", err)
	}

	t.Logf("Consumer can read %d messages", len(messages))

	if len(messages) != 10 {
		t.Errorf("Consumer can't see all messages: expected 10, got %d", len(messages))
	}
}