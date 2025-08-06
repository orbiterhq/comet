package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestReaderIndexRefresh tests that the reader can detect when new files are added to the index
func TestReaderIndexRefresh(t *testing.T) {
	dataDir := t.TempDir()

	// Configure for frequent rotations
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10     // 10ms flush
	config.Storage.MaxFileSize = 2 * 1024 // 2KB files to force rotations

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stream := "test:v1:shard:0000"
	shardID := uint32(0)
	ctx := context.Background()

	// Write initial messages to get one file
	entries := make([][]byte, 0)
	for i := 0; i < 20; i++ {
		msg := fmt.Sprintf("initial-message-%03d-with-much-more-padding-to-make-it-really-large-and-exceed-file-size-limits-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)
		entries = append(entries, []byte(msg))
	}

	_, err = client.Append(ctx, stream, entries)
	if err != nil {
		t.Fatalf("Failed to write initial messages: %v", err)
	}

	// Sync to ensure data is written
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("Failed to sync initial data: %v", err)
	}

	// Create consumer and get first reader
	consumer := NewConsumer(client, ConsumerOptions{Group: "refresh-test"})
	defer consumer.Close()

	// Read first few messages to establish a reader
	t.Log("Reading initial messages to create reader...")
	var msgs []StreamMessage
	for attempt := 0; attempt < 10; attempt++ {
		msgs, err = consumer.Read(ctx, []uint32{shardID}, 5)
		if err != nil {
			t.Logf("Read attempt %d failed: %v", attempt, err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if len(msgs) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Failed to read initial messages after retries: %v", err)
	}
	t.Logf("Read %d initial messages", len(msgs))

	// Check initial shard state
	shard, err := client.getOrCreateShard(shardID)
	if err != nil {
		t.Fatalf("Failed to get shard: %v", err)
	}

	shard.mu.RLock()
	initialFileCount := len(shard.index.Files)
	initialEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("Initial state: %d files, %d entries", initialFileCount, initialEntries)

	// Write more messages to trigger file rotation
	t.Log("Writing messages to trigger file rotation...")
	moreEntries := make([][]byte, 0)
	for i := 10; i < 50; i++ {
		msg := fmt.Sprintf("rotation-message-%03d-with-extra-padding-to-force-rotation-xxxxxxxxxxxx", i)
		moreEntries = append(moreEntries, []byte(msg))
	}

	_, err = client.Append(ctx, stream, moreEntries)
	if err != nil {
		t.Fatalf("Failed to write rotation messages: %v", err)
	}

	// Wait for rotation to complete
	time.Sleep(100 * time.Millisecond)

	// Check new shard state
	shard.mu.RLock()
	newFileCount := len(shard.index.Files)
	newEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("After rotation: %d files, %d entries", newFileCount, newEntries)

	if newFileCount <= initialFileCount {
		t.Fatalf("Expected file rotation but file count didn't increase: %d -> %d", initialFileCount, newFileCount)
	}

	// Now try to read beyond what the initial reader knew about
	t.Log("Attempting to read messages beyond initial file...")

	for attempt := 0; attempt < 10; attempt++ {
		msgs, err = consumer.Read(ctx, []uint32{shardID}, 10)
		if err != nil {
			t.Fatalf("Failed to read after rotation: %v", err)
		}

		if len(msgs) == 0 {
			t.Logf("Attempt %d: Got 0 messages", attempt)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		t.Logf("Attempt %d: Successfully read %d messages", attempt, len(msgs))

		// ACK the messages so consumer offset advances
		messageIDs := make([]MessageID, len(msgs))
		for i, msg := range msgs {
			messageIDs[i] = msg.ID
		}
		if err := consumer.Ack(ctx, messageIDs...); err != nil {
			t.Fatalf("Failed to ACK messages: %v", err)
		}

		// Check if we're getting messages from the new file
		lastMsg := msgs[len(msgs)-1]
		t.Logf("Last message entry number: %d", lastMsg.ID.EntryNumber)

		// If we read messages beyond the initial file, the reader refresh worked
		if lastMsg.ID.EntryNumber >= initialEntries {
			t.Logf("✅ Reader successfully detected index updates! Read entry %d (initial entries: %d)",
				lastMsg.ID.EntryNumber, initialEntries)
			return
		}
	}

	t.Errorf("Reader failed to detect index updates after 10 attempts")
}
