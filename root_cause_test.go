package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestRootCauseOffsetCalculation tests the exact root cause
func TestRootCauseOffsetCalculation(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write some entries
	for i := 0; i < 3; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("entry-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for periodic flush
	time.Sleep(30 * time.Millisecond)

	// Get shard and create reader
	shard, _ := client.getOrCreateShard(0)

	shard.mu.RLock()
	indexSnapshot := shard.cloneIndex()
	fileInfo := indexSnapshot.Files[0]
	t.Logf("File info: StartEntry=%d, Entries=%d, EndOffset=%d",
		fileInfo.StartEntry, fileInfo.Entries, fileInfo.EndOffset)
	shard.mu.RUnlock()

	// The question: if we have 3 entries (0, 1, 2), what happens when we try to read entry 3?
	// The file says it has 3 entries, so valid entries are [0, 1, 2]
	// But if consumer sees CurrentEntryNumber = 4, it might try to read entry 3

	// Simulate what consumer does
	shard.mu.RLock()
	currentEntryNum := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("CurrentEntryNumber = %d", currentEntryNum)

	// If CurrentEntryNumber = 3, consumer would try to read entries [0, 1, 2]
	// But if CurrentEntryNumber = 4, consumer would try to read entries [0, 1, 2, 3]
	// And entry 3 doesn't exist!

	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Logf("Read error: %v", err)

		// Write one more entry to see what happens
		_, err := client.Append(ctx, stream, [][]byte{[]byte("entry-3")})
		if err != nil {
			t.Fatal(err)
		}

		// Don't wait for flush - the entry is in buffer

		// Check state
		shard.mu.RLock()
		newCurrentEntry := shard.index.CurrentEntryNumber
		if len(shard.index.Files) > 0 {
			newFileInfo := shard.index.Files[0]
			t.Logf("After write: CurrentEntryNumber=%d, File.Entries=%d",
				newCurrentEntry, newFileInfo.Entries)
		}
		shard.mu.RUnlock()

		// Try to read again
		messages2, err2 := consumer.Read(ctx, []uint32{0}, 10)
		if err2 != nil {
			t.Fatalf("Still failed: %v", err2)
		}
		t.Logf("After writing entry-3: Read %d messages", len(messages2))
	} else {
		t.Logf("Read %d messages successfully", len(messages))
	}
}
