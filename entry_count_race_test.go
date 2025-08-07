package comet

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestEntryCountRace tests if file metadata entry count can be wrong
func TestEntryCountRace(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 20 // Periodic flush

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write 5 entries
	for i := 0; i < 5; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte("x")})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Don't sync - leave them in buffer

	// Get shard state before periodic flush
	shard, _ := client.getOrCreateShard(0)

	shard.mu.RLock()
	beforeFlush := struct {
		CurrentEntryNumber int64
		FileEntries        int64
		FileEndOffset      int64
	}{
		CurrentEntryNumber: shard.index.CurrentEntryNumber,
	}
	if len(shard.index.Files) > 0 {
		beforeFlush.FileEntries = shard.index.Files[0].Entries
		beforeFlush.FileEndOffset = shard.index.Files[0].EndOffset
	}
	shard.mu.RUnlock()

	lastWritten := atomic.LoadInt64(&shard.lastWrittenEntryNumber)

	t.Logf("Before periodic flush: CurrentEntryNumber=%d, lastWrittenEntryNumber=%d, FileEntries=%d",
		beforeFlush.CurrentEntryNumber, lastWritten, beforeFlush.FileEntries)

	// Wait for periodic flush
	time.Sleep(50 * time.Millisecond)

	// Check state after periodic flush
	shard.mu.RLock()
	afterFlush := struct {
		CurrentEntryNumber int64
		FileEntries        int64
		FileEndOffset      int64
	}{
		CurrentEntryNumber: shard.index.CurrentEntryNumber,
	}
	if len(shard.index.Files) > 0 {
		afterFlush.FileEntries = shard.index.Files[0].Entries
		afterFlush.FileEndOffset = shard.index.Files[0].EndOffset
	}
	shard.mu.RUnlock()

	t.Logf("After periodic flush: CurrentEntryNumber=%d, FileEntries=%d, FileEndOffset=%d",
		afterFlush.CurrentEntryNumber, afterFlush.FileEntries, afterFlush.FileEndOffset)

	// The bug: FileEntries might be set to lastWrittenEntryNumber (6)
	// but CurrentEntryNumber should only be 5
	if afterFlush.FileEntries > afterFlush.CurrentEntryNumber {
		t.Errorf("BUG: FileEntries (%d) > CurrentEntryNumber (%d) - file claims more entries than are durable!",
			afterFlush.FileEntries, afterFlush.CurrentEntryNumber)
	}

	// Try to read - this might trigger the race
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	t.Logf("Read %d messages", len(messages))
}
