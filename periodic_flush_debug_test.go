package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestPeriodicFlushDebug tests if periodic flush is working
func TestPeriodicFlushDebug(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 // 50ms flush interval

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write 5 entries
	for i := 0; i < 5; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get shard to monitor
	shard, _ := client.getOrCreateShard(0)

	// Check initial state
	shard.mu.RLock()
	initialCurrent := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()
	lastWritten := atomic.LoadInt64(&shard.lastWrittenEntryNumber)

	t.Logf("Initial state: CurrentEntryNumber=%d, lastWrittenEntryNumber=%d",
		initialCurrent, lastWritten)

	// Wait for periodic flush
	time.Sleep(100 * time.Millisecond)

	// Check state after flush
	shard.mu.RLock()
	afterFlushCurrent := shard.index.CurrentEntryNumber
	var fileEntries int64
	if len(shard.index.Files) > 0 {
		fileEntries = shard.index.Files[0].Entries
	}
	shard.mu.RUnlock()

	t.Logf("After flush: CurrentEntryNumber=%d, FileEntries=%d",
		afterFlushCurrent, fileEntries)

	// Create consumer to check visibility
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	t.Logf("Consumer can see %d messages", len(messages))

	// The issue: if CurrentEntryNumber was updated but consumer can't see messages,
	// then we have the mmap coherence issue
	if afterFlushCurrent > initialCurrent && len(messages) == 0 {
		t.Error("Periodic flush updated CurrentEntryNumber but messages aren't visible to consumer")
	}
}
