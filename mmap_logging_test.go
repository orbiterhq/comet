package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestMmapLogging tests with detailed logging to understand the issue
func TestMmapLogging(t *testing.T) {
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

	// Write a few entries
	for i := 0; i < 5; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for periodic flush
	time.Sleep(30 * time.Millisecond)

	// Get shard info
	shard, _ := client.getOrCreateShard(0)

	// Log state before consumer creation
	shard.mu.RLock()
	t.Logf("Before consumer: CurrentEntryNumber=%d", shard.index.CurrentEntryNumber)
	if len(shard.index.Files) > 0 {
		f := shard.index.Files[0]
		t.Logf("File 0: StartEntry=%d, Entries=%d, EndOffset=%d", f.StartEntry, f.Entries, f.EndOffset)
	}
	shard.mu.RUnlock()

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	// Try to read - this should work
	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Initial read failed: %v", err)
	}
	t.Logf("Read %d messages initially", len(messages))

	// Now write more entries to trigger the race
	for i := 5; i < 10; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for periodic flush
	time.Sleep(30 * time.Millisecond)

	// Log state after more writes
	shard.mu.RLock()
	currentEntry := shard.index.CurrentEntryNumber
	lastWritten := atomic.LoadInt64(&shard.lastWrittenEntryNumber)
	var fileEntries int64
	var fileEndOffset int64
	if len(shard.index.Files) > 0 {
		f := shard.index.Files[0]
		fileEntries = f.Entries
		fileEndOffset = f.EndOffset
	}
	shard.mu.RUnlock()

	t.Logf("After more writes: CurrentEntryNumber=%d, lastWrittenEntryNumber=%d", currentEntry, lastWritten)
	t.Logf("File 0: Entries=%d, EndOffset=%d", fileEntries, fileEndOffset)

	// Now try to read again - this might trigger the race
	messages2, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Logf("Second read failed: %v", err)

		// Check what the reader sees
		if r, ok := consumer.readers.Load(uint32(0)); ok {
			reader := r.(*Reader)
			reader.mappingMu.RLock()
			if mapped, ok := reader.mappedFiles[0]; ok {
				t.Logf("Reader mapped file: lastSize=%d", mapped.lastSize)
				// Try to get the actual data size
				if data := mapped.data.Load(); data != nil {
					t.Logf("Reader mapped data len: %d", len(data.([]byte)))
				}
			}
			reader.mappingMu.RUnlock()
		}

		// Check binary index
		shard.mu.RLock()
		t.Logf("Binary index nodes: %d", len(shard.index.BinaryIndex.Nodes))
		for i, node := range shard.index.BinaryIndex.Nodes {
			t.Logf("  Node %d: EntryNumber=%d, FileIndex=%d, ByteOffset=%d",
				i, node.EntryNumber, node.Position.FileIndex, node.Position.ByteOffset)
		}
		shard.mu.RUnlock()

		t.Fatalf("Failed to read after more writes")
	}

	t.Logf("Successfully read %d more messages", len(messages2))
}
