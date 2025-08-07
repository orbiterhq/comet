package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestMmapFixDebug tests why the mmap fix isn't working
func TestMmapFixDebug(t *testing.T) {
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

	// Start a writer goroutine
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	var written int64

	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stopWriter:
				return
			default:
				_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
				if err != nil {
					t.Logf("Write error: %v", err)
					return
				}
				atomic.AddInt64(&written, 1)
			}
		}
	}()

	// Wait a bit for some writes
	time.Sleep(50 * time.Millisecond)

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	// Try to read continuously
	var read int64
	for i := 0; i < 10; i++ {
		messages, err := consumer.Read(ctx, []uint32{0}, 100)
		if err != nil {
			t.Logf("Read error at iteration %d after %d messages: %v", i, read, err)

			// Get debug info
			shard, _ := client.getOrCreateShard(0)
			shard.mu.RLock()
			currentEntry := shard.index.CurrentEntryNumber
			var fileEntries int64
			var fileSize int64
			if len(shard.index.Files) > 0 {
				f := shard.index.Files[len(shard.index.Files)-1]
				fileEntries = f.Entries
				fileSize = f.EndOffset
			}
			shard.mu.RUnlock()

			lastWritten := atomic.LoadInt64(&shard.lastWrittenEntryNumber)

			t.Logf("Debug info: CurrentEntryNumber=%d, lastWrittenEntryNumber=%d, fileEntries=%d, fileSize=%d",
				currentEntry, lastWritten, fileEntries, fileSize)

			// Check reader state
			if r, ok := consumer.readers.Load(uint32(0)); ok {
				reader := r.(*Reader)
				reader.mappingMu.RLock()
				if mapped, ok := reader.mappedFiles[0]; ok {
					t.Logf("Reader mapped file: lastSize=%d", mapped.lastSize)
				}
				reader.mappingMu.RUnlock()
			}

			break
		}

		read += int64(len(messages))
		t.Logf("Read %d messages (total: %d)", len(messages), read)

		// Small delay to let writer get ahead
		time.Sleep(10 * time.Millisecond)
	}

	close(stopWriter)
	<-writerDone

	t.Logf("Final: Written=%d, Read=%d", atomic.LoadInt64(&written), read)
}
