package comet

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentFlushDebug tests concurrent writes during periodic flush
func TestConcurrentFlushDebug(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 100 // 100ms flush interval

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Get shard
	shard, _ := client.getOrCreateShard(0)

	// Start concurrent writers
	var wg sync.WaitGroup
	var totalWritten int64
	stopWriters := make(chan struct{})

	// Multiple concurrent writers
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for i := 0; ; i++ {
				select {
				case <-stopWriters:
					return
				case <-ticker.C:
					msg := fmt.Sprintf("writer-%d-msg-%d", writerID, i)
					_, err := client.Append(ctx, stream, [][]byte{[]byte(msg)})
					if err != nil {
						t.Logf("Write error: %v", err)
						return
					}
					atomic.AddInt64(&totalWritten, 1)
				}
			}
		}(w)
	}

	// Monitor for 300ms (3 flush cycles)
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	start := time.Now()
	checkTicker := time.NewTicker(50 * time.Millisecond)
	defer checkTicker.Stop()

	var observations []string

	for i := 0; i < 6; i++ { // 6 checks over 300ms
		<-checkTicker.C

		// Check state
		shard.mu.RLock()
		currentEntry := shard.index.CurrentEntryNumber
		var fileEntries int64
		var fileEndOffset int64
		if len(shard.index.Files) > 0 {
			fileEntries = shard.index.Files[0].Entries
			fileEndOffset = shard.index.Files[0].EndOffset
		}
		shard.mu.RUnlock()

		lastWritten := atomic.LoadInt64(&shard.lastWrittenEntryNumber)
		totalSoFar := atomic.LoadInt64(&totalWritten)

		// Try to read
		messages, err := consumer.Read(ctx, []uint32{0}, 1000)

		elapsed := time.Since(start)

		obs := fmt.Sprintf("T+%3dms: written=%d, lastWritten=%d, current=%d, fileEntries=%d, fileOffset=%d, visible=%d",
			elapsed.Milliseconds(), totalSoFar, lastWritten, currentEntry, fileEntries, fileEndOffset, len(messages))

		if err != nil {
			obs += fmt.Sprintf(" ERROR: %v", err)
		}

		observations = append(observations, obs)
		t.Log(obs)
	}

	// Stop writers
	close(stopWriters)
	wg.Wait()

	// Final check after writers stop
	time.Sleep(150 * time.Millisecond) // Wait for final flush

	finalMessages, err := consumer.Read(ctx, []uint32{0}, 1000)
	if err != nil {
		t.Fatalf("Final read failed: %v", err)
	}

	finalTotal := atomic.LoadInt64(&totalWritten)
	t.Logf("Final: wrote %d messages, consumer can see %d", finalTotal, len(finalMessages))

	// Check if we had progressive visibility
	hadProgress := false
	prevVisible := 0
	for _, obs := range observations {
		// Parse visible count from observation
		var visible int
		fmt.Sscanf(obs[strings.LastIndex(obs, "visible=")+8:], "%d", &visible)
		if visible > prevVisible {
			hadProgress = true
			prevVisible = visible
		}
	}

	if !hadProgress {
		t.Error("No progressive visibility during concurrent writes")
	}

	if len(finalMessages) < int(finalTotal) {
		t.Errorf("Lost messages: only %d/%d visible after final flush", len(finalMessages), finalTotal)
	}
}
