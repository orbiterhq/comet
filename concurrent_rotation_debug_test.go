package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentRotationDebug reproduces the exact scenario where reader gets stuck
func TestConcurrentRotationDebug(t *testing.T) {
	SetDebug(true) // Enable debug logging
	defer SetDebug(false)
	dataDir := t.TempDir()

	// Use same config as failing test
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10      // 10ms flush
	config.Storage.MaxFileSize = 10 * 1024 // 10KB files to force rotations

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stream := "concurrent-debug:v1:shard:0000"
	shardID := uint32(0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Start consumer first
	consumer := NewConsumer(client, ConsumerOptions{Group: "concurrent-test"})
	defer consumer.Close()

	// Counters
	var totalWritten int64
	var totalRead int64
	var rotations int64

	t.Log("🚀 === Starting Concurrent Writer ===")

	// Start continuous writer (like the failing test)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(20 * time.Millisecond) // Write every 20ms
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count := atomic.LoadInt64(&totalWritten)

				// Create message with padding to trigger rotations
				msg := fmt.Sprintf("concurrent-message-%06d-padding-%s", count,
					"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
				entries := [][]byte{[]byte(msg)}

				_, err := client.Append(ctx, stream, entries)
				if err != nil {
					if ctx.Err() == nil {
						t.Logf("❌ Write error at message %d: %v", count, err)
					}
					return
				}

				atomic.AddInt64(&totalWritten, 1)

				// Check for rotations
				if count > 0 && count%50 == 0 {
					shard, err := client.getOrCreateShard(shardID)
					if err == nil {
						shard.mu.RLock()
						fileCount := len(shard.index.Files)
						currentEntries := shard.index.CurrentEntryNumber
						shard.mu.RUnlock()

						if fileCount > int(atomic.LoadInt64(&rotations))+1 {
							atomic.StoreInt64(&rotations, int64(fileCount-1))
							t.Logf("🔄 FILE ROTATION: Now have %d files after %d messages (index: %d entries)",
								fileCount, count, currentEntries)
						}
					}
				}
			}
		}
	}()

	t.Log("📖 === Starting Consumer Reader ===")

	// Consumer reader loop
	readerDone := make(chan struct{})
	var lastReadEntry int64 = -1

	go func() {
		defer close(readerDone)

		emptyReads := 0
		maxEmptyReads := 50 // 5 seconds at 100ms interval

		for {
			select {
			case <-ctx.Done():
				return
			default:
				messages, err := consumer.Read(ctx, []uint32{shardID}, 25)
				if err != nil {
					if ctx.Err() == nil {
						t.Logf("❌ Read error after %d messages: %v", atomic.LoadInt64(&totalRead), err)
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if len(messages) > 0 {
					emptyReads = 0

					// Check for gaps in entry sequence
					for _, msg := range messages {
						expectedEntry := atomic.LoadInt64(&lastReadEntry) + 1
						if msg.ID.EntryNumber != expectedEntry {
							t.Logf("⚠️  ENTRY GAP: expected %d, got %d", expectedEntry, msg.ID.EntryNumber)

							// Try to understand why - check current shard state
							if shard, err := client.getOrCreateShard(shardID); err == nil {
								shard.mu.RLock()
								fileCount := len(shard.index.Files)
								indexEntries := shard.index.CurrentEntryNumber
								shard.mu.RUnlock()

								t.Logf("🔍 Shard state during gap: files=%d, indexEntries=%d, written=%d",
									fileCount, indexEntries, atomic.LoadInt64(&totalWritten))

								// Check reader cache state
								if readerInterface, exists := consumer.readers.Load(shardID); exists {
									reader := readerInterface.(*Reader)
									reader.mappingMu.RLock()
									cachedFiles := len(reader.fileInfos)
									lastKnown := atomic.LoadInt64(&reader.lastKnownIndexUpdate)
									reader.mappingMu.RUnlock()

									var currentUpdate int64
									if reader.state != nil {
										currentUpdate = reader.state.GetLastIndexUpdate()
									}

									t.Logf("🔍 Reader cache during gap: cachedFiles=%d, lastKnown=%d, current=%d, stale=%v",
										cachedFiles, lastKnown, currentUpdate, lastKnown < currentUpdate)
								}
							}
						}
						atomic.StoreInt64(&lastReadEntry, msg.ID.EntryNumber)
					}

					atomic.AddInt64(&totalRead, int64(len(messages)))

					// ACK each batch to ensure consumer offset advances
					messageIDs := make([]MessageID, len(messages))
					for i, msg := range messages {
						messageIDs[i] = msg.ID
					}
					if err := consumer.Ack(ctx, messageIDs...); err != nil {
						t.Logf("❌ ACK error: %v", err)
					}

					// Log progress
					if atomic.LoadInt64(&totalRead)%100 == 0 {
						written := atomic.LoadInt64(&totalWritten)
						read := atomic.LoadInt64(&totalRead)
						lag := written - read
						t.Logf("📊 Progress: written=%d, read=%d, lag=%d", written, read, lag)
					}
				} else {
					emptyReads++
					if emptyReads > maxEmptyReads {
						written := atomic.LoadInt64(&totalWritten)
						read := atomic.LoadInt64(&totalRead)
						if written > read+20 { // Significant lag
							t.Logf("❌ CONSUMER STUCK: written=%d, read=%d, lag=%d, lastEntry=%d",
								written, read, written-read, atomic.LoadInt64(&lastReadEntry))
							return
						}
					}
				}

				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Let it run
	time.Sleep(10 * time.Second)

	// Stop writer
	cancel()
	<-writerDone

	// Give reader time to catch up
	time.Sleep(2 * time.Second)

	// Final stats
	finalWritten := atomic.LoadInt64(&totalWritten)
	finalRead := atomic.LoadInt64(&totalRead)
	finalRotations := atomic.LoadInt64(&rotations)

	t.Logf("🏁 FINAL RESULTS:")
	t.Logf("   Written: %d", finalWritten)
	t.Logf("   Read: %d", finalRead)
	t.Logf("   Rotations: %d", finalRotations)
	t.Logf("   Last read entry: %d", atomic.LoadInt64(&lastReadEntry))

	if finalRead < finalWritten-10 {
		t.Errorf("❌ Consumer fell behind: written=%d, read=%d", finalWritten, finalRead)
	}

	if finalRotations < 2 {
		t.Errorf("❌ Not enough rotations for meaningful test: %d", finalRotations)
	}
}
