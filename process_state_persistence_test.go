package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestProcessStatePersistence tests the complete scenario:
// - Continuous writes causing file rotations
// - Consumer reading continuously
// - Index updates being detected
// - State persistence across process restarts
func TestProcessStatePersistence(t *testing.T) {
	dataDir := t.TempDir()

	// Configure for frequent rotations to test file boundary handling
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10      // 10ms flush
	config.Storage.MaxFileSize = 10 * 1024 // 10KB files to force rotations
	config.Indexing.BoundaryInterval = 10  // Index every 10 entries

	// Phase 1: Start writer process that will run continuously
	t.Log("=== PHASE 1: Starting continuous writer ===")

	writerClient, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create writer client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := "prod:v1:shard:0000"
	shardID := uint32(0)

	// Metrics tracking
	var totalWritten atomic.Int64
	var writeErrors atomic.Int64
	var rotations atomic.Int64

	// Writer goroutine
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(10 * time.Millisecond) // Write every 10ms for high throughput
		defer ticker.Stop()

		lastFileCount := 0

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count := totalWritten.Load()

				// Create a message with some payload to trigger rotations
				msg := fmt.Sprintf("production-message-%06d-payload-%s", count,
					"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
				entries := [][]byte{[]byte(msg)}

				_, err := writerClient.Append(ctx, stream, entries)
				if err != nil {
					writeErrors.Add(1)
					if ctx.Err() == nil {
						t.Logf("Write error at message %d: %v", count, err)
					}
					continue
				}

				totalWritten.Add(1)

				// Check for rotations
				shard := writerClient.getShard(shardID)
				if shard != nil {
					shard.mu.RLock()
					fileCount := len(shard.index.Files)
					currentEntries := shard.index.CurrentEntryNumber
					currentFile := shard.index.CurrentFile
					shard.mu.RUnlock()

					if fileCount > lastFileCount {
						rotations.Add(1)
						t.Logf("FILE ROTATION: Now have %d files after %d messages", fileCount, count)
						lastFileCount = fileCount
					}

					// Log progress every 100 messages
					if count > 0 && count%100 == 0 {
						t.Logf("Writer: %d messages, %d files, index shows %d entries (current file: %s)",
							count, fileCount, currentEntries, currentFile)
					}
				}
			}
		}
	}()

	// Let writer establish some data
	time.Sleep(2 * time.Second)

	// Phase 2: Start consumer in parallel
	t.Log("\n=== PHASE 2: Starting consumer (writer still running) ===")

	consumer := NewConsumer(writerClient, ConsumerOptions{Group: "prod-consumer"})

	var totalRead atomic.Int64
	var readErrors atomic.Int64
	var lastReadEntry int64 = -1

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)

		emptyReads := 0
		maxEmptyReads := 100 // 10 seconds at 100ms interval

		for {
			select {
			case <-ctx.Done():
				return
			default:
				messages, err := consumer.Read(ctx, []uint32{shardID}, 50)
				if err != nil {
					readErrors.Add(1)
					if ctx.Err() == nil {
						t.Logf("Read error after %d messages: %v", totalRead.Load(), err)
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if len(messages) > 0 {
					emptyReads = 0

					// Verify sequential order
					for i, msg := range messages {
						expectedEntry := lastReadEntry + 1
						if msg.ID.EntryNumber != expectedEntry {
							t.Errorf("Entry number gap: expected %d, got %d", expectedEntry, msg.ID.EntryNumber)
						}
						lastReadEntry = msg.ID.EntryNumber

						// Verify content
						expectedContent := fmt.Sprintf("production-message-%06d", msg.ID.EntryNumber)
						if len(string(msg.Data)) < len(expectedContent) ||
							string(msg.Data)[:len(expectedContent)] != expectedContent {
							t.Errorf("Wrong content at entry %d: got %q", msg.ID.EntryNumber, string(msg.Data))
						}

						// Track file boundaries
						if i > 0 && messages[i-1].ID.ShardID == msg.ID.ShardID &&
							messages[i-1].ID.EntryNumber == msg.ID.EntryNumber-1 {
							// Check if we crossed a file boundary
							// This would show up as different positions in the messages
						}
					}

					totalRead.Add(int64(len(messages)))

					// ACK all messages in the batch
					messageIDs := make([]MessageID, len(messages))
					for i, msg := range messages {
						messageIDs[i] = msg.ID
					}
					if err := consumer.Ack(ctx, messageIDs...); err != nil {
						t.Logf("ACK error: %v", err)
					}

					// Log progress
					if totalRead.Load()%100 == 0 {
						lag := totalWritten.Load() - totalRead.Load()
						t.Logf("Consumer: Read %d messages (lag: %d)", totalRead.Load(), lag)
					}
				} else {
					emptyReads++
					if emptyReads > maxEmptyReads {
						written := totalWritten.Load()
						read := totalRead.Load()
						if written > read+50 { // Significant lag
							t.Errorf("Consumer stuck: written=%d, read=%d, lag=%d", written, read, written-read)
							return
						}
					}
				}

				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Phase 3: Simulate process restart after 10 seconds
	time.Sleep(10 * time.Second)

	t.Log("\n=== PHASE 3: Simulating process restart ===")
	pauseWritten := totalWritten.Load()
	pauseRead := totalRead.Load()
	t.Logf("Before restart: Written=%d, Read=%d, Rotations=%d", pauseWritten, pauseRead, rotations.Load())

	// Stop writer goroutine first to avoid data race
	cancel()       // This will stop both goroutines
	<-writerDone   // Wait for writer to exit
	<-consumerDone // Wait for consumer reader to exit

	// Create new context for phase 3 and 4
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()

	// Restart writer with new context
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx2.Done():
				return
			case <-ticker.C:
				count := totalWritten.Load()
				msg := fmt.Sprintf("production-message-%06d-payload-%s", count,
					"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
				entries := [][]byte{[]byte(msg)}

				_, err := writerClient.Append(ctx2, stream, entries)
				if err != nil {
					writeErrors.Add(1)
					if ctx2.Err() == nil {
						t.Logf("Write error at message %d: %v", count, err)
					}
					continue
				}

				totalWritten.Add(1)

				// Log progress every 100 messages
				if count > 0 && count%100 == 0 {
					shard := writerClient.getShard(shardID)
					if shard != nil {
						shard.mu.RLock()
						fileCount := len(shard.index.Files)
						shard.mu.RUnlock()
						t.Logf("Writer (phase 3/4): %d messages, %d files", count, fileCount)
					}
				}
			}
		}
	}()

	// Now close consumer safely
	consumer.Close()

	// Create new client to simulate full process restart
	newClient, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create new client: %v", err)
	}
	defer newClient.Close()

	// Check if shard state is properly loaded
	newShard := newClient.getShard(shardID)
	if newShard == nil {
		// Should be loaded by consumer's getOrCreateShard
		t.Log("Shard not automatically loaded (expected)")
	}

	// Create new consumer - should pick up where it left off
	newConsumer := NewConsumer(newClient, ConsumerOptions{Group: "prod-consumer"})
	defer newConsumer.Close()

	// Continue reading with new consumer
	restartRead := int64(0)
	phase3Ctx, phase3Cancel := context.WithTimeout(ctx2, 10*time.Second)
	defer phase3Cancel()

	for {
		select {
		case <-phase3Ctx.Done():
			goto phase4
		default:
			messages, err := newConsumer.Read(phase3Ctx, []uint32{shardID}, 50)
			if err != nil && phase3Ctx.Err() == nil {
				t.Logf("Post-restart read error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if len(messages) > 0 {
				// Verify we continued from the right place
				if restartRead == 0 {
					firstEntry := messages[0].ID.EntryNumber
					t.Logf("After restart: First message entry number = %d (last read was around %d)",
						firstEntry, pauseRead)

					// Should be close to where we left off
					if firstEntry < pauseRead-10 {
						t.Errorf("Consumer went backwards after restart: was at %d, now at %d",
							pauseRead, firstEntry)
					}
				}

				restartRead += int64(len(messages))
				totalRead.Add(int64(len(messages)))
			}

			time.Sleep(100 * time.Millisecond)
		}
	}

phase4:
	// Phase 4: Final verification
	t.Log("\n=== PHASE 4: Final verification ===")

	// Stop writer
	cancel2()
	// Note: writer goroutine from phase 3 will exit on context cancellation
	writerClient.Close()

	// Do final reads to catch up
	finalCtx, finalCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finalCancel()

	for {
		select {
		case <-finalCtx.Done():
			goto done
		default:
			messages, err := newConsumer.Read(finalCtx, []uint32{shardID}, 100)
			if err != nil {
				break
			}
			if len(messages) == 0 {
				break
			}
			totalRead.Add(int64(len(messages)))
		}
	}

done:
	// Final statistics
	finalWritten := totalWritten.Load()
	finalRead := totalRead.Load()
	finalRotations := rotations.Load()
	finalWriteErrors := writeErrors.Load()
	finalReadErrors := readErrors.Load()

	t.Logf("\n=== FINAL STATISTICS ===")
	t.Logf("Messages written: %d", finalWritten)
	t.Logf("Messages read: %d", finalRead)
	t.Logf("File rotations: %d", finalRotations)
	t.Logf("Write errors: %d", finalWriteErrors)
	t.Logf("Read errors: %d", finalReadErrors)
	t.Logf("Read after restart: %d", restartRead)

	// Verify final shard state
	finalShard, _ := newClient.getOrCreateShard(shardID)
	if finalShard != nil {
		finalShard.mu.RLock()
		indexEntries := finalShard.index.CurrentEntryNumber
		fileCount := len(finalShard.index.Files)
		consumerOffset := finalShard.index.ConsumerOffsets["prod-consumer"]

		// Check index stats
		var totalFileEntries int64
		for _, f := range finalShard.index.Files {
			totalFileEntries += f.Entries
		}
		finalShard.mu.RUnlock()

		t.Logf("\nIndex state: CurrentEntry=%d, Files=%d, TotalFileEntries=%d, ConsumerOffset=%d",
			indexEntries, fileCount, totalFileEntries, consumerOffset)

		// Verify index consistency
		if indexEntries != totalFileEntries {
			t.Errorf("Index inconsistent: CurrentEntryNumber=%d but sum of file entries=%d",
				indexEntries, totalFileEntries)
		}
	}

	// Key assertions
	if finalRotations < 2 {
		t.Errorf("Expected multiple file rotations but only got %d", finalRotations)
	}

	if finalRead < finalWritten-50 { // Allow some lag
		t.Errorf("Consumer fell too far behind: written=%d, read=%d, diff=%d",
			finalWritten, finalRead, finalWritten-finalRead)
	}

	if restartRead < 100 {
		t.Errorf("Consumer didn't read enough after restart: %d messages", restartRead)
	}

	if finalWritten < 1000 { // Should write many messages in 30 seconds
		t.Errorf("Not enough messages written for a comprehensive test: %d", finalWritten)
	}
}
