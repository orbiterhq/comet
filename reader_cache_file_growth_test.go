package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestReaderCacheFileGrowth tests that reader cache properly handles file growth
func TestReaderCacheFileGrowth(t *testing.T) {
	dataDir := t.TempDir()

	// Create client with a short flush interval
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 // 50ms flush interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create a consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-reader-cache"})
	defer consumer.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write some initial messages
	t.Log("Writing initial messages...")
	initialMessages := [][]byte{
		[]byte("initial-1"),
		[]byte("initial-2"),
		[]byte("initial-3"),
	}

	ids, err := client.Append(ctx, stream, initialMessages)
	if err != nil {
		t.Fatalf("Failed to append initial messages: %v", err)
	}
	t.Logf("Appended %d initial messages with IDs: %v", len(ids), ids)

	// Wait for flush interval plus buffer
	time.Sleep(time.Duration(config.Storage.FlushInterval+50) * time.Millisecond)

	// Read initial messages to establish position
	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Failed to read initial messages: %v", err)
	}
	t.Logf("Read %d initial messages", len(messages))

	// ACK them
	messageIDs := make([]MessageID, len(messages))
	for i, msg := range messages {
		messageIDs[i] = msg.ID
	}
	if err := consumer.Ack(ctx, messageIDs...); err != nil {
		t.Fatalf("Failed to ACK: %v", err)
	}

	// Now start concurrent writer and reader
	writerDone := make(chan struct{})
	readerDone := make(chan struct{})
	messagesSent := 0
	messagesRead := 0
	var mu sync.Mutex

	// Writer goroutine
	go func() {
		defer close(writerDone)
		for i := 0; i < 20; i++ {
			msg := fmt.Sprintf("concurrent-message-%d", i)
			entries := [][]byte{[]byte(msg)}

			if _, err := client.Append(ctx, stream, entries); err != nil {
				t.Logf("Write error: %v", err)
				return
			}

			mu.Lock()
			messagesSent++
			mu.Unlock()

			t.Logf("Writer: sent message %d", i)
			time.Sleep(100 * time.Millisecond)
		}
		t.Log("Writer: finished sending messages")
	}()

	// Reader goroutine - should keep receiving messages
	go func() {
		defer close(readerDone)
		noDataCount := 0
		maxNoDataAttempts := 50 // Allow up to 5 seconds of no data

		for {
			messages, err := consumer.Read(ctx, []uint32{0}, 10)
			if err != nil {
				t.Logf("Reader error: %v", err)
				return
			}

			// Debug: check shard state and consumer offset
			shard := client.getShard(0)
			if shard != nil {
				shard.mu.RLock()
				currentEntries := shard.index.CurrentEntryNumber
				fileCount := len(shard.index.Files)
				consumerOffset := shard.index.ConsumerOffsets[consumer.group]
				shard.mu.RUnlock()

				// Also check memory offset
				consumer.memOffsetsMu.RLock()
				memOffset, hasMemOffset := consumer.memOffsets[0]
				consumer.memOffsetsMu.RUnlock()

				if noDataCount > 0 && noDataCount%10 == 0 {
					t.Logf("Reader debug: shard has %d entries in %d files, consumer offset: %d (mem: %v/%d)",
						currentEntries, fileCount, consumerOffset, hasMemOffset, memOffset)
				}
			}

			if len(messages) > 0 {
				mu.Lock()
				messagesRead += len(messages)
				mu.Unlock()

				t.Logf("Reader: received %d messages (total: %d)", len(messages), messagesRead)

				// ACK the messages
				messageIDs := make([]MessageID, len(messages))
				for i, msg := range messages {
					messageIDs[i] = msg.ID
				}
				if err := consumer.Ack(ctx, messageIDs...); err != nil {
					t.Logf("ACK error: %v", err)
				}

				// Reset no data counter
				noDataCount = 0
			} else {
				noDataCount++
				if noDataCount > maxNoDataAttempts {
					t.Log("Reader: no new data for too long, exiting")
					return
				}
			}

			// Check if writer is done and we've read everything
			select {
			case <-writerDone:
				mu.Lock()
				sent := messagesSent
				read := messagesRead
				mu.Unlock()

				if read >= sent {
					t.Logf("Reader: writer done and caught up (sent: %d, read: %d)", sent, read)
					return
				}
			default:
			}

			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Wait for both to complete
	<-writerDone
	<-readerDone

	// Final check
	mu.Lock()
	finalSent := messagesSent
	finalRead := messagesRead
	mu.Unlock()

	t.Logf("Final: Sent %d messages, Read %d messages", finalSent, finalRead)

	if finalRead < finalSent {
		t.Errorf("Reader didn't catch up: sent %d but only read %d messages", finalSent, finalRead)
	}
}

// TestReaderIndexUpdateDetection specifically tests if readers detect index updates
func TestReaderIndexUpdateDetection(t *testing.T) {
	dataDir := t.TempDir()

	// Create client with short flush interval
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 // 50ms flush interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"
	shardID := uint32(0)

	// Get the shard to access its index directly
	shard, _ := client.getOrCreateShard(shardID)

	// Create a reader directly on the shard
	reader, err := NewReader(shardID, shard.index)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Set the shard's state for the reader
	reader.SetState(shard.state)

	// Write initial message
	entries := [][]byte{[]byte("test-message-0")}
	if _, err := client.Append(ctx, stream, entries); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	// Wait for flush and then sync to ensure index is persisted
	time.Sleep(time.Duration(config.Storage.FlushInterval+50) * time.Millisecond)

	// Force sync to persist the index and update LastIndexUpdate timestamp
	if err := client.Sync(ctx); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Read the first message
	data, err := reader.ReadEntryByNumber(0)
	if err != nil {
		t.Fatalf("Failed to read entry 0: %v", err)
	}
	t.Logf("Read entry 0: %s", string(data))

	// Now write more messages in a loop and verify reader can see them
	for i := 1; i <= 10; i++ {
		// Write a new message
		msg := fmt.Sprintf("test-message-%d", i)
		entries := [][]byte{[]byte(msg)}
		if _, err := client.Append(ctx, stream, entries); err != nil {
			t.Fatalf("Failed to append message %d: %v", i, err)
		}

		// Sync to ensure index is persisted and LastIndexUpdate is updated
		if err := client.Sync(ctx); err != nil {
			t.Fatalf("Failed to sync message %d: %v", i, err)
		}

		t.Logf("Wrote message %d", i)

		// Try to read it immediately
		attempts := 0
		maxAttempts := 20 // 2 seconds max
		var readErr error
		var readData []byte

		for attempts < maxAttempts {
			readData, readErr = reader.ReadEntryByNumber(int64(i))
			if readErr == nil {
				break
			}
			attempts++
			time.Sleep(100 * time.Millisecond)
		}

		if readErr != nil {
			t.Errorf("Failed to read entry %d after %d attempts: %v", i, attempts, readErr)

			// Debug: Check what the reader thinks
			shard.mu.RLock()
			currentEntries := shard.index.CurrentEntryNumber
			fileCount := len(shard.index.Files)
			shard.mu.RUnlock()

			t.Logf("Debug: Index shows %d entries in %d files", currentEntries, fileCount)

			// Check if reader's file info is stale
			reader.mappingMu.RLock()
			readerFileCount := len(reader.fileInfos)
			reader.mappingMu.RUnlock()

			t.Logf("Debug: Reader has %d files cached", readerFileCount)
		} else {
			t.Logf("Successfully read entry %d after %d attempts: %s", i, attempts, string(readData))
		}
	}
}
