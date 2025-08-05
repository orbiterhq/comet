package comet

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestListRecent tests the ListRecent browse functionality
func TestListRecent(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Test empty stream
	t.Run("EmptyStream", func(t *testing.T) {
		messages, err := client.ListRecent(ctx, streamName, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) != 0 {
			t.Errorf("Expected 0 messages from empty stream, got %d", len(messages))
		}
	})

	// Write some test data
	testData := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		testData[i] = []byte(fmt.Sprintf(`{"id": %d, "data": "test message %d"}`, i, i))
	}

	_, err = client.Append(ctx, streamName, testData)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure data is flushed before reading
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Test listing recent messages
	t.Run("ListRecent10", func(t *testing.T) {
		messages, err := client.ListRecent(ctx, streamName, 10)
		if err != nil {
			t.Fatal(err)
		}

		if len(messages) != 10 {
			t.Errorf("Expected 10 messages, got %d", len(messages))
		}

		// Verify we got the last 10 messages (90-99)
		for i, msg := range messages {
			expectedID := 90 + i
			expected := fmt.Sprintf(`{"id": %d, "data": "test message %d"}`, expectedID, expectedID)
			if string(msg.Data) != expected {
				t.Errorf("Message %d: expected %s, got %s", i, expected, string(msg.Data))
			}
			if msg.ID.EntryNumber != int64(expectedID) {
				t.Errorf("Message %d: expected entry number %d, got %d", i, expectedID, msg.ID.EntryNumber)
			}
		}
	})

	// Test listing more than available
	t.Run("ListMoreThanAvailable", func(t *testing.T) {
		messages, err := client.ListRecent(ctx, streamName, 200)
		if err != nil {
			t.Fatal(err)
		}

		if len(messages) != 100 {
			t.Errorf("Expected 100 messages (all available), got %d", len(messages))
		}

		// Verify first message
		if string(messages[0].Data) != `{"id": 0, "data": "test message 0"}` {
			t.Errorf("First message incorrect: %s", string(messages[0].Data))
		}
	})

	// Test with context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		messages, err := client.ListRecent(ctx, streamName, 50)
		if err == nil || err != context.Canceled {
			t.Errorf("Expected context.Canceled error, got %v", err)
		}
		// Should still return partial results
		t.Logf("Got %d messages before cancellation", len(messages))
	})

	// Test zero limit
	t.Run("ZeroLimit", func(t *testing.T) {
		messages, err := client.ListRecent(ctx, streamName, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) != 0 {
			t.Errorf("Expected 0 messages with zero limit, got %d", len(messages))
		}
	})
}

// TestScanAll tests the ScanAll browse functionality
func TestScanAll(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Write test data
	for i := 0; i < 50; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ensure data is flushed before reading
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Test scanning all entries
	t.Run("ScanAllEntries", func(t *testing.T) {
		var count int
		var lastID int64 = -1

		err := client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
			count++

			// Verify sequential order
			if msg.ID.EntryNumber != lastID+1 {
				t.Errorf("Expected entry number %d, got %d", lastID+1, msg.ID.EntryNumber)
			}
			lastID = msg.ID.EntryNumber

			// Verify data
			expected := fmt.Sprintf(`{"id": %d}`, msg.ID.EntryNumber)
			if string(msg.Data) != expected {
				t.Errorf("Expected data %s, got %s", expected, string(msg.Data))
			}

			return true // Continue scanning
		})

		if err != nil {
			t.Fatal(err)
		}

		if count != 50 {
			t.Errorf("Expected to scan 50 entries, got %d", count)
		}
	})

	// Test early termination
	t.Run("EarlyTermination", func(t *testing.T) {
		var count int

		err := client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
			count++
			return count < 10 // Stop after 10
		})

		if err != nil {
			t.Fatal(err)
		}

		if count != 10 {
			t.Errorf("Expected to scan 10 entries before stopping, got %d", count)
		}
	})

	// Test context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var count int

		// Cancel after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		err := client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
			count++
			time.Sleep(5 * time.Millisecond) // Slow down to ensure cancellation
			return true
		})

		if err != context.Canceled {
			t.Errorf("Expected context.Canceled error, got %v", err)
		}

		t.Logf("Scanned %d entries before cancellation", count)
	})

	// Test empty stream - use a different shard to ensure it's truly empty
	t.Run("EmptyStream", func(t *testing.T) {
		emptyStream := "empty:v1:shard:0999"
		var called bool

		err := client.ScanAll(ctx, emptyStream, func(ctx context.Context, msg StreamMessage) bool {
			called = true
			return true
		})

		if err != nil {
			t.Fatal(err)
		}

		if called {
			t.Error("Callback should not be called for empty stream")
		}
	})
}

// TestBrowseMultipleShards tests browse operations across multiple shards
func TestBrowseMultipleShards(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write to multiple shards
	shards := []string{
		"test:v1:shard:0000",
		"test:v1:shard:0002",
		"test:v1:shard:0003",
	}

	for _, shard := range shards {
		for i := 0; i < 10; i++ {
			data := []byte(fmt.Sprintf(`{"shard": "%s", "id": %d}`, shard, i))
			_, err := client.Append(ctx, shard, [][]byte{data})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// Ensure data is flushed before reading
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Test ListRecent on each shard independently
	for _, shard := range shards {
		t.Run(fmt.Sprintf("ListRecent_%s", shard), func(t *testing.T) {
			messages, err := client.ListRecent(ctx, shard, 5)
			if err != nil {
				t.Fatal(err)
			}

			if len(messages) != 5 {
				t.Errorf("Expected 5 messages, got %d", len(messages))
			}

			// Verify shard isolation
			for _, msg := range messages {
				if msg.Stream != shard {
					t.Errorf("Expected stream %s, got %s", shard, msg.Stream)
				}
			}
		})
	}
}

// TestBrowseConcurrentAccess tests browse operations with concurrent writes
func TestBrowseConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.ProcessCount = 0 // Ensure single-process mode
	// Use frequent checkpoints to ensure data is persisted
	config.Storage.CheckpointTime = 10
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Start concurrent writers
	var writeCount atomic.Int64
	stopWriting := make(chan struct{})
	var wg sync.WaitGroup

	// Track writes per writer for debugging
	var writerCounts [3]atomic.Int64

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			localCount := 0
			stopping := false
			for !stopping {
				select {
				case <-stopWriting:
					stopping = true
					// Don't return immediately - complete any in-progress work
				default:
					// Continue with write
				}

				// Only write if we haven't been asked to stop yet
				if !stopping {
					data := []byte(fmt.Sprintf(`{"writer": %d, "count": %d}`, writerID, localCount))
					ids, err := client.Append(ctx, streamName, [][]byte{data})
					if err != nil {
						t.Logf("Writer %d error on count %d: %v", writerID, localCount, err)
					} else {
						oldTotal := writeCount.Add(1)
						localCount++
						writerCounts[writerID].Add(1)
						if len(ids) != 1 {
							t.Logf("Writer %d: expected 1 ID, got %d", writerID, len(ids))
						}
						// Log successful writes for entry #6
						if localCount == 7 {
							t.Logf("Writer %d successfully wrote entry #6 (total writes now: %d)", writerID, oldTotal+1)
						}
					}
					time.Sleep(10 * time.Millisecond)
				}
			}

			// Ensure our writes are flushed before exiting
			client.Sync(ctx)
			t.Logf("Writer %d stopping, wrote %d entries", writerID, localCount)
		}(i)
	}

	// Give writers time to write some data
	time.Sleep(75 * time.Millisecond) // Enough time for 7 writes (0-6) with 10ms between each

	// Browse while writing
	t.Run("ListRecentDuringWrites", func(t *testing.T) {
		// Sync to ensure all writes are visible
		if err := client.Sync(ctx); err != nil {
			t.Fatal(err)
		}

		messages, err := client.ListRecent(ctx, streamName, 10)
		if err != nil {
			t.Fatal(err)
		}

		if len(messages) < 5 {
			t.Errorf("Expected at least 5 messages, got %d", len(messages))
		}

		// Verify data integrity
		for _, msg := range messages {
			var data map[string]int
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				t.Errorf("Failed to unmarshal message: %v", err)
			}
		}
	})

	// Stop writers
	close(stopWriting)
	wg.Wait()

	// Get the final write count immediately after writers stop
	totalWrites := writeCount.Load()

	// Log per-writer counts
	t.Logf("Writer 0: %d, Writer 1: %d, Writer 2: %d",
		writerCounts[0].Load(), writerCounts[1].Load(), writerCounts[2].Load())

	// Wait a bit for any periodic flush to complete
	time.Sleep(20 * time.Millisecond)

	// Sync to ensure all writes are visible
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("Total writes: %d", totalWrites)

	// Force a second sync to be absolutely sure
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Debug: check how many entries are in the shard
	length, err := client.Len(ctx, streamName)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Shard length: %d", length)

	// Also check via direct shard access
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}
	shard.mu.RLock()
	var directCount int64
	for _, f := range shard.index.Files {
		directCount += f.Entries
	}
	t.Logf("Direct shard count: %d entries in %d files", directCount, len(shard.index.Files))
	shard.mu.RUnlock()

	// Verify we can read all entries
	var scanCount int64
	seenWriters := make(map[int]map[int]bool)
	for i := 0; i < 3; i++ {
		seenWriters[i] = make(map[int]bool)
	}

	err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		scanCount++
		// Try to parse the message to see which writer/count it is
		var data map[string]int
		if err := json.Unmarshal(msg.Data, &data); err == nil {
			if writer, ok := data["writer"]; ok {
				if count, ok := data["count"]; ok {
					seenWriters[writer][count] = true
				}
			}
		}
		return true
	})

	if err != nil {
		t.Fatal(err)
	}

	if scanCount != totalWrites {
		t.Errorf("Scanned %d entries, but wrote %d", scanCount, totalWrites)
		// Try to understand the discrepancy
		if scanCount < totalWrites {
			t.Logf("Missing %d entries", totalWrites-scanCount)
			// Check which specific entries are missing
			for writer := 0; writer < 3; writer++ {
				expectedCount := int(writerCounts[writer].Load())
				for count := 0; count < expectedCount; count++ {
					if !seenWriters[writer][count] {
						t.Logf("  Missing: writer=%d, count=%d", writer, count)
					}
				}
			}
		}
	}
}

// TestBrowseDoesNotAffectConsumers tests that browse operations don't interfere with consumers
func TestBrowseDoesNotAffectConsumers(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Write test data
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Sync to ensure data is visible
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Create consumer and read some messages
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{0}, 5)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages) != 5 {
		t.Fatalf("Expected 5 messages, got %d", len(messages))
	}

	// ACK first 3 messages
	for i := 0; i < 3; i++ {
		err = consumer.Ack(ctx, messages[i].ID)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force consumer to flush its in-memory offsets before checking
	if err := consumer.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Get initial consumer offset
	shard, _ := client.getOrCreateShard(0)
	shard.mu.RLock()
	initialOffset := shard.index.ConsumerOffsets["test-group"]
	shard.mu.RUnlock()

	t.Logf("Initial consumer offset: %d", initialOffset)

	// Perform browse operations
	browseMessages, err := client.ListRecent(ctx, streamName, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Browsed %d recent messages", len(browseMessages))

	var scanCount int
	err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		scanCount++
		return scanCount < 15
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Scanned %d messages", scanCount)

	// Force consumer to flush its in-memory offsets before checking
	if err := consumer.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Check consumer offset hasn't changed
	shard.mu.RLock()
	finalOffset := shard.index.ConsumerOffsets["test-group"]
	shard.mu.RUnlock()

	if finalOffset != initialOffset {
		t.Errorf("Consumer offset changed from %d to %d after browse operations", initialOffset, finalOffset)
	}

	// Verify consumer can continue from where it left off
	moreMessages, err := consumer.Read(ctx, []uint32{0}, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Should get messages starting from entry 3 (first unACKed message)
	// We ACKed entries 0,1,2 so offset is now 3. Next read should start from entry 3.
	if len(moreMessages) != 5 {
		t.Errorf("Expected 5 more messages, got %d", len(moreMessages))
	}

	expectedStart := int64(3)
	if moreMessages[0].ID.EntryNumber != expectedStart {
		t.Errorf("Expected first message to be entry %d, got %d", expectedStart, moreMessages[0].ID.EntryNumber)
	}
}
