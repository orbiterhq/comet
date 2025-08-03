package comet

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
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
	streamName := "test:v1:shard:0001"

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
	streamName := "test:v1:shard:0001"

	// Write test data
	for i := 0; i < 50; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
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

// TestTail tests the Tail browse functionality
func TestTail(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	// Use test-specific stream name to ensure isolation between test iterations
	// This base stream name is only used for initial writes
	streamName := fmt.Sprintf("tail:v1:shard:%04d", rand.Intn(9000)+1000)

	// Write initial data
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf(`{"initial": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("TailNewEntries", func(t *testing.T) {
		// Use a unique stream name for this subtest
		subStreamName := fmt.Sprintf("tail:v1:shard:%04d", rand.Intn(8000)+1000)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var received []StreamMessage
		var mu sync.Mutex
		var wg sync.WaitGroup
		wg.Add(1)

		// Start tailing in goroutine
		go func() {
			defer wg.Done()
			err := client.Tail(ctx, subStreamName, func(ctx context.Context, msg StreamMessage) error {
				mu.Lock()
				received = append(received, msg)
				mu.Unlock()
				return nil
			})
			if err != nil && err != context.Canceled {
				t.Errorf("Tail error: %v", err)
			}
		}()

		// Give tail time to start and ensure it's positioned at the current end
		// This is critical for test isolation across platforms
		time.Sleep(300 * time.Millisecond)

		// Write new entries with longer initial delay to ensure tail catches first message
		for i := 0; i < 5; i++ {
			// Extra delay before first message to ensure tail is fully ready
			if i == 0 {
				time.Sleep(100 * time.Millisecond)
			}
			data := []byte(fmt.Sprintf(`{"new": %d}`, i))
			_, err := client.Append(ctx, subStreamName, [][]byte{data})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond) // Space out writes
		}

		// Give time to receive
		time.Sleep(300 * time.Millisecond)

		// Check received messages
		mu.Lock()
		count := len(received)
		mu.Unlock()

		if count != 5 {
			t.Errorf("Expected to receive 5 new messages, got %d", count)
		}

		// Verify messages
		mu.Lock()
		for i, msg := range received {
			expected := fmt.Sprintf(`{"new": %d}`, i)
			if string(msg.Data) != expected {
				t.Errorf("Message %d: expected %s, got %s", i, expected, string(msg.Data))
			}
		}
		mu.Unlock()

		// Cancel and wait for goroutine
		cancel()
		wg.Wait()
	})

	t.Run("TailErrorHandling", func(t *testing.T) {
		// Use a unique stream name for this subtest
		subStreamName := fmt.Sprintf("tail:v1:shard:%04d", rand.Intn(1000)+9000)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		expectedErr := fmt.Errorf("test error")
		var gotErr error
		var wg sync.WaitGroup
		wg.Add(1)

		// Start tail first
		go func() {
			defer wg.Done()
			gotErr = client.Tail(ctx, subStreamName, func(ctx context.Context, msg StreamMessage) error {
				return expectedErr // Return error immediately
			})
		}()

		// Give tail time to start, then write a message to trigger the callback
		// Use longer delay on all platforms for consistency
		time.Sleep(200 * time.Millisecond)
		_, err := client.Append(ctx, subStreamName, [][]byte{[]byte(`{"trigger": true}`)})
		if err != nil {
			t.Fatal(err)
		}

		// Wait for error with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-ctx.Done():
			t.Fatal("Test timed out waiting for tail error")
		}

		if gotErr != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, gotErr)
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
		"test:v1:shard:0001",
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
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Start concurrent writers
	var writeCount atomic.Int64
	stopWriting := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopWriting:
					return
				default:
					count := writeCount.Add(1)
					data := []byte(fmt.Sprintf(`{"writer": %d, "seq": %d}`, writerID, count))
					_, err := client.Append(ctx, streamName, [][]byte{data})
					if err != nil {
						t.Logf("Writer %d error: %v", writerID, err)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let writers run
	time.Sleep(200 * time.Millisecond)

	// Browse while writing
	t.Run("ListRecentDuringWrites", func(t *testing.T) {
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

	totalWrites := writeCount.Load()
	t.Logf("Total writes: %d", totalWrites)

	// Verify we can read all entries
	var scanCount int64
	err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		scanCount++
		return true
	})

	if err != nil {
		t.Fatal(err)
	}

	if scanCount != totalWrites {
		t.Errorf("Scanned %d entries, but wrote %d", scanCount, totalWrites)
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
	streamName := "test:v1:shard:0001"

	// Write test data
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create consumer and read some messages
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 5)
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

	// Get initial consumer offset
	shard, _ := client.getOrCreateShard(1)
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

	// Check consumer offset hasn't changed
	shard.mu.RLock()
	finalOffset := shard.index.ConsumerOffsets["test-group"]
	shard.mu.RUnlock()

	if finalOffset != initialOffset {
		t.Errorf("Consumer offset changed from %d to %d after browse operations", initialOffset, finalOffset)
	}

	// Verify consumer can continue from where it left off
	moreMessages, err := consumer.Read(ctx, []uint32{1}, 5)
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
