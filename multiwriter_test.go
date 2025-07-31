package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMultiWriter_Safety verifies that multiple clients can safely write to the same shard
func TestMultiWriter_Safety(t *testing.T) {
	dir := t.TempDir()

	// NOTE: File locks are per-process on most systems, so creating multiple
	// clients in the same process won't properly test file locking.
	// This test simulates sequential access from different processes.

	// First "process" writes
	client1, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client1: %v", err)
	}
	defer client1.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	const writesPerClient = 100

	// Client 1 writes its entries
	for j := 0; j < writesPerClient; j++ {
		entry := []byte(fmt.Sprintf(`{"client":0,"write":%d,"msg":"multi-writer test"}`, j))
		_, err := client1.Append(ctx, streamName, [][]byte{entry})
		if err != nil {
			t.Fatalf("client 0 write %d failed: %v", j, err)
		}
	}

	// Sync to ensure data is persisted
	if err := client1.Sync(ctx); err != nil {
		t.Fatalf("failed to sync client1: %v", err)
	}

	// Close client1 to simulate process exit
	client1.Close()

	// Second "process" writes
	client2, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client2: %v", err)
	}
	defer client2.Close()

	// Client 2 writes its entries (should continue from where client1 left off)
	for j := 0; j < writesPerClient; j++ {
		entry := []byte(fmt.Sprintf(`{"client":1,"write":%d,"msg":"multi-writer test"}`, j))
		_, err := client2.Append(ctx, streamName, [][]byte{entry})
		if err != nil {
			t.Fatalf("client 1 write %d failed: %v", j, err)
		}
	}

	// Sync to ensure data is persisted
	if err := client2.Sync(ctx); err != nil {
		t.Fatalf("failed to sync client2: %v", err)
	}

	// Log shard state from client2
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	t.Logf("Client2 shard state after writes: CurrentEntryNumber=%d", shard2.index.CurrentEntryNumber)
	shard2.mu.RUnlock()

	// The real test: Create a fresh client to read the actual data from disk
	// This tests data integrity rather than in-memory state consistency
	client3, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create fresh client: %v", err)
	}
	defer client3.Close()

	// Log fresh client's shard state
	shard3, _ := client3.getOrCreateShard(1)
	shard3.mu.RLock()
	t.Logf("Client3 (fresh) shard state: CurrentEntryNumber=%d", shard3.index.CurrentEntryNumber)
	shard3.mu.RUnlock()

	// Create fresh consumer to read all data
	consumer := NewConsumer(client3, ConsumerOptions{
		Group: "safety-test",
	})
	defer consumer.Close()

	// Read all available entries
	messages, err := consumer.Read(ctx, []uint32{1}, 1000) // Read up to 1000
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	expectedTotal := 2 * writesPerClient
	if len(messages) != expectedTotal {
		t.Errorf("consumer read %d messages, expected %d", len(messages), expectedTotal)
	}

	// Verify no data corruption - each message should be valid JSON
	clientCounts := make(map[int]int)
	for _, msg := range messages {
		// Basic validation - should contain client and write fields
		msgStr := string(msg.Data)
		if !strings.Contains(msgStr, `"client":`) || !strings.Contains(msgStr, `"write":`) {
			t.Errorf("corrupted message: %s", msgStr)
		}

		// Count messages per client to verify both clients' data is present
		if strings.Contains(msgStr, `"client":0`) {
			clientCounts[0]++
		} else if strings.Contains(msgStr, `"client":1`) {
			clientCounts[1]++
		}
	}

	// Both clients should have contributed data
	if clientCounts[0] == 0 || clientCounts[1] == 0 {
		t.Errorf("Missing data from one client: client0=%d, client1=%d", clientCounts[0], clientCounts[1])
	}

	t.Logf("Successfully wrote and read %d entries with 2 sequential clients (client0: %d, client1: %d)",
		len(messages), clientCounts[0], clientCounts[1])
}

// TestMultiWriter_DisabledLocking verifies behavior when file locking is disabled
func TestMultiWriter_DisabledLocking(t *testing.T) {
	dir := t.TempDir()

	// Create config with file locking disabled
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Simple write should work fine without locking
	entry := []byte(`{"msg":"no locking test"}`)
	ids, err := client.Append(ctx, streamName, [][]byte{entry})
	if err != nil {
		t.Fatalf("failed to add entry: %v", err)
	}

	if len(ids) != 1 {
		t.Errorf("expected 1 ID, got %d", len(ids))
	}

	// Verify lock file was not created
	shardDir := fmt.Sprintf("%s/shard-0001", dir)
	lockPath := fmt.Sprintf("%s/shard.lock", shardDir)

	if _, err := os.Stat(lockPath); err == nil {
		t.Error("lock file was created when locking is disabled")
	}

	t.Log("File locking disabled mode works correctly")
}

// TestMultiWriter_Configuration tests different locking configurations
func TestMultiWriter_Configuration(t *testing.T) {
	testCases := []struct {
		name          string
		enableLocking bool
		expectLock    bool
	}{
		{"locking_enabled", true, true},
		{"locking_disabled", false, false},
		{"default_config", false, false}, // Default is now single-process mode for performance
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			var client *Client
			var err error

			if tc.name == "default_config" {
				client, err = NewClient(dir)
			} else {
				config := DefaultCometConfig()
				config.Concurrency.EnableMultiProcessMode = tc.enableLocking
				client, err = NewClientWithConfig(dir, config)
			}

			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}
			defer client.Close()

			ctx := context.Background()
			streamName := "events:v1:shard:0001"

			// Write an entry to create the shard
			entry := []byte(`{"msg":"config test"}`)
			_, err = client.Append(ctx, streamName, [][]byte{entry})
			if err != nil {
				t.Fatalf("failed to add entry: %v", err)
			}

			// Check if lock file exists
			shardDir := fmt.Sprintf("%s/shard-0001", dir)
			lockPath := fmt.Sprintf("%s/shard.lock", shardDir)

			_, err = os.Stat(lockPath)
			lockExists := err == nil

			if lockExists != tc.expectLock {
				t.Errorf("expected lock file existence: %v, got: %v", tc.expectLock, lockExists)
			}
		})
	}
}

// TestMmapMultiProcessCoordination tests the mmap-based coordination between multiple client instances
// This simulates multiple processes accessing the same shard data
func TestMmapMultiProcessCoordination(t *testing.T) {
	dir := t.TempDir()

	// Create config with file locking enabled (this enables mmap coordination)
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = true

	streamName := "events:v1:shard:0001"
	ctx := context.Background()

	// Phase 1: Writer process
	t.Run("Writer", func(t *testing.T) {
		client1, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("failed to create writer client: %v", err)
		}
		defer client1.Close()

		// Debug: Check initial shard state
		initialShard, _ := client1.getOrCreateShard(1)
		initialShard.mu.RLock()
		t.Logf("Initial writer shard state: Files=%d, CurrentWriteOffset=%d, CurrentEntryNumber=%d",
			len(initialShard.index.Files), initialShard.index.CurrentWriteOffset, initialShard.index.CurrentEntryNumber)
		if len(initialShard.index.Files) > 0 {
			t.Logf("  Initial File[0]: entries=%d", initialShard.index.Files[0].Entries)
		}
		initialShard.mu.RUnlock()
		
		// Write some initial data
		testData := [][]byte{
			[]byte(`{"id": 1, "message": "from writer"}`),
			[]byte(`{"id": 2, "message": "from writer"}`),
			[]byte(`{"id": 3, "message": "from writer"}`),
		}

		ids, err := client1.Append(ctx, streamName, testData)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}

		if len(ids) != 3 {
			t.Errorf("expected 3 IDs, got %d", len(ids))
		}

		// Force checkpoint to persist to disk and update mmap state
		err = client1.Sync(ctx)
		if err != nil {
			t.Fatalf("failed to sync: %v", err)
		}
		
		// Force index persistence for multi-process visibility
		shard1, _ := client1.getOrCreateShard(1)
		shard1.mu.Lock()
		t.Logf("Writer shard state: CurrentFile=%s, Files=%d, CurrentWriteOffset=%d, CurrentEntryNumber=%d",
			shard1.index.CurrentFile, len(shard1.index.Files), shard1.index.CurrentWriteOffset, shard1.index.CurrentEntryNumber)
		for i, f := range shard1.index.Files {
			t.Logf("  File[%d]: %s (entries=%d, startEntry=%d)", i, f.Path, f.Entries, f.StartEntry)
		}
		
		// DEBUG: Check what the actual data in the file is
		if shard1.mmapWriter != nil {
			coordState := shard1.mmapWriter.CoordinationState()
			t.Logf("MmapWriter state: WriteOffset=%d, FileSize=%d", 
				coordState.WriteOffset.Load(), coordState.FileSize.Load())
		}
		
		shard1.persistIndex()
		shard1.mu.Unlock()

		t.Logf("Writer completed: wrote %d entries", len(ids))
	})

	// Phase 2: Reader process (simulates separate process)
	t.Run("Reader", func(t *testing.T) {
		client2, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("failed to create reader client: %v", err)
		}
		defer client2.Close()

		// Debug: Check what the reader sees
		shard2, _ := client2.getOrCreateShard(1)
		shard2.mu.RLock()
		t.Logf("Reader shard state: CurrentFile=%s, Files=%d, CurrentWriteOffset=%d, CurrentEntryNumber=%d",
			shard2.index.CurrentFile, len(shard2.index.Files), shard2.index.CurrentWriteOffset, shard2.index.CurrentEntryNumber)
		for i, f := range shard2.index.Files {
			t.Logf("  File[%d]: %s (entries=%d, startEntry=%d)", i, f.Path, f.Entries, f.StartEntry)
		}
		
		// Check mmap writer state
		if shard2.mmapWriter != nil {
			t.Logf("Reader mmap state: WriteOffset=%d", shard2.mmapWriter.CoordinationState().WriteOffset.Load())
		}
		shard2.mu.RUnlock()

		consumer := NewConsumer(client2, ConsumerOptions{Group: "test-group"})
		defer consumer.Close()

		// Should be able to read the data written by writer
		messages, err := consumer.Read(ctx, []uint32{1}, 10)
		if err != nil {
			t.Fatalf("failed to read data: %v", err)
		}

		if len(messages) != 3 {
			t.Errorf("expected 3 messages, got %d", len(messages))
		}

		// Verify message contents
		// In multi-process mode, entries start from 1 due to shared sequence counter
		for i, msg := range messages {
			expectedID := MessageID{EntryNumber: int64(i + 1), ShardID: 1}
			if msg.ID != expectedID {
				t.Errorf("message %d: expected ID %v, got %v", i, expectedID, msg.ID)
			}
		}

		t.Logf("Reader completed: read %d messages", len(messages))
	})

	// Phase 3: Concurrent writer and reader
	t.Run("ConcurrentWriteRead", func(t *testing.T) {
		var wg sync.WaitGroup
		var writeCount, readCount int64

		// Writer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()

			client3, err := NewClientWithConfig(dir, config)
			if err != nil {
				t.Errorf("failed to create concurrent writer: %v", err)
				return
			}
			defer client3.Close()

			for i := 0; i < 10; i++ {
				data := [][]byte{[]byte(fmt.Sprintf(`{"concurrent": %d}`, i))}
				_, err := client3.Append(ctx, streamName, data)
				if err != nil {
					t.Errorf("concurrent write failed: %v", err)
					return
				}
				atomic.AddInt64(&writeCount, 1)
				time.Sleep(10 * time.Millisecond)
			}

			// Force sync to ensure mmap state is updated
			client3.Sync(ctx)
		}()

		// Reader goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()

			client4, err := NewClientWithConfig(dir, config)
			if err != nil {
				t.Errorf("failed to create concurrent reader: %v", err)
				return
			}
			defer client4.Close()

			consumer := NewConsumer(client4, ConsumerOptions{Group: "concurrent-group"})
			defer consumer.Close()

			// Keep reading until we see some messages from the concurrent writer
			startTime := time.Now()
			for time.Since(startTime) < 5*time.Second {
				messages, err := consumer.Read(ctx, []uint32{1}, 100)
				if err != nil {
					t.Errorf("concurrent read failed: %v", err)
					return
				}

				newMessages := int64(len(messages))
				if newMessages > 0 {
					atomic.AddInt64(&readCount, newMessages)
					// ACK the messages to advance offset
					for _, msg := range messages {
						consumer.Ack(ctx, msg.ID)
					}
				}

				time.Sleep(20 * time.Millisecond)
			}
		}()

		wg.Wait()

		finalWriteCount := atomic.LoadInt64(&writeCount)
		finalReadCount := atomic.LoadInt64(&readCount)

		t.Logf("Concurrent test: wrote %d entries, read %d entries", finalWriteCount, finalReadCount)

		// We should have read at least some of the concurrent writes
		// (plus the 3 initial entries from the writer test)
		if finalReadCount < 3 {
			t.Errorf("expected to read at least 3 entries, got %d", finalReadCount)
		}
	})
}

// TestMmapStateFile tests the mmap state file creation and management
func TestMmapStateFile(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = true

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Write some data to trigger shard creation
	streamName := "test:v1:shard:0001"
	testData := [][]byte{[]byte(`{"test": "data"}`)}

	_, err = client.Append(context.Background(), streamName, testData)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// Check that index state file was created
	shardDir := filepath.Join(dir, "shard-0001")
	indexStatePath := filepath.Join(shardDir, "index.state")

	if _, err := os.Stat(indexStatePath); os.IsNotExist(err) {
		t.Errorf("index state file was not created at %s", indexStatePath)
	}

	// Check file size (should be exactly 8 bytes)
	stat, err := os.Stat(indexStatePath)
	if err != nil {
		t.Fatalf("failed to stat index state file: %v", err)
	}

	if stat.Size() != 8 {
		t.Errorf("expected index state file size 8, got %d", stat.Size())
	}

	t.Logf("Index state file created successfully: %s (%d bytes)", indexStatePath, stat.Size())
}

// TestMmapTimestampUpdates tests that the mmap timestamp is updated on index changes
func TestMmapTimestampUpdates(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = true

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	streamName := "test:v1:shard:0001"
	ctx := context.Background()

	// Get the shard to access mmap state directly
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatalf("failed to get shard: %v", err)
	}

	// Get initial timestamp
	initialTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
	t.Logf("Initial mmap timestamp: %d", initialTimestamp)

	// Write some data
	testData := [][]byte{[]byte(`{"test": "update1"}`)}
	_, err = client.Append(ctx, streamName, testData)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// Force sync to update mmap state
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Check that timestamp was updated
	updatedTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
	t.Logf("Updated mmap timestamp: %d", updatedTimestamp)

	if updatedTimestamp <= initialTimestamp {
		t.Errorf("mmap timestamp was not updated: initial=%d, updated=%d", initialTimestamp, updatedTimestamp)
	}

	// Write more data and verify timestamp updates again
	time.Sleep(1 * time.Millisecond) // Ensure different timestamp
	testData2 := [][]byte{[]byte(`{"test": "update2"}`)}
	_, err = client.Append(ctx, streamName, testData2)
	if err != nil {
		t.Fatalf("failed to write second data: %v", err)
	}

	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync second write: %v", err)
	}

	finalTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
	t.Logf("Final mmap timestamp: %d", finalTimestamp)

	if finalTimestamp <= updatedTimestamp {
		t.Errorf("mmap timestamp was not updated on second write: updated=%d, final=%d", updatedTimestamp, finalTimestamp)
	}

	t.Logf("Mmap timestamp updates verified successfully")
}
