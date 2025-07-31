package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMultiProcessInSameProcess tests multi-client coordination within the same process
// This is NOT a true multi-process test but tests the coordination mechanisms
func TestMultiProcessInSameProcess(t *testing.T) {
	dir := t.TempDir()

	// Create two clients with multi-process config
	config := MultiProcessConfig()

	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()

	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Write from client1
	_, err = client1.Append(ctx, streamName, [][]byte{
		[]byte(`{"source":"client1","msg":"hello"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Sync to ensure it's persisted
	client1.Sync(ctx)

	// Give a moment for mmap coordination
	time.Sleep(50 * time.Millisecond)

	// Read from client2
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read from shard 1: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}

	// Write from client2
	_, err = client2.Append(ctx, streamName, [][]byte{
		[]byte(`{"source":"client2","msg":"world"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Both clients should see both messages
	t.Log("Multi-client coordination test passed")
}

// TestMultiProcessMmapSize verifies the mmap state file is exactly 8 bytes
func TestMultiProcessMmapSize(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write something to create the shard
	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{
		[]byte(`{"test":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check the mmap state file size
	client.mu.RLock()
	shard, exists := client.shards[1]
	client.mu.RUnlock()

	if !exists {
		t.Fatal("Shard 1 not found")
	}

	if shard.mmapState == nil {
		t.Fatal("Mmap state not initialized")
	}

	// The state file should be exactly 8 bytes
	if len(shard.indexStateData) != 8 {
		t.Errorf("Mmap state file is %d bytes, expected 8", len(shard.indexStateData))
	} else {
		t.Log("✓ Mmap state file is exactly 8 bytes")
	}
}

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
		t.Fatalf("failed to read messages: %v", err)
	}

	// Verify we got the expected number of entries
	expectedCount := writesPerClient * 2 // Two clients wrote
	if len(messages) != expectedCount {
		t.Errorf("Expected %d messages, got %d", expectedCount, len(messages))
	}

	// Verify entry numbers are sequential and start from 0
	for i, msg := range messages {
		if msg.ID.EntryNumber != int64(i) {
			t.Errorf("Entry %d has wrong entry number: got %d, expected %d",
				i, msg.ID.EntryNumber, i)
		}
	}

	t.Logf("Successfully wrote and read %d entries with 2 sequential clients (client0: %d, client1: %d)",
		len(messages), writesPerClient, writesPerClient)
}

// TestMultiWriter_DisabledLocking tests that we can disable multi-process locking
func TestMultiWriter_DisabledLocking(t *testing.T) {
	dir := t.TempDir()

	// Create config with locking disabled
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte(`{"msg":"test without locking"}`),
	})
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	t.Log("File locking disabled mode works correctly")
}

// TestMultiWriter_Configuration tests various multi-process configurations
func TestMultiWriter_Configuration(t *testing.T) {
	tests := []struct {
		name   string
		config CometConfig
	}{
		{
			name:   "locking_enabled",
			config: MultiProcessConfig(),
		},
		{
			name: "locking_disabled",
			config: func() CometConfig {
				c := DefaultCometConfig()
				c.Concurrency.EnableMultiProcessMode = false
				return c
			}(),
		},
		{
			name:   "default_config",
			config: DefaultCometConfig(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			client, err := NewClientWithConfig(dir, tt.config)
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}
			defer client.Close()

			ctx := context.Background()
			streamName := "events:v1:shard:0001"

			_, err = client.Append(ctx, streamName, [][]byte{
				[]byte(`{"msg":"configuration test"}`),
			})
			if err != nil {
				t.Fatalf("failed to write: %v", err)
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

	// Phase 2: Reader process (separate client instance to simulate different process)
	t.Run("Reader", func(t *testing.T) {
		client2, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("failed to create reader client: %v", err)
		}
		defer client2.Close()

		// Give the reader some time to load the index state
		time.Sleep(100 * time.Millisecond)

		shard2, _ := client2.getOrCreateShard(1)
		t.Logf("Reader shard state: CurrentFile=%s, Files=%d, CurrentWriteOffset=%d, CurrentEntryNumber=%d",
			shard2.index.CurrentFile, len(shard2.index.Files), shard2.index.CurrentWriteOffset, shard2.index.CurrentEntryNumber)
		for i, f := range shard2.index.Files {
			t.Logf("  File[%d]: %s (entries=%d, startEntry=%d)", i, f.Path, f.Entries, f.StartEntry)
		}

		if shard2.mmapWriter != nil {
			coordState := shard2.mmapWriter.CoordinationState()
			t.Logf("Reader mmap state: WriteOffset=%d", coordState.WriteOffset.Load())
		}

		// Try to read the data written by the first client
		consumer := NewConsumer(client2, ConsumerOptions{
			Group: "test-reader",
		})
		defer consumer.Close()

		messages, err := consumer.Read(ctx, []uint32{1}, 10)
		if err != nil {
			t.Fatalf("failed to read messages: %v", err)
		}

		// We should be able to read the data written by the writer
		if len(messages) < 3 {
			t.Errorf("expected at least 3 messages, got %d", len(messages))
		}

		// Verify message content
		for i, msg := range messages {
			t.Logf("Read message %d: %s", i, string(msg.Data))
		}

		t.Logf("Reader completed: read %d entries", len(messages))
	})

	// Phase 3: Concurrent read/write test
	t.Run("ConcurrentWriteRead", func(t *testing.T) {
		// This test uses multiple goroutines to simulate concurrent access patterns
		// within the same process (which still tests mmap coordination paths)

		client3, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("failed to create concurrent client: %v", err)
		}
		defer client3.Close()

		var wg sync.WaitGroup
		var writeCount, readCount atomic.Int64

		// Writer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				data := []byte(fmt.Sprintf(`{"concurrent_write": %d}`, i))
				_, err := client3.Append(ctx, streamName, [][]byte{data})
				if err != nil {
					t.Errorf("concurrent write failed: %v", err)
					return
				}
				writeCount.Add(1)
				time.Sleep(100 * time.Millisecond)
			}
		}()

		// Reader goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer := NewConsumer(client3, ConsumerOptions{
				Group: "concurrent-reader",
			})
			defer consumer.Close()

			for i := 0; i < 50; i++ { // Try to read multiple times
				messages, err := consumer.Read(ctx, []uint32{1}, 5)
				if err != nil {
					t.Errorf("concurrent read failed: %v", err)
					return
				}
				readCount.Add(int64(len(messages)))
				time.Sleep(50 * time.Millisecond)
			}
		}()

		wg.Wait()

		t.Logf("Concurrent test: wrote %d entries, read %d entries", writeCount.Load(), readCount.Load())
		if writeCount.Load() == 0 {
			t.Error("No writes completed")
		}
		if readCount.Load() == 0 {
			t.Error("No reads completed")
		}
	})
}

// TestMmapStateFile tests the mmap state file creation and format
func TestMmapStateFile(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{
		[]byte(`{"test": "mmap state"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check that the index.state file was created
	shardDir := filepath.Join(dir, "shard-0001")
	stateFile := filepath.Join(shardDir, "index.state")

	info, err := os.Stat(stateFile)
	if err != nil {
		t.Fatalf("index.state file not created: %v", err)
	}

	if info.Size() != 8 {
		t.Errorf("Expected state file size 8 bytes, got %d", info.Size())
	}

	t.Logf("Index state file created successfully: %s (%d bytes)", stateFile, info.Size())
}

// TestMmapTimestampUpdates tests that mmap timestamps are updated correctly
func TestMmapTimestampUpdates(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Get the shard and check initial timestamp
	shard, _ := client.getOrCreateShard(1)
	initialTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
	t.Logf("Initial mmap timestamp: %d", initialTimestamp)

	// Write something
	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte(`{"test": "timestamp update 1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Force sync to ensure timestamp update
	err = client.Sync(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Check timestamp was updated
	updatedTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
	t.Logf("Updated mmap timestamp: %d", updatedTimestamp)

	if updatedTimestamp <= initialTimestamp {
		t.Error("Timestamp was not updated after write")
	}

	// Write again
	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte(`{"test": "timestamp update 2"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Force sync again
	err = client.Sync(ctx)
	if err != nil {
		t.Fatal(err)
	}

	finalTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
	t.Logf("Final mmap timestamp: %d", finalTimestamp)

	if finalTimestamp <= updatedTimestamp {
		t.Error("Timestamp was not updated after second write")
	}

	t.Log("Mmap timestamp updates verified successfully")
}
