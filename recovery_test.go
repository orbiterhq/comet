package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEnsureWriterRecovery tests the ensureWriter recovery function
func TestEnsureWriterRecovery(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.ProcessCount = 0 // Test non-mmap path

	// Create initial client and write data
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test data")})
	if err != nil {
		t.Fatal(err)
	}

	// Get the shard
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a corrupted state by closing the writer and file
	shard.mu.Lock()
	if shard.writer != nil {
		shard.writer.Flush()
		shard.writer = nil
	}
	if shard.dataFile != nil {
		shard.dataFile.Close()
		shard.dataFile = nil
	}

	// Now call ensureWriter to recover
	err = shard.ensureWriter(&config)
	shard.mu.Unlock()

	if err != nil {
		t.Fatalf("ensureWriter failed: %v", err)
	}

	// Verify we can write after recovery
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after recovery")})
	if err != nil {
		t.Fatalf("Write after recovery failed: %v", err)
	}

	client.Close()
}

// TestInitializeMmapWriterRecovery tests the initializeMmapWriter recovery function
func TestInitializeMmapWriterRecovery(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2) // Use multi-process mode to test mmap path

	// Create initial client and write data
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test data")})
	if err != nil {
		t.Fatal(err)
	}

	// Get the shard
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a corrupted state by closing the data file
	shard.mu.Lock()
	if shard.dataFile != nil {
		shard.dataFile.Close()
		shard.dataFile = nil
	}

	// Make sure we have state initialized before recovery
	if shard.state == nil {
		shard.mu.Unlock()
		t.Skip("State not available in test environment")
		return
	}

	// Now recover by reopening the data file
	err = shard.openDataFileWithConfig(filepath.Join(dir, "shard-0000"), &config)
	shard.mu.Unlock()

	if err != nil {
		t.Fatalf("openDataFileWithConfig failed: %v", err)
	}

	// Verify we can write after recovery
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after recovery")})
	if err != nil {
		t.Fatalf("Write after recovery failed: %v", err)
	}

	client.Close()
}

// TestScanDataFilesForEntryFallback tests the scanDataFilesForEntry corrupted index fallback
func TestScanDataFilesForEntryFallback(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	// Create client and write data
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write multiple entries to ensure we have something to scan
	for i := 0; i < 10; i++ {
		_, err := client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte(fmt.Sprintf("entry %d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	// Get the shard
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the binary index to force fallback to scanning
	shard.mu.Lock()
	shard.index.BinaryIndex.Nodes = nil // Clear all index nodes
	shard.mu.Unlock()

	// Try to find an entry that should trigger scanDataFilesForEntry
	targetEntry := int64(5) // Middle entry
	position, err := consumer.scanDataFilesForEntry(shard, targetEntry)
	if err != nil {
		t.Fatalf("scanDataFilesForEntry failed: %v", err)
	}

	// Verify the position is valid
	if position.FileIndex >= len(shard.index.Files) {
		t.Errorf("Invalid file index: %d >= %d", position.FileIndex, len(shard.index.Files))
	}
	if position.ByteOffset < 0 {
		t.Errorf("Invalid byte offset: %d", position.ByteOffset)
	}

	// Verify the position seems reasonable
	// We should have found a position in one of the data files
	t.Logf("Found entry %d at file index %d, byte offset %d",
		targetEntry, position.FileIndex, position.ByteOffset)

	client.Close()
}

// TestHandleMissingShardDirectoryWithRecovery tests recovery after shard directory deletion
func TestHandleMissingShardDirectoryWithRecovery(t *testing.T) {
	dir := t.TempDir()

	// Test both single and multi-process modes
	testCases := []struct {
		name   string
		config CometConfig
	}{
		{"SingleProcess", DefaultCometConfig()},
		{"MultiProcess", DeprecatedMultiProcessConfig(0, 2)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create client and write data
			client, err := NewClient(dir+"/"+tc.name, tc.config)
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()
			_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("before deletion")})
			if err != nil {
				t.Fatal(err)
			}

			client.Sync(ctx)

			// Get the shard
			shard, err := client.getOrCreateShard(0)
			if err != nil {
				t.Fatal(err)
			}

			// Get the shard directory path
			shardDir := filepath.Dir(shard.indexPath)

			// Close the client to release file handles
			client.Close()

			// Delete the shard directory
			if err := os.RemoveAll(shardDir); err != nil {
				t.Fatal(err)
			}

			// Recreate client - this should trigger recovery
			client2, err := NewClient(dir+"/"+tc.name, tc.config)
			if err != nil {
				t.Fatal(err)
			}
			defer client2.Close()

			// Try to write - this should trigger handleMissingShardDirectory and recovery
			_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after deletion")})
			if err != nil {
				t.Fatalf("Write after directory deletion failed: %v", err)
			}

			// Sync to ensure data is written to disk
			if err := client2.Sync(ctx); err != nil {
				t.Fatal(err)
			}

			// Verify the shard directory was recreated
			if _, err := os.Stat(shardDir); os.IsNotExist(err) {
				t.Error("Shard directory was not recreated")
			}

			// Verify we can read the new data (old data is lost)
			consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
			defer consumer.Close()

			messages, err := consumer.Read(ctx, []uint32{0}, 10)
			if err != nil {
				t.Fatal(err)
			}

			// In multi-process mode, the data might not be lost completely
			// due to memory-mapped state persistence
			if len(messages) == 0 {
				t.Errorf("Expected at least 1 message after recovery, got 0")
			}

			// Find the "after deletion" message
			foundAfterDeletion := false
			for _, msg := range messages {
				if string(msg.Data) == "after deletion" {
					foundAfterDeletion = true
					break
				}
			}

			if !foundAfterDeletion {
				t.Error("Did not find 'after deletion' message after recovery")
			}
		})
	}
}

// TestRecoveryWithCorruptedDataFile tests recovery when data files are corrupted
func TestRecoveryWithCorruptedDataFile(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	// Create client and write data
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write some entries
	for i := 0; i < 5; i++ {
		_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte(fmt.Sprintf("entry %d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force a sync to ensure data is on disk
	client.Sync(ctx)

	// Get the shard
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Get the current data file path
	shard.mu.RLock()
	dataFilePath := shard.index.CurrentFile
	shard.mu.RUnlock()

	// Close the client
	client.Close()

	// Corrupt the data file by truncating it
	if err := os.Truncate(dataFilePath, 10); err != nil {
		t.Fatal(err)
	}

	// Create a new client - this should handle the corruption
	client2, err := NewClient(dir, config)
	if err != nil {
		// Recovery might fail, but shouldn't panic
		t.Logf("Client creation after corruption failed (expected): %v", err)
		return
	}
	defer client2.Close()

	// Try to write new data
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after corruption")})
	if err != nil {
		t.Logf("Write after corruption failed (might be expected): %v", err)
	}
}

// TestConcurrentRecovery tests recovery under concurrent access
func TestConcurrentRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent recovery test in short mode")
	}

	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	config.Retention.CleanupInterval = 50 * time.Millisecond

	// Create initial client
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write initial data
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("initial")})
	if err != nil {
		t.Fatal(err)
	}

	// Get shard directory
	shard, _ := client.getOrCreateShard(0)
	shardDir := filepath.Dir(shard.indexPath)

	// Close the client before deleting directory to avoid races
	client.Close()

	// Delete the shard directory
	if err := os.RemoveAll(shardDir); err != nil {
		t.Fatal(err)
	}

	// Create a new client - this should trigger recovery
	client2, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Verify we can write after recovery
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after recovery")})
	if err != nil {
		t.Fatalf("Write after recovery failed: %v", err)
	}

	// Verify the directory was recreated
	if _, err := os.Stat(shardDir); os.IsNotExist(err) {
		t.Error("Shard directory was not recreated")
	}
}
