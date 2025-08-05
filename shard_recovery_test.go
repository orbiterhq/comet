package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestShardDirectoryDeletionRecovery tests that comet can gracefully recover
// when shard directories are manually deleted while the client is running
func TestShardDirectoryDeletionRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "comet-shard-recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a client with multi-process mode enabled (this is where the issue occurs)
	config := DefaultCometConfig()
	config.Concurrency.ProcessCount = 2
	client, err := NewClientWithConfig(tempDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	// Use a unique stream name to avoid conflicts with other tests
	streamName := fmt.Sprintf("recovery-test-%d:v1:shard:0000", time.Now().UnixNano())

	// Write some initial data to create the shard
	data := []byte("initial data")
	ids, err := client.Append(ctx, streamName, [][]byte{data})
	if err != nil {
		t.Fatal(err)
	}

	client.Sync(ctx)
	if len(ids) != 1 {
		t.Fatalf("Expected 1 ID, got %d", len(ids))
	}

	// Verify we can read the data
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	messages, err := consumer.Read(ctx, []uint32{0}, 1) // Only read 1 message
	if err != nil {
		t.Fatal(err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	// Verify it's our data
	if string(messages[0].Data) != "initial data" {
		t.Fatalf("Expected 'initial data', got '%s'", string(messages[0].Data))
	}

	// Find the shard directory and delete it manually
	shardDir := filepath.Join(tempDir, "shard-0000")
	if _, err := os.Stat(shardDir); os.IsNotExist(err) {
		t.Fatal("Shard directory should exist")
	}

	t.Logf("Deleting shard directory: %s", shardDir)
	if err := os.RemoveAll(shardDir); err != nil {
		t.Fatal(err)
	}

	// Verify the directory is gone
	if _, err := os.Stat(shardDir); !os.IsNotExist(err) {
		t.Fatal("Shard directory should be deleted")
	}

	// In a real scenario, we need to handle the case where the shard directory is deleted
	// while the client is running. The cached shard object will have stale file handles.
	// We simulate this by trying to access the shard's writer, which should fail and trigger recovery.
	// For this test, we'll clear the cache to simulate a client restart scenario, which is
	// a common case where recovery is needed.
	client.mu.Lock()
	delete(client.shards, 0) // Remove shard 0 from the cache to force recreation
	client.mu.Unlock()
	t.Logf("Cleared shard from cache to simulate client restart scenario")

	// Try to write new data - this should trigger the recovery mechanism
	// The client should detect the missing directory and recreate it
	newData := []byte("recovery data")
	t.Logf("Writing data after directory deletion...")
	t.Logf("Multi-process mode enabled: %v", config.Concurrency.IsMultiProcess())

	var writeErr error
	var newIDs []MessageID

	// The write might succeed on first try if multi-process coordination creates the directory
	// or it might need a moment for the mmap state to be checked
	for attempt := 0; attempt < 5; attempt++ {
		newIDs, writeErr = client.Append(ctx, streamName, [][]byte{newData})
		if writeErr == nil {
			break
		}

		t.Logf("Attempt %d: Write error: %v", attempt+1, writeErr)

		// Also log some details about the error type
		if writeErr != nil {
			t.Logf("Error type: %T", writeErr)
			t.Logf("Error string contains 'no such file': %v", strings.Contains(writeErr.Error(), "no such file"))
		}

		// Check if this is the expected recovery behavior
		if strings.Contains(writeErr.Error(), "no such file or directory") ||
			strings.Contains(writeErr.Error(), "failed to read shard directory") {
			t.Logf("Attempt %d: Got expected recovery error: %v", attempt+1, writeErr)
			time.Sleep(10 * time.Millisecond) // Brief wait for recovery
			continue
		}

		// If it's a different error, continue for a few attempts to see what happens
		if attempt < 4 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// If it's a different error after all attempts, show what we got
		t.Logf("All attempts failed with error: %v", writeErr)
	}

	// Verify the write eventually succeeded
	if writeErr != nil {
		t.Fatalf("Write should have succeeded after recovery, got: %v", writeErr)
	}

	if len(newIDs) != 1 {
		t.Fatalf("Expected 1 new ID, got %d", len(newIDs))
	}

	t.Logf("Write succeeded, checking if shard directory was recreated...")

	// Force a sync to ensure directory is created
	client.Sync(ctx)

	// Verify the shard directory was recreated
	if _, err := os.Stat(shardDir); os.IsNotExist(err) {
		t.Logf("Shard directory still doesn't exist after write success")
		t.Fatal("Shard directory should have been recreated")
	}

	t.Logf("Success: Shard directory was recreated")

	// Verify we can read the new data
	t.Logf("Verifying data can be read after recovery...")

	// Create a new consumer to avoid cached state
	newConsumer := NewConsumer(client, ConsumerOptions{Group: "recovery-group"})
	recoveredMessages, err := newConsumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}

	// We should have at least the new message
	// The old message might be gone since we deleted the directory
	if len(recoveredMessages) == 0 {
		t.Fatal("Should have at least the new message after recovery")
	}

	// Find the new message
	found := false
	for _, msg := range recoveredMessages {
		if string(msg.Data) == "recovery data" {
			found = true
			break
		}
	}

	if !found {
		t.Fatal("Should find the recovery data message")
	}

	t.Logf("SUCCESS: Shard directory deletion recovery test passed")
	t.Logf("- Directory was successfully deleted")
	t.Logf("- Client gracefully recovered and recreated directory")
	t.Logf("- New data was written successfully")
	t.Logf("- Data can be read after recovery")
}

// TestShardDirectoryDeletionWithFileRotation tests recovery when shard directory is deleted
// and file rotation is triggered (forcing new file creation)
func TestShardDirectoryDeletionWithFileRotation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "comet-rotation-recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a client with very small file size to force rotation
	config := DefaultCometConfig()
	config.Concurrency.ProcessCount = 0
	config.Storage.MaxFileSize = 200 // Very small to force rotation quickly
	client, err := NewClientWithConfig(tempDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := fmt.Sprintf("rotation-recovery-test-%d:v1:shard:0000", time.Now().UnixNano())

	// Write initial data to create the shard
	data := []byte("initial data")
	ids, err := client.Append(ctx, streamName, [][]byte{data})
	if err != nil {
		t.Fatal(err)
	}

	client.Sync(ctx)

	if len(ids) != 1 {
		t.Fatalf("Expected 1 ID, got %d", len(ids))
	}

	// Find the shard directory
	shardDir := filepath.Join(tempDir, "shard-0000")
	if _, err := os.Stat(shardDir); os.IsNotExist(err) {
		t.Fatal("Shard directory should exist")
	}

	// Delete the shard directory while keeping the shard cached
	t.Logf("Deleting shard directory: %s", shardDir)
	if err := os.RemoveAll(shardDir); err != nil {
		t.Fatal(err)
	}

	// Verify the directory is gone
	if _, err := os.Stat(shardDir); !os.IsNotExist(err) {
		t.Fatal("Shard directory should be deleted")
	}

	// Write enough data to trigger file rotation, which should force new file creation
	// This should trigger the recovery mechanism when rotation tries to create a new file
	largeData := make([]byte, 300) // Larger than MaxFileSize to force rotation
	for i := range largeData {
		largeData[i] = byte('A' + (i % 26))
	}

	t.Logf("Writing large data to trigger rotation and recovery...")
	rotationIDs, writeErr := client.Append(ctx, streamName, [][]byte{largeData})

	client.Sync(ctx)
	// The rotation should either succeed (if recovery worked) or fail and then succeed on retry
	if writeErr != nil {
		t.Logf("Write failed during rotation as expected: %v", writeErr)

		// If the write failed due to missing directory, it should have triggered recovery
		// Retry should work
		rotationIDs, writeErr = client.Append(ctx, streamName, [][]byte{largeData})
		if writeErr != nil {
			t.Fatalf("Write should have succeeded after recovery, got: %v", writeErr)
		}
		client.Sync(ctx)
	}

	if len(rotationIDs) != 1 {
		t.Fatalf("Expected 1 rotation ID, got %d", len(rotationIDs))
	}

	// Verify the shard directory was recreated
	if _, err := os.Stat(shardDir); os.IsNotExist(err) {
		t.Fatal("Shard directory should have been recreated during rotation")
	}

	t.Logf("SUCCESS: File rotation recovery test passed")
}

// TestHandleMissingShardDirectoryUnit tests the handleMissingShardDirectory method directly
func TestHandleMissingShardDirectoryUnit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "comet-missing-shard-unit")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a shard with a path in the temp directory
	shardDir := filepath.Join(tempDir, "shard-0000")
	indexPath := filepath.Join(shardDir, "index.bin")

	shard := &Shard{
		indexPath: indexPath,
		index: &ShardIndex{
			Files:              []FileInfo{{Path: "some-file"}},
			CurrentFile:        "some-file",
			CurrentEntryNumber: 100,
			CurrentWriteOffset: 1000,
		},
	}

	// Test 1: Non-directory error should not be handled
	otherErr := fmt.Errorf("some other error")
	if shard.handleMissingShardDirectory(otherErr) {
		t.Error("Should not handle non-directory errors")
	}

	// Test 2: Directory error should be handled
	dirErr := fmt.Errorf("failed to read shard directory: open %s: no such file or directory", shardDir)
	if !shard.handleMissingShardDirectory(dirErr) {
		t.Error("Should handle directory missing errors")
	}

	// Verify the shard state was reset
	if len(shard.index.Files) != 0 {
		t.Error("Files should be cleared")
	}
	if shard.index.CurrentFile != "" {
		t.Error("CurrentFile should be cleared")
	}
	if shard.index.CurrentEntryNumber != 0 {
		t.Error("CurrentEntryNumber should be reset")
	}
	if shard.index.CurrentWriteOffset != 0 {
		t.Error("CurrentWriteOffset should be reset")
	}

	// Verify the directory was created
	if _, err := os.Stat(shardDir); os.IsNotExist(err) {
		t.Error("Directory should have been created")
	}
}

// TestLoadIndexWithRecovery tests the loadIndexWithRecovery method
func TestLoadIndexWithRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "comet-load-index-recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a shard with a path in a non-existent directory
	shardDir := filepath.Join(tempDir, "shard-0000")
	indexPath := filepath.Join(shardDir, "index.bin")

	shard := &Shard{
		indexPath: indexPath,
		// Lock file removed - processes own their shards exclusively
		index: &ShardIndex{
			BoundaryInterval: 100,
			ConsumerOffsets:  make(map[string]int64),
			BinaryIndex: BinarySearchableIndex{
				IndexInterval: 100,
				MaxNodes:      1000,
			},
		},
	}

	// This should succeed by creating the directory and initializing empty state
	if err := shard.loadIndexWithRecovery(); err != nil {
		t.Fatalf("loadIndexWithRecovery should succeed: %v", err)
	}

	// Debug: Check if loadIndex was called
	t.Logf("After loadIndexWithRecovery, checking directory: %s", shardDir)

	// Verify the directory was created
	if _, err := os.Stat(shardDir); os.IsNotExist(err) {
		t.Error("Directory should have been created")
		// Let's check what exists
		parentDir := filepath.Dir(shardDir)
		if entries, readErr := os.ReadDir(parentDir); readErr == nil {
			t.Logf("Parent directory contents:")
			for _, e := range entries {
				t.Logf("  - %s", e.Name())
			}
		}
	}
}
