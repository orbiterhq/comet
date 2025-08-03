//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestRetentionIndexConsistency tests if retention properly updates the index when deleting files
func TestRetentionIndexConsistency(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 200

	t.Logf("=== PHASE 1: Creating files ===")
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	streamName := "events:v1:shard:0001"
	shard, _ := client.getOrCreateShard(1)

	// Create multiple files
	for i := 0; i < 10; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d}`, i))}
		_, err := client.Append(context.Background(), streamName, data)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}

		// Force rotation every 2 entries
		if (i+1)%2 == 0 {
			shard.mu.Lock()
			err = shard.rotateFile(&client.metrics, &config)
			shard.mu.Unlock()
			if err != nil {
				t.Fatalf("Rotation %d failed: %v", i, err)
			}
		}
	}

	// Log initial state
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	t.Logf("Created %d files:", initialFiles)
	for i, file := range shard.index.Files {
		t.Logf("  File %d: %s (entries: %d, startEntry: %d)", 
			i, file.Path, file.Entries, file.StartEntry)
	}
	currentFile := shard.index.CurrentFile
	shard.mu.RUnlock()

	t.Logf("=== PHASE 2: Making files old ===")
	// Mark all non-current files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-500 * time.Millisecond)
	filesMarked := 0
	for i := range shard.index.Files {
		if shard.index.Files[i].Path != currentFile {
			t.Logf("Marking file as old: %s", shard.index.Files[i].Path)
			shard.index.Files[i].EndTime = oldTime
			filesMarked++
		}
	}
	shard.persistIndex()
	shard.mu.Unlock()
	t.Logf("Marked %d files as old", filesMarked)

	client.Sync(context.Background())
	client.Close()

	t.Logf("=== PHASE 3: Running retention ===")
	// Create new client and run retention
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Get stats before retention
	statsBefore := client2.GetRetentionStats()
	t.Logf("Files before retention: %d", statsBefore.TotalFiles)

	// Run retention
	client2.ForceRetentionCleanup()

	// Get stats after retention
	statsAfter := client2.GetRetentionStats()
	t.Logf("Files after retention: %d (deleted %d)", 
		statsAfter.TotalFiles, statsBefore.TotalFiles-statsAfter.TotalFiles)

	t.Logf("=== PHASE 4: Checking index consistency ===")
	// Get the shard and check if index is consistent with filesystem
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	t.Logf("Index reports %d files:", len(shard2.index.Files))
	for i, file := range shard2.index.Files {
		// Check if file actually exists
		if _, err := os.Stat(file.Path); os.IsNotExist(err) {
			t.Errorf("❌ INDEX INCONSISTENCY: File %d in index doesn't exist: %s", i, file.Path)
		} else {
			t.Logf("✅ File %d exists: %s (entries: %d)", i, file.Path, file.Entries)
		}
	}
	shard2.mu.RUnlock()

	t.Logf("=== PHASE 5: Testing data access ===")
	// Try to read data - this should not fail
	consumer := NewConsumer(client2, ConsumerOptions{Group: "debug-test"})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 10)
	if err != nil {
		t.Logf("❌ DATA ACCESS FAILED: %v", err)
		// This is the core issue we need to fix
	} else {
		t.Logf("✅ Successfully read %d messages", len(messages))
	}
}

// TestRetentionWithLogging runs retention with extensive logging to understand the process
func TestRetentionWithLogging(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 200

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	streamName := "events:v1:shard:0001"
	
	// Create some files
	for i := 0; i < 6; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d}`, i))}
		_, err := client.Append(context.Background(), streamName, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get shard for direct manipulation
	shard, _ := client.getOrCreateShard(1)

	// Mark files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-500 * time.Millisecond)
	if len(shard.index.Files) > 1 {
		// Mark first file as old, keep the rest
		shard.index.Files[0].EndTime = oldTime
		t.Logf("Marked file as old: %s", shard.index.Files[0].Path)
	}
	shard.persistIndex()
	shard.mu.Unlock()

	client.Sync(context.Background())

	t.Logf("=== RUNNING RETENTION WITH DETAILED LOGGING ===")
	
	// Log retention process step by step
	// This will help us see exactly what happens during retention
	client.ForceRetentionCleanup()

	t.Logf("=== POST-RETENTION STATE ===")
	shard.mu.RLock()
	t.Logf("Files in index after retention: %d", len(shard.index.Files))
	for i, file := range shard.index.Files {
		exists := "EXISTS"
		if _, err := os.Stat(file.Path); os.IsNotExist(err) {
			exists = "DELETED"
		}
		t.Logf("  File %d: %s (%s, entries: %d)", i, file.Path, exists, file.Entries)
	}
	shard.mu.RUnlock()
}

// TestRetentionMinimal creates the smallest possible reproduction
func TestRetentionMinimal(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 200

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write 4 entries to create 2 files (2 entries each due to small MaxFileSize)
	for i := 0; i < 4; i++ {
		_, err := client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
			[]byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get shard and mark first file as old
	shard, _ := client.getOrCreateShard(1)
	shard.mu.Lock()
	if len(shard.index.Files) >= 2 {
		oldTime := time.Now().Add(-500 * time.Millisecond)
		shard.index.Files[0].EndTime = oldTime
		t.Logf("Marked first file as old: %s", shard.index.Files[0].Path)
	}
	shard.persistIndex()
	filesBeforeRetention := len(shard.index.Files)
	shard.mu.Unlock()

	client.Sync(context.Background())
	client.Close()

	// New client for retention
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	t.Logf("Files before retention: %d", filesBeforeRetention)
	
	// Run retention
	client2.ForceRetentionCleanup()
	
	// Check result
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	filesAfterRetention := len(shard2.index.Files)
	t.Logf("Files after retention: %d", filesAfterRetention)
	
	// THE KEY TEST: Can we still read data?
	shard2.mu.RUnlock()
	
	consumer := NewConsumer(client2, ConsumerOptions{Group: "minimal-test"})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 10)
	if err != nil {
		t.Fatalf("❌ REPRODUCTION CONFIRMED: %v", err)
	}
	t.Logf("✅ Read %d messages successfully", len(messages))
}