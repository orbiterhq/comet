package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestIsolatedIndexRebuildIssue reproduces the exact issue with minimal complexity
func TestIsolatedIndexRebuildIssue(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Use multi-process config with small files like the failing test
	config := MultiProcessConfig()
	config.Storage.MaxFileSize = 2048 // Small files to force rotation
	
	streamName := "events:v1:shard:0001"

	// Phase 1: Write 65 entries like the failing test
	t.Log("=== PHASE 1: Writing 65 entries ===")
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write 50 entries first
	for i := 0; i < 50; i++ {
		// Use larger data to force file rotation like the failing test
		data := fmt.Sprintf(`{"id": %d, "test": "isolated_rebuild", "data": "entry_%d", "padding": "this is extra data to make entries larger and force file rotation more easily"}`, i, i)
		_, err := client1.Append(ctx, streamName, [][]byte{[]byte(data)})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force file rotation to create multiple files like the failing test
	shard, _ := client1.getOrCreateShard(1)
	shard.mu.Lock()
	err = shard.rotateFile(&client1.metrics, &config)
	shard.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Write more data in the new file like the failing test
	for i := 50; i < 65; i++ {
		data := fmt.Sprintf(`{"id": %d, "test": "isolated_rebuild", "data": "entry_%d", "padding": "additional data"}`, i, i)
		_, err := client1.Append(ctx, streamName, [][]byte{[]byte(data)})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check initial state
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	initialEntries := shard.index.CurrentEntryNumber
	t.Logf("Initial state: %d files, %d entries", initialFiles, initialEntries)
	for i, f := range shard.index.Files {
		t.Logf("  File %d: entries=%d, start=%d, path=%s", 
			i, f.Entries, f.StartEntry, filepath.Base(f.Path))
	}
	shard.mu.RUnlock()

	// Ensure all data is flushed to disk before closing in multi-process mode
	shard.mu.Lock()
	if shard.mmapWriter != nil {
		shard.mmapWriter.Sync() // Force sync to disk
	}
	if shard.writer != nil {
		shard.writer.Flush()
	}
	if shard.dataFile != nil {
		shard.dataFile.Sync() // Force OS to flush to disk
	}
	shard.mu.Unlock()

	client1.Close()

	// Phase 2: Corrupt index to force rebuild (but keep state to maintain file consistency)
	t.Log("=== PHASE 2: Forcing index rebuild ===")
	indexPath := filepath.Join(dir, "shard-0001", "index.bin")

	// Corrupt the index file by writing garbage data
	if err := os.WriteFile(indexPath, []byte("corrupted"), 0644); err != nil {
		t.Fatal(err)
	}
	// Keep the state file so multi-process coordination still works

	// Phase 3: Create new client to trigger rebuild
	t.Log("=== PHASE 3: Rebuilding index ===")
	
	// DEBUG: Check what files exist on disk before rebuild
	shardDir := filepath.Join(dir, "shard-0001")
	files, _ := os.ReadDir(shardDir)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "log-") && strings.HasSuffix(f.Name(), ".comet") {
			info, _ := f.Info()
			t.Logf("DEBUG: Data file %s size=%d", f.Name(), info.Size())
		}
	}
	
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Check rebuilt state
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	rebuiltFiles := len(shard2.index.Files)
	rebuiltEntries := shard2.index.CurrentEntryNumber
	t.Logf("After rebuild: %d files, %d entries", rebuiltFiles, rebuiltEntries)
	for i, f := range shard2.index.Files {
		t.Logf("  File %d: entries=%d, start=%d, path=%s", 
			i, f.Entries, f.StartEntry, filepath.Base(f.Path))
	}
	shard2.mu.RUnlock()

	// Force index persistence to ensure rebuild is complete
	shard2.mu.Lock()
	shard2.persistIndex()
	t.Logf("Index persisted. Consumer offsets: %v", shard2.index.ConsumerOffsets)
	shard2.mu.Unlock()

	// Phase 4: Try reading with consumer - created AFTER index rebuild is complete
	t.Log("=== PHASE 4: Testing consumer reads ===")
	
	// Debug: Check what the consumer would see as earliest entry
	shard2.mu.RLock()
	if len(shard2.index.Files) > 0 {
		t.Logf("Consumer logic: Files[0].StartEntry = %d", shard2.index.Files[0].StartEntry)
	}
	shard2.mu.RUnlock()
	
	// Add detailed consumer debugging
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test-isolated"})
	defer consumer.Close()
	
	// Debug: Check consumer state before reading
	shard2.mu.RLock()
	actualOffset, exists := shard2.index.ConsumerOffsets["test-isolated"]
	t.Logf("Before read - ConsumerOffset[test-isolated] exists: %v, value: %d", exists, actualOffset)
	t.Logf("Before read - CurrentEntryNumber: %d", shard2.index.CurrentEntryNumber)
	t.Logf("Before read - Files[0] details: entries=%d, start=%d, end=%d", 
		shard2.index.Files[0].Entries, shard2.index.Files[0].StartEntry, shard2.index.Files[0].StartEntry + shard2.index.Files[0].Entries - 1)
	shard2.mu.RUnlock()

	messages, err := consumer.Read(ctx, []uint32{1}, 100)
	if err != nil {
		t.Fatal(err)
	}
	
	// Debug: Check consumer state after reading
	shard2.mu.RLock()
	actualOffset2, exists2 := shard2.index.ConsumerOffsets["test-isolated"]
	t.Logf("After read - ConsumerOffset[test-isolated] exists: %v, value: %d", exists2, actualOffset2)
	shard2.mu.RUnlock()

	t.Logf("Consumer read %d messages", len(messages))
	
	// Show which entries were read
	var entryNumbers []int64
	for _, msg := range messages {
		entryNumbers = append(entryNumbers, msg.ID.EntryNumber)
	}
	t.Logf("Entry numbers read: %v", entryNumbers)

	// Phase 5: Try direct file reading
	t.Log("=== PHASE 5: Testing ScanAll ===")
	
	var scanEntries []int64
	err = client2.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		scanEntries = append(scanEntries, msg.ID.EntryNumber)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ScanAll found %d entries: %v", len(scanEntries), scanEntries)

	// Phase 6: Analysis - check specific entry access
	t.Log("=== PHASE 6: Testing specific entry access ===")
	
	// Test reading specific entries using the consumer one at a time
	consumer2 := NewConsumer(client2, ConsumerOptions{Group: "test-individual"})
	defer consumer2.Close()

	// Test reading entries 0-5 individually
	for i := int64(0); i < 6; i++ {
		singleMsg, err := consumer2.Read(ctx, []uint32{1}, 1)
		if err != nil {
			t.Logf("Failed to read entry %d: %v", i, err)
		} else if len(singleMsg) == 0 {
			t.Logf("No message available for entry %d", i)
		} else {
			t.Logf("Successfully read entry %d", singleMsg[0].ID.EntryNumber)
		}
	}

	// Final check: ensure we can read all 65 entries
	if len(messages) < 65 {
		t.Errorf("Expected to read at least 65 messages, got %d", len(messages))
	}

	// Compare consumer vs ScanAll results
	if len(messages) != len(scanEntries) {
		t.Errorf("Mismatch: Consumer=%d, ScanAll=%d", len(messages), len(scanEntries))
		
		// Find which entries are missing from consumer
		entryMap := make(map[int64]bool)
		for _, num := range entryNumbers {
			entryMap[num] = true
		}
		
		var missing []int64
		for _, num := range scanEntries {
			if !entryMap[num] {
				missing = append(missing, num)
			}
		}
		
		if len(missing) > 0 {
			t.Errorf("Consumer missing entries: %v", missing)
		}
	}
}