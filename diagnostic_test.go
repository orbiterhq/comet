package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestSyncPersistenceDiagnostic checks if Sync() properly persists indexes for all shards
func TestSyncPersistenceDiagnostic(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultCometConfig()
	cfg.Storage.FlushInterval = 1000 // 1s flush
	ctx := context.Background()

	// Create writer and write exactly 1 message to each of 16 shards
	writer, err := NewClient(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	shardNames := make([]string, 16)
	for i := 0; i < 16; i++ {
		shardNames[i] = fmt.Sprintf("shard:%04d", i)
		
		// Write exactly 1 message to this shard
		_, err := writer.Append(ctx, shardNames[i], [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
		if err != nil {
			t.Fatalf("Failed to write to shard %d: %v", i, err)
		}
		t.Logf("[WRITER] Wrote message to shard %d", i)
	}

	// Now sync - this should persist all indexes
	t.Logf("[WRITER] Calling Sync() to persist all indexes...")
	if err := writer.Sync(ctx); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	t.Logf("[WRITER] Sync completed")
	
	// Check IndexPersistErrors counter to see if there were silent failures
	t.Logf("[DIAGNOSTIC] Checking for persist errors and actual paths...")
	for i := 0; i < 16; i++ {
		if shard := writer.shards[uint32(i)]; shard != nil && shard.state != nil {
			errors := shard.state.IndexPersistErrors
			successes := shard.state.IndexPersistCount  
			t.Logf("[DIAGNOSTIC] Shard %d: %d successes, %d errors, indexPath='%s'", i, successes, errors, shard.indexPath)
			
			// Check if file exists at the actual indexPath
			if _, err := os.Stat(shard.indexPath); err == nil {
				t.Logf("[DIAGNOSTIC] ✅ Index file found at actual path: %s", shard.indexPath)
			} else {
				t.Logf("[DIAGNOSTIC] ❌ Index file missing at actual path: %s", shard.indexPath)
			}
		}
	}

	// Check filesystem - verify that index files exist for all shards  
	t.Logf("[DIAGNOSTIC] Checking filesystem for index files...")
	indexFilesFound := 0
	for i := 0; i < 16; i++ {
		shardDir := filepath.Join(dir, fmt.Sprintf("shard-%04d", i))
		indexPath := filepath.Join(shardDir, "index.bin")
		
		if _, err := os.Stat(indexPath); err == nil {
			indexFilesFound++
			t.Logf("[DIAGNOSTIC] ✅ Index file exists: %s", indexPath)
		} else {
			t.Logf("[DIAGNOSTIC] ❌ Index file missing: %s", indexPath)
		}
	}
	t.Logf("[DIAGNOSTIC] Index files found: %d/16", indexFilesFound)

	// Check memory-mapped state - verify LastIndexUpdate timestamps
	t.Logf("[DIAGNOSTIC] Checking memory-mapped state timestamps...")
	timestampsUpdated := 0
	for i := 0; i < 16; i++ {
		if shard := writer.shards[uint32(i)]; shard != nil && shard.state != nil {
			timestamp := shard.state.GetLastIndexUpdate()
			if timestamp > 0 {
				timestampsUpdated++
				t.Logf("[DIAGNOSTIC] ✅ Shard %d timestamp: %d", i, timestamp)
			} else {
				t.Logf("[DIAGNOSTIC] ❌ Shard %d timestamp: %d (zero)", i, timestamp)
			}
		} else {
			t.Logf("[DIAGNOSTIC] ❌ Shard %d not found in writer.shards", i)
		}
	}
	t.Logf("[DIAGNOSTIC] Timestamps updated: %d/16", timestampsUpdated)

	writer.Close()

	// Now create a separate consumer and see what it can read
	t.Logf("[CONSUMER] Creating consumer to test index reloading...")
	consumer, err := NewClient(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Try to read from each shard individually to see which ones are visible
	visibleShards := 0
	for i := 0; i < 16; i++ {
		// Try to get or create shard (this should trigger on-demand creation + index loading)
		shard, err := consumer.getOrCreateShard(uint32(i))
		if err != nil {
			t.Logf("[CONSUMER] ❌ Shard %d not accessible: %v", i, err)
			continue
		}

		// Check if shard has any entries in its index
		entries := shard.index.CurrentEntryNumber
		if entries > 0 {
			visibleShards++
			t.Logf("[CONSUMER] ✅ Shard %d visible with %d entries", i, entries)
		} else {
			t.Logf("[CONSUMER] ❌ Shard %d has 0 entries in index", i)
		}
	}
	
	t.Logf("[CONSUMER] Visible shards: %d/16", visibleShards)

	// Summary
	t.Logf("\n=== DIAGNOSTIC SUMMARY ===")
	t.Logf("Index files on disk: %d/16 (%.1f%%)", indexFilesFound, float64(indexFilesFound)*100/16)
	t.Logf("State timestamps updated: %d/16 (%.1f%%)", timestampsUpdated, float64(timestampsUpdated)*100/16)
	t.Logf("Consumer visible shards: %d/16 (%.1f%%)", visibleShards, float64(visibleShards)*100/16)

	if indexFilesFound < 16 {
		t.Errorf("ISSUE: Sync() is not persisting index files for all shards")
	}
	if timestampsUpdated < 16 {
		t.Errorf("ISSUE: Sync() is not updating LastIndexUpdate timestamps for all shards")
	}
	if visibleShards < 16 {
		t.Errorf("ISSUE: Consumer cannot see data from all shards after sync")
	}
}