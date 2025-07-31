package comet

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// TestIndexRebuild tests index behavior when index is missing
func TestIndexRebuild(t *testing.T) {
	t.Skip("Index rebuild from data files not yet implemented")
	
	// TODO: Implement index rebuilding from data files
	// Currently when index is missing, a new empty index is created
	// which means historical data is not accessible without the index.
	// 
	// To implement this feature:
	// 1. Scan all data files in shard directory
	// 2. Parse entries and rebuild FileInfo metadata
	// 3. Rebuild binary search index nodes
	// 4. Restore consumer offsets from checkpoint files if available
}

// TestIndexRebuildWithMultipleFiles tests rebuilding with multiple data files
func TestIndexRebuildWithMultipleFiles(t *testing.T) {
	t.Skip("Index rebuild from data files not yet implemented")
}

// TestIndexMissingDetection tests detection of missing index
func TestIndexMissingDetection(t *testing.T) {
	t.Skip("Index rebuild from data files not yet implemented")
}

// TestScanFileForEntries tests the file scanning functionality
func TestScanFileForEntries(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	streamName := "test:v1:shard:0001"
	
	// Write some entries
	numEntries := 5
	for i := 0; i < numEntries; i++ {
		_, err := client.Append(ctx, streamName, [][]byte{
			[]byte(fmt.Sprintf(`{"entry": %d}`, i)),
		})
		if err != nil {
			t.Fatalf("failed to write entry %d: %v", i, err)
		}
	}
	
	// Get shard and file info
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	if len(shard.index.Files) == 0 {
		t.Fatal("no files created")
	}
	filePath := shard.index.Files[0].Path
	shard.mu.RUnlock()
	
	client.Close()
	
	// Test scanFileForEntries directly
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client2.Close()
	
	shard2, _ := client2.getOrCreateShard(1)
	
	// Clear the index to force scanning
	shard2.mu.Lock()
	shard2.index.Files = nil
	shard2.index.CurrentEntryNumber = 0
	shard2.mu.Unlock()
	
	// Get file info
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	
	// Scan the file
	result := shard2.scanFileForEntries(filePath, fileInfo.Size(), 0, 0)
	
	if result.entryCount != int64(numEntries) {
		t.Errorf("expected %d entries, got %d", numEntries, result.entryCount)
	}
	
	// Verify we have index nodes (for binary search)
	if len(result.indexNodes) == 0 {
		t.Error("expected at least one index node")
	}
}