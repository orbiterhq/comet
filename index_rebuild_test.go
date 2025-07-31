package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestIndexRebuild tests index rebuilding when index file is missing
func TestIndexRebuild(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	
	// Create initial client and write data
	config := DefaultCometConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	
	// Write test data
	streamName := "events:v1:shard:0001"
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "test": "rebuild"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}
	
	// Force file rotation to create multiple files
	shard, _ := client.getOrCreateShard(1)
	shard.mu.Lock()
	err = shard.rotateFile(&client.metrics, &config)
	shard.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	
	// Write more data
	for i := numEntries; i < numEntries+10; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "test": "rebuild"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}
	
	// Verify we have multiple files
	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	totalEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()
	
	if fileCount < 2 {
		t.Fatalf("Expected at least 2 files, got %d", fileCount)
	}
	
	// Force index persistence before closing
	shard.mu.Lock()
	shard.persistIndex()
	shard.mu.Unlock()
	
	client.Close()
	
	// Delete the index file to simulate corruption/loss
	indexPath := filepath.Join(dir, "shard-0001", "index.bin")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Fatalf("Index file was not created: %s", indexPath)
	}
	
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	
	// Create new client - should rebuild index from data files
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()
	
	// Verify index was rebuilt
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	rebuiltFiles := len(shard2.index.Files)
	rebuiltEntries := shard2.index.CurrentEntryNumber
	shard2.mu.RUnlock()
	
	if rebuiltFiles != fileCount {
		t.Errorf("Expected %d files after rebuild, got %d", fileCount, rebuiltFiles)
	}
	
	if rebuiltEntries != totalEntries {
		t.Errorf("Expected %d entries after rebuild, got %d", totalEntries, rebuiltEntries)
	}
	
	// Verify we can read all data
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()
	
	messages, err := consumer.Read(ctx, []uint32{1}, 100)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(messages) != 60 {
		t.Errorf("Expected to read 60 messages after rebuild, got %d", len(messages))
	}
	
	// Verify binary index was rebuilt
	shard2.mu.RLock()
	indexNodes := len(shard2.index.BinaryIndex.Nodes)
	shard2.mu.RUnlock()
	
	if indexNodes == 0 {
		t.Error("Binary index nodes were not rebuilt")
	}
}

// TestIndexRebuildWithCorruptedFile tests rebuilding when one file is corrupted
func TestIndexRebuildWithCorruptedFile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	
	// Create initial client and write data
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 1024 // Small files to force rotation
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	
	// Write data to create multiple files
	streamName := "events:v1:shard:0001"
	for i := 0; i < 30; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "message": "test data for corrupted file test"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}
	
	// Get file info before corruption
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()
	
	if fileCount < 3 {
		t.Fatalf("Expected at least 3 files, got %d", fileCount)
	}
	
	// Force index persistence before closing
	shard.mu.Lock()
	shard.persistIndex()
	shard.mu.Unlock()
	
	client.Close()
	
	// Corrupt the middle file
	files, _ := os.ReadDir(filepath.Join(dir, "shard-0001"))
	var dataFiles []string
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "log-") && strings.HasSuffix(f.Name(), ".comet") {
			dataFiles = append(dataFiles, f.Name())
		}
	}
	sort.Strings(dataFiles)
	
	if len(dataFiles) >= 2 {
		// Remove the second file entirely to simulate corruption/deletion
		corruptPath := filepath.Join(dir, "shard-0001", dataFiles[1])
		if err := os.Remove(corruptPath); err != nil {
			t.Fatal(err)
		}
	}
	
	// Delete the index file
	indexPath := filepath.Join(dir, "shard-0001", "index.bin")
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	
	// Create new client - should rebuild index skipping corrupted file
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()
	
	// Verify index was rebuilt (should have one less file due to corruption)
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	rebuiltFiles := len(shard2.index.Files)
	shard2.mu.RUnlock()
	
	// Should have fileCount-1 files (skipped corrupted one)
	if rebuiltFiles != fileCount-1 {
		t.Errorf("Expected %d files after rebuild (skipping corrupted), got %d", fileCount-1, rebuiltFiles)
	}
}

// TestIndexMissingDetection tests detection of missing index
func TestIndexMissingDetection(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	
	// First, create a client and write some data to generate real data files
	config := DefaultCometConfig()
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	
	// Write some test data
	streamName := "events:v1:shard:0001"
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "test": "missing_index"}`, i))
		_, err := client1.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}
	
	// Force index persistence and close
	shard1, _ := client1.getOrCreateShard(1)
	shard1.mu.Lock()
	shard1.persistIndex()
	shard1.mu.Unlock()
	client1.Close()
	
	// Delete the index file to simulate missing index
	indexPath := filepath.Join(dir, "shard-0001", "index.bin")
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	
	// Create new client - should detect missing index and rebuild
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()
	
	// Force shard access to ensure it's initialized
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	filesCount := len(shard2.index.Files)
	t.Logf("After rebuild: %d files found", filesCount)
	shard2.mu.RUnlock()
	
	// Force index persistence
	shard2.mu.Lock()
	if err := shard2.persistIndex(); err != nil {
		t.Logf("Failed to persist index: %v", err)
	}
	shard2.mu.Unlock()
	
	// Verify index was recreated
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("Index file was not created during rebuild detection")
	} else {
		t.Log("Index file successfully created after rebuild")
	}
	
	// Verify we can read the data after rebuild
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()
	
	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(messages) != 5 {
		t.Errorf("Expected to read 5 messages after index rebuild, got %d", len(messages))
	}
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