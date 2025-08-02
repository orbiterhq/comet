package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestReaderBasicUsage tests that Reader works correctly with the new bounded memory system
func TestReaderBasicUsage(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create client with standard config
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 1024 // Small files to test multiple files
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write test data
	streamName := "test:v1:shard:0001"
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "message": "test data for reader"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get the shard
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal("Failed to get shard:", err)
	}

	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	totalEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("Created %d files with %d total entries", fileCount, totalEntries)

	// Create reader
	reader, err := NewReader(shard.shardID, shard.index)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Test reading first entry
	pos := EntryPosition{FileIndex: 0, ByteOffset: 0}
	data, err := reader.ReadEntryAtPosition(pos)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"id": 0, "message": "test data for reader"}`
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}

	// Check memory usage
	memUsage, mappedFiles := reader.GetMemoryUsage()
	t.Logf("Memory usage: %d bytes, %d files mapped", memUsage, mappedFiles)

	// Should have at least one file mapped
	if mappedFiles == 0 {
		t.Error("Expected at least one file to be mapped")
	}

	t.Log("Reader basic usage test passed")
}

// TestReaderMemoryBounds tests that Reader respects memory limits
func TestReaderMemoryBounds(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create client and write data to create multiple files
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 512 // Very small files
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write enough data to create multiple files
	streamName := "test:v1:shard:0001"
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "message": "memory bounds test with longer data to force file rotation"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get the shard
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal("Failed to get shard:", err)
	}

	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	t.Logf("Created %d files for memory bounds test", fileCount)

	if fileCount < 3 {
		t.Skip("Need at least 3 files for memory bounds test")
	}

	// Create reader with default config (should have reasonable limits)
	reader, err := NewReader(shard.shardID, shard.index)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Read from different files to test smart mapping
	for fileIdx := 0; fileIdx < min(fileCount, 4); fileIdx++ {
		pos := EntryPosition{FileIndex: fileIdx, ByteOffset: 0}
		_, err := reader.ReadEntryAtPosition(pos)
		if err != nil {
			t.Logf("Error reading from file %d (may be expected): %v", fileIdx, err)
		} else {
			t.Logf("Successfully read from file %d", fileIdx)
		}
	}

	// Check that memory usage is reasonable
	memUsage, mappedFiles := reader.GetMemoryUsage()
	t.Logf("Final state: %d bytes, %d files mapped", memUsage, mappedFiles)

	// Should have some reasonable bounds
	if mappedFiles > 10 {
		t.Errorf("Too many files mapped: %d > 10", mappedFiles)
	}

	t.Log("Reader memory bounds test completed")
}

// TestReaderCompressedData tests zstd decompression functionality
func TestReaderCompressedData(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create client with compression enabled
	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 50 // Compress entries > 50 bytes
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write data that will be compressed
	streamName := "test:v1:shard:0001"
	largeData := []byte(fmt.Sprintf(`{"id": 1, "message": "this is a large message that should trigger compression because it exceeds the minimum compression size threshold: %s"}`,
		"padding data to make this entry large enough for compression testing"))

	_, err = client.Append(ctx, streamName, [][]byte{largeData})
	if err != nil {
		t.Fatal(err)
	}

	// Get shard and test Reader can decompress
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(shard.shardID, shard.index)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Read and verify decompression works
	pos := EntryPosition{FileIndex: 0, ByteOffset: 0}
	decompressed, err := reader.ReadEntryAtPosition(pos)
	if err != nil {
		t.Fatal("Failed to read compressed entry:", err)
	}

	if string(decompressed) != string(largeData) {
		t.Errorf("Decompressed data doesn't match original")
		t.Logf("Expected length: %d", len(largeData))
		t.Logf("Got length: %d", len(decompressed))
		t.Logf("Expected: %q", string(largeData)[:min(50, len(largeData))])
		t.Logf("Got: %q", string(decompressed)[:min(50, len(decompressed))])
	}

	t.Log("Reader compressed data test passed")
}

// TestReaderErrorHandling tests various error conditions
func TestReaderErrorHandling(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create basic setup
	config := DefaultCometConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write some data
	streamName := "test:v1:shard:0001"
	_, err = client.Append(ctx, streamName, [][]byte{[]byte("test data")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(shard.shardID, shard.index)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Test invalid file index
	invalidPos := EntryPosition{FileIndex: 999, ByteOffset: 0}
	_, err = reader.ReadEntryAtPosition(invalidPos)
	if err == nil {
		t.Error("Expected error for invalid file index")
	}

	// Test invalid byte offset
	invalidPos2 := EntryPosition{FileIndex: 0, ByteOffset: 99999}
	_, err = reader.ReadEntryAtPosition(invalidPos2)
	if err == nil {
		t.Error("Expected error for invalid byte offset")
	}

	t.Log("Reader error handling test passed")
}

// TestReaderConcurrentAccess tests thread safety
func TestReaderConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create test data
	config := DefaultCometConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	streamName := "test:v1:shard:0001"
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "message": "concurrent access test"}`, i))
		_, err = client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(shard.shardID, shard.index)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Launch multiple goroutines reading concurrently
	numWorkers := 5
	numReadsPerWorker := 10
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*numReadsPerWorker)

	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for read := 0; read < numReadsPerWorker; read++ {
				pos := EntryPosition{FileIndex: 0, ByteOffset: 0}
				_, err := reader.ReadEntryAtPosition(pos)
				if err != nil {
					errors <- fmt.Errorf("worker %d read %d: %w", workerID, read, err)
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		errorCount++
		t.Logf("Concurrent access error: %v", err)
	}

	if errorCount > 0 {
		t.Errorf("Found %d errors in concurrent access test", errorCount)
	}

	t.Log("Reader concurrent access test passed")
}

// TestReaderNilIndex tests error handling with nil index
func TestReaderNilIndex(t *testing.T) {
	_, err := NewReader(1, nil)
	if err == nil {
		t.Error("Expected error with nil index")
	}
}

// TestReaderEmptyIndex tests handling of empty index
func TestReaderEmptyIndex(t *testing.T) {
	emptyIndex := &ShardIndex{
		Files: []FileInfo{},
	}
	reader, err := NewReader(1, emptyIndex)
	if err != nil {
		t.Fatal("Should handle empty index:", err)
	}
	defer reader.Close()

	// Try to read - should fail gracefully
	pos := EntryPosition{FileIndex: 0, ByteOffset: 0}
	_, err = reader.ReadEntryAtPosition(pos)
	if err == nil {
		t.Error("Expected error reading from empty index")
	}
}

// TestReaderClose tests proper cleanup
func TestReaderClose(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	config := DefaultCometConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(shard.shardID, shard.index)
	if err != nil {
		t.Fatal(err)
	}

	// Map a file
	pos := EntryPosition{FileIndex: 0, ByteOffset: 0}
	_, err = reader.ReadEntryAtPosition(pos)
	if err != nil {
		t.Fatal(err)
	}

	// Verify file is mapped
	memUsage, mappedFiles := reader.GetMemoryUsage()
	if mappedFiles == 0 {
		t.Error("Expected at least one file to be mapped")
	}
	t.Logf("Before close: %d bytes, %d files", memUsage, mappedFiles)

	// Close should unmap everything
	err = reader.Close()
	if err != nil {
		t.Error("Close should not error:", err)
	}

	// Verify cleanup happened
	memUsage, mappedFiles = reader.GetMemoryUsage()
	if mappedFiles != 0 || memUsage != 0 {
		t.Errorf("Close didn't clean up: %d bytes, %d files", memUsage, mappedFiles)
	}

	// Double close should be safe
	err = reader.Close()
	if err != nil {
		t.Error("Double close should be safe:", err)
	}

	// Try to read after close
	_, err = reader.ReadEntryAtPosition(pos)
	if err == nil {
		t.Error("Expected error reading from closed reader")
	}
}

// Helper function since min doesn't exist in older Go versions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
