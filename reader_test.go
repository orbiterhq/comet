package comet

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// TestReaderMemorySafetyConcurrent verifies that data returned by the reader
// remains valid even when files are being remapped concurrently
func TestReaderMemorySafetyConcurrent(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 4096 // Small-ish files but not too extreme
	// Reader config is managed internally, we'll use small files to force remapping

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	const (
		numWriters   = 2
		numReaders   = 4
		numEntries   = 100
		testDuration = 2 * time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	var writeCount atomic.Int64
	var readCount atomic.Int64
	var errorCount atomic.Int64

	// Start writers
	writerWg := &sync.WaitGroup{}
	for i := 0; i < numWriters; i++ {
		writerWg.Add(1)
		go func(writerID int) {
			defer writerWg.Done()
			for j := 0; ctx.Err() == nil && j < numEntries/numWriters; j++ {
				data := map[string]interface{}{
					"writer": writerID,
					"seq":    j,
					"data":   fmt.Sprintf("test-data-%d-%d", writerID, j),
				}
				jsonData, _ := json.Marshal(data)

				_, err := client.Append(ctx, "test:v1:shard:0001", [][]byte{jsonData})
				if err != nil {
					errorCount.Add(1)
					t.Logf("Write error: %v", err)
				} else {
					writeCount.Add(1)
				}

				// Small random delay
				time.Sleep(time.Duration(rand.Intn(10)) * time.Microsecond)
			}
		}(i)
	}

	// Start readers that continuously read and validate data
	readerWg := &sync.WaitGroup{}
	for i := 0; i < numReaders; i++ {
		readerWg.Add(1)
		go func(readerID int) {
			defer readerWg.Done()
			consumer := NewConsumer(client, ConsumerOptions{
				Group: fmt.Sprintf("reader-%d", readerID),
			})
			defer consumer.Close()

			for ctx.Err() == nil {
				messages, err := consumer.Read(ctx, []uint32{1}, 10)
				if err != nil {
					if ctx.Err() == nil {
						// Some errors are expected in this stress test
						// Only fail on unexpected errors
						if !strings.Contains(err.Error(), "file not memory mapped") {
							errorCount.Add(1)
							// Log first few errors
							if errorCount.Load() <= 3 {
								t.Logf("Read error: %v", err)
							}
						}
					}
					continue
				}

				// Validate each message
				for _, msg := range messages {
					// Add delay to increase chance of memory being unmapped
					time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)

					// This should not segfault even if the underlying memory was unmapped
					var decoded map[string]interface{}
					if err := json.Unmarshal(msg.Data, &decoded); err != nil {
						errorCount.Add(1)
						t.Logf("Failed to unmarshal data: %v", err)
					} else {
						readCount.Add(1)

						// Verify data integrity
						if _, ok := decoded["writer"]; !ok {
							t.Errorf("Missing writer field in decoded data")
						}
						if _, ok := decoded["seq"]; !ok {
							t.Errorf("Missing seq field in decoded data")
						}
					}

					// Ack the message
					consumer.Ack(ctx, msg.ID)
				}

				// Force GC occasionally to stress memory management
				if rand.Intn(100) < 5 {
					runtime.GC()
				}
			}
		}(i)
	}

	// Let it run for the test duration
	<-ctx.Done()

	// Wait for writers to finish
	writerWg.Wait()

	// Give readers a bit more time to process remaining messages
	time.Sleep(100 * time.Millisecond)
	cancel() // Cancel context to stop readers
	readerWg.Wait()

	t.Logf("Writes: %d, Reads: %d, Errors: %d", writeCount.Load(), readCount.Load(), errorCount.Load())

	if errorCount.Load() > 0 {
		t.Errorf("Encountered %d errors during concurrent test", errorCount.Load())
	}

	if readCount.Load() == 0 {
		t.Error("No successful reads completed")
	}
}

// TestReaderDataValidityAfterUnmap ensures data remains valid after the source file is unmapped
func TestReaderDataValidityAfterUnmap(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 512 // Very small files
	// Reader will handle memory mapping internally

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write entries across multiple files
	var storedData []map[string]interface{}
	for i := 0; i < 20; i++ {
		data := map[string]interface{}{
			"id":   i,
			"data": fmt.Sprintf("test-entry-%d", i),
		}
		storedData = append(storedData, data)

		jsonData, _ := json.Marshal(data)
		_, err := client.Append(ctx, "test:v1:shard:0001", [][]byte{jsonData})
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	// Read all messages but don't process them immediately
	var messages []StreamMessage
	for len(messages) < len(storedData) {
		batch, err := consumer.Read(ctx, []uint32{1}, 5)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}
		messages = append(messages, batch...)
	}

	// Force unmapping by reading from a different shard to trigger cleanup
	consumer2 := NewConsumer(client, ConsumerOptions{Group: "test2"})
	consumer2.Read(ctx, []uint32{2}, 1)
	consumer2.Close()

	// Now process the messages - the data should still be valid
	for i, msg := range messages {
		var decoded map[string]interface{}
		if err := json.Unmarshal(msg.Data, &decoded); err != nil {
			t.Errorf("Failed to unmarshal message %d: %v", i, err)
			continue
		}

		// Verify the data matches what we wrote
		id := int(decoded["id"].(float64))
		expectedData := storedData[id]["data"].(string)
		if decoded["data"] != expectedData {
			t.Errorf("Data mismatch for entry %d: got %v, want %v", id, decoded["data"], expectedData)
		}
	}
}

// TestReaderMemoryPressure tests reader behavior under memory pressure
func TestReaderMemoryPressure(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 1024 // 1KB files
	// Reader will manage memory internally

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write many entries to create many files
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		// Create varying sizes of data
		dataSize := 50 + rand.Intn(100)
		data := map[string]interface{}{
			"id":   i,
			"data": string(make([]byte, dataSize)),
			"size": dataSize,
		}
		jsonData, _ := json.Marshal(data)

		_, err := client.Append(ctx, fmt.Sprintf("test:v1:shard:%04d", i%4), [][]byte{jsonData})
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	// Create multiple consumers reading from different shards concurrently
	var wg sync.WaitGroup
	for shard := uint32(0); shard < 4; shard++ {
		wg.Add(1)
		go func(shardID uint32) {
			defer wg.Done()

			consumer := NewConsumer(client, ConsumerOptions{
				Group: fmt.Sprintf("consumer-%d", shardID),
			})
			defer consumer.Close()

			totalRead := 0
			for totalRead < numEntries/4 {
				messages, err := consumer.Read(ctx, []uint32{shardID}, 5)
				if err != nil {
					t.Errorf("Read error on shard %d: %v", shardID, err)
					break
				}

				for _, msg := range messages {
					// Verify data can be decoded
					var decoded map[string]interface{}
					if err := json.Unmarshal(msg.Data, &decoded); err != nil {
						t.Errorf("Failed to decode message on shard %d: %v", shardID, err)
					}
				}

				totalRead += len(messages)
				consumer.Ack(ctx, getMessageIDs(messages)...)
			}
		}(shard)
	}

	wg.Wait()
}

// TestReaderDelayedDataAccess tests that data remains valid even with delays
func TestReaderDelayedDataAccess(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 2048

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write test data
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		data := map[string]interface{}{
			"id":      i,
			"content": fmt.Sprintf("delayed-access-test-%d", i),
		}
		jsonData, _ := json.Marshal(data)
		_, err := client.Append(ctx, "test:v1:shard:0001", [][]byte{jsonData})
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "delayed"})
	defer consumer.Close()

	// Read messages in batches with delays
	totalRead := 0
	for totalRead < numEntries {
		messages, err := consumer.Read(ctx, []uint32{1}, 10)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		// Store message data
		type delayedMessage struct {
			data []byte
			id   MessageID
		}
		var delayedMessages []delayedMessage

		for _, msg := range messages {
			// Store a reference to the data
			delayedMessages = append(delayedMessages, delayedMessage{
				data: msg.Data,
				id:   msg.ID,
			})
		}

		// Add significant delay
		time.Sleep(50 * time.Millisecond)

		// Force some file operations to potentially trigger remapping
		client.Append(ctx, "test:v1:shard:0002", [][]byte{[]byte("trigger-remap")})

		// Now access the delayed data - should not segfault
		for _, dm := range delayedMessages {
			var decoded map[string]interface{}
			if err := json.Unmarshal(dm.data, &decoded); err != nil {
				t.Errorf("Failed to decode delayed message: %v", err)
			}
			consumer.Ack(ctx, dm.id)
		}

		totalRead += len(messages)
	}
}

// TestReaderMixedCompressionSafety tests safety with mixed compressed/uncompressed data
func TestReaderMixedCompressionSafety(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 4096
	config.Compression.MinCompressSize = 100 // Only compress larger entries

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write mixed compressed and uncompressed data
	for i := 0; i < 100; i++ {
		var data map[string]interface{}
		if i%2 == 0 {
			// Small data - won't be compressed
			data = map[string]interface{}{
				"id":   i,
				"type": "small",
				"data": "x",
			}
		} else {
			// Large data - will be compressed
			data = map[string]interface{}{
				"id":   i,
				"type": "large",
				"data": string(make([]byte, 200)),
			}
		}

		jsonData, _ := json.Marshal(data)
		_, err := client.Append(ctx, "test:v1:shard:0001", [][]byte{jsonData})
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	// Read concurrently from multiple consumers
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			consumer := NewConsumer(client, ConsumerOptions{
				Group: fmt.Sprintf("mixed-%d", consumerID),
			})
			defer consumer.Close()

			totalRead := 0
			for totalRead < 20 { // Each consumer reads 20 entries
				messages, err := consumer.Read(ctx, []uint32{1}, 5)
				if err != nil {
					t.Errorf("Read error: %v", err)
					break
				}

				for _, msg := range messages {
					// Decode and verify
					var decoded map[string]interface{}
					if err := json.Unmarshal(msg.Data, &decoded); err != nil {
						t.Errorf("Failed to decode: %v", err)
						continue
					}

					// Verify data integrity
					id := int(decoded["id"].(float64))
					expectedType := "small"
					if id%2 == 1 {
						expectedType = "large"
					}
					if decoded["type"] != expectedType {
						t.Errorf("Wrong type for entry %d: got %v, want %v",
							id, decoded["type"], expectedType)
					}
				}

				totalRead += len(messages)
				consumer.Ack(ctx, getMessageIDs(messages)...)
			}
		}(i)
	}

	wg.Wait()
}

// Helper function to extract message IDs
func getMessageIDs(messages []StreamMessage) []MessageID {
	ids := make([]MessageID, len(messages))
	for i, msg := range messages {
		ids[i] = msg.ID
	}
	return ids
}
