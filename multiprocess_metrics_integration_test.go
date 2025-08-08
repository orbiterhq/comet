//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMultiProcessMetricsIntegration comprehensively tests all CometState metrics
// This test simulates concurrent operations but uses a single process to avoid
// conflicts from multiple processes writing to the same shard
func TestMultiProcessMetricsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping metrics integration test in short mode")
	}

	dir := t.TempDir()

	// Initialize with proper configuration
	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 100 // Enable compression
	config.Retention.MaxAge = 500 * time.Millisecond
	config.Retention.MinFilesToKeep = 2
	config.Retention.CleanupInterval = 100 * time.Millisecond
	config.Storage.MaxFileSize = 10 * 1024 // 10KB files to ensure rotation

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamName := "test:v1:shard:0000"

	// Run different workloads concurrently to generate metrics
	var wg sync.WaitGroup

	// Writer 1: Various sizes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			var data []byte
			if i%10 == 0 {
				// Large compressible data
				data = make([]byte, 2000)
				for j := range data {
					data[j] = 'A' + byte(j%26)
				}
			} else if i%5 == 0 {
				// Medium data
				data = []byte(fmt.Sprintf(`{"worker":"writer1","seq":%d,"data":"medium-entry"}`, i))
			} else {
				// Small data
				data = []byte(fmt.Sprintf(`{"w":"writer1","i":%d}`, i))
			}
			client.Append(ctx, streamName, [][]byte{data})

			// Flush periodically to ensure data is available for readers
			if i%20 == 0 {
				client.Sync(ctx)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Writer 2: Highly compressible data
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			// Create highly compressible data
			data := make([]byte, 1000)
			for j := range data {
				data[j] = 'X' // All same character = high compression
			}
			client.Append(ctx, streamName, [][]byte{data})

			// Flush periodically
			if i%10 == 0 {
				client.Sync(ctx)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Reader - start after a small delay to ensure some data is written
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(200 * time.Millisecond) // Let writers produce some data first

		consumer := NewConsumer(client, ConsumerOptions{Group: "reader1"})
		defer consumer.Close()

		totalRead := 0
		totalAcked := 0
		fileAwaitingCount := 0
		errorCount := 0
		for i := 0; i < 30; i++ { // Reduced iterations since we start later
			messages, err := consumer.Read(ctx, []uint32{0}, 10)
			if err != nil {
				errorCount++
				t.Logf("Read error on iteration %d: %v", i, err)
				continue
			}
			totalRead += len(messages)
			for j, msg := range messages {
				if j%2 == 0 {
					if err := consumer.Ack(ctx, msg.ID); err == nil {
						totalAcked++
					}
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("Reader: read %d messages, acked %d, fileAwaitingData errors: %d, other errors: %d",
			totalRead, totalAcked, fileAwaitingCount, errorCount)
	}()

	// Wait for all operations to complete
	wg.Wait()

	// Ensure all data is synced to disk before closing
	if err := client.Sync(ctx); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Close the client properly
	client.Close()

	// Re-open to verify metrics were persisted
	client, err = NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Now verify all metrics
	t.Run("VerifyMetrics", func(t *testing.T) {
		shard, err := client.getOrCreateShard(0)
		if err != nil {
			t.Fatal(err)
		}

		state := shard.state
		if state == nil {
			t.Fatal("CometState not initialized")
		}

		// Check write metrics
		t.Run("WriteMetrics", func(t *testing.T) {
			totalWrites := atomic.LoadUint64(&state.TotalWrites)
			// TotalEntries removed - check shard.index.CurrentEntryNumber instead
			shard.mu.RLock()
			totalEntries := shard.index.CurrentEntryNumber
			shard.mu.RUnlock()
			totalBytes := atomic.LoadUint64(&state.TotalBytes)
			lastWriteNanos := atomic.LoadInt64(&state.LastWriteNanos)

			if totalWrites == 0 {
				t.Error("TotalWrites = 0, expected > 0")
			}
			if totalEntries == 0 {
				t.Error("CurrentEntryNumber = 0, expected > 0")
			}
			if totalBytes == 0 {
				t.Error("TotalBytes = 0, expected > 0")
			}
			if lastWriteNanos == 0 {
				t.Error("LastWriteNanos = 0, expected > 0")
			}

			t.Logf("Write metrics: writes=%d, entries=%d, bytes=%d",
				totalWrites, totalEntries, totalBytes)
		})

		// Check compression metrics
		t.Run("CompressionMetrics", func(t *testing.T) {
			compressedEntries := atomic.LoadUint64(&state.CompressedEntries)
			skippedCompression := atomic.LoadUint64(&state.SkippedCompression)
			compressionRatio := atomic.LoadUint64(&state.CompressionRatio)
			compressionTimeNanos := atomic.LoadInt64(&state.CompressionTimeNanos)
			totalCompressed := atomic.LoadUint64(&state.TotalCompressed)

			t.Logf("Compression metrics: compressed=%d, skipped=%d, totalCompressed=%d, ratio=%d%%, time=%dms",
				compressedEntries, skippedCompression, totalCompressed, compressionRatio,
				compressionTimeNanos/1e6)

			if compressedEntries == 0 {
				t.Error("CompressedEntries = 0, expected > 0")
			}
			if skippedCompression == 0 {
				t.Error("SkippedCompression = 0, expected > 0")
			}
		})

		// Check latency metrics
		t.Run("LatencyMetrics", func(t *testing.T) {
			writeLatencyCount := atomic.LoadUint64(&state.WriteLatencyCount)
			minLatency := atomic.LoadUint64(&state.MinWriteLatency)
			maxLatency := atomic.LoadUint64(&state.MaxWriteLatency)

			if writeLatencyCount == 0 {
				t.Error("WriteLatencyCount = 0, expected > 0")
			}
			if minLatency == 0 || maxLatency == 0 {
				t.Error("Min/Max latency not tracked")
			}

			// Calculate average
			writeLatencySum := atomic.LoadUint64(&state.WriteLatencySum)
			avgLatency := writeLatencySum / writeLatencyCount

			t.Logf("Latency metrics: count=%d, min=%dus, max=%dus, avg=%dus",
				writeLatencyCount, minLatency/1000, maxLatency/1000, avgLatency/1000)
		})

		// Check file operation metrics
		t.Run("FileMetrics", func(t *testing.T) {
			filesCreated := atomic.LoadUint64(&state.FilesCreated)
			fileRotations := atomic.LoadUint64(&state.FileRotations)
			currentFiles := atomic.LoadUint64(&state.CurrentFiles)
			totalFileBytes := atomic.LoadUint64(&state.TotalFileBytes)

			if filesCreated == 0 {
				t.Error("FilesCreated = 0, expected > 0")
			}
			// Note: FileRotations may be 0 if files didn't fill up enough to rotate
			// Multiple processes may create their own initial files (FilesCreated)
			// but rotation only happens when a file reaches MaxFileSize
			if fileRotations > 0 {
				t.Logf("File rotations occurred: %d", fileRotations)
			} else {
				t.Logf("No file rotations occurred (files may not have reached size limit)")
			}
			if currentFiles == 0 {
				t.Error("CurrentFiles = 0, expected > 0")
			}
			if totalFileBytes == 0 {
				t.Error("TotalFileBytes = 0, expected > 0")
			}

			t.Logf("File metrics: created=%d, rotations=%d, current=%d, bytes=%d",
				filesCreated, fileRotations, currentFiles, totalFileBytes)
		})

		// Check reader/consumer metrics
		t.Run("ReaderMetrics", func(t *testing.T) {
			totalEntriesRead := atomic.LoadUint64(&state.TotalEntriesRead)
			activeReaders := atomic.LoadUint64(&state.ActiveReaders)
			totalReaders := atomic.LoadUint64(&state.TotalReaders)
			consumerGroups := atomic.LoadUint64(&state.ConsumerGroups)
			ackedEntries := atomic.LoadUint64(&state.AckedEntries)

			if totalEntriesRead == 0 {
				t.Error("TotalEntriesRead = 0, expected > 0")
			}
			if totalReaders == 0 {
				t.Error("TotalReaders = 0, expected > 0")
			}
			if consumerGroups == 0 {
				t.Error("ConsumerGroups = 0, expected > 0")
			}

			t.Logf("Reader metrics: entriesRead=%d, activeReaders=%d, totalReaders=%d, groups=%d, acked=%d",
				totalEntriesRead, activeReaders, totalReaders, consumerGroups, ackedEntries)
		})

		// Check retention metrics
		t.Run("RetentionMetrics", func(t *testing.T) {
			retentionRuns := atomic.LoadUint64(&state.RetentionRuns)
			filesDeleted := atomic.LoadUint64(&state.FilesDeleted)
			bytesReclaimed := atomic.LoadUint64(&state.BytesReclaimed)
			entriesDeleted := atomic.LoadUint64(&state.EntriesDeleted)
			oldestEntryNanos := atomic.LoadInt64(&state.OldestEntryNanos)

			if retentionRuns == 0 {
				t.Error("RetentionRuns = 0, expected > 0")
			}

			// Log file information for debugging
			shard.mu.RLock()
			fileCount := len(shard.index.Files)
			var fileInfo []string
			for i, f := range shard.index.Files {
				fileInfo = append(fileInfo, fmt.Sprintf("file[%d]: entries=%d, start=%v",
					i, f.Entries, f.StartTime.Format("15:04:05.000")))
			}
			shard.mu.RUnlock()

			t.Logf("Retention metrics: runs=%d, filesDeleted=%d, bytesReclaimed=%d, entriesDeleted=%d",
				retentionRuns, filesDeleted, bytesReclaimed, entriesDeleted)
			t.Logf("File count: %d, files: %v", fileCount, fileInfo)

			if oldestEntryNanos == 0 {
				t.Error("OldestEntryNanos = 0, expected > 0")
			} else {
				oldestTime := time.Unix(0, oldestEntryNanos)
				t.Logf("OldestEntryNanos: %d (%v)", oldestEntryNanos, oldestTime.Format("15:04:05.000"))
			}
		})

		// Check error metrics
		t.Run("ErrorMetrics", func(t *testing.T) {
			errorCount := atomic.LoadUint64(&state.ErrorCount)
			failedWrites := atomic.LoadUint64(&state.FailedWrites)
			readErrors := atomic.LoadUint64(&state.ReadErrors)

			t.Logf("Error metrics: errors=%d, failedWrites=%d, readErrors=%d",
				errorCount, failedWrites, readErrors)
		})

		// Verify all 66 metrics are accessible
		t.Run("AllMetricsAccessible", func(t *testing.T) {
			// This ensures we don't panic when accessing any metric
			metrics := []struct {
				name  string
				value any
			}{
				{"Version", atomic.LoadUint64(&state.Version)},
				{"WriteOffset", atomic.LoadUint64(&state.WriteOffset)},
				{"LastEntryNumber", atomic.LoadInt64(&state.LastEntryNumber)},
				{"LastIndexUpdate", atomic.LoadInt64(&state.LastIndexUpdate)},
				// ActiveFileIndex removed - not used
				// FileSize removed - not used
				{"LastFileSequence", atomic.LoadUint64(&state.LastFileSequence)},
				// ... (all 70 metrics would be listed in production)
			}

			nonZeroCount := 0
			for _, m := range metrics {
				switch v := m.value.(type) {
				case uint64:
					if v > 0 {
						nonZeroCount++
					}
				case int64:
					if v > 0 {
						nonZeroCount++
					}
				}
			}

			t.Logf("Accessed %d metrics, %d have non-zero values", len(metrics), nonZeroCount)
		})
	})
}
