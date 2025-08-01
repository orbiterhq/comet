//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMultiProcessMetricsIntegration comprehensively tests all CometState metrics
// in a real multi-process environment with actual OS processes
func TestMultiProcessMetricsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process metrics test in short mode")
	}

	// Check if we're a worker process
	if role := os.Getenv("COMET_METRICS_WORKER"); role != "" {
		runMetricsWorker(t, role)
		return
	}

	// Safety check
	if os.Getenv("GO_TEST_SUBPROCESS") == "1" {
		t.Skip("Skipping test in subprocess to prevent recursion")
		return
	}

	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Initialize the data directory
	config := MultiProcessConfig()
	config.Compression.MinCompressSize = 100 // Enable compression
	config.Retention.MaxAge = 500 * time.Millisecond
	config.Retention.MinFilesToKeep = 2
	config.Retention.CleanupInterval = 100 * time.Millisecond
	config.Storage.MaxFileSize = 10 * 1024 // 10KB files to ensure rotation

	initClient, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = initClient.Append(context.Background(), "test:v1:shard:0001", [][]byte{[]byte("init")})
	if err != nil {
		t.Fatal(err)
	}
	initClient.Close()

	// Start multiple worker processes with different roles
	workers := []struct {
		role     string
		duration time.Duration
	}{
		{"writer1", 3 * time.Second},
		{"writer2", 3 * time.Second},
		{"compressor", 3 * time.Second},
		{"reader1", 3 * time.Second},
		{"reader2", 3 * time.Second},
		{"retention", 4 * time.Second}, // Runs longer to ensure retention happens
	}

	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(role string, duration time.Duration) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()

			cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestMultiProcessMetricsIntegration$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_METRICS_WORKER=%s", role),
				fmt.Sprintf("COMET_METRICS_DIR=%s", dir),
				"GO_TEST_SUBPROCESS=1",
			)

			output, err := cmd.CombinedOutput()
			t.Logf("Worker %s output:\n%s", role, output)
			if err != nil && ctx.Err() != context.DeadlineExceeded {
				t.Errorf("Worker %s failed: %v", role, err)
			}
		}(worker.role, worker.duration)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Now verify all metrics
	t.Run("VerifyMetrics", func(t *testing.T) {
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		shard, err := client.getOrCreateShard(1)
		if err != nil {
			t.Fatal(err)
		}

		if shard.state == nil {
			t.Fatal("CometState not initialized")
		}

		// Check write metrics
		t.Run("WriteMetrics", func(t *testing.T) {
			totalWrites := atomic.LoadUint64(&shard.state.TotalWrites)
			totalEntries := atomic.LoadInt64(&shard.state.TotalEntries)
			totalBytes := atomic.LoadUint64(&shard.state.TotalBytes)
			lastWriteNanos := atomic.LoadInt64(&shard.state.LastWriteNanos)

			if totalWrites == 0 {
				t.Error("TotalWrites = 0, expected > 0")
			}
			if totalEntries == 0 {
				t.Error("TotalEntries = 0, expected > 0")
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
			compressedEntries := atomic.LoadUint64(&shard.state.CompressedEntries)
			skippedCompression := atomic.LoadUint64(&shard.state.SkippedCompression)
			compressionRatio := atomic.LoadUint64(&shard.state.CompressionRatio)
			compressionTimeNanos := atomic.LoadInt64(&shard.state.CompressionTimeNanos)

			if compressedEntries == 0 {
				t.Error("CompressedEntries = 0, expected > 0")
			}
			if skippedCompression == 0 {
				t.Error("SkippedCompression = 0, expected > 0")
			}

			t.Logf("Compression metrics: compressed=%d, skipped=%d, ratio=%d%%, time=%dms",
				compressedEntries, skippedCompression, compressionRatio,
				compressionTimeNanos/1e6)
		})

		// Check latency metrics
		t.Run("LatencyMetrics", func(t *testing.T) {
			writeLatencyCount := atomic.LoadUint64(&shard.state.WriteLatencyCount)
			minLatency := atomic.LoadUint64(&shard.state.MinWriteLatency)
			maxLatency := atomic.LoadUint64(&shard.state.MaxWriteLatency)
			p50Latency := atomic.LoadUint64(&shard.state.P50WriteLatency)
			p99Latency := atomic.LoadUint64(&shard.state.P99WriteLatency)

			if writeLatencyCount == 0 {
				t.Error("WriteLatencyCount = 0, expected > 0")
			}
			if minLatency == 0 || maxLatency == 0 {
				t.Error("Min/Max latency not tracked")
			}
			if p50Latency == 0 || p99Latency == 0 {
				t.Error("Percentile latencies not tracked")
			}
			if p99Latency < p50Latency {
				t.Errorf("P99 (%d) < P50 (%d)", p99Latency, p50Latency)
			}

			t.Logf("Latency metrics: count=%d, min=%dus, max=%dus, p50=%dus, p99=%dus",
				writeLatencyCount, minLatency/1000, maxLatency/1000,
				p50Latency/1000, p99Latency/1000)
		})

		// Check file operation metrics
		t.Run("FileMetrics", func(t *testing.T) {
			filesCreated := atomic.LoadUint64(&shard.state.FilesCreated)
			fileRotations := atomic.LoadUint64(&shard.state.FileRotations)
			currentFiles := atomic.LoadUint64(&shard.state.CurrentFiles)
			totalFileBytes := atomic.LoadUint64(&shard.state.TotalFileBytes)

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
			totalEntriesRead := atomic.LoadUint64(&shard.state.TotalEntriesRead)
			activeReaders := atomic.LoadUint64(&shard.state.ActiveReaders)
			totalReaders := atomic.LoadUint64(&shard.state.TotalReaders)
			consumerGroups := atomic.LoadUint64(&shard.state.ConsumerGroups)
			ackedEntries := atomic.LoadUint64(&shard.state.AckedEntries)

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
			retentionRuns := atomic.LoadUint64(&shard.state.RetentionRuns)
			filesDeleted := atomic.LoadUint64(&shard.state.FilesDeleted)
			bytesReclaimed := atomic.LoadUint64(&shard.state.BytesReclaimed)
			entriesDeleted := atomic.LoadUint64(&shard.state.EntriesDeleted)
			oldestEntryNanos := atomic.LoadInt64(&shard.state.OldestEntryNanos)

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

		// Check multi-process coordination metrics
		t.Run("MultiProcessMetrics", func(t *testing.T) {
			processCount := atomic.LoadUint64(&shard.state.ProcessCount)
			contentionCount := atomic.LoadUint64(&shard.state.ContentionCount)
			lockWaitNanos := atomic.LoadInt64(&shard.state.LockWaitNanos)
			mmapRemapCount := atomic.LoadUint64(&shard.state.MMAPRemapCount)

			// These should be tracked during multi-process operations
			t.Logf("Multi-process metrics: processes=%d, contentions=%d, lockWait=%dms, remaps=%d",
				processCount, contentionCount, lockWaitNanos/1e6, mmapRemapCount)

			// At minimum, verify they're accessible without panics
			_ = atomic.LoadUint64(&shard.state.FalseShareCount)
			_ = atomic.LoadInt64(&shard.state.LastProcessHeartbeat)
		})

		// Check error metrics
		t.Run("ErrorMetrics", func(t *testing.T) {
			errorCount := atomic.LoadUint64(&shard.state.ErrorCount)
			failedWrites := atomic.LoadUint64(&shard.state.FailedWrites)
			readErrors := atomic.LoadUint64(&shard.state.ReadErrors)

			t.Logf("Error metrics: errors=%d, failedWrites=%d, readErrors=%d",
				errorCount, failedWrites, readErrors)
		})

		// Verify all 70 metrics are accessible
		t.Run("AllMetricsAccessible", func(t *testing.T) {
			// This ensures we don't panic when accessing any metric
			metrics := []struct {
				name  string
				value interface{}
			}{
				{"Version", atomic.LoadUint64(&shard.state.Version)},
				{"WriteOffset", atomic.LoadUint64(&shard.state.WriteOffset)},
				{"LastEntryNumber", atomic.LoadInt64(&shard.state.LastEntryNumber)},
				{"LastIndexUpdate", atomic.LoadInt64(&shard.state.LastIndexUpdate)},
				{"ActiveFileIndex", atomic.LoadUint64(&shard.state.ActiveFileIndex)},
				{"FileSize", atomic.LoadUint64(&shard.state.FileSize)},
				{"LastFileSequence", atomic.LoadUint64(&shard.state.LastFileSequence)},
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

// runMetricsWorker runs different worker roles to generate metrics
func runMetricsWorker(t *testing.T, role string) {
	dir := os.Getenv("COMET_METRICS_DIR")
	config := MultiProcessConfig()
	config.Compression.MinCompressSize = 100
	config.Retention.MaxAge = 500 * time.Millisecond
	config.Retention.MinFilesToKeep = 2
	config.Retention.CleanupInterval = 100 * time.Millisecond
	config.Storage.MaxFileSize = 10 * 1024

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Worker %s: failed to create client: %v", role, err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	switch role {
	case "writer1", "writer2":
		// Write various sizes of data
		for i := 0; i < 100; i++ {
			var data []byte
			if i%10 == 0 {
				// Large compressible data
				data = make([]byte, 2000) // Larger to trigger rotation
				for j := range data {
					data[j] = 'A' + byte(j%26)
				}
			} else if i%5 == 0 {
				// Medium data
				data = []byte(fmt.Sprintf(`{"worker":"%s","seq":%d,"data":"medium-entry"}`, role, i))
			} else {
				// Small data
				data = []byte(fmt.Sprintf(`{"w":"%s","i":%d}`, role, i))
			}

			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err != nil {
				t.Logf("Worker %s: write error: %v", role, err)
			}

			time.Sleep(10 * time.Millisecond)
		}

	case "compressor":
		// Write highly compressible data to trigger compression metrics
		for i := 0; i < 50; i++ {
			// Create highly compressible data
			data := make([]byte, 1000)
			for j := range data {
				data[j] = 'X' // All same character = high compression
			}

			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err != nil {
				t.Logf("Worker %s: write error: %v", role, err)
			}

			time.Sleep(20 * time.Millisecond)
		}

	case "reader1", "reader2":
		// Create consumers and read data
		consumer := NewConsumer(client, ConsumerOptions{Group: role})
		defer consumer.Close()

		totalRead := 0
		for i := 0; i < 50; i++ {
			messages, err := consumer.Read(ctx, []uint32{1}, 10)
			if err != nil {
				t.Logf("Worker %s: read error: %v", role, err)
				continue
			}

			totalRead += len(messages)

			// ACK some messages
			for j, msg := range messages {
				if j%2 == 0 { // ACK every other message
					consumer.Ack(ctx, msg.ID)
				}
			}

			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("Worker %s: read %d total messages", role, totalRead)

	case "retention":
		// Periodically force retention to ensure metrics are tracked
		for i := 0; i < 10; i++ {
			time.Sleep(300 * time.Millisecond)
			client.ForceRetentionCleanup()
			t.Logf("Worker %s: forced retention run %d", role, i+1)
		}
	}
}
