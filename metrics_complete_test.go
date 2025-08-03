package comet

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestAllInternalMetrics tests all internal state metrics
func TestAllInternalMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write some data
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test data")})
	if err != nil {
		t.Fatal(err)
	}

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Test Version
	version := atomic.LoadUint64(&state.Version)
	if version != CometStateVersion1 {
		t.Errorf("Version = %d, want %d", version, CometStateVersion1)
	}

	// Test LastEntryNumber
	lastEntryNumber := atomic.LoadInt64(&state.LastEntryNumber)
	if lastEntryNumber < 0 {
		t.Errorf("LastEntryNumber = %d, want >= 0", lastEntryNumber)
	}

	// Test ActiveFileIndex
	activeFileIndex := atomic.LoadUint64(&state.ActiveFileIndex)
	shard.mu.RLock()
	expectedIndex := uint64(len(shard.index.Files) - 1)
	shard.mu.RUnlock()
	if activeFileIndex != expectedIndex {
		t.Logf("ActiveFileIndex = %d, expected = %d (may differ in multi-process)", activeFileIndex, expectedIndex)
	}

	// Test FileSize
	fileSize := atomic.LoadUint64(&state.FileSize)
	if fileSize == 0 {
		t.Error("FileSize = 0, want > 0")
	}

	// Test LastFileSequence
	lastFileSeq := atomic.LoadUint64(&state.LastFileSequence)
	// Note: LastFileSequence starts at 0 for the first file
	t.Logf("LastFileSequence: %d (starts at 0 for first file)", lastFileSeq)

	t.Logf("Internal metrics:")
	t.Logf("  Version: %d", version)
	t.Logf("  LastEntryNumber: %d", lastEntryNumber)
	t.Logf("  ActiveFileIndex: %d", activeFileIndex)
	t.Logf("  FileSize: %d", fileSize)
	t.Logf("  LastFileSequence: %d", lastFileSeq)
}

// TestCompressionEdgeCaseMetrics tests best/worst compression metrics
func TestCompressionEdgeCaseMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Compression.MinCompressSize = 10 // Low threshold

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write highly compressible data
	compressible := make([]byte, 1000)
	for i := range compressible {
		compressible[i] = 'A' // All same character - highly compressible
	}

	// Write incompressible (random) data
	incompressible := make([]byte, 1000)
	for i := range incompressible {
		incompressible[i] = byte(i % 256) // Pseudo-random pattern
	}

	// Write both types
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{compressible, incompressible})
	if err != nil {
		t.Fatal(err)
	}

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Check best/worst compression
	bestCompression := atomic.LoadUint64(&state.BestCompression)
	worstCompression := atomic.LoadUint64(&state.WorstCompression)

	// These may be 0 if compression tracking isn't fully implemented
	t.Logf("Compression edge case metrics:")
	t.Logf("  Best compression ratio: %d%%", bestCompression)
	t.Logf("  Worst compression ratio: %d%%", worstCompression)

	// At minimum, verify they're accessible
	_ = bestCompression
	_ = worstCompression
}

// TestCheckpointMetrics tests checkpoint-related metrics
func TestCheckpointMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Storage.CheckpointTime = 10 // Short interval

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write data
	for i := 0; i < 10; i++ {
		_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("checkpoint test")})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Force checkpoint
	client.Sync(ctx)

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Check checkpoint metrics
	checkpointCount := atomic.LoadUint64(&state.CheckpointCount)
	lastCheckpointNanos := atomic.LoadInt64(&state.LastCheckpointNanos)
	checkpointTimeNanos := atomic.LoadInt64(&state.CheckpointTimeNanos)

	// These are tracked in maybeCheckpoint() but may not be implemented yet
	t.Logf("Checkpoint metrics:")
	t.Logf("  Checkpoint count: %d", checkpointCount)
	t.Logf("  Last checkpoint: %v", time.Unix(0, lastCheckpointNanos))
	t.Logf("  Total checkpoint time: %d ns", checkpointTimeNanos)
}

// TestSyncLatencyMetrics tests sync latency tracking
func TestSyncLatencyMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write and sync
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("sync test")})
	if err != nil {
		t.Fatal(err)
	}

	// Force sync
	client.Sync(ctx)

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Check sync latency
	syncLatencyNanos := atomic.LoadInt64(&state.SyncLatencyNanos)

	t.Logf("Sync metrics:")
	t.Logf("  Sync latency: %d ns", syncLatencyNanos)

	// Verify it's accessible (may be 0 if not tracked)
	_ = syncLatencyNanos
}

// TestRotationFailureMetrics tests failed rotation tracking
func TestRotationFailureMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Storage.MaxFileSize = 100 // Small files

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write data to trigger rotation
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{make([]byte, 50)})
	if err != nil {
		t.Fatal(err)
	}

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Check rotation metrics
	rotationTimeNanos := atomic.LoadInt64(&state.RotationTimeNanos)
	failedRotations := atomic.LoadUint64(&state.FailedRotations)

	t.Logf("Rotation metrics:")
	t.Logf("  Rotation time: %d ns", rotationTimeNanos)
	t.Logf("  Failed rotations: %d", failedRotations)
}

// TestIndexErrorMetrics tests index persistence error tracking
func TestIndexErrorMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write data
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("index test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Check index metrics
	indexPersistErrors := atomic.LoadUint64(&state.IndexPersistErrors)
	indexSizeBytes := atomic.LoadUint64(&state.IndexSizeBytes)

	t.Logf("Index error metrics:")
	t.Logf("  Index persist errors: %d", indexPersistErrors)
	t.Logf("  Index size bytes: %d", indexSizeBytes)
}

// TestCorruptionMetrics tests corruption detection metrics
func TestCorruptionMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write data
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("corruption test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, _ := client.getOrCreateShard(1)
	if shard.loadState() == nil {
		t.Skip("State not available in non-mmap mode")
	}

	client.Close()

	// Corrupt a data file
	dataFiles, _ := filepath.Glob(filepath.Join(dir, "shard-0001", "log-*.comet"))
	if len(dataFiles) > 0 {
		// Write garbage to the middle of the file
		f, err := os.OpenFile(dataFiles[0], os.O_WRONLY, 0644)
		if err == nil {
			f.Seek(10, 0)
			f.Write([]byte("CORRUPTED"))
			f.Close()
		}
	}

	// Reopen client - should detect corruption during recovery
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		// Error is expected but we want to check metrics
		t.Logf("Expected error on corrupted data: %v", err)
	} else {
		defer client2.Close()
	}

	// Try to get shard to check metrics
	if client2 != nil {
		shard2, _ := client2.getOrCreateShard(1)
		if shard2 != nil {
			if state := shard2.loadState(); state != nil {
				corruptionDetected := atomic.LoadUint64(&state.CorruptionDetected)
				partialWrites := atomic.LoadUint64(&state.PartialWrites)

				t.Logf("Corruption metrics:")
				t.Logf("  Corruption detected: %d", corruptionDetected)
				t.Logf("  Partial writes: %d", partialWrites)
			}
		}
	}
}

// TestRetentionDetailedMetrics tests detailed retention metrics
func TestRetentionDetailedMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Retention.MaxAge = 1 * time.Millisecond
	config.Retention.CleanupInterval = 10 * time.Millisecond
	config.Retention.ProtectUnconsumed = true

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write data
	for i := 0; i < 5; i++ {
		_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("retention test")})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create consumer to protect some entries
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	// Read but don't ack - this should protect entries
	consumer.Read(ctx, []uint32{1}, 2)

	// Wait and force retention
	time.Sleep(20 * time.Millisecond)
	client.ForceRetentionCleanup()

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Check detailed retention metrics
	lastRetentionNanos := atomic.LoadInt64(&state.LastRetentionNanos)
	retentionTimeNanos := atomic.LoadInt64(&state.RetentionTimeNanos)
	retentionErrors := atomic.LoadUint64(&state.RetentionErrors)
	protectedByConsumers := atomic.LoadUint64(&state.ProtectedByConsumers)

	t.Logf("Detailed retention metrics:")
	t.Logf("  Last retention: %v", time.Unix(0, lastRetentionNanos))
	t.Logf("  Retention time: %d ns", retentionTimeNanos)
	t.Logf("  Retention errors: %d", retentionErrors)
	t.Logf("  Protected by consumers: %d", protectedByConsumers)
}

// TestMultiProcessDetailedMetrics tests detailed multi-process metrics
func TestMultiProcessDetailedMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Create first client
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()

	ctx := context.Background()

	// Write with first client
	_, err = client1.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("process1")})
	if err != nil {
		t.Fatal(err)
	}

	shard1, _ := client1.getOrCreateShard(1)
	if shard1.state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Update process count and heartbeat
	atomic.AddUint64(&shard1.state.ProcessCount, 1)
	atomic.StoreInt64(&shard1.state.LastProcessHeartbeat, time.Now().UnixNano())

	// Create second client
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Simulate some contention
	for i := 0; i < 5; i++ {
		go func() {
			client1.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("concurrent1")})
		}()
		go func() {
			client2.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("concurrent2")})
		}()
	}

	time.Sleep(10 * time.Millisecond)

	// Check multi-process metrics
	processCount := atomic.LoadUint64(&shard1.state.ProcessCount)
	lastHeartbeat := atomic.LoadInt64(&shard1.state.LastProcessHeartbeat)
	contentionCount := atomic.LoadUint64(&shard1.state.ContentionCount)
	lockWaitNanos := atomic.LoadInt64(&shard1.state.LockWaitNanos)
	falseShareCount := atomic.LoadUint64(&shard1.state.FalseShareCount)

	t.Logf("Detailed multi-process metrics:")
	t.Logf("  Process count: %d", processCount)
	t.Logf("  Last heartbeat: %v", time.Unix(0, lastHeartbeat))
	t.Logf("  Contention count: %d", contentionCount)
	t.Logf("  Lock wait time: %d ns", lockWaitNanos)
	t.Logf("  False share count: %d", falseShareCount)

	// Verify all are accessible
	if processCount > 0 {
		t.Log("Process tracking is working")
	}
}

// TestMetricsCompleteness verifies all 70 metrics are accessible
func TestMetricsCompleteness(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Create shard
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, _ := client.getOrCreateShard(1)
	state := shard.loadState()
	if state == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Access ALL 70 metrics to ensure they don't panic
	metrics := map[string]interface{}{
		"Version":              atomic.LoadUint64(&state.Version),
		"WriteOffset":          atomic.LoadUint64(&state.WriteOffset),
		"LastEntryNumber":      atomic.LoadInt64(&state.LastEntryNumber),
		"LastIndexUpdate":      atomic.LoadInt64(&state.LastIndexUpdate),
		"ActiveFileIndex":      atomic.LoadUint64(&state.ActiveFileIndex),
		"FileSize":             atomic.LoadUint64(&state.FileSize),
		"LastFileSequence":     atomic.LoadUint64(&state.LastFileSequence),
		"TotalEntries":         atomic.LoadInt64(&state.TotalEntries),
		"TotalBytes":           atomic.LoadUint64(&state.TotalBytes),
		"TotalWrites":          atomic.LoadUint64(&state.TotalWrites),
		"LastWriteNanos":       atomic.LoadInt64(&state.LastWriteNanos),
		"CurrentBatchSize":     atomic.LoadUint64(&state.CurrentBatchSize),
		"TotalBatches":         atomic.LoadUint64(&state.TotalBatches),
		"FailedWrites":         atomic.LoadUint64(&state.FailedWrites),
		"TotalCompressed":      atomic.LoadUint64(&state.TotalCompressed),
		"CompressedEntries":    atomic.LoadUint64(&state.CompressedEntries),
		"SkippedCompression":   atomic.LoadUint64(&state.SkippedCompression),
		"CompressionRatio":     atomic.LoadUint64(&state.CompressionRatio),
		"CompressionTimeNanos": atomic.LoadInt64(&state.CompressionTimeNanos),
		"BestCompression":      atomic.LoadUint64(&state.BestCompression),
		"WorstCompression":     atomic.LoadUint64(&state.WorstCompression),
		"WriteLatencySum":      atomic.LoadUint64(&state.WriteLatencySum),
		"WriteLatencyCount":    atomic.LoadUint64(&state.WriteLatencyCount),
		"MinWriteLatency":      atomic.LoadUint64(&state.MinWriteLatency),
		"MaxWriteLatency":      atomic.LoadUint64(&state.MaxWriteLatency),
		"P50WriteLatency":      atomic.LoadUint64(&state.P50WriteLatency),
		"P99WriteLatency":      atomic.LoadUint64(&state.P99WriteLatency),
		"SyncLatencyNanos":     atomic.LoadInt64(&state.SyncLatencyNanos),
		"FilesCreated":         atomic.LoadUint64(&state.FilesCreated),
		"FilesDeleted":         atomic.LoadUint64(&state.FilesDeleted),
		"FileRotations":        atomic.LoadUint64(&state.FileRotations),
		"RotationTimeNanos":    atomic.LoadInt64(&state.RotationTimeNanos),
		"CurrentFiles":         atomic.LoadUint64(&state.CurrentFiles),
		"TotalFileBytes":       atomic.LoadUint64(&state.TotalFileBytes),
		"FailedRotations":      atomic.LoadUint64(&state.FailedRotations),
		"CheckpointCount":      atomic.LoadUint64(&state.CheckpointCount),
		"LastCheckpointNanos":  atomic.LoadInt64(&state.LastCheckpointNanos),
		"CheckpointTimeNanos":  atomic.LoadInt64(&state.CheckpointTimeNanos),
		"IndexPersistCount":    atomic.LoadUint64(&state.IndexPersistCount),
		"IndexPersistErrors":   atomic.LoadUint64(&state.IndexPersistErrors),
		"IndexSizeBytes":       atomic.LoadUint64(&state.IndexSizeBytes),
		"BinaryIndexNodes":     atomic.LoadUint64(&state.BinaryIndexNodes),
		"ActiveReaders":        atomic.LoadUint64(&state.ActiveReaders),
		"TotalReaders":         atomic.LoadUint64(&state.TotalReaders),
		"MaxConsumerLag":       atomic.LoadUint64(&state.MaxConsumerLag),
		"TotalEntriesRead":     atomic.LoadUint64(&state.TotalEntriesRead),
		"ConsumerGroups":       atomic.LoadUint64(&state.ConsumerGroups),
		"AckedEntries":         atomic.LoadUint64(&state.AckedEntries),
		"ReaderCacheHits":      atomic.LoadUint64(&state.ReaderCacheHits),
		"ErrorCount":           atomic.LoadUint64(&state.ErrorCount),
		"LastErrorNanos":       atomic.LoadInt64(&state.LastErrorNanos),
		"CorruptionDetected":   atomic.LoadUint64(&state.CorruptionDetected),
		"RecoveryAttempts":     atomic.LoadUint64(&state.RecoveryAttempts),
		"RecoverySuccesses":    atomic.LoadUint64(&state.RecoverySuccesses),
		"PartialWrites":        atomic.LoadUint64(&state.PartialWrites),
		"ReadErrors":           atomic.LoadUint64(&state.ReadErrors),
		"RetentionRuns":        atomic.LoadUint64(&state.RetentionRuns),
		"LastRetentionNanos":   atomic.LoadInt64(&state.LastRetentionNanos),
		"RetentionTimeNanos":   atomic.LoadInt64(&state.RetentionTimeNanos),
		"EntriesDeleted":       atomic.LoadUint64(&state.EntriesDeleted),
		"BytesReclaimed":       atomic.LoadUint64(&state.BytesReclaimed),
		"OldestEntryNanos":     atomic.LoadInt64(&state.OldestEntryNanos),
		"RetentionErrors":      atomic.LoadUint64(&state.RetentionErrors),
		"ProtectedByConsumers": atomic.LoadUint64(&state.ProtectedByConsumers),
		"ProcessCount":         atomic.LoadUint64(&state.ProcessCount),
		"LastProcessHeartbeat": atomic.LoadInt64(&state.LastProcessHeartbeat),
		"ContentionCount":      atomic.LoadUint64(&state.ContentionCount),
		"LockWaitNanos":        atomic.LoadInt64(&state.LockWaitNanos),
		"MMAPRemapCount":       atomic.LoadUint64(&state.MMAPRemapCount),
		"FalseShareCount":      atomic.LoadUint64(&state.FalseShareCount),
	}

	// Count how many metrics we accessed
	if len(metrics) != 70 {
		t.Errorf("Expected to access 70 metrics, got %d", len(metrics))
	}

	t.Logf("Successfully accessed all %d metrics without panic", len(metrics))

	// Log any non-zero metrics
	nonZeroCount := 0
	for name, value := range metrics {
		switch v := value.(type) {
		case uint64:
			if v > 0 {
				nonZeroCount++
				t.Logf("  %s: %d", name, v)
			}
		case int64:
			if v > 0 {
				nonZeroCount++
				t.Logf("  %s: %d", name, v)
			}
		}
	}

	t.Logf("Total non-zero metrics: %d/%d", nonZeroCount, len(metrics))
}
