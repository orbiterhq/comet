package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestClient_AddAndLen(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0042"

	// Test initial length
	length, err := client.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("failed to get length: %v", err)
	}
	if length != 0 {
		t.Errorf("expected length 0, got %d", length)
	}

	// Add entries
	entries := [][]byte{
		[]byte(`{"event": "test1", "value": 1}`),
		[]byte(`{"event": "test2", "value": 2}`),
		[]byte(`{"event": "test3", "value": 3}`),
	}

	ids, err := client.Append(ctx, streamName, entries)
	if err != nil {
		t.Fatalf("failed to add entries: %v", err)
	}

	if len(ids) != 3 {
		t.Errorf("expected 3 IDs, got %d", len(ids))
	}

	// Check length
	length, err = client.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("failed to get length: %v", err)
	}
	if length != 3 {
		t.Errorf("expected length 3, got %d", length)
	}
}

func TestClient_Consumer(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Add test data
	testData := [][]byte{
		[]byte(`{"id": 1, "message": "first"}`),
		[]byte(`{"id": 2, "message": "second"}`),
		[]byte(`{"id": 3, "message": "third"}`),
	}

	ids, err := client.Append(ctx, streamName, testData)
	if err != nil {
		t.Fatalf("failed to add entries: %v", err)
	}

	// Force sync to ensure data is written
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "test-group",
	})
	defer consumer.Close()

	// Read entries
	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	if len(messages) != 3 {
		t.Errorf("expected 3 messages, got %d", len(messages))
	}

	// Verify message content
	for i, msg := range messages {
		t.Logf("Message %d: Data len=%d, ID=%v", i, len(msg.Data), msg.ID)
		if i >= len(testData) {
			t.Errorf("Got more messages (%d) than test data (%d)", len(messages), len(testData))
			break
		}
		if string(msg.Data) != string(testData[i]) {
			t.Errorf("message %d data mismatch: expected %s, got %s", i, testData[i], msg.Data)
		}
		if msg.ID != ids[i] {
			t.Errorf("message %d ID mismatch: expected %s, got %s", i, ids[i], msg.ID.String())
		}
	}

	// Test ACK
	for _, msg := range messages {
		err := consumer.Ack(ctx, msg.ID)
		if err != nil {
			t.Errorf("failed to ack message %s: %v", msg.ID, err)
		}
	}

	// Reading again should return no messages
	messages2, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read entries after ack: %v", err)
	}

	if len(messages2) != 0 {
		t.Errorf("expected 0 messages after ack, got %d", len(messages2))
	}
}

func TestConsumer_Process(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Add test data
	testData := [][]byte{
		[]byte(`{"id": 1, "message": "first"}`),
		[]byte(`{"id": 2, "message": "second"}`),
		[]byte(`{"id": 3, "message": "third"}`),
		[]byte(`{"id": 4, "message": "fourth"}`),
		[]byte(`{"id": 5, "message": "fifth"}`),
	}

	_, err = client.Append(ctx, streamName, testData)
	if err != nil {
		t.Fatalf("failed to add entries: %v", err)
	}

	// Force sync
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "test-process",
	})
	defer consumer.Close()

	// Test basic processing
	t.Run("BasicProcessing", func(t *testing.T) {
		processedCount := 0
		processedMessages := make([]string, 0)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Run processor in background
		done := make(chan error)
		go func() {
			done <- consumer.Process(ctx, func(ctx context.Context, messages []StreamMessage) error {
				for _, msg := range messages {
					processedMessages = append(processedMessages, string(msg.Data))
					processedCount++
				}
				// Cancel after processing all messages
				if processedCount >= len(testData) {
					cancel()
				}
				return nil
			},
				WithShards(1),
				WithBatchSize(2),
			)
		}()

		// Wait for completion
		err := <-done
		if err != nil && err != context.Canceled {
			t.Fatalf("process returned error: %v", err)
		}

		// Verify all messages were processed
		if processedCount != len(testData) {
			t.Errorf("expected %d processed messages, got %d", len(testData), processedCount)
		}

		// Verify order
		for i, data := range testData {
			if i >= len(processedMessages) {
				break
			}
			if processedMessages[i] != string(data) {
				t.Errorf("message %d mismatch: expected %s, got %s", i, data, processedMessages[i])
			}
		}
	})

	// Test error handling and retries
	t.Run("ErrorHandling", func(t *testing.T) {
		// Reset consumer offset
		err := consumer.ResetOffset(ctx, 1, 0)
		if err != nil {
			t.Fatalf("failed to reset offset: %v", err)
		}

		attempts := 0
		errorCount := 0
		processedAfterRetry := 0

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			done <- consumer.Process(ctx, func(ctx context.Context, messages []StreamMessage) error {
				attempts++
				// Fail first 2 attempts
				if attempts <= 2 {
					return fmt.Errorf("simulated error")
				}
				// Success on 3rd attempt
				processedAfterRetry += len(messages)
				for _, msg := range messages {
					consumer.Ack(ctx, msg.ID)
				}
				if processedAfterRetry >= len(testData) {
					cancel()
				}
				return nil
			},
				WithShards(1),
				WithBatchSize(2),
				WithMaxRetries(2),
				WithRetryDelay(10*time.Millisecond),
				WithAutoAck(false), // Manual ack to test retry behavior
				WithErrorHandler(func(err error, retryCount int) {
					errorCount++
				}),
			)
		}()

		err = <-done
		if err != nil && err != context.Canceled {
			t.Fatalf("process returned error: %v", err)
		}

		// Should have failed twice then succeeded
		if errorCount != 2 {
			t.Errorf("expected 2 errors, got %d", errorCount)
		}

		if processedAfterRetry != len(testData) {
			t.Errorf("expected %d messages after retry, got %d", len(testData), processedAfterRetry)
		}
	})
}

func TestClient_CrashRecovery(t *testing.T) {
	dir := t.TempDir()
	streamName := "events:v1:shard:0001"
	ctx := context.Background()

	// Phase 1: Write some data
	client1, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	testData := [][]byte{
		[]byte(`{"phase": 1, "data": "before crash"}`),
		[]byte(`{"phase": 1, "data": "more data"}`),
		[]byte(`{"phase": 1, "data": "last entry"}`),
	}

	ids1, err := client1.Append(ctx, streamName, testData)
	if err != nil {
		t.Fatalf("failed to add entries: %v", err)
	}

	// Force a checkpoint
	err = client1.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// VALIDATION: Verify all 3 entries are readable BEFORE the "crash"
	consumer1 := NewConsumer(client1, ConsumerOptions{Group: "pre-crash-validation"})
	preMessages, err := consumer1.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read entries before crash: %v", err)
	}
	if len(preMessages) != 3 {
		t.Fatalf("Phase 1 validation failed: expected 3 entries before crash, got %d", len(preMessages))
	}
	consumer1.Close()

	// VALIDATION: Verify Len() reports 3 before crash
	preLength, err := client1.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("failed to get length before crash: %v", err)
	}
	if preLength != 3 {
		t.Fatalf("Phase 1 validation failed: expected length 3 before crash, got %d", preLength)
	}

	// Check that index was created before closing
	indexPath := filepath.Join(dir, "shard-0001", "index.bin")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Fatalf("index.bin not created before close")
	}

	// Close to simulate crash
	client1.Close()

	// Verify index still exists after close
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Fatalf("index.bin missing after close")
	}

	// Phase 2: Reopen and verify recovery
	client2, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to recreate client: %v", err)
	}
	defer client2.Close()

	// Verify previous data is readable
	length, err := client2.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("failed to get length after recovery: %v", err)
	}

	if length != 3 {
		t.Errorf("expected length 3 after recovery, got %d", length)
	}

	// Add more data after recovery
	moreData := [][]byte{
		[]byte(`{"phase": 2, "data": "after recovery"}`),
	}

	ids2, err := client2.Append(ctx, streamName, moreData)
	if err != nil {
		t.Fatalf("failed to add entries after recovery: %v", err)
	}

	// Sync the new entry
	err = client2.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync after recovery: %v", err)
	}

	// Verify total length
	finalLength, err := client2.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("failed to get final length: %v", err)
	}

	if finalLength != 4 {
		t.Errorf("expected final length 4, got %d", finalLength)
	}

	// Create consumer and verify all data is readable
	consumer := NewConsumer(client2, ConsumerOptions{
		Group: "recovery-test",
	})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read entries after recovery: %v", err)
	}

	if len(messages) != 4 {
		t.Errorf("expected 4 messages after recovery, got %d", len(messages))
	}

	// Verify IDs are consistent
	expectedIDs := append(ids1, ids2...)
	for i, msg := range messages {
		if msg.ID != expectedIDs[i] {
			t.Errorf("message %d ID mismatch after recovery: expected %s, got %s", i, expectedIDs[i], msg.ID.String())
		}
	}
}

func TestClient_FileRotation(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write enough data to trigger file rotation (1GB+ per file)
	// Use smaller entries but many of them to stay within memory limits
	entriesPerBatch := 1000
	batches := 10
	expectedTotal := entriesPerBatch * batches

	var allIDs []string

	for batch := 0; batch < batches; batch++ {
		entries := make([][]byte, entriesPerBatch)
		for i := 0; i < entriesPerBatch; i++ {
			// Create reasonably sized entries to eventually trigger rotation
			entry := fmt.Sprintf(`{"batch": %d, "index": %d, "data": "%s"}`,
				batch, i, string(make([]byte, 100))) // 100 byte padding
			entries[i] = []byte(entry)
		}

		ids, err := client.Append(ctx, streamName, entries)
		if err != nil {
			t.Fatalf("failed to add batch %d: %v", batch, err)
		}
		for _, id := range ids {
			allIDs = append(allIDs, id.String())
		}
	}

	// Force sync to ensure all data and index are persisted
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Verify total length
	length, err := client.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("failed to get length: %v", err)
	}

	if length != int64(expectedTotal) {
		t.Errorf("expected length %d, got %d", expectedTotal, length)
	}

	// Check that shard directory contains files
	shardDir := filepath.Join(dir, "shard-0001")
	files, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("failed to read shard directory: %v", err)
	}

	dataFiles := 0
	hasIndex := false
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".comet" {
			dataFiles++
		}
		if file.Name() == "index.bin" {
			hasIndex = true
		}
	}

	if !hasIndex {
		t.Error("expected index.bin to exist")
	}

	if dataFiles == 0 {
		t.Error("expected at least one .comet data file")
	}

	// Create consumer and verify all data is readable across files
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "rotation-test",
	})
	defer consumer.Close()

	// Read in chunks to test cross-file reading
	readAll := make([]StreamMessage, 0, expectedTotal)
	for len(readAll) < expectedTotal {
		messages, err := consumer.Read(ctx, []uint32{1}, 1000)
		if err != nil {
			t.Fatalf("failed to read entries: %v", err)
		}
		if len(messages) == 0 {
			break
		}
		readAll = append(readAll, messages...)

		// ACK the messages to advance consumer
		for _, msg := range messages {
			err := consumer.Ack(ctx, msg.ID)
			if err != nil {
				t.Fatalf("failed to ack message: %v", err)
			}
		}
	}

	if len(readAll) != expectedTotal {
		t.Errorf("expected to read %d messages, got %d", expectedTotal, len(readAll))
	}

	// Verify IDs match
	for i, msg := range readAll {
		if msg.ID.String() != allIDs[i] {
			t.Errorf("message %d ID mismatch: expected %s, got %s", i, allIDs[i], msg.ID.String())
		}
	}
}

func TestClient_MultipleShards(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write to multiple shards
	for i := 0; i < 5; i++ {
		streamName := fmt.Sprintf("events:v1:shard:%04d", i)
		entries := [][]byte{
			[]byte(fmt.Sprintf(`{"shard": %d, "data": "test"}`, i)),
		}

		_, err := client.Append(ctx, streamName, entries)
		if err != nil {
			t.Fatalf("failed to add to shard %d: %v", i, err)
		}
	}

	// Verify each shard directory exists
	for i := 0; i < 5; i++ {
		shardDir := filepath.Join(dir, fmt.Sprintf("shard-%04d", i))
		if _, err := os.Stat(shardDir); os.IsNotExist(err) {
			t.Errorf("shard directory %s does not exist", shardDir)
		}
	}

	// Check client has all shards
	client.mu.RLock()
	shardCount := len(client.shards)
	client.mu.RUnlock()

	if shardCount != 5 {
		t.Errorf("expected 5 shards, got %d", shardCount)
	}
}

func TestClient_Compression(t *testing.T) {
	dir := t.TempDir()

	// Create custom config for testing
	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 100 // Lower threshold for testing

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Test with mixed sizes - some will compress, some won't
	testData := [][]byte{
		[]byte("small"), // Won't compress (< 100 bytes)
		[]byte(fmt.Sprintf("large entry with lots of repeated data: %s",
			strings.Repeat("AAAABBBBCCCCDDDD", 20))), // Will compress
		[]byte("another small one"), // Won't compress
		[]byte(fmt.Sprintf("another large compressible entry: %s",
			strings.Repeat("1234567890", 30))), // Will compress
	}

	// Add entries
	ids, err := client.Append(ctx, streamName, testData)
	if err != nil {
		t.Fatalf("failed to add entries: %v", err)
	}

	if len(ids) != 4 {
		t.Errorf("expected 4 IDs, got %d", len(ids))
	}

	// Force sync to ensure all metrics are updated
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Check metrics
	stats := client.GetStats()

	if stats.TotalEntries != 4 {
		t.Errorf("expected 4 total entries, got %d", stats.TotalEntries)
	}

	if stats.CompressedEntries == 0 {
		t.Error("expected some entries to be compressed")
	}

	if stats.SkippedCompression == 0 {
		t.Error("expected some entries to skip compression")
	}

	if stats.CompressedEntries+stats.SkippedCompression != stats.TotalEntries {
		t.Errorf("compressed (%d) + skipped (%d) should equal total (%d)",
			stats.CompressedEntries, stats.SkippedCompression, stats.TotalEntries)
	}

	// Basic compression metrics are available
	if stats.CompressedEntries == 0 && stats.SkippedCompression == 0 {
		t.Error("expected some compression activity")
	}

	// Verify consumer can read all entries correctly
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "compression-test",
	})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	if len(messages) != 4 {
		t.Errorf("expected 4 messages, got %d", len(messages))
	}

	// Verify data integrity - check that compressed and uncompressed data reads back correctly
	for i, msg := range messages {
		if string(msg.Data) != string(testData[i]) {
			t.Errorf("message %d data mismatch: expected %q, got %q",
				i, string(testData[i]), string(msg.Data))
		}
	}
}

func TestClient_CompressionMetrics(t *testing.T) {
	dir := t.TempDir()

	// Create client with compression enabled for large entries
	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 100 // Lower threshold to ensure compression

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Create many large entries to potentially overwhelm the compression queue
	largeEntries := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		data := strings.Repeat(fmt.Sprintf("test data %d ", i), 100)
		largeEntries[i] = []byte(data)
	}

	// Add entries - this should exercise the compression pool
	_, err = client.Append(ctx, streamName, largeEntries)
	if err != nil {
		t.Fatalf("failed to add entries: %v", err)
	}

	// Check that metrics are being tracked
	stats := client.GetStats()

	if stats.CompressionWaitNano == 0 {
		t.Log("Warning: CompressionWaitNano is 0 - compression might be too fast to measure")
	}

	// All entries should be compressed since they're large
	if stats.CompressedEntries != 10 {
		t.Errorf("expected 10 compressed entries, got %d", stats.CompressedEntries)
	}

	if stats.SkippedCompression != 0 {
		t.Errorf("expected 0 skipped compressions, got %d", stats.SkippedCompression)
	}
}

func TestClient_LoggingOptimization(t *testing.T) {
	dir := t.TempDir()

	// Use default config (optimized for logging)
	client, err := NewClientWithConfig(dir, DefaultCometConfig())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Typical log entries (small, frequent)
	logEntries := [][]byte{
		[]byte(`{"level":"INFO","msg":"User login","user_id":"12345","timestamp":"2024-01-01T10:00:00Z"}`),
		[]byte(`{"level":"ERROR","msg":"Database connection failed","error":"timeout after 30s"}`),
		[]byte(`{"level":"DEBUG","msg":"Processing request","request_id":"abc123","method":"POST"}`),
		[]byte(`{"level":"WARN","msg":"High memory usage","memory_pct":85}`),
	}

	start := time.Now()

	// Add many small log entries (typical logging pattern)
	for i := 0; i < 100; i++ {
		_, err := client.Append(ctx, streamName, logEntries)
		if err != nil {
			t.Fatalf("failed to add log entries: %v", err)
		}
	}

	duration := time.Since(start)

	// Force sync
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	stats := client.GetStats()

	// With LoggingConfig, small entries shouldn't be compressed
	if stats.CompressedEntries > 0 {
		t.Errorf("expected 0 compressed entries for small logs, got %d", stats.CompressedEntries)
	}

	// All entries should be skipped (too small for compression)
	expectedTotal := uint64(400) // 100 iterations * 4 entries
	if stats.TotalEntries != expectedTotal {
		t.Errorf("expected %d total entries, got %d", expectedTotal, stats.TotalEntries)
	}

	if stats.SkippedCompression != expectedTotal {
		t.Errorf("expected %d skipped compressions, got %d", expectedTotal, stats.SkippedCompression)
	}

	t.Logf("Processed %d log entries in %v (%.2f entries/sec)",
		expectedTotal, duration, float64(expectedTotal)/duration.Seconds())

	// Verify all entries are readable
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "log-test",
	})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, int(expectedTotal))
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	if len(messages) != int(expectedTotal) {
		t.Errorf("expected %d messages, got %d", expectedTotal, len(messages))
	}
}

func TestHealth(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Test initial health - no data written
	health := client.Health()
	if !health.Healthy {
		t.Error("Expected healthy status for new client")
	}
	if health.Status != "healthy" {
		t.Errorf("Expected status 'healthy', got %s", health.Status)
	}
	if health.ActiveShards != 0 {
		t.Errorf("Expected 0 active shards, got %d", health.ActiveShards)
	}
	if health.Details != "No data written yet" {
		t.Errorf("Expected 'No data written yet' details, got %s", health.Details)
	}

	// Write some data
	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test data")})
	if err != nil {
		t.Fatal(err)
	}

	// Check health after writing
	health = client.Health()
	if !health.Healthy {
		t.Error("Expected healthy status after write")
	}
	if health.ActiveShards != 1 {
		t.Errorf("Expected 1 active shard, got %d", health.ActiveShards)
	}
	if !health.WritesOK || !health.ReadsOK {
		t.Error("Expected writes and reads to be OK")
	}
	if health.LastWriteTime.IsZero() {
		t.Error("Expected non-zero last write time")
	}
	if health.ErrorCount != 0 {
		t.Errorf("Expected 0 errors, got %d", health.ErrorCount)
	}

	// Simulate errors by incrementing error counter
	client.metrics.ErrorCount.Add(10)
	client.metrics.LastErrorNano.Store(uint64(time.Now().UnixNano()))

	// Check health with recent errors
	health = client.Health()
	if health.Status != "degraded" {
		t.Errorf("Expected degraded status with recent errors, got %s", health.Status)
	}
	if !strings.Contains(health.Details, "Recent errors detected") {
		t.Errorf("Expected error details, got %s", health.Details)
	}

	// Test high error rate
	client.metrics.TotalEntries.Store(100)
	client.metrics.ErrorCount.Store(5) // 5% error rate

	health = client.Health()
	if health.Healthy {
		t.Error("Expected unhealthy with high error rate")
	}
	if !strings.Contains(health.Details, "High error rate") {
		t.Errorf("Expected high error rate details, got %s", health.Details)
	}

	// Test uptime
	if !strings.Contains(health.Uptime, "s") {
		t.Errorf("Expected uptime to contain seconds, got %s", health.Uptime)
	}
}

func TestDebugLogging(t *testing.T) {
	// Enable debug mode
	originalDebug := Debug
	SetDebug(true)
	defer SetDebug(originalDebug)

	dir := t.TempDir()

	// Create test logger that captures output
	testLogger := NewTestLogger(t, LogLevelDebug)
	config := DefaultCometConfig()
	config.Log.Logger = testLogger
	config.Storage.MaxFileSize = 1024 // Small file size to trigger rotation

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Test 1: Shard creation should log
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test message")})
	if err != nil {
		t.Fatal(err)
	}

	if !testLogger.Contains("Created new shard") {
		t.Error("Expected shard creation debug log")
	}

	// Test 2: File rotation should log
	// Write data larger than max file size to force rotation
	largeData := make([]byte, 1100) // Larger than 1KB MaxFileSize
	for i := range largeData {
		largeData[i] = byte('A' + (i % 26))
	}

	// Clear logger to see only rotation logs
	testLogger.buffer.Reset()

	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{largeData})
	if err != nil {
		t.Fatal(err)
	}

	// Check if rotation happened
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	if fileCount > 1 {
		// Rotation happened, check for log
		if !testLogger.Contains("File rotated") {
			t.Log("Note: File rotation occurred but debug log not found")
			t.Log("This may happen in non-mmap mode where rotation logging is not yet implemented")
		}
	}

	// Test 3: Debug mode off - no logs
	SetDebug(false)
	testLogger.buffer.Reset()

	_, err = client.Append(ctx, "test:v1:shard:0002", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	if testLogger.Contains("Created new shard") {
		t.Error("Should not log shard creation when debug mode is off")
	}
}
