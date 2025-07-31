package comet

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// ============================================================================
// Write Benchmarks
// ============================================================================

// BenchmarkWrite_SingleEntry measures single entry write performance
func BenchmarkWrite_SingleEntry(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Typical log entry (~100 bytes)
	entry := []byte(`{"level":"INFO","timestamp":"2024-01-01T10:00:00Z","msg":"Request processed","request_id":"abc123","duration_ms":45}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, [][]byte{entry})
		if err != nil {
			b.Errorf("failed to add: %v", err)
		}
	}

	// Report throughput metrics
	stats := client.GetStats()
	totalBytes := stats.TotalBytes
	if totalBytes > 0 {
		b.ReportMetric(float64(totalBytes)/1e6, "MB_written")
		b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	}
}

// BenchmarkWrite_SmallBatch measures batched writes (realistic logging pattern)
func BenchmarkWrite_SmallBatch(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Batch of 10 typical log entries
	batch := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		batch[i] = []byte(fmt.Sprintf(`{"level":"INFO","timestamp":"2024-01-01T10:00:%02d","msg":"Processing item %d","item_id":%d}`, i, i, i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, batch)
		if err != nil {
			b.Errorf("failed to add batch: %v", err)
		}
	}

	// Report throughput metrics
	stats := client.GetStats()
	b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_written")
}

// BenchmarkWrite_LargeBatch measures large batch performance (100 entries)
func BenchmarkWrite_LargeBatch(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Batch of 100 entries
	batch := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		batch[i] = []byte(fmt.Sprintf(`{"batch_id":%d,"index":%d,"event":"batch_test","data":"payload_%d","timestamp":%d}`,
			i/100, i, i, time.Now().UnixNano()))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, batch)
		if err != nil {
			b.Errorf("failed to add batch: %v", err)
		}
	}

	// Report metrics
	stats := client.GetStats()
	b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_written")
	if stats.CompressedEntries > 0 {
		b.ReportMetric(float64(stats.CompressionRatio)/100, "compression_ratio")
	}
}

// BenchmarkWrite_HugeBatch measures very large batch performance (1000 entries)
func BenchmarkWrite_HugeBatch(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Batch of 1000 entries for maximum throughput
	batch := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		batch[i] = []byte(fmt.Sprintf(`{"huge_batch_id":%d,"index":%d,"event":"huge_batch_test","data":"payload_%d","timestamp":%d}`,
			i/1000, i, i, time.Now().UnixNano()))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, batch)
		if err != nil {
			b.Fatalf("failed to append batch: %v", err)
		}
	}

	// Report custom metrics
	entriesWritten := int64(b.N * 1000)
	totalBytes := int64(0)
	for _, entry := range batch {
		totalBytes += int64(len(entry))
	}
	totalBytes *= int64(b.N)

	b.ReportMetric(float64(totalBytes)/(1024*1024), "MB_written")
	b.ReportMetric(float64(entriesWritten), "entries_written")
}

// BenchmarkWrite_MegaBatch measures extremely large batch performance (10000 entries)
func BenchmarkWrite_MegaBatch(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Batch of 10,000 entries for extreme throughput testing
	batch := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		batch[i] = []byte(fmt.Sprintf(`{"mega_batch_id":%d,"index":%d,"event":"mega_batch_test","data":"payload_%d","timestamp":%d}`,
			i/10000, i, i, time.Now().UnixNano()))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, batch)
		if err != nil {
			b.Fatalf("failed to append mega batch: %v", err)
		}
	}

	// Report custom metrics
	entriesWritten := int64(b.N * 10000)
	totalBytes := int64(0)
	for _, entry := range batch {
		totalBytes += int64(len(entry))
	}
	totalBytes *= int64(b.N)

	b.ReportMetric(float64(totalBytes)/(1024*1024), "MB_written")
	b.ReportMetric(float64(entriesWritten), "entries_written")
}

// BenchmarkWrite_CompressibleEntries tests compression performance
func BenchmarkWrite_CompressibleEntries(b *testing.B) {
	dir := b.TempDir()

	// Use lower compression threshold to test realistic compression
	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 256 // Compress logs larger than 256 bytes

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Create realistic JSON logs that might come from a web service
	logTemplates := []string{
		`{"timestamp":"%d","level":"INFO","service":"api-gateway","trace_id":"%s","span_id":"%s","method":"GET","path":"/api/v1/users/%d","status_code":200,"duration_ms":%.2f,"user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)","ip":"192.168.1.%d","response_size":%d,"request_id":"%s"}`,
		`{"timestamp":"%d","level":"ERROR","service":"payment-service","trace_id":"%s","span_id":"%s","error":"Payment processing failed","error_code":"PAYMENT_DECLINED","user_id":%d,"amount":%.2f,"currency":"USD","payment_method":"credit_card","processor":"stripe","retry_count":%d,"stack_trace":"at processPayment (payment.js:123)\n  at handleRequest (server.js:456)\n  at process._tickCallback (node.js:789)"}`,
		`{"timestamp":"%d","level":"WARN","service":"inventory-service","trace_id":"%s","span_id":"%s","message":"Low inventory warning","product_id":"SKU-%d","current_stock":%d,"threshold":%d,"warehouse":"us-west-2","supplier":"ACME Corp","last_restock_date":"%s","estimated_depletion_days":%d}`,
		`{"timestamp":"%d","level":"INFO","service":"user-service","trace_id":"%s","span_id":"%s","event":"user_login","user_id":%d,"email":"user%d@example.com","login_method":"oauth2","provider":"google","session_id":"%s","geo_country":"US","geo_city":"San Francisco","device_type":"mobile","app_version":"2.3.1"}`,
		`{"timestamp":"%d","level":"DEBUG","service":"cache-service","trace_id":"%s","span_id":"%s","operation":"cache_miss","key":"user:profile:%d","cache_type":"redis","node":"cache-node-3","latency_ms":%.2f,"fallback":"database","ttl_seconds":3600,"memory_usage_mb":%.2f}`,
	}

	// Generate a batch of realistic logs
	batch := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		template := logTemplates[i%len(logTemplates)]
		ts := time.Now().UnixNano()
		traceID := fmt.Sprintf("%016x%016x", ts, i)
		spanID := fmt.Sprintf("%016x", ts+int64(i))
		requestID := fmt.Sprintf("req-%d-%d", ts, i)

		var entry string
		switch i % len(logTemplates) {
		case 0: // API Gateway log
			entry = fmt.Sprintf(template, ts, traceID, spanID, 1000+i, 15.5+float64(i), 100+i, 2048+i*100, requestID)
		case 1: // Payment service error
			entry = fmt.Sprintf(template, ts, traceID, spanID, 2000+i, 99.99+float64(i)*10, i%3)
		case 2: // Inventory warning
			entry = fmt.Sprintf(template, ts, traceID, spanID, 3000+i, 10+i, 50, "2024-01-15", 5+i)
		case 3: // User login
			entry = fmt.Sprintf(template, ts, traceID, spanID, 4000+i, 4000+i, requestID)
		case 4: // Cache service debug
			entry = fmt.Sprintf(template, ts, traceID, spanID, 5000+i, 0.5+float64(i)*0.1, 256.5+float64(i))
		}
		batch[i] = []byte(entry)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, batch)
		if err != nil {
			b.Errorf("failed to add large batch: %v", err)
		}
	}

	// Report compression metrics
	stats := client.GetStats()
	b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_original")
	b.ReportMetric(float64(stats.TotalCompressed)/1e6, "MB_compressed")
	if stats.TotalBytes > 0 {
		ratio := float64(stats.TotalCompressed) / float64(stats.TotalBytes)
		b.ReportMetric(ratio, "compression_ratio")
	}
	b.ReportMetric(float64(stats.CompressedEntries), "entries_compressed")
	b.ReportMetric(float64(stats.SkippedCompression), "entries_uncompressed")
}

// BenchmarkWrite_CompressionComparison compares write performance with and without compression
func BenchmarkWrite_CompressionComparison(b *testing.B) {
	// Create realistic JSON log that's large enough to compress
	logTemplate := `{"timestamp":%d,"level":"INFO","service":"api-gateway","trace_id":"%s","span_id":"%s","method":"POST","path":"/api/v1/orders","status_code":201,"duration_ms":%.2f,"user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","ip":"10.0.0.%d","request_body":{"order_id":"%s","customer_id":%d,"items":[{"product_id":"PROD-%d","quantity":%d,"price":%.2f},{"product_id":"PROD-%d","quantity":%d,"price":%.2f}],"shipping_address":{"street":"123 Main St","city":"San Francisco","state":"CA","zip":"94105","country":"US"},"payment":{"method":"credit_card","last4":"1234","processor":"stripe","amount":%.2f}},"response_size":%d,"request_id":"%s","session_id":"%s","correlation_id":"%s"}`

	makeEntry := func(i int) []byte {
		ts := time.Now().UnixNano()
		traceID := fmt.Sprintf("%032x", ts+int64(i))
		spanID := fmt.Sprintf("%016x", ts+int64(i)*2)
		requestID := fmt.Sprintf("req-%d-%d", ts, i)
		sessionID := fmt.Sprintf("sess-%d", ts/1000000)
		correlationID := fmt.Sprintf("corr-%d-%d", ts, i)
		orderID := fmt.Sprintf("ORD-%010d", i)

		entry := fmt.Sprintf(logTemplate,
			ts, traceID, spanID, 25.5+float64(i%100),
			10+i%250, orderID, 1000+i,
			2000+i, 1+i%5, 29.99+float64(i%10),
			3000+i, 2+i%3, 49.99+float64(i%20),
			79.98+float64(i%30), 2048+i*10,
			requestID, sessionID, correlationID)
		return []byte(entry)
	}

	// Test different scenarios
	scenarios := []struct {
		name            string
		compressionSize int
		batchSize       int
	}{
		{"NoCompression_Single", 1 << 30, 1},   // Compression disabled
		{"WithCompression_Single", 256, 1},     // Compression enabled
		{"NoCompression_Batch10", 1 << 30, 10}, // Batch without compression
		{"WithCompression_Batch10", 256, 10},   // Batch with compression
		{"NoCompression_Batch100", 1 << 30, 100},
		{"WithCompression_Batch100", 256, 100},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			dir := b.TempDir()
			config := DefaultCometConfig()
			config.Compression.MinCompressSize = scenario.compressionSize

			client, err := NewClientWithConfig(dir, config)
			if err != nil {
				b.Fatalf("failed to create client: %v", err)
			}
			defer client.Close()

			ctx := context.Background()
			streamName := "events:v1:shard:0001"

			// Prepare batch
			batch := make([][]byte, scenario.batchSize)
			for i := 0; i < scenario.batchSize; i++ {
				batch[i] = makeEntry(i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := client.Append(ctx, streamName, batch)
				if err != nil {
					b.Fatalf("failed to append: %v", err)
				}
			}

			// Report metrics
			stats := client.GetStats()
			b.ReportMetric(float64(stats.TotalEntries), "entries_written")
			if stats.CompressedEntries > 0 {
				b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_original")
				b.ReportMetric(float64(stats.TotalCompressed)/1e6, "MB_compressed")
				ratio := float64(stats.TotalCompressed) / float64(stats.TotalBytes)
				b.ReportMetric(ratio, "compression_ratio")
			}
		})
	}
}

// ============================================================================
// Concurrent Write Benchmarks
// ============================================================================

// BenchmarkWrite_Concurrent measures concurrent writer performance (real-world scenario)
func BenchmarkWrite_Concurrent(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Typical log entry
	logEntry := []byte(`{"level":"INFO","msg":"Request processed","request_id":"abc123","duration_ms":45}`)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Append(ctx, streamName, [][]byte{logEntry})
			if err != nil {
				b.Errorf("failed to add: %v", err)
			}
		}
	})

	// Report throughput metrics
	stats := client.GetStats()
	b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_written")
}

// BenchmarkWrite_ConcurrentMultiShard measures performance across multiple shards
func BenchmarkWrite_ConcurrentMultiShard(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	logEntry := []byte(`{"level":"INFO","msg":"Multi-shard request","request_id":"xyz789","duration_ms":23}`)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		shardID := 0
		for pb.Next() {
			// Round-robin across 4 shards
			streamName := fmt.Sprintf("events:v1:shard:%04d", shardID%4)
			shardID++

			_, err := client.Append(ctx, streamName, [][]byte{logEntry})
			if err != nil {
				b.Errorf("failed to add to shard: %v", err)
			}
		}
	})

	// Report throughput metrics
	stats := client.GetStats()
	b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_written")
}

// BenchmarkWrite_ConcurrentBatched measures concurrent batched writes
func BenchmarkWrite_ConcurrentBatched(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Create batch of 5 entries (realistic batch size for logging)
	batch := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		batch[i] = []byte(fmt.Sprintf(`{"level":"INFO","msg":"Batch entry %d","request_id":"batch_%d","duration_ms":%d}`, i, i, 10+i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Append(ctx, streamName, batch)
			if err != nil {
				b.Errorf("failed to add batch: %v", err)
			}
		}
	})

	// Report throughput metrics
	stats := client.GetStats()
	b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_written")
}

// BenchmarkWrite_ConcurrentReadWrite measures mixed read/write performance
func BenchmarkWrite_ConcurrentReadWrite(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"
	logEntry := []byte(`{"level":"INFO","msg":"Mixed workload","request_id":"mixed123","duration_ms":15}`)

	// Pre-populate some data for readers
	initialBatch := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		initialBatch[i] = []byte(fmt.Sprintf(`{"level":"INFO","msg":"Initial entry %d","id":%d}`, i, i))
	}
	_, err = client.Append(ctx, streamName, initialBatch)
	if err != nil {
		b.Fatalf("failed to add initial data: %v", err)
	}

	// Sync to ensure data is available for reading
	err = client.Sync(ctx)
	if err != nil {
		b.Fatalf("failed to sync initial data: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		writeCount := 0
		for pb.Next() {
			writeCount++
			// Simple mixed workload: mostly writes with some reads
			if writeCount%10 == 0 {
				// Read operation - just check that we can read existing data
				// Get shard directly for reading without consumer offset complexity
				shard, err := client.getOrCreateShard(1)
				if err != nil {
					b.Errorf("failed to get shard: %v", err)
					continue
				}

				shard.mu.RLock()
				entryCount := shard.index.CurrentEntryNumber
				shard.mu.RUnlock()

				// Only try to read if there are entries
				if entryCount > 0 {
					reader, err := NewReader(1, shard.index)
					if err == nil {
						// Try to read the first entry
						pos := EntryPosition{FileIndex: 0, ByteOffset: 0}
						_, err := reader.ReadEntryAtPosition(pos)
						if err != nil {
							// Not an error - file might be rotating
						}
						reader.Close()
					}
				}
			} else {
				// Write operation
				_, err := client.Append(ctx, streamName, [][]byte{logEntry})
				if err != nil {
					b.Errorf("failed to add: %v", err)
				}
			}
		}
	})

	// Report throughput metrics
	stats := client.GetStats()
	b.ReportMetric(float64(stats.TotalEntries), "entries_written")
	b.ReportMetric(float64(stats.TotalBytes)/1e6, "MB_written")
}

// ============================================================================
// Consumer/Read Benchmarks
// ============================================================================

// BenchmarkConsumer_SequentialRead measures consumer sequential read performance
func BenchmarkConsumer_SequentialRead(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Pre-populate with test data
	entries := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = []byte(fmt.Sprintf(`{"level":"INFO","msg":"Benchmark entry %d","id":%d,"timestamp":%d}`, i, i, i*1000))
	}

	_, err = client.Append(ctx, streamName, entries)
	if err != nil {
		b.Fatalf("failed to add test data: %v", err)
	}

	err = client.Sync(ctx)
	if err != nil {
		b.Fatalf("failed to sync: %v", err)
	}

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "bench-consumer",
	})
	defer consumer.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset consumer offset for each iteration
		err := consumer.ResetOffset(ctx, 1, 0)
		if err != nil {
			b.Fatalf("failed to reset offset: %v", err)
		}

		// Read all entries
		var totalRead int
		for totalRead < 1000 {
			messages, err := consumer.Read(ctx, []uint32{1}, 50)
			if err != nil {
				b.Fatalf("failed to read entries: %v", err)
			}
			if len(messages) == 0 {
				break
			}

			// ACK the messages
			for _, msg := range messages {
				err = consumer.Ack(ctx, msg.ID)
				if err != nil {
					b.Fatalf("failed to ack: %v", err)
				}
			}
			totalRead += len(messages)
		}
	}

	// Report metrics
	b.ReportMetric(1000.0, "entries_per_iteration")
	b.ReportMetric(float64(1000*len(entries[0]))/1e6, "MB_per_iteration")
}

// BenchmarkConsumer_RandomAccess measures random entry access performance
func BenchmarkConsumer_RandomAccess(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Pre-populate with test data
	entries := make([][]byte, 500)
	for i := 0; i < 500; i++ {
		entries[i] = []byte(fmt.Sprintf(`{"level":"INFO","msg":"Random access entry %d","id":%d,"data":"payload_%d"}`, i, i, i))
	}

	_, err = client.Append(ctx, streamName, entries)
	if err != nil {
		b.Fatalf("failed to add test data: %v", err)
	}

	err = client.Sync(ctx)
	if err != nil {
		b.Fatalf("failed to sync: %v", err)
	}

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "random-bench",
	})
	defer consumer.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Seek to a random position
		randomOffset := int64(i % 400) // Random offset within range
		err := consumer.ResetOffset(ctx, 1, randomOffset)
		if err != nil {
			b.Fatalf("failed to reset offset: %v", err)
		}

		// Read a small batch from that position
		messages, err := consumer.Read(ctx, []uint32{1}, 5)
		if err != nil {
			b.Fatalf("failed to read entries: %v", err)
		}

		// Process the messages (simulate work)
		for _, msg := range messages {
			_ = len(msg.Data) // Simulate processing
		}
	}

	b.ReportMetric(5.0, "entries_per_read")
}

// BenchmarkConsumer_LagMeasurement measures consumer lag calculation performance
func BenchmarkConsumer_LagMeasurement(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Pre-populate with test data
	entries := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = []byte(fmt.Sprintf(`{"level":"INFO","msg":"Lag test entry %d","id":%d}`, i, i))
	}

	_, err = client.Append(ctx, streamName, entries)
	if err != nil {
		b.Fatalf("failed to add test data: %v", err)
	}

	// Create consumer with some lag
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "lag-bench",
	})
	defer consumer.Close()

	// Set consumer behind by 500 entries
	err = consumer.ResetOffset(ctx, 1, 500)
	if err != nil {
		b.Fatalf("failed to set initial offset: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Measure lag
		lag, err := consumer.GetLag(ctx, 1)
		if err != nil {
			b.Fatalf("failed to get lag: %v", err)
		}

		// Verify lag is reasonable
		if lag < 0 || lag > 1000 {
			b.Fatalf("unexpected lag value: %d", lag)
		}
	}

	b.ReportMetric(500.0, "expected_lag_entries")
}

// BenchmarkConsumer_MultiConsumer measures multiple consumers on same shard
func BenchmarkConsumer_MultiConsumer(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Pre-populate with test data
	entries := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = []byte(fmt.Sprintf(`{"level":"INFO","msg":"Multi consumer entry %d","id":%d}`, i, i))
	}

	_, err = client.Append(ctx, streamName, entries)
	if err != nil {
		b.Fatalf("failed to add test data: %v", err)
	}

	err = client.Sync(ctx)
	if err != nil {
		b.Fatalf("failed to sync: %v", err)
	}

	// Create multiple consumers
	consumer1 := NewConsumer(client, ConsumerOptions{Group: "group1"})
	consumer2 := NewConsumer(client, ConsumerOptions{Group: "group2"})
	consumer3 := NewConsumer(client, ConsumerOptions{Group: "group3"})
	defer consumer1.Close()
	defer consumer2.Close()
	defer consumer3.Close()

	consumers := []*Consumer{consumer1, consumer2, consumer3}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Each consumer reads some entries
		consumerIdx := i % 3
		consumer := consumers[consumerIdx]

		messages, err := consumer.Read(ctx, []uint32{1}, 10)
		if err != nil {
			b.Fatalf("failed to read entries: %v", err)
		}

		// ACK first message if any
		if len(messages) > 0 {
			err = consumer.Ack(ctx, messages[0].ID)
			if err != nil {
				b.Fatalf("failed to ack: %v", err)
			}
		}
	}

	b.ReportMetric(float64(len(consumers)), "active_consumers")
}

// BenchmarkSimpleConsumerRead benchmarks basic consumer read operations
func BenchmarkSimpleConsumerRead(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false
	config.Indexing.BoundaryInterval = 100
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "bench:v1:shard:0001"

	// Pre-populate with entries
	const numEntries = 1000
	for i := 0; i < numEntries; i++ {
		data := []byte(fmt.Sprintf(`{"id":%d,"data":"benchmark entry"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			b.Fatalf("failed to add entry %d: %v", i, err)
		}
	}

	// Force sync
	if err := client.Sync(ctx); err != nil {
		b.Fatalf("failed to sync: %v", err)
	}

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "bench",
	})
	defer consumer.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark reading entries
	const batchSize = 100
	for i := 0; i < b.N; i++ {
		// Read a batch
		messages, err := consumer.Read(ctx, []uint32{1}, batchSize)
		if err != nil {
			b.Fatalf("failed to read entries: %v", err)
		}

		if len(messages) > 0 {
			// Ack the batch
			var messageIDs []MessageID
			for _, msg := range messages {
				messageIDs = append(messageIDs, msg.ID)
			}
			err = consumer.Ack(ctx, messageIDs...)
			if err != nil {
				b.Fatalf("failed to ack batch: %v", err)
			}
		} else {
			// Reset to beginning if we've consumed all
			err = consumer.ResetOffset(ctx, 1, 0)
			if err != nil {
				b.Fatalf("failed to reset offset: %v", err)
			}
		}
	}

	b.ReportMetric(float64(batchSize), "batch_size")
	b.ReportMetric(float64(numEntries), "total_entries")
}

// ============================================================================
// ACK Benchmarks
// ============================================================================

// BenchmarkConsumerAck benchmarks the new simplified ACK API
func BenchmarkConsumerAck(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Create test data across multiple shards
	shards := []uint32{0, 1, 2, 3}
	var allMessages []StreamMessage

	for _, shardID := range shards {
		streamName := ShardStreamName("bench", "v1", shardID)
		data := []byte(`{"test": "data"}`)
		ids, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			b.Fatalf("failed to add data to shard %d: %v", shardID, err)
		}

		// Create a message for benchmarking
		msg := StreamMessage{
			ID:     ids[0],
			Data:   data,
			Stream: streamName,
		}
		allMessages = append(allMessages, msg)
	}

	// Sync to ensure data is written
	if err := client.Sync(ctx); err != nil {
		b.Fatalf("failed to sync: %v", err)
	}

	consumer := NewConsumer(client, ConsumerOptions{
		Group: "bench-group",
	})
	defer consumer.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark single ACKs
	b.Run("SingleAck", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			msg := allMessages[i%len(allMessages)]
			if err := consumer.Ack(ctx, msg.ID); err != nil {
				b.Fatalf("failed to ack: %v", err)
			}
		}
	})

	// Benchmark batch ACKs across shards
	b.Run("BatchAckMultiShard", func(b *testing.B) {
		// Prepare message IDs from all shards
		var messageIDs []MessageID
		for _, msg := range allMessages {
			messageIDs = append(messageIDs, msg.ID)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := consumer.Ack(ctx, messageIDs...); err != nil {
				b.Fatalf("failed to batch ack: %v", err)
			}
		}
	})
}

// BenchmarkACKPerformance compares ACK performance with binary index
func BenchmarkACKPerformance(b *testing.B) {
	ctx := context.Background()

	b.Run("BinaryIndex", func(b *testing.B) {
		dir := b.TempDir()
		client, _ := NewClient(dir)
		defer client.Close()

		// Add test data
		streamName := "test:v1:shard:0001"
		entries := make([][]byte, 1000)
		for i := range entries {
			entries[i] = []byte(fmt.Sprintf(`{"id":%d}`, i))
		}
		ids, _ := client.Append(ctx, streamName, entries)
		client.Sync(ctx)

		// Create consumer
		consumer := NewConsumer(client, ConsumerOptions{Group: "bench"})
		defer consumer.Close()

		b.ResetTimer()
		b.ReportAllocs()

		// Benchmark ACKs
		for i := 0; i < b.N; i++ {
			idx := i % len(ids)
			consumer.Ack(ctx, ids[idx])
		}

		b.StopTimer()

		// Report index size
		shard, _ := client.getOrCreateShard(1)
		shard.mu.RLock()
		indexCopy := shard.cloneIndex()
		shard.mu.RUnlock()

		// Get actual file size
		client.Close() // Force final persist
		stats := client.GetRetentionStats()

		b.ReportMetric(float64(len(indexCopy.BinaryIndex.Nodes)), "index_nodes")
		if len(stats.ShardStats) > 0 {
			b.ReportMetric(float64(stats.ShardStats[1].SizeBytes)/1024, "index_kb")
		}
	})
}

// ============================================================================
// Index Benchmarks
// ============================================================================

// BenchmarkBinarySearchableIndex_DirectOperation benchmarks the binary search operations directly
func BenchmarkBinarySearchableIndex_DirectOperation(b *testing.B) {
	// Create a binary index with many entries
	var bi BinarySearchableIndex
	bi.IndexInterval = 100

	// Populate with index nodes
	const numNodes = 1000
	for i := int64(0); i < numNodes; i++ {
		bi.AddIndexNode(i*100, EntryPosition{
			FileIndex:  int(i / 100),
			ByteOffset: i * 1000,
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark lookups
	for i := 0; i < b.N; i++ {
		entryNum := int64(i % (numNodes * 100))
		_, _, _ = bi.GetScanStartPosition(entryNum)
	}

	b.ReportMetric(float64(numNodes), "index_nodes")
	b.ReportMetric(float64(numNodes*100), "searchable_entries")
}

// BenchmarkIndexFormat benchmarks binary index performance
func BenchmarkIndexFormat(b *testing.B) {
	dir := b.TempDir()

	// Create test data - typical index with 10k entries
	entries := make([]EntryIndexNode, 10000)
	for i := range entries {
		entries[i] = EntryIndexNode{
			EntryNumber: int64(i * 100), // Every 100th entry
			Position: EntryPosition{
				FileIndex:  int(i / 1000),   // ~1000 entries per file
				ByteOffset: int64(i * 1024), // ~1KB per entry
			},
		}
	}

	// Benchmark binary format write
	b.Run("Binary_Write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := filepath.Join(dir, "index_binary.dat")
			f, _ := os.Create(path)

			// Header: version(4) + entry_count(8) + write_offset(8) + consumer_count(4)
			binary.Write(f, binary.LittleEndian, uint32(1))       // version
			binary.Write(f, binary.LittleEndian, uint64(1000000)) // current entry
			binary.Write(f, binary.LittleEndian, uint64(1<<30))   // write offset
			binary.Write(f, binary.LittleEndian, uint32(1))       // consumer count

			// Consumer offsets
			f.WriteString("group1\x00")
			binary.Write(f, binary.LittleEndian, uint64(500000))

			// Index entries
			binary.Write(f, binary.LittleEndian, uint32(len(entries)))
			for _, e := range entries {
				binary.Write(f, binary.LittleEndian, uint64(e.EntryNumber))
				binary.Write(f, binary.LittleEndian, int32(e.Position.FileIndex))
				binary.Write(f, binary.LittleEndian, uint64(e.Position.ByteOffset))
			}
			f.Close()
		}
	})

	// Create binary file for read benchmark
	binaryPath := filepath.Join(dir, "index.dat")
	bf, _ := os.Create(binaryPath)
	binary.Write(bf, binary.LittleEndian, uint32(1))
	binary.Write(bf, binary.LittleEndian, uint64(1000000))
	binary.Write(bf, binary.LittleEndian, uint64(1<<30))
	binary.Write(bf, binary.LittleEndian, uint32(1))
	bf.WriteString("group1\x00")
	binary.Write(bf, binary.LittleEndian, uint64(500000))
	binary.Write(bf, binary.LittleEndian, uint32(len(entries)))
	for _, e := range entries {
		binary.Write(bf, binary.LittleEndian, uint64(e.EntryNumber))
		binary.Write(bf, binary.LittleEndian, int32(e.Position.FileIndex))
		binary.Write(bf, binary.LittleEndian, uint64(e.Position.ByteOffset))
	}
	bf.Close()

	b.Run("Binary_Read", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data, _ := os.ReadFile(binaryPath)
			// Simulate parsing
			offset := 0
			_ = binary.LittleEndian.Uint32(data[offset:]) // version
			offset += 4
			_ = binary.LittleEndian.Uint64(data[offset:]) // entry
			offset += 8
			_ = binary.LittleEndian.Uint64(data[offset:]) // write offset
			offset += 8
			consumerCount := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			// Skip consumers
			for j := uint32(0); j < consumerCount; j++ {
				// Find null terminator
				for offset < len(data) && data[offset] != 0 {
					offset++
				}
				offset++    // skip null
				offset += 8 // skip offset
			}

			// Read entries
			entryCount := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			entries := make([]EntryIndexNode, entryCount)
			for j := range entries {
				if offset+20 > len(data) {
					break
				}
				entries[j].EntryNumber = int64(binary.LittleEndian.Uint64(data[offset:]))
				offset += 8
				entries[j].Position.FileIndex = int(binary.LittleEndian.Uint32(data[offset:]))
				offset += 4
				entries[j].Position.ByteOffset = int64(binary.LittleEndian.Uint64(data[offset:]))
				offset += 8
			}
		}
	})

	// Report file size
	binaryStat, _ := os.Stat(binaryPath)
	b.Logf("Binary index size: %d bytes for %d entries", binaryStat.Size(), len(entries))
}

// BenchmarkBinarySearch compares lookup performance
func BenchmarkBinarySearch(b *testing.B) {
	// Create index with 10k entries
	nodes := make([]EntryIndexNode, 10000)
	for i := range nodes {
		nodes[i] = EntryIndexNode{
			EntryNumber: int64(i * 100),
			Position: EntryPosition{
				FileIndex:  int(i / 1000),
				ByteOffset: int64(i * 1024),
			},
		}
	}

	index := BinarySearchableIndex{
		Nodes: nodes,
	}

	b.Run("CurrentJSONBinarySearch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Search for random entries
			targetEntry := int64((i % 10000) * 100)
			// Find nearest entry
			low, high := 0, len(index.Nodes)-1
			for low <= high {
				mid := (low + high) / 2
				if index.Nodes[mid].EntryNumber < targetEntry {
					low = mid + 1
				} else if index.Nodes[mid].EntryNumber > targetEntry {
					high = mid - 1
				} else {
					break
				}
			}
			if low < len(index.Nodes) {
				pos := index.Nodes[low]
				_ = pos
			}
		}
	})

	// Simulate mmap'd binary search
	b.Run("MmapBinarySearch", func(b *testing.B) {
		// Create binary data
		data := make([]byte, len(nodes)*20)
		for i, node := range nodes {
			offset := i * 20
			binary.LittleEndian.PutUint64(data[offset:], uint64(node.EntryNumber))
			binary.LittleEndian.PutUint32(data[offset+8:], uint32(node.Position.FileIndex))
			binary.LittleEndian.PutUint64(data[offset+12:], uint64(node.Position.ByteOffset))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			targetEntry := int64((i % 10000) * 100)

			// Binary search on mmap'd data
			low, high := 0, len(nodes)-1
			for low <= high {
				mid := (low + high) / 2
				offset := mid * 20
				entryNum := int64(binary.LittleEndian.Uint64(data[offset:]))

				if entryNum < targetEntry {
					low = mid + 1
				} else if entryNum > targetEntry {
					high = mid - 1
				} else {
					break
				}
			}
		}
	})
}

// ============================================================================
// Allocation/Memory Benchmarks
// ============================================================================

// BenchmarkAllocation_DetailedTracking provides detailed memory allocation analysis
func BenchmarkAllocation_DetailedTracking(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Different entry sizes to test allocation behavior
	sizes := []int{50, 100, 500, 1000, 2000} // bytes per entry

	for _, size := range sizes {
		// Create test entry of specific size
		entryData := make([]byte, size)
		for i := range entryData {
			entryData[i] = byte('A' + (i % 26)) // Fill with pattern
		}

		testName := fmt.Sprintf("size_%d_bytes", size)
		b.Run(testName, func(b *testing.B) {
			// Reset benchmark timer and enable allocation tracking
			b.ResetTimer()
			b.ReportAllocs()

			// Capture initial memory stats
			var startMemStats runtime.MemStats
			runtime.ReadMemStats(&startMemStats)

			for i := 0; i < b.N; i++ {
				_, err := client.Append(ctx, streamName, [][]byte{entryData})
				if err != nil {
					b.Errorf("failed to add: %v", err)
				}
			}

			// Capture final memory stats
			var endMemStats runtime.MemStats
			runtime.ReadMemStats(&endMemStats)

			// Calculate additional metrics
			if b.N > 0 {
				totalAllocs := endMemStats.TotalAlloc - startMemStats.TotalAlloc
				allocsPerOp := float64(totalAllocs) / float64(b.N)

				b.ReportMetric(allocsPerOp, "bytes_per_op_total")
				b.ReportMetric(float64(size), "entry_size_bytes")

				// Report allocation efficiency (bytes allocated per byte stored)
				efficiency := allocsPerOp / float64(size)
				b.ReportMetric(efficiency, "allocation_overhead_ratio")
			}
		})
	}
}

// BenchmarkAllocation_BatchSizeImpact measures how batch size affects allocations
func BenchmarkAllocation_BatchSizeImpact(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Standard log entry
	logEntry := []byte(`{"level":"INFO","msg":"Test log entry","request_id":"test123","duration_ms":42}`)

	// Different batch sizes
	batchSizes := []int{1, 5, 10, 50, 100}

	for _, batchSize := range batchSizes {
		// Create batch
		batch := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			batch[i] = logEntry
		}

		testName := fmt.Sprintf("batch_size_%d", batchSize)
		b.Run(testName, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := client.Append(ctx, streamName, batch)
				if err != nil {
					b.Errorf("failed to add batch: %v", err)
				}
			}

			// Report metrics normalized per entry
			if b.N > 0 && batchSize > 0 {
				b.ReportMetric(float64(batchSize), "entries_per_batch")
			}
		})
	}
}

// BenchmarkAllocation_CompressionImpact measures allocation impact of compression
func BenchmarkAllocation_CompressionImpact(b *testing.B) {
	dir := b.TempDir()

	// Test with different compression thresholds
	configs := []struct {
		name      string
		threshold int
	}{
		{"compression_disabled", 10000}, // Very high threshold = no compression
		{"compression_2KB", 2048},       // Default threshold
		{"compression_1KB", 1024},       // Lower threshold
		{"compression_256B", 256},       // Very low threshold
	}

	for _, config := range configs {
		cfg := DefaultCometConfig()
		cfg.Compression.MinCompressSize = config.threshold
		cfg.Indexing.BoundaryInterval = 100
		cfg.Storage.MaxFileSize = 1 << 30
		cfg.Storage.CheckpointTime = 2000
		client, err := NewClientWithConfig(dir, cfg)
		if err != nil {
			b.Fatalf("failed to create client: %v", err)
		}

		b.Run(config.name, func(b *testing.B) {
			ctx := context.Background()
			streamName := "events:v1:shard:0001"

			// Create large compressible entry (3KB)
			largeEntry := []byte(fmt.Sprintf(`{"level":"INFO","msg":"Large compressible entry","data":"%s"}`,
				string(make([]byte, 3000)))) // 3KB entry

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := client.Append(ctx, streamName, [][]byte{largeEntry})
				if err != nil {
					b.Errorf("failed to add: %v", err)
				}
			}

			// Report compression stats
			stats := client.GetStats()
			if stats.TotalEntries > 0 {
				compressionRatio := float64(stats.CompressionRatio) / 100.0
				b.ReportMetric(compressionRatio, "compression_ratio")
				b.ReportMetric(float64(stats.CompressedEntries), "entries_compressed")
				b.ReportMetric(float64(stats.SkippedCompression), "entries_uncompressed")
			}
		})

		client.Close()
	}
}

// BenchmarkAllocation_GarbageCollection measures GC impact during heavy writes
func BenchmarkAllocation_GarbageCollection(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Standard log entry
	logEntry := []byte(`{"level":"INFO","msg":"GC test entry","request_id":"gc123","duration_ms":15}`)

	b.ResetTimer()
	b.ReportAllocs()

	// Capture initial GC stats
	var startGCStats runtime.MemStats
	runtime.ReadMemStats(&startGCStats)

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, [][]byte{logEntry})
		if err != nil {
			b.Errorf("failed to add: %v", err)
		}
	}

	// Capture final GC stats
	var endGCStats runtime.MemStats
	runtime.ReadMemStats(&endGCStats)

	// Report GC metrics
	if b.N > 0 {
		gcCycles := endGCStats.NumGC - startGCStats.NumGC
		b.ReportMetric(float64(gcCycles), "gc_cycles")

		if gcCycles > 0 {
			gcCyclesPerOp := float64(gcCycles) / float64(b.N)
			b.ReportMetric(gcCyclesPerOp*1e6, "gc_cycles_per_million_ops")
		}

		// Report heap growth
		heapGrowth := int64(endGCStats.HeapInuse) - int64(startGCStats.HeapInuse)
		if heapGrowth > 0 {
			heapGrowthPerOp := float64(heapGrowth) / float64(b.N)
			b.ReportMetric(heapGrowthPerOp, "heap_growth_bytes_per_op")
		}
	}
}

// BenchmarkAllocation_MemoryLeaks runs a sustained test to detect memory leaks
func BenchmarkAllocation_MemoryLeaks(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	logEntry := []byte(`{"level":"INFO","msg":"Memory leak test","request_id":"leak123"}`)

	// Force GC to get clean baseline
	runtime.GC()
	runtime.GC() // Call twice to ensure full collection

	var baselineMemStats runtime.MemStats
	runtime.ReadMemStats(&baselineMemStats)

	b.ResetTimer()
	b.ReportAllocs()

	// Run a sustained workload
	const sustainedOps = 10000
	for i := 0; i < b.N; i++ {
		// Write many entries
		for j := 0; j < sustainedOps/b.N; j++ {
			_, err := client.Append(ctx, streamName, [][]byte{logEntry})
			if err != nil {
				b.Errorf("failed to add: %v", err)
			}
		}

		// Periodic sync to ensure data is written
		if i%100 == 0 {
			err = client.Sync(ctx)
			if err != nil {
				b.Errorf("failed to sync: %v", err)
			}
		}
	}

	// Force GC and measure memory retention
	runtime.GC()
	runtime.GC()

	var finalMemStats runtime.MemStats
	runtime.ReadMemStats(&finalMemStats)

	// Report memory retention (potential leaks)
	memoryRetained := int64(finalMemStats.HeapInuse) - int64(baselineMemStats.HeapInuse)
	if memoryRetained > 0 {
		b.ReportMetric(float64(memoryRetained)/1e6, "heap_retained_MB")
		retentionPerOp := float64(memoryRetained) / float64(sustainedOps)
		b.ReportMetric(retentionPerOp, "retention_bytes_per_op")
	}
}

// ============================================================================
// File Locking Benchmarks
// ============================================================================

// BenchmarkFileLocking_Enabled benchmarks write performance with file locking enabled (default)
func BenchmarkFileLocking_Enabled(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = true // Explicitly enable for clarity
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "benchmark:v1:shard:v1:shard:0000"

	// Pre-generate test data
	data := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		data[i] = []byte(fmt.Sprintf(`{"id":%d,"data":"benchmark data for file locking enabled"}`, i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, [][]byte{data[i]})
		if err != nil {
			b.Fatalf("failed to add entry: %v", err)
		}
	}
}

// BenchmarkFileLocking_Disabled benchmarks write performance with file locking disabled
func BenchmarkFileLocking_Disabled(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false // Disable file locking
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "benchmark:v1:shard:0001"

	// Pre-generate test data
	data := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		data[i] = []byte(fmt.Sprintf(`{"id":%d,"data":"benchmark data for file locking disabled"}`, i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, [][]byte{data[i]})
		if err != nil {
			b.Fatalf("failed to add entry: %v", err)
		}
	}
}

// BenchmarkFileLocking_ConcurrentEnabled benchmarks concurrent writes with file locking enabled
func BenchmarkFileLocking_ConcurrentEnabled(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = true
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "benchmark:v1:shard:0002"
	const numWorkers = 4

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	entriesPerWorker := b.N / numWorkers

	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < entriesPerWorker; i++ {
				data := []byte(fmt.Sprintf(`{"worker":%d,"id":%d,"data":"concurrent benchmark with locking"}`, workerID, i))
				_, err := client.Append(ctx, streamName, [][]byte{data})
				if err != nil {
					b.Errorf("worker %d failed to add entry: %v", workerID, err)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
}

// BenchmarkFileLocking_ConcurrentDisabled benchmarks concurrent writes with file locking disabled
func BenchmarkFileLocking_ConcurrentDisabled(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "benchmark:v1:shard:0003"
	const numWorkers = 4

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	entriesPerWorker := b.N / numWorkers

	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < entriesPerWorker; i++ {
				data := []byte(fmt.Sprintf(`{"worker":%d,"id":%d,"data":"concurrent benchmark without locking"}`, workerID, i))
				_, err := client.Append(ctx, streamName, [][]byte{data})
				if err != nil {
					b.Errorf("worker %d failed to add entry: %v", workerID, err)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
}

// BenchmarkFileLocking_BatchWrites_Enabled compares batch write performance with locking enabled
func BenchmarkFileLocking_BatchWrites_Enabled(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = true
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "benchmark:v1:shard:0004"
	const batchSize = 100

	// Pre-generate batches
	batches := make([][][]byte, b.N)
	for i := 0; i < b.N; i++ {
		batch := make([][]byte, batchSize)
		for j := 0; j < batchSize; j++ {
			batch[j] = []byte(fmt.Sprintf(`{"batch":%d,"entry":%d,"data":"batch benchmark with locking"}`, i, j))
		}
		batches[i] = batch
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, batches[i])
		if err != nil {
			b.Fatalf("failed to add batch: %v", err)
		}
	}

	b.ReportMetric(float64(batchSize), "entries_per_batch")
}

// BenchmarkFileLocking_BatchWrites_Disabled compares batch write performance with locking disabled
func BenchmarkFileLocking_BatchWrites_Disabled(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "benchmark:v1:shard:0005"
	const batchSize = 100

	// Pre-generate batches
	batches := make([][][]byte, b.N)
	for i := 0; i < b.N; i++ {
		batch := make([][]byte, batchSize)
		for j := 0; j < batchSize; j++ {
			batch[j] = []byte(fmt.Sprintf(`{"batch":%d,"entry":%d,"data":"batch benchmark without locking"}`, i, j))
		}
		batches[i] = batch
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, batches[i])
		if err != nil {
			b.Fatalf("failed to add batch: %v", err)
		}
	}

	b.ReportMetric(float64(batchSize), "entries_per_batch")
}

// ============================================================================
// Multi-Writer Benchmarks
// ============================================================================

// BenchmarkMultiWriter_FileLocking benchmarks file locking overhead
func BenchmarkMultiWriter_FileLocking(b *testing.B) {
	dir := b.TempDir()

	// Create lock file
	lockPath := fmt.Sprintf("%s/shard-0001.lock", dir)
	lockFile, err := os.Create(lockPath)
	if err != nil {
		b.Fatalf("failed to create lock file: %v", err)
	}
	defer lockFile.Close()

	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"
	logEntry := []byte(`{"level":"INFO","msg":"Lock test","request_id":"lock123"}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate file locking overhead
		err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX)
		if err != nil {
			b.Fatalf("failed to acquire lock: %v", err)
		}

		_, err = client.Append(ctx, streamName, [][]byte{logEntry})
		if err != nil {
			b.Errorf("failed to add: %v", err)
		}

		err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		if err != nil {
			b.Fatalf("failed to release lock: %v", err)
		}
	}
}

// BenchmarkMultiWriter_Contention simulates lock contention across goroutines
func BenchmarkMultiWriter_Contention(b *testing.B) {
	dir := b.TempDir()

	// Create lock file
	lockPath := fmt.Sprintf("%s/shard-0001.lock", dir)
	lockFile, err := os.Create(lockPath)
	if err != nil {
		b.Fatalf("failed to create lock file: %v", err)
	}
	defer lockFile.Close()

	numWriters := []int{1, 2, 4, 8}

	for _, writers := range numWriters {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			client, err := NewClient(dir)
			if err != nil {
				b.Fatalf("failed to create client: %v", err)
			}
			defer client.Close()

			ctx := context.Background()
			streamName := "events:v1:shard:0001"
			logEntry := []byte(`{"level":"INFO","msg":"Contention test","request_id":"cont123"}`)

			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			writesPerWorker := b.N / writers

			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < writesPerWorker; i++ {
						// Acquire exclusive lock
						err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX)
						if err != nil {
							b.Errorf("failed to acquire lock: %v", err)
							return
						}

						_, err = client.Append(ctx, streamName, [][]byte{logEntry})
						if err != nil {
							b.Errorf("failed to add: %v", err)
						}

						// Release lock
						err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
						if err != nil {
							b.Errorf("failed to release lock: %v", err)
						}
					}
				}()
			}

			wg.Wait()
			b.ReportMetric(float64(writers), "concurrent_writers")
		})
	}
}

// BenchmarkMultiWriter_NoLocking baseline without locking
func BenchmarkMultiWriter_NoLocking(b *testing.B) {
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"
	logEntry := []byte(`{"level":"INFO","msg":"No lock test","request_id":"nolock123"}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, [][]byte{logEntry})
		if err != nil {
			b.Errorf("failed to add: %v", err)
		}
	}
}
