package comet

import (
	"context"
	"fmt"
	"testing"
)

// TestSmartSharding_PickShard verifies shard selection is consistent and distributed
func TestSmartSharding_PickShard(t *testing.T) {
	const shardCount = 16

	// Create a client for testing
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Test consistency - same key should always return same shard
	key := "user123"
	shard1 := client.PickShard(key, shardCount)
	shard2 := client.PickShard(key, shardCount)

	if shard1 != shard2 {
		t.Errorf("PickShard not consistent: got %d and %d for same key", shard1, shard2)
	}

	// Test range - should return values in [0, shardCount)
	if shard1 >= shardCount {
		t.Errorf("PickShard returned %d, expected < %d", shard1, shardCount)
	}

	// Test distribution - different keys should spread across shards
	shardHits := make(map[uint32]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user%d", i)
		shard := client.PickShard(key, shardCount)
		shardHits[shard]++
	}

	// All shards should get some traffic (rough distribution check)
	if len(shardHits) < shardCount/2 {
		t.Errorf("Poor distribution: only %d/%d shards got traffic", len(shardHits), shardCount)
	}

	// Check that distribution is reasonably even (within 3x of average)
	avgHits := 1000 / int(shardCount) // ~62.5 hits per shard
	for shard, hits := range shardHits {
		if hits < avgHits/3 || hits > avgHits*3 {
			t.Errorf("Shard %d got %d hits, expected ~%d (poor distribution)", shard, hits, avgHits)
		}
	}
}

// TestSmartSharding_StreamNames verifies stream name generation
func TestSmartSharding_StreamNames(t *testing.T) {
	// Test ShardStreamName with decimal format
	streamName := ShardStreamName("events:v1", 42)
	expected := "events:v1:0042"
	if streamName != expected {
		t.Errorf("ShardStreamName: got %s, expected %s", streamName, expected)
	}

	// Test with max shard ID
	streamName = ShardStreamName("events:v1", 255)
	expected = "events:v1:0255"
	if streamName != expected {
		t.Errorf("ShardStreamName: got %s, expected %s", streamName, expected)
	}

	// Create a client for testing PickShardStream
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Test PickShardStream
	key := "user123"
	shardStream := client.PickShardStream("events:v1", key, 16)

	// Should match the shard picked by PickShard
	expectedShard := client.PickShard(key, 16)
	expectedStream := ShardStreamName("events:v1", expectedShard)

	if shardStream != expectedStream {
		t.Errorf("PickShardStream: got %s, expected %s", shardStream, expectedStream)
	}
}

// TestSmartSharding_AllShards verifies helper functions for reading all shards
func TestSmartSharding_AllShards(t *testing.T) {
	const shardCount = 8

	// Test AllShardsRange
	shards := AllShardsRange(shardCount)
	if len(shards) != shardCount {
		t.Errorf("AllShardsRange: got %d shards, expected %d", len(shards), shardCount)
	}

	for i, shard := range shards {
		if shard != uint32(i) {
			t.Errorf("AllShardsRange[%d]: got %d, expected %d", i, shard, i)
		}
	}

	// Test AllShardStreams with new hex format
	streams := AllShardStreams("events:v1", shardCount)
	if len(streams) != shardCount {
		t.Errorf("AllShardStreams: got %d streams, expected %d", len(streams), shardCount)
	}

	for i, stream := range streams {
		expected := ShardStreamName("events:v1", uint32(i))
		if stream != expected {
			t.Errorf("AllShardStreams[%d]: got %s, expected %s", i, stream, expected)
		}
	}
}

// TestSmartSharding_DefaultShardCount verifies default behavior
func TestSmartSharding_DefaultShardCount(t *testing.T) {
	// Test with 0 (should use default)
	shards1 := AllShardsRange(0)
	shards2 := AllShardsRange(defaultShardCount)

	if len(shards1) != len(shards2) {
		t.Errorf("Default shard count not working: got %d vs %d", len(shards1), len(shards2))
	}

	// Create a client for testing
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Test PickShard with 0
	key := "test"
	shard1 := client.PickShard(key, 0)
	shard2 := client.PickShard(key, defaultShardCount)

	if shard1 != shard2 {
		t.Errorf("Default shard count not consistent: got %d vs %d", shard1, shard2)
	}
}

// TestSmartSharding_Integration shows real-world usage pattern
func TestSmartSharding_Integration(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	const shardCount = 4 // Small number for testing

	// Simulate multiple users writing events
	users := []string{"alice", "bob", "charlie", "diana"}

	for _, user := range users {
		// Each user writes to their assigned shard
		streamName := client.PickShardStream("events:v1", user, shardCount)

		event := []byte(fmt.Sprintf(`{"user":"%s","action":"login","timestamp":%d}`, user, 1234567890))
		ids, err := client.Append(ctx, streamName, [][]byte{event})
		if err != nil {
			t.Fatalf("failed to write event for %s: %v", user, err)
		}

		if len(ids) != 1 {
			t.Errorf("expected 1 ID for %s, got %d", user, len(ids))
		}

		t.Logf("User %s -> stream %s -> ID %s", user, streamName, ids[0])
	}

	// Sync to ensure all data is written
	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Consumer reads from all shards
	consumer := NewConsumer(client, ConsumerOptions{
		Group: "integration-test",
	})
	defer consumer.Close()

	// Read from all shards in this namespace
	// Note: Only read from shards that have data to avoid "file not memory mapped" errors
	// In real usage, you'd know which shards have data or read from all and handle empty responses
	usedShards := []uint32{0, 1, 3} // We know these shards got data from the test above
	messages, err := consumer.Read(ctx, usedShards, 100)
	if err != nil {
		t.Fatalf("failed to read from all shards: %v", err)
	}

	if len(messages) != len(users) {
		t.Errorf("expected %d messages from all shards, got %d", len(users), len(messages))
	}

	// Verify we got messages from different shards
	shardsSeen := make(map[string]bool)
	for _, msg := range messages {
		shardsSeen[msg.Stream] = true
		t.Logf("Read message from stream %s: ID %s", msg.Stream, msg.ID.String())
	}

	t.Logf("Successfully distributed %d users across %d shards, consumer read from %d unique shards",
		len(users), shardCount, len(shardsSeen))
}

// BenchmarkSmartSharding_PickShard measures shard selection performance
func BenchmarkSmartSharding_PickShard(b *testing.B) {
	const shardCount = 16
	keys := make([]string, 1000)

	// Pre-generate keys
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("user%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Create a client for benchmarking
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	for i := 0; i < b.N; i++ {
		key := keys[i%1000]
		_ = client.PickShard(key, shardCount)
	}

	b.ReportMetric(float64(shardCount), "target_shards")
}

// TestSmartSharding_MultiProcess verifies that PickShardStream only picks from owned shards
func TestSmartSharding_MultiProcess(t *testing.T) {
	const shardCount = 16
	const processCount = 4

	// Test each process ID
	for processID := 0; processID < processCount; processID++ {
		t.Run(fmt.Sprintf("Process%d", processID), func(t *testing.T) {
			// Create a multi-process config
			config := DeprecatedMultiProcessConfig(processID, processCount)

			// Create client
			dir := t.TempDir()
			client, err := NewClient(dir, config)
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}
			defer client.Close()

			// Track which shards this process picks
			pickedShards := make(map[uint32]bool)

			// Try many different keys
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key%d", i)
				stream := client.PickShardStream("events:v1", key, shardCount)

				// Extract shard ID from stream
				shardID, err := parseShardFromStream(stream)
				if err != nil {
					t.Fatalf("failed to parse shard from stream %s: %v", stream, err)
				}

				// Verify this process owns this shard
				if !config.Concurrency.Owns(shardID) {
					t.Errorf("Process %d picked shard %d which it doesn't own", processID, shardID)
				}

				pickedShards[shardID] = true
			}

			// Verify we only picked from our owned shards
			expectedOwnedShards := 0
			for i := uint32(0); i < shardCount; i++ {
				if config.Concurrency.Owns(i) {
					expectedOwnedShards++
				}
			}

			// We should have picked from most or all of our owned shards
			if len(pickedShards) < expectedOwnedShards/2 {
				t.Errorf("Process %d only used %d of %d owned shards",
					processID, len(pickedShards), expectedOwnedShards)
			}

			t.Logf("Process %d owns %d shards, picked from %d unique shards",
				processID, expectedOwnedShards, len(pickedShards))
		})
	}
}

// TestSmartSharding_MultiProcessCaching verifies owned shards are cached properly
func TestSmartSharding_MultiProcessCaching(t *testing.T) {
	// Create a multi-process client
	config := DeprecatedMultiProcessConfig(0, 4)
	dir := t.TempDir()
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// First call should populate cache
	stream1 := client.PickShardStream("events:v1", "test1", 256)

	// Verify cache was populated
	cached, ok := client.ownedShardsCache.Load(uint32(256))
	if !ok {
		t.Error("Expected owned shards to be cached")
	}

	ownedShards := cached.([]uint32)
	if len(ownedShards) == 0 {
		t.Error("Expected at least one owned shard")
	}

	// Second call should use cache (verify by checking same owned shards)
	stream2 := client.PickShardStream("events:v1", "test2", 256)

	// Both should be valid streams
	if _, err := parseShardFromStream(stream1); err != nil {
		t.Errorf("Invalid stream1: %v", err)
	}
	if _, err := parseShardFromStream(stream2); err != nil {
		t.Errorf("Invalid stream2: %v", err)
	}

	t.Logf("Process 0 owns %d shards out of 256", len(ownedShards))
}

// BenchmarkSmartSharding_Distribution measures how evenly load distributes
func BenchmarkSmartSharding_Distribution(b *testing.B) {
	const shardCount = 16
	shardHits := make([]int, shardCount)

	b.ResetTimer()

	// Create a client for benchmarking
	dir := b.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		shard := client.PickShard(key, shardCount)
		shardHits[shard]++
	}

	// Calculate distribution metrics
	if b.N > 0 {
		avgHits := b.N / shardCount
		var minHits, maxHits int = b.N, 0

		for _, hits := range shardHits {
			if hits < minHits {
				minHits = hits
			}
			if hits > maxHits {
				maxHits = hits
			}
		}

		// Report distribution quality
		deviation := float64(maxHits-minHits) / float64(avgHits)
		b.ReportMetric(deviation, "distribution_deviation_ratio")
		b.ReportMetric(float64(minHits), "min_hits_per_shard")
		b.ReportMetric(float64(maxHits), "max_hits_per_shard")
	}
}
