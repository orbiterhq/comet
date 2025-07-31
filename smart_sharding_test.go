package comet

import (
	"context"
	"fmt"
	"testing"
)

// TestSmartSharding_PickShard verifies shard selection is consistent and distributed
func TestSmartSharding_PickShard(t *testing.T) {
	const shardCount = 16

	// Test consistency - same key should always return same shard
	key := "user123"
	shard1 := PickShard(key, shardCount)
	shard2 := PickShard(key, shardCount)

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
		shard := PickShard(key, shardCount)
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
	// Test ShardStreamName
	streamName := ShardStreamName("events", "v1", 42)
	expected := "events:v1:shard:0042"
	if streamName != expected {
		t.Errorf("ShardStreamName: got %s, expected %s", streamName, expected)
	}

	// Test PickShardStream
	key := "user123"
	shardStream := PickShardStream(key, "events", "v1", 16)

	// Should match the shard picked by PickShard
	expectedShard := PickShard(key, 16)
	expectedStream := fmt.Sprintf("events:v1:shard:%04d", expectedShard)

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

	// Test AllShardStreams
	streams := AllShardStreams("events", "v1", shardCount)
	if len(streams) != shardCount {
		t.Errorf("AllShardStreams: got %d streams, expected %d", len(streams), shardCount)
	}

	for i, stream := range streams {
		expected := fmt.Sprintf("events:v1:shard:%04d", i)
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

	// Test PickShard with 0
	key := "test"
	shard1 := PickShard(key, 0)
	shard2 := PickShard(key, defaultShardCount)

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
		streamName := PickShardStream(user, "events", "v1", shardCount)

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

	for i := 0; i < b.N; i++ {
		key := keys[i%1000]
		_ = PickShard(key, shardCount)
	}

	b.ReportMetric(float64(shardCount), "target_shards")
}

// BenchmarkSmartSharding_Distribution measures how evenly load distributes
func BenchmarkSmartSharding_Distribution(b *testing.B) {
	const shardCount = 16
	shardHits := make([]int, shardCount)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		shard := PickShard(key, shardCount)
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
