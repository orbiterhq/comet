// +build integration

package comet

import (
	"context"
	"sync/atomic"
	"testing"
)

func init() {
	Debug = true
}

// TestTimestampCheck - check if timestamp mismatch is causing the issue
func TestTimestampCheck(t *testing.T) {
	dir := t.TempDir()
	
	config := DefaultCometConfig()
	config.Log.EnableDebug = true
	
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Write one entry 
	_, err = client.Append(ctx, streamName, [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	// Check shard timestamps before consumer read
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	var lastIndexUpdate, lastMmapCheck int64
	if shard.state != nil {
		lastIndexUpdate = shard.state.GetLastIndexUpdate()
	}
	lastMmapCheck = atomic.LoadInt64(&shard.lastMmapCheck)
	t.Logf("Before consumer read: lastIndexUpdate=%d, lastMmapCheck=%d, equal=%v", 
		lastIndexUpdate, lastMmapCheck, lastIndexUpdate == lastMmapCheck)
	shard.mu.RUnlock()

	// Create consumer and check what happens
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	t.Logf("About to call consumer.Read...")
	messages, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Consumer.Read returned %d messages", len(messages))

	// Check timestamps after consumer read
	shard.mu.RLock()
	if shard.state != nil {
		lastIndexUpdate = shard.state.GetLastIndexUpdate()
	}
	lastMmapCheck = atomic.LoadInt64(&shard.lastMmapCheck)
	t.Logf("After consumer read: lastIndexUpdate=%d, lastMmapCheck=%d, equal=%v", 
		lastIndexUpdate, lastMmapCheck, lastIndexUpdate == lastMmapCheck)
	shard.mu.RUnlock()
}