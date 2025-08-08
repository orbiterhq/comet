//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestConsumerPeriodicShardDiscovery tests that consumers discover new shards created after startup
func TestConsumerPeriodicShardDiscovery(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Step 1: Start consumer BEFORE any shards exist
	t.Log("Step 1: Starting consumer with no shards...")
	reader, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	consumer := NewConsumer(reader, ConsumerOptions{
		Group: "early-bird-consumer",
	})
	defer consumer.Close()

	// Track messages received
	var messagesReceived atomic.Int64
	consumerErrors := make(chan error, 10)

	// Start consumer with fast shard discovery
	go func() {
		err := consumer.Process(ctx, func(ctx context.Context, messages []StreamMessage) error {
			messagesReceived.Add(int64(len(messages)))
			for _, msg := range messages {
				t.Logf("  Consumer received: %s", string(msg.Data))
			}
			return nil
		},
			WithStream("dynamic:v1:shard:*"),
			WithShardDiscoveryInterval(1*time.Second), // Fast discovery for testing
			WithPollInterval(100*time.Millisecond),
			WithAutoAck(true),
		)
		if err != nil && ctx.Err() == nil {
			consumerErrors <- err
		}
	}()

	// Give consumer time to start
	time.Sleep(500 * time.Millisecond)

	// Verify consumer found no shards initially
	t.Log("Step 2: Verifying consumer started with no shards...")
	if messagesReceived.Load() > 0 {
		t.Error("Consumer should not have received any messages yet")
	}

	// Step 3: Create writer and add shards dynamically
	t.Log("\nStep 3: Creating writer and adding shards dynamically...")
	writer, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Write to first batch of shards
	firstBatchShards := []uint32{0, 1, 2}
	for _, shardID := range firstBatchShards {
		stream := fmt.Sprintf("dynamic:v1:shard:%04d", shardID)
		for i := 0; i < 5; i++ {
			data := fmt.Sprintf("shard-%d-msg-%d", shardID, i)
			_, err := writer.Append(ctx, stream, [][]byte{[]byte(data)})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	writer.Sync(ctx)
	t.Logf("  Created shards %v with 5 messages each", firstBatchShards)

	// Wait for consumer to discover and process first batch
	t.Log("\nStep 4: Waiting for consumer to discover first batch...")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if messagesReceived.Load() >= 15 { // 3 shards * 5 messages
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	firstBatchReceived := messagesReceived.Load()
	t.Logf("  Consumer received %d messages from first batch", firstBatchReceived)
	if firstBatchReceived < 15 {
		t.Errorf("Expected 15 messages, got %d", firstBatchReceived)
	}

	// Step 5: Add more shards while consumer is running
	t.Log("\nStep 5: Adding more shards while consumer is running...")
	secondBatchShards := []uint32{10, 20, 30}
	for _, shardID := range secondBatchShards {
		stream := fmt.Sprintf("dynamic:v1:shard:%04d", shardID)
		for i := 0; i < 3; i++ {
			data := fmt.Sprintf("shard-%d-new-msg-%d", shardID, i)
			_, err := writer.Append(ctx, stream, [][]byte{[]byte(data)})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	writer.Sync(ctx)
	t.Logf("  Created new shards %v with 3 messages each", secondBatchShards)

	// Wait for consumer to discover and process second batch
	t.Log("\nStep 6: Waiting for consumer to discover second batch...")
	deadline = time.Now().Add(5 * time.Second)
	expectedTotal := firstBatchReceived + 9 // 3 new shards * 3 messages
	for time.Now().Before(deadline) {
		if messagesReceived.Load() >= expectedTotal {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	finalReceived := messagesReceived.Load()
	secondBatchReceived := finalReceived - firstBatchReceived
	t.Logf("  Consumer received %d additional messages from second batch", secondBatchReceived)

	if secondBatchReceived < 9 {
		t.Errorf("Expected 9 messages from second batch, got %d", secondBatchReceived)
	}

	// Check for consumer errors
	select {
	case err := <-consumerErrors:
		t.Errorf("Consumer error: %v", err)
	default:
	}

	// Summary
	t.Log("\n=== Summary ===")
	t.Logf("Total messages received: %d", finalReceived)
	t.Logf("First batch (shards %v): %d messages", firstBatchShards, firstBatchReceived)
	t.Logf("Second batch (shards %v): %d messages", secondBatchShards, secondBatchReceived)

	if finalReceived >= 24 {
		t.Log("✓ Consumer successfully discovered new shards created after startup")
	} else {
		t.Error("✗ Consumer failed to discover all new shards")
	}
}

// TestShardDiscoveryIntervalConfig tests different discovery interval configurations
func TestShardDiscoveryIntervalConfig(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create initial shards
	writer, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Write to one shard initially
	stream := "interval:v1:shard:0000"
	_, err = writer.Append(ctx, stream, [][]byte{[]byte("initial")})
	if err != nil {
		t.Fatal(err)
	}
	writer.Sync(ctx)

	// Test 1: Default interval (5 seconds)
	t.Log("Test 1: Default discovery interval...")
	reader1, _ := NewClient(dir, config)
	consumer1 := NewConsumer(reader1, ConsumerOptions{Group: "test1"})

	go consumer1.Process(ctx, func(ctx context.Context, messages []StreamMessage) error {
		return nil
	}, WithStream("interval:v1:shard:*"))

	// Wait and count discoveries (would need to instrument the code to count properly)
	time.Sleep(6 * time.Second)
	consumer1.Close()
	reader1.Close()

	// Test 2: Custom fast interval
	t.Log("\nTest 2: Fast discovery interval (500ms)...")
	reader2, _ := NewClient(dir, config)
	consumer2 := NewConsumer(reader2, ConsumerOptions{Group: "test2"})

	// Add a new shard after consumer starts
	go func() {
		time.Sleep(1 * time.Second)
		stream2 := "interval:v1:shard:0001"
		writer.Append(ctx, stream2, [][]byte{[]byte("new-shard")})
		writer.Sync(ctx)
	}()

	messagesSeen := atomic.Int32{}
	startTime := time.Now()
	go consumer2.Process(ctx, func(ctx context.Context, messages []StreamMessage) error {
		if messagesSeen.Add(int32(len(messages))) == 2 { // Got message from new shard
			discoveryLatency := time.Since(startTime)
			t.Logf("  New shard discovered after %v", discoveryLatency)
		}
		return nil
	},
		WithStream("interval:v1:shard:*"),
		WithShardDiscoveryInterval(500*time.Millisecond),
	)

	time.Sleep(3 * time.Second)
	consumer2.Close()
	reader2.Close()

	// Test 3: Disabled discovery (interval = 0)
	t.Log("\nTest 3: Disabled periodic discovery...")
	reader3, _ := NewClient(dir, config)
	consumer3 := NewConsumer(reader3, ConsumerOptions{Group: "test3"})

	// This consumer should only discover shards at startup
	shardsSeen := make(map[uint32]bool)
	go consumer3.Process(ctx, func(ctx context.Context, messages []StreamMessage) error {
		for _, msg := range messages {
			shardsSeen[msg.ID.ShardID] = true
		}
		return nil
	},
		WithStream("interval:v1:shard:*"),
		WithShardDiscoveryInterval(0), // Disabled
	)

	// Add another shard - consumer with disabled discovery shouldn't see it
	time.Sleep(500 * time.Millisecond)
	stream3 := "interval:v1:shard:0002"
	writer.Append(ctx, stream3, [][]byte{[]byte("unseen-shard")})
	writer.Sync(ctx)

	time.Sleep(2 * time.Second)

	if _, found := shardsSeen[2]; found {
		t.Error("Consumer with disabled discovery should not have seen new shard")
	} else {
		t.Log("  ✓ Consumer correctly did not discover new shard with discovery disabled")
	}

	consumer3.Close()
	reader3.Close()
}
