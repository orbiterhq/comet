//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDeterministicAssignment tests that deterministic shard assignment works
func TestDeterministicAssignment(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Write test data to multiple shards
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	streams := []string{
		"test:v1:shard:0000",
		"test:v1:shard:0002",
		"test:v1:shard:0003",
		"test:v1:shard:0004",
	}

	for _, stream := range streams {
		var messages [][]byte
		for i := 0; i < 20; i++ {
			messages = append(messages, []byte(fmt.Sprintf("%s-msg-%03d", stream, i)))
		}

		ctx := context.Background()
		_, err = client.Append(ctx, stream, messages)
		if err != nil {
			t.Fatal(err)
		}
	}
	client.Close()

	time.Sleep(100 * time.Millisecond)

	// Test deterministic assignment with 2 consumers
	t.Run("TwoConsumers_DeterministicAssignment", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make([]map[uint32]int, 2) // Track which consumer got which shard

		for i := 0; i < 2; i++ {
			results[i] = make(map[uint32]int)
			wg.Add(1)
			go func(consumerID int) {
				defer wg.Done()

				client, err := NewClientWithConfig(dir, config)
				if err != nil {
					t.Errorf("Consumer %d failed: %v", consumerID, err)
					return
				}
				defer client.Close()

				consumer := NewConsumer(client, ConsumerOptions{
					Group:         "deterministic-group",
					ConsumerID:    consumerID,
					ConsumerCount: 2,
				})
				defer consumer.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
					for _, msg := range msgs {
						results[consumerID][msg.ID.ShardID]++
					}
					return nil
				}, WithStream("test:v1:shard:*"), WithBatchSize(10), WithAutoAck(true))

				t.Logf("Consumer %d processed shards: %v", consumerID, results[consumerID])
			}(i)
		}

		wg.Wait()

		// Verify deterministic assignment: each shard should go to exactly one consumer
		shardOwners := make(map[uint32]int) // shardID -> consumerID
		totalMessages := 0

		for consumerID, shards := range results {
			for shardID, count := range shards {
				if count > 0 {
					if existingOwner, exists := shardOwners[shardID]; exists {
						t.Errorf("Shard %d assigned to multiple consumers: %d and %d",
							shardID, existingOwner, consumerID)
					} else {
						shardOwners[shardID] = consumerID
						totalMessages += count
					}
				}
			}
		}

		// Verify all shards were assigned
		expectedShards := []uint32{1, 2, 3, 4}
		for _, shardID := range expectedShards {
			if _, assigned := shardOwners[shardID]; !assigned {
				t.Errorf("Shard %d was not assigned to any consumer", shardID)
			}
		}

		// Verify all messages were processed
		if totalMessages != 80 { // 4 shards * 20 messages each
			t.Errorf("Expected 80 total messages, got %d", totalMessages)
		}

		// Verify assignment is deterministic (shard % consumerCount)
		for shardID, ownerID := range shardOwners {
			expectedOwner := int(shardID % 2)
			if ownerID != expectedOwner {
				t.Errorf("Shard %d assigned to consumer %d, expected %d",
					shardID, ownerID, expectedOwner)
			}
		}

		t.Logf("✅ Deterministic assignment working: %v", shardOwners)
	})
}

// TestHybridACKBatching tests that hybrid ACK batching works correctly
func TestHybridACKBatching(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Write test data
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	stream := "batch:v1:shard:0000"
	var messages [][]byte
	for i := 0; i < 50; i++ {
		messages = append(messages, []byte(fmt.Sprintf("batch-msg-%03d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	time.Sleep(100 * time.Millisecond)

	// Test that ACK batching works
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	consumer := NewConsumer(client2, ConsumerOptions{
		Group: "batch-test",
		// No ConsumerID/ConsumerCount means no deterministic assignment filtering
	})
	defer consumer.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	processedCount := 0
	startTime := time.Now()

	err = consumer.Process(ctx2, func(ctx context.Context, msgs []StreamMessage) error {
		processedCount += len(msgs)
		t.Logf("Processed batch of %d messages (total: %d)", len(msgs), processedCount)
		return nil
	}, WithStream("batch:v1:shard:*"), WithBatchSize(5), WithAutoAck(true))

	duration := time.Since(startTime)
	t.Logf("Processed %d messages in %v", processedCount, duration)

	if processedCount != 50 {
		t.Errorf("Expected 50 messages, got %d", processedCount)
	}

	t.Logf("✅ Hybrid ACK batching test completed successfully")
}
