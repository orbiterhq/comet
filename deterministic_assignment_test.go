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

	// Write test data to multiple shards using default config (no process restrictions)
	client, err := NewClientWithConfig(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	streams := []string{
		"test:v1:shard:0000",
		"test:v1:shard:0001",
		"test:v1:shard:0002",
		"test:v1:shard:0003",
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

				// Each consumer gets its own process ID and only handles its owned shards
				processConfig := DeprecatedMultiProcessConfig(consumerID, 2)
				client, err := NewClientWithConfig(dir, processConfig)
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

				// Only process shards that this process owns
				ownedShards := []uint32{}
				for shardID := uint32(0); shardID < 4; shardID++ {
					if processConfig.Concurrency.Owns(shardID) {
						ownedShards = append(ownedShards, shardID)
					}
				}

				t.Logf("Consumer %d (Process %d) owns shards: %v", consumerID, consumerID, ownedShards)

				consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
					for _, msg := range msgs {
						results[consumerID][msg.ID.ShardID]++
					}
					return nil
				}, WithShards(ownedShards...), WithBatchSize(10), WithAutoAck(true))

				t.Logf("Consumer %d processed shards: %v", consumerID, results[consumerID])
			}(i)
		}

		wg.Wait()

		// Verify shard ownership: each consumer should only process shards it owns
		processConfig0 := DeprecatedMultiProcessConfig(0, 2)
		processConfig1 := DeprecatedMultiProcessConfig(1, 2)

		totalMessages := 0
		for consumerID := 0; consumerID < 2; consumerID++ {
			var processConfig CometConfig
			if consumerID == 0 {
				processConfig = processConfig0
			} else {
				processConfig = processConfig1
			}

			for shardID, count := range results[consumerID] {
				if count > 0 {
					totalMessages += count

					// Verify this consumer should own this shard
					if !processConfig.Concurrency.Owns(shardID) {
						t.Errorf("Consumer %d processed shard %d but doesn't own it", consumerID, shardID)
					} else {
						t.Logf("✓ Consumer %d correctly processed %d messages from owned shard %d", consumerID, count, shardID)
					}

					// Verify no other consumer processed this shard
					for otherConsumerID := 0; otherConsumerID < 2; otherConsumerID++ {
						if otherConsumerID != consumerID && results[otherConsumerID][shardID] > 0 {
							t.Errorf("Shard %d processed by both consumer %d and %d", shardID, consumerID, otherConsumerID)
						}
					}
				}
			}
		}

		// Verify expected shard ownership
		expectedOwnership := map[uint32]int{
			0: 0, // Process 0 owns shard 0 (0 % 2 == 0)
			1: 1, // Process 1 owns shard 1 (1 % 2 == 1)
			2: 0, // Process 0 owns shard 2 (2 % 2 == 0)
			3: 1, // Process 1 owns shard 3 (3 % 2 == 1)
		}

		allCorrect := true
		for shardID, expectedOwner := range expectedOwnership {
			if results[expectedOwner][shardID] == 0 {
				t.Logf("⚠️  Shard %d: no messages processed by expected owner (consumer %d)", shardID, expectedOwner)
				allCorrect = false
			}
		}

		// Expected total: 4 shards * 20 messages each = 80 messages
		if totalMessages != 80 {
			t.Errorf("Expected 80 total messages (4 shards * 20 each), got %d", totalMessages)
		}

		if allCorrect {
			t.Logf("✅ Multi-process shard ownership working correctly")
		}
	})
}

// TestHybridACKBatching tests that hybrid ACK batching works correctly
func TestHybridACKBatching(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)

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
	done := make(chan error, 1)

	// Run Process in a goroutine since it's a blocking operation
	go func() {
		err := consumer.Process(ctx2, func(ctx context.Context, msgs []StreamMessage) error {
			processedCount += len(msgs)
			t.Logf("Processed batch of %d messages (total: %d)", len(msgs), processedCount)

			// Once we've processed all messages, cancel the context to exit
			if processedCount >= 50 {
				cancel2()
			}
			return nil
		}, WithStream("batch:v1:shard:*"), WithBatchSize(5), WithAutoAck(true))
		done <- err
	}()

	// Wait for Process to complete or timeout
	select {
	case err = <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Process failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for messages to be processed")
	}

	duration := time.Since(startTime)
	t.Logf("Processed %d messages in %v", processedCount, duration)

	if processedCount != 50 {
		t.Errorf("Expected 50 messages, got %d", processedCount)
	}

	t.Logf("✅ Hybrid ACK batching test completed successfully")
}
