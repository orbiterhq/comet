//go:build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConsumerGroupShardAssignment tests that only one consumer per group can claim a shard
func TestConsumerGroupShardAssignment(t *testing.T) {
	dir := t.TempDir()

	// Write test data using default config (no process restrictions)
	client, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	stream := "test:v1:shard:0000"
	var messages [][]byte
	for i := 0; i < 100; i++ {
		messages = append(messages, []byte(fmt.Sprintf("msg-%03d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Small delay to ensure multi-process state is fully persisted
	time.Sleep(100 * time.Millisecond)

	// Test: Multiple consumers in same group with proper multi-process coordination
	t.Run("SameGroup_ExclusiveAccess", func(t *testing.T) {
		var totalProcessed int64
		var wg sync.WaitGroup

		// Start 3 consumers in same group, each with their own process ID
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(consumerID int) {
				defer wg.Done()

				// Each consumer gets its own process ID to avoid race conditions
				processConfig := DeprecatedMultiProcessConfig(consumerID, 3)
				client, err := NewClient(dir, processConfig)
				if err != nil {
					t.Errorf("Consumer %d failed to create client: %v", consumerID, err)
					return
				}
				defer client.Close()

				consumer := NewConsumer(client, ConsumerOptions{
					Group: "exclusive-group", // Same group
				})
				defer consumer.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				// Only process shards owned by this process
				ownedShards := []uint32{}
				for shardID := uint32(0); shardID < 1; shardID++ {
					if processConfig.Concurrency.Owns(shardID) {
						ownedShards = append(ownedShards, shardID)
					}
				}

				if len(ownedShards) == 0 {
					t.Logf("Consumer %d: No owned shards, skipping", consumerID)
					return
				}

				processed := 0
				err = consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
					processed += len(msgs)
					atomic.AddInt64(&totalProcessed, int64(len(msgs)))
					return nil
				}, WithShards(ownedShards...), WithBatchSize(10), WithAutoAck(true))

				t.Logf("Consumer %d (Process %d) processed %d messages from shards %v", consumerID, consumerID, processed, ownedShards)
			}(i)
		}

		wg.Wait()

		// Verify: All messages processed exactly once
		if totalProcessed != 100 {
			t.Errorf("Expected 100 messages processed, got %d", totalProcessed)
		}
	})

	// Test: Different groups should work independently
	t.Run("DifferentGroups_IndependentAccess", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make([]int, 3)

		// Use a single shared client to avoid race conditions from separate clients
		sharedClient2, err := NewClient(dir, DefaultCometConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer sharedClient2.Close()

		// Start 3 consumers in different groups
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(consumerID int) {
				defer wg.Done()

				consumer := NewConsumer(sharedClient2, ConsumerOptions{
					Group: fmt.Sprintf("group-%d", consumerID), // Different groups
				})
				defer consumer.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				processed := 0
				consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
					processed += len(msgs)
					return nil
				}, WithStream("test:v1:shard:*"), WithBatchSize(10), WithAutoAck(true))

				results[consumerID] = processed
				t.Logf("Consumer %d (group-%d) processed %d messages", consumerID, consumerID, processed)
			}(i)
		}

		wg.Wait()

		// Verify: Each group processed all messages independently
		for i, processed := range results {
			if processed != 100 {
				t.Errorf("Group %d should process all 100 messages, got %d", i, processed)
			}
		}
	})
}

// TestConsumerFailover tests that when a consumer dies, another can take over
func TestConsumerFailover(t *testing.T) {
	dir := t.TempDir()

	// Write test data using default config
	client, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	stream := "failover:v1:shard:0000"
	var messages [][]byte
	for i := 0; i < 50; i++ {
		messages = append(messages, []byte(fmt.Sprintf("msg-%03d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Small delay to ensure multi-process state is fully persisted
	time.Sleep(100 * time.Millisecond)

	// Consumer 1: Process some messages then "die"
	client1, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	consumer1 := NewConsumer(client1, ConsumerOptions{Group: "failover-group"})

	processed1 := 0
	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	consumer1.Process(ctx1, func(ctx context.Context, msgs []StreamMessage) error {
		processed1 += len(msgs)
		return nil
	}, WithStream("failover:v1:shard:*"), WithBatchSize(5), WithAutoAck(true))

	// Consumer 1 "dies"
	consumer1.Close()
	client1.Close()
	cancel1()

	t.Logf("Consumer 1 processed %d messages before dying", processed1)

	// Consumer 2: Should take over and process remaining messages
	client2, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	consumer2 := NewConsumer(client2, ConsumerOptions{Group: "failover-group"})
	defer consumer2.Close()

	processed2 := 0
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	consumer2.Process(ctx2, func(ctx context.Context, msgs []StreamMessage) error {
		processed2 += len(msgs)
		return nil
	}, WithStream("failover:v1:shard:*"), WithBatchSize(5), WithAutoAck(true))

	t.Logf("Consumer 2 processed %d messages after takeover", processed2)

	// Verify: Total processed = 50, no duplicates, no losses
	totalProcessed := processed1 + processed2
	if totalProcessed != 50 {
		t.Errorf("Expected 50 total messages processed, got %d (consumer1: %d, consumer2: %d)",
			totalProcessed, processed1, processed2)
	}

	// Verify: Consumer 2 processed the remaining messages (no overlap)
	if processed2 != (50 - processed1) {
		t.Errorf("Expected consumer2 to process %d remaining messages, got %d",
			50-processed1, processed2)
	}
}

// TestMultiShardConsumerGroup tests consumer group behavior across multiple shards
func TestMultiShardConsumerGroup(t *testing.T) {
	dir := t.TempDir()

	// Write data to multiple shards using default config (no process restrictions)
	client, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	streams := []string{
		"multi:v1:shard:0000",
		"multi:v1:shard:0001",
		"multi:v1:shard:0002",
	}

	for _, stream := range streams {
		var messages [][]byte
		for i := 0; i < 30; i++ {
			messages = append(messages, []byte(fmt.Sprintf("%s-msg-%03d", stream, i)))
		}

		ctx := context.Background()
		_, err = client.Append(ctx, stream, messages)
		if err != nil {
			t.Fatal(err)
		}
	}
	client.Close()

	// Small delay to ensure multi-process state is fully persisted
	time.Sleep(100 * time.Millisecond)

	// Start 3 consumers in same group - each gets its own process ID
	var wg sync.WaitGroup
	results := make([]int, 3)
	shardAssignments := make([]map[uint32]int, 3) // Track which consumer got which shard

	for i := 0; i < 3; i++ {
		shardAssignments[i] = make(map[uint32]int)
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			// Each consumer gets its own process ID to avoid race conditions
			processConfig := DeprecatedMultiProcessConfig(consumerID, 3)
			client, err := NewClient(dir, processConfig)
			if err != nil {
				t.Errorf("Consumer %d failed: %v", consumerID, err)
				return
			}
			defer client.Close()

			consumer := NewConsumer(client, ConsumerOptions{
				Group: "multi-shard-group", // Same group
			})
			defer consumer.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Only process shards owned by this process
			ownedShards := []uint32{}
			for shardID := uint32(0); shardID < 3; shardID++ {
				if processConfig.Concurrency.Owns(shardID) {
					ownedShards = append(ownedShards, shardID)
				}
			}

			t.Logf("Consumer %d (Process %d) owns shards: %v", consumerID, consumerID, ownedShards)

			processed := 0
			expectedMessages := 30 // 30 messages per shard, each consumer owns 1 shard

			consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
				for _, msg := range msgs {
					shardAssignments[consumerID][msg.ID.ShardID]++
					processed++
				}

				// Cancel context once we've processed all expected messages
				if processed >= expectedMessages {
					cancel()
				}

				return nil
			}, WithShards(ownedShards...), WithBatchSize(10), WithAutoAck(true))

			results[consumerID] = processed
			t.Logf("Consumer %d processed %d messages from shards: %v",
				consumerID, processed, shardAssignments[consumerID])
		}(i)
	}

	wg.Wait()

	// Verify: Total messages = 90 (30 per shard * 3 shards)
	total := 0
	for _, processed := range results {
		total += processed
	}
	if total != 90 {
		t.Errorf("Expected 90 total messages, got %d", total)
	}

	// Verify: Each shard was assigned to exactly one consumer
	shardOwners := make(map[uint32]int) // shardID -> consumerID
	for consumerID, shards := range shardAssignments {
		for shardID, count := range shards {
			if count > 0 {
				if existingOwner, exists := shardOwners[shardID]; exists {
					t.Errorf("Shard %d assigned to multiple consumers: %d and %d",
						shardID, existingOwner, consumerID)
				} else {
					shardOwners[shardID] = consumerID
				}
			}
		}
	}

	// Verify: All shards (0,1,2) were assigned
	expectedShards := []uint32{0, 1, 2}
	for _, shardID := range expectedShards {
		if _, assigned := shardOwners[shardID]; !assigned {
			t.Errorf("Shard %d was not assigned to any consumer", shardID)
		}
	}
}

// TestDebugMessageLoss - isolate and debug the 20 missing messages
func TestDebugMessageLoss(t *testing.T) {
	dir := t.TempDir()

	SetDebug(true)
	defer SetDebug(false)

	// Write exactly 100 messages
	client, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	stream := "debug:v1:shard:0000"
	var messages [][]byte
	for i := 0; i < 100; i++ {
		messages = append(messages, []byte(fmt.Sprintf("debug-msg-%03d", i)))
	}

	ctx := context.Background()
	result, err := client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Append result: %v", result)
	client.Close()

	time.Sleep(100 * time.Millisecond)

	// Verify data was written
	client2, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	length, err := client2.Len(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Stream length after write: %d", length)

	if length != 100 {
		t.Logf("WARNING: Stream length is %d, expected 100. This suggests test isolation issues.", length)
		t.Logf("Temp dir: %s", dir)

		// Check if there are existing files
		files, _ := os.ReadDir(dir)
		t.Logf("Directory contents: %d items", len(files))
		for _, file := range files {
			t.Logf("  - %s", file.Name())
		}
	}

	client2.Close()

	// Now try to read all messages with detailed logging
	client3, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client3.Close()

	consumer := NewConsumer(client3, ConsumerOptions{Group: "debug-group"})
	defer consumer.Close()

	// Read with longer timeout and detailed tracking
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	processedMessages := make(map[string]bool)
	totalProcessed := 0
	batchCount := 0

	// Run Process in a goroutine since it's a blocking operation
	done := make(chan error, 1)
	go func() {
		err := consumer.Process(ctx2, func(ctx context.Context, msgs []StreamMessage) error {
			batchCount++
			t.Logf("BATCH %d: Processing %d messages", batchCount, len(msgs))

			for i, msg := range msgs {
				msgStr := string(msg.Data)
				t.Logf("  Message %d: %s (ID: %s)", i, msgStr, msg.ID.String())

				if processedMessages[msgStr] {
					t.Errorf("DUPLICATE MESSAGE: %s", msgStr)
				}
				processedMessages[msgStr] = true
				totalProcessed++
			}

			// Cancel context once we've processed all expected messages
			if totalProcessed >= 100 {
				cancel2()
			}

			return nil
		}, WithStream("debug:v1:shard:*"), WithBatchSize(10), WithAutoAck(true))
		done <- err
	}()

	// Wait for Process to complete or timeout
	select {
	case err = <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("Process failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Error("Test timed out waiting for messages to be processed")
		cancel2()
		err = <-done // Wait for goroutine to finish
	}

	t.Logf("Process error: %v", err)
	t.Logf("Total processed: %d/100", totalProcessed)
	t.Logf("Total batches: %d", batchCount)

	// Check which messages are missing
	missing := []string{}
	for i := 0; i < 100; i++ {
		expectedMsg := fmt.Sprintf("debug-msg-%03d", i)
		if !processedMessages[expectedMsg] {
			missing = append(missing, expectedMsg)
		}
	}

	if len(missing) > 0 {
		t.Errorf("MISSING MESSAGES (%d): %v", len(missing), missing)
	}

	// Check consumer offset state
	shard, err := client3.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	shard.mu.RLock()
	offset := shard.index.ConsumerOffsets["debug-group"]
	currentEntry := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("Consumer offset: %d", offset)
	t.Logf("Current entry number: %d", currentEntry)

	if totalProcessed != 100 {
		t.Fatalf("DATA LOSS: Only processed %d/100 messages", totalProcessed)
	}
}
