package comet

import (
	"context"
	"fmt"
	"testing"
)

// TestConsumerProcessedMessagesCleanup verifies that the processedMsgs map doesn't grow unbounded
func TestConsumerProcessedMessagesCleanup(t *testing.T) {
	config := DefaultCometConfig()
	config.Concurrency.ProcessCount = 0 // Ensure single-process mode
	client, err := NewClient(t.TempDir(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{
		Group:         "test-group",
		ConsumerID:    1,
		ConsumerCount: 1,
	})
	defer consumer.Close()

	// Write many messages in batches for efficiency
	const numMessages = 10000
	const batchSize = 100

	for i := 0; i < numMessages; i += batchSize {
		batch := make([][]byte, 0, batchSize)
		for j := 0; j < batchSize && i+j < numMessages; j++ {
			data := []byte(fmt.Sprintf("message-%d", i+j))
			batch = append(batch, data)
		}
		if _, err := client.Append(ctx, stream, batch); err != nil {
			t.Fatal(err)
		}
	}

	// Process messages in batches
	totalProcessed := 0
	for totalProcessed < numMessages {
		shardID, _ := parseShardFromStream(stream)
		messages, err := consumer.Read(ctx, []uint32{shardID}, 100)
		if err != nil {
			t.Fatal(err)
		}

		if len(messages) == 0 {
			break
		}

		// ACK messages
		for _, msg := range messages {
			if err := consumer.Ack(ctx, msg.ID); err != nil {
				t.Fatal(err)
			}
		}

		totalProcessed += len(messages)

		// Check the size of processedMsgs periodically
		if totalProcessed%2000 == 0 {
			consumer.processedMsgsMu.RLock()
			mapSize := len(consumer.processedMsgs)
			consumer.processedMsgsMu.RUnlock()

			t.Logf("After processing %d messages, processedMsgs size: %d", totalProcessed, mapSize)

			// The map should not grow unbounded
			// With our cleanup strategy, it should stay under 6000 entries
			if mapSize > 6000 {
				t.Errorf("processedMsgs map too large: %d entries", mapSize)
			}
		}
	}

	// Final check
	consumer.processedMsgsMu.RLock()
	finalSize := len(consumer.processedMsgs)
	consumer.processedMsgsMu.RUnlock()

	t.Logf("Final processedMsgs size after processing %d messages: %d", numMessages, finalSize)

	// After processing all messages, the map should be relatively small
	// since most entries should have been cleaned up
	if finalSize > 1000 {
		t.Errorf("Final processedMsgs map too large: %d entries", finalSize)
	}
}

// TestConsumerProcessedMessagesWithReprocess verifies cleanup works correctly with reprocessing
func TestConsumerProcessedMessagesWithReprocess(t *testing.T) {
	config := DefaultCometConfig()
	config.Concurrency.ProcessCount = 0 // Ensure single-process mode
	client, err := NewClient(t.TempDir(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write messages in batches for efficiency
	const numMessages = 100
	batch := make([][]byte, 0, 10)
	for i := 0; i < numMessages; i++ {
		data := []byte(fmt.Sprintf("message-%d", i))
		batch = append(batch, data)

		// Write in batches of 10
		if len(batch) == 10 || i == numMessages-1 {
			if _, err := client.Append(ctx, stream, batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}

	// Ensure data is synced
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Create first consumer and process half the messages
	consumer1 := NewConsumer(client, ConsumerOptions{
		Group:         "test-group",
		ConsumerID:    1,
		ConsumerCount: 1,
	})

	// Read and ACK first 50 messages
	shardID, _ := parseShardFromStream(stream)
	messages, err := consumer1.Read(ctx, []uint32{shardID}, 50)
	if err != nil {
		t.Fatal(err)
	}

	for _, msg := range messages {
		if err := consumer1.Ack(ctx, msg.ID); err != nil {
			t.Fatal(err)
		}
	}

	// Check that messages are marked as processed
	consumer1.processedMsgsMu.RLock()
	processedCount := len(consumer1.processedMsgs)
	consumer1.processedMsgsMu.RUnlock()

	if processedCount == 0 {
		t.Error("No messages marked as processed")
	}

	consumer1.Close()

	// Create new consumer with same group - it should continue from offset 50
	consumer2 := NewConsumer(client, ConsumerOptions{
		Group:         "test-group",
		ConsumerID:    1,
		ConsumerCount: 1,
	})
	defer consumer2.Close()

	// Read remaining messages
	messages, err = consumer2.Read(ctx, []uint32{shardID}, 100)
	if err != nil {
		t.Fatal(err)
	}

	// Should get messages 50-99
	if len(messages) != 50 {
		t.Errorf("Expected 50 messages, got %d", len(messages))
	}

	// Verify the first message is #50
	if len(messages) > 0 && messages[0].ID.EntryNumber != 50 {
		t.Errorf("Expected first message to be entry 50, got %d", messages[0].ID.EntryNumber)
	}
}
