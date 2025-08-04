//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestACKConcurrentPersistence tests concurrent ACK persistence
func TestACKConcurrentPersistence(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	stream := "concurrent:v1:shard:0000"

	// Write messages
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

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

	// Run two consumers concurrently that ACK at the same time
	t.Logf("=== Running concurrent consumers ===")

	var wg sync.WaitGroup
	errors := make(chan error, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			client, err := NewClientWithConfig(dir, config)
			if err != nil {
				errors <- fmt.Errorf("consumer %d: failed to create client: %w", consumerID, err)
				return
			}
			defer client.Close()

			consumer := NewConsumer(client, ConsumerOptions{
				Group: fmt.Sprintf("consumer-%d", consumerID),
			})
			defer consumer.Close()

			// Read and ACK 10 messages
			msgs, err := consumer.Read(ctx, []uint32{0}, 10)
			if err != nil {
				errors <- fmt.Errorf("consumer %d: read error: %w", consumerID, err)
				return
			}

			t.Logf("Consumer %d: Read %d messages", consumerID, len(msgs))

			// ACK all messages
			for _, msg := range msgs {
				if err := consumer.Ack(ctx, msg.ID); err != nil {
					errors <- fmt.Errorf("consumer %d: ACK error: %w", consumerID, err)
					return
				}
			}

			t.Logf("Consumer %d: ACKed all messages", consumerID)

			// Get offset before close
			shard, _ := client.getOrCreateShard(0)
			shard.mu.RLock()
			offset := shard.index.ConsumerOffsets[fmt.Sprintf("consumer-%d", consumerID)]
			shard.mu.RUnlock()

			t.Logf("Consumer %d: Offset before close: %d", consumerID, offset)
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Check persisted offsets
	t.Logf("\n=== Checking persisted offsets ===")
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	shard, err := client2.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	shard.mu.RLock()
	for i := 0; i < 2; i++ {
		group := fmt.Sprintf("consumer-%d", i)
		offset := shard.index.ConsumerOffsets[group]
		t.Logf("Consumer %d persisted offset: %d", i, offset)
		if offset != 10 {
			t.Errorf("Consumer %d: Expected offset 10, got %d", i, offset)
		}
	}
	shard.mu.RUnlock()
}
