//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMessageLoss tests for message loss with multiple concurrent consumers
func TestMessageLoss(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	stream := "loss:v1:shard:0000"

	// Enable debug logging
	SetDebug(true)
	defer SetDebug(false)

	// Write 100 messages
	t.Logf("=== Writing 100 messages ===")
	client, err := NewClient(dir, config)
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

	// Ensure data is flushed before closing
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Test 1: Single consumer should read all messages
	t.Logf("\n=== Test 1: Single Consumer ===")
	client1, _ := NewClient(dir, config)
	consumer1 := NewConsumer(client1, ConsumerOptions{Group: "test-single"})

	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	var read1 []string
	consumer1.Process(ctx1, func(ctx context.Context, msgs []StreamMessage) error {
		for _, msg := range msgs {
			read1 = append(read1, string(msg.Data))
			t.Logf("Single consumer read: %s (offset=%d)", string(msg.Data), msg.ID.EntryNumber)
		}
		return nil
	}, WithStream("loss:v1:shard:*"), WithBatchSize(10), WithAutoAck(true))

	consumer1.Close()
	client1.Close()

	t.Logf("Single consumer read %d messages", len(read1))
	if len(read1) != 100 {
		t.Errorf("Single consumer: Expected 100 messages, got %d", len(read1))
	}

	// Test 2: Three consumers, different groups
	t.Logf("\n=== Test 2: Three Consumers, Different Groups ===")
	var wg sync.WaitGroup
	messagesPerConsumer := make([][]string, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			client, _ := NewClient(dir, config)
			consumer := NewConsumer(client, ConsumerOptions{
				Group: fmt.Sprintf("test-group-%d", consumerID),
			})

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
				for _, msg := range msgs {
					messagesPerConsumer[consumerID] = append(messagesPerConsumer[consumerID], string(msg.Data))
					t.Logf("Consumer %d read: %s (offset=%d)", consumerID, string(msg.Data), msg.ID.EntryNumber)
				}
				return nil
			}, WithStream("loss:v1:shard:*"), WithBatchSize(10), WithAutoAck(true))

			consumer.Close()
			client.Close()

			t.Logf("Consumer %d finished, read %d messages", consumerID, len(messagesPerConsumer[consumerID]))
		}(i)
	}

	wg.Wait()

	// Each consumer in different group should read all 100 messages
	for i := 0; i < 3; i++ {
		if len(messagesPerConsumer[i]) != 100 {
			t.Errorf("Consumer %d (different group): Expected 100 messages, got %d", i, len(messagesPerConsumer[i]))
		}
	}

	// Test 3: Three consumers, SAME group - this is where we expect issues
	t.Logf("\n=== Test 3: Three Consumers, SAME Group ===")

	// First, check what offset the shared group is at
	checkClient, _ := NewClient(dir, config)
	checkShard, _ := checkClient.getOrCreateShard(0)
	checkShard.mu.RLock()
	initialOffset := checkShard.index.ConsumerOffsets["shared-group"]
	checkShard.mu.RUnlock()
	checkClient.Close()
	t.Logf("Initial offset for shared-group: %d", initialOffset)

	var totalRead atomic.Int64
	uniqueMessages := &sync.Map{}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			client, _ := NewClient(dir, config)
			consumer := NewConsumer(client, ConsumerOptions{
				Group: "shared-group",
			})

			// Check offset before reading
			shard, _ := client.getOrCreateShard(0)
			shard.mu.RLock()
			startOffset := shard.index.ConsumerOffsets["shared-group"]
			shard.mu.RUnlock()
			t.Logf("Consumer %d starting at offset: %d", consumerID, startOffset)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			localRead := 0
			consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
				for _, msg := range msgs {
					msgStr := string(msg.Data)
					localRead++
					totalRead.Add(1)

					// Track unique messages
					if _, loaded := uniqueMessages.LoadOrStore(msgStr, consumerID); loaded {
						t.Logf("DUPLICATE: Consumer %d read %s (already processed)", consumerID, msgStr)
					} else {
						t.Logf("Consumer %d read: %s (offset=%d) [UNIQUE]", consumerID, msgStr, msg.ID.EntryNumber)
					}
				}
				return nil
			}, WithStream("loss:v1:shard:*"), WithBatchSize(10), WithAutoAck(true))

			// Check offset after reading
			shard.mu.RLock()
			endOffset := shard.index.ConsumerOffsets["shared-group"]
			shard.mu.RUnlock()

			consumer.Close()
			client.Close()

			t.Logf("Consumer %d finished: read %d messages, offset moved from %d to %d",
				consumerID, localRead, startOffset, endOffset)
		}(i)
	}

	wg.Wait()

	// Count unique messages
	uniqueCount := 0
	uniqueMessages.Range(func(key, value interface{}) bool {
		uniqueCount++
		return true
	})

	t.Logf("\n=== SAME GROUP RESULTS ===")
	t.Logf("Total reads across all consumers: %d", totalRead.Load())
	t.Logf("Unique messages read: %d", uniqueCount)
	t.Logf("Expected unique messages: 100")

	if uniqueCount != 100 {
		t.Errorf("CRITICAL: Message loss detected! Only %d/100 unique messages were processed", uniqueCount)

		// Find which messages were lost
		seen := make(map[string]bool)
		uniqueMessages.Range(func(key, value interface{}) bool {
			seen[key.(string)] = true
			return true
		})

		t.Logf("\n=== MISSING MESSAGES ===")
		for i := 0; i < 100; i++ {
			msg := fmt.Sprintf("msg-%03d", i)
			if !seen[msg] {
				t.Logf("LOST: %s", msg)
			}
		}
	}
}
