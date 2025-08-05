//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestOffsetMergeLogic tests the offset merge logic that might cause message loss
func TestOffsetMergeLogic(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	stream := "merge:v1:shard:0000"

	// Enable debug
	SetDebug(true)
	defer SetDebug(false)

	// Write 20 messages
	t.Logf("=== Writing 20 messages ===")
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < 20; i++ {
		messages = append(messages, []byte(fmt.Sprintf("msg-%02d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Simulate the problematic scenario:
	// Consumer 1 reads messages 0-9 and ACKs offset 10
	// Consumer 2 starts reading from 0 (before Consumer 1's ACK persists)
	// Consumer 2 reads messages 0-4 and ACKs offset 5
	// Consumer 1's offset 10 persists
	// Consumer 2's offset 5 is ignored (lower than 10)
	// Result: messages 5-9 are never processed by Consumer 2

	t.Logf("\n=== Simulating offset race condition ===")

	// Consumer 1: Read and ACK first 10 messages
	client1, _ := NewClientWithConfig(dir, config)
	consumer1 := NewConsumer(client1, ConsumerOptions{Group: "test-group"})

	msgs1, err := consumer1.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Consumer 1: Read %d messages", len(msgs1))

	// ACK all messages
	for _, msg := range msgs1 {
		if err := consumer1.Ack(ctx, msg.ID); err != nil {
			t.Fatal(err)
		}
	}

	// Get offset before closing
	shard1, _ := client1.getOrCreateShard(0)
	shard1.mu.RLock()
	offset1 := shard1.index.ConsumerOffsets["test-group"]
	shard1.mu.RUnlock()
	t.Logf("Consumer 1: Offset before close = %d", offset1)

	// Start Consumer 2 BEFORE Consumer 1 closes (simulating concurrent access)
	client2, _ := NewClientWithConfig(dir, config)
	consumer2 := NewConsumer(client2, ConsumerOptions{Group: "test-group"})

	// Check what offset Consumer 2 sees
	shard2, _ := client2.getOrCreateShard(0)
	shard2.mu.RLock()
	offset2Start := shard2.index.ConsumerOffsets["test-group"]
	shard2.mu.RUnlock()
	t.Logf("Consumer 2: Starting offset = %d", offset2Start)

	// Now close Consumer 1 (this triggers persist)
	consumer1.Close()
	client1.Close()

	// Consumer 2 reads 5 messages
	msgs2, err := consumer2.Read(ctx, []uint32{0}, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Consumer 2: Read %d messages starting from entry %d", len(msgs2), msgs2[0].ID.EntryNumber)

	// ACK messages
	for _, msg := range msgs2 {
		if err := consumer2.Ack(ctx, msg.ID); err != nil {
			t.Fatal(err)
		}
	}

	// Check offset before close
	shard2.mu.RLock()
	offset2End := shard2.index.ConsumerOffsets["test-group"]
	shard2.mu.RUnlock()
	t.Logf("Consumer 2: Offset before close = %d", offset2End)

	// Close Consumer 2
	consumer2.Close()
	client2.Close()

	// Wait for persistence
	time.Sleep(100 * time.Millisecond)

	// Check final persisted offset
	client3, _ := NewClientWithConfig(dir, config)
	shard3, _ := client3.getOrCreateShard(0)
	shard3.mu.RLock()
	finalOffset := shard3.index.ConsumerOffsets["test-group"]
	shard3.mu.RUnlock()
	client3.Close()

	t.Logf("\n=== RESULTS ===")
	t.Logf("Consumer 1 processed: messages 0-9, set offset to %d", offset1)
	t.Logf("Consumer 2 started at offset %d, processed 5 messages, set offset to %d", offset2Start, offset2End)
	t.Logf("Final persisted offset: %d", finalOffset)

	// The issue: if final offset is 10 but Consumer 2 only processed up to 5,
	// then messages 5-9 are lost for Consumer 2's processing
	if finalOffset == 10 && offset2End < 10 {
		t.Errorf("CRITICAL: Offset merge caused message loss! Consumer 2 only processed up to %d but offset jumped to %d", offset2End, finalOffset)
	}

	// Now test with proper locking
	t.Logf("\n=== Testing concurrent offset updates ===")

	var wg sync.WaitGroup
	processed := make(map[int][]string)
	var mu sync.Mutex

	// Two consumers reading concurrently
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			client, _ := NewClientWithConfig(dir, config)
			consumer := NewConsumer(client, ConsumerOptions{Group: "concurrent-group"})

			// Read 5 messages
			msgs, err := consumer.Read(ctx, []uint32{0}, 5)
			if err != nil {
				return
			}

			mu.Lock()
			for _, msg := range msgs {
				processed[consumerID] = append(processed[consumerID], string(msg.Data))
			}
			mu.Unlock()

			// ACK messages
			for _, msg := range msgs {
				consumer.Ack(ctx, msg.ID)
			}

			consumer.Close()
			client.Close()
		}(i)
	}

	wg.Wait()

	t.Logf("\n=== Concurrent processing results ===")
	for id, msgs := range processed {
		t.Logf("Consumer %d processed: %v", id, msgs)
	}

	// Check for duplicates or gaps
	seen := make(map[string]int)
	for _, msgs := range processed {
		for _, msg := range msgs {
			seen[msg]++
		}
	}

	duplicates := 0
	for msg, count := range seen {
		if count > 1 {
			t.Logf("Message %s processed %d times", msg, count)
			duplicates++
		}
	}

	if duplicates > 0 {
		t.Logf("Found %d duplicate messages in concurrent processing", duplicates)
	}
}
