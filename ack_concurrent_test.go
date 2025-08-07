//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestACKConcurrentPersistence demonstrates correct multi-client usage with process IDs
// and shows what happens when clients are misconfigured
func TestACKConcurrentPersistence(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	stream := "concurrent:v1:shard:0000"

	// Write messages
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

	// Sync to ensure data is readable
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Test sequential consumers first to establish baseline
	t.Logf("=== Running sequential consumers ===")

	// Consumer 0
	consumer0 := NewConsumer(client, ConsumerOptions{Group: "consumer-0"})
	msgs0, err := consumer0.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs0 {
		if err := consumer0.Ack(ctx, msg.ID); err != nil {
			t.Fatal(err)
		}
	}
	consumer0.Close()
	t.Logf("Consumer 0: Read and ACKed %d messages", len(msgs0))

	// Consumer 1
	consumer1 := NewConsumer(client, ConsumerOptions{Group: "consumer-1"})
	msgs1, err := consumer1.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs1 {
		if err := consumer1.Ack(ctx, msg.ID); err != nil {
			t.Fatal(err)
		}
	}
	consumer1.Close()
	t.Logf("Consumer 1: Read and ACKed %d messages", len(msgs1))

	// Check offsets before close
	shard, _ := client.getOrCreateShard(0)
	shard.mu.RLock()
	offset0 := int64(0)
	offset1 := int64(0)
	if shard.offsetMmap != nil {
		offset0, _ = shard.offsetMmap.Get("consumer-0")
		offset1, _ = shard.offsetMmap.Get("consumer-1")
	}
	shard.mu.RUnlock()

	t.Logf("Before close: consumer-0 offset=%d, consumer-1 offset=%d", offset0, offset1)

	// Close and persist
	client.Close()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Check persisted offsets
	t.Logf("\n=== Checking persisted offsets ===")
	client2, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	shard2, _ := client2.getOrCreateShard(0)

	shard2.mu.RLock()
	// Both should be persisted when using the same client
	offset0Check := int64(0)
	offset1Check := int64(0)
	if shard2.offsetMmap != nil {
		offset0Check, _ = shard2.offsetMmap.Get("consumer-0")
		offset1Check, _ = shard2.offsetMmap.Get("consumer-1")
	}
	shard2.mu.RUnlock()

	if offset0Check != 10 {
		t.Errorf("Consumer 0: Expected offset 10, got %d", offset0Check)
	}
	if offset1Check != 10 {
		t.Errorf("Consumer 1: Expected offset 10, got %d", offset1Check)
	}
	if offset0Check == 10 && offset1Check == 10 {
		t.Logf("✓ Sequential test passed: both consumer offsets persisted correctly")
	}
	client2.Close()

	// Now test the problematic scenario with separate clients
	t.Logf("\n=== Testing concurrent persistence with separate clients ===")

	// Reset by removing consumer offsets
	client3, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	shard3, _ := client3.getOrCreateShard(0)
	shard3.mu.Lock()
	if shard3.offsetMmap != nil {
		shard3.offsetMmap.Remove("consumer-0")
		shard3.offsetMmap.Remove("consumer-1")
	}
	shard3.mu.Unlock()
	client3.Close()

	// Run concurrent consumers with separate clients
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			// Each gets its own client
			c, err := NewClient(dir, DefaultCometConfig())
			if err != nil {
				t.Errorf("Failed to create client %d: %v", consumerID, err)
				return
			}
			defer c.Close()

			consumer := NewConsumer(c, ConsumerOptions{
				Group: fmt.Sprintf("consumer-%d", consumerID),
			})
			defer consumer.Close()

			// Read and ACK
			msgs, err := consumer.Read(ctx, []uint32{0}, 10)
			if err != nil {
				t.Errorf("Consumer %d read error: %v", consumerID, err)
				return
			}

			for _, msg := range msgs {
				if err := consumer.Ack(ctx, msg.ID); err != nil {
					t.Errorf("Consumer %d ACK error: %v", consumerID, err)
					return
				}
			}

			t.Logf("Consumer %d (separate client): ACKed %d messages", consumerID, len(msgs))

			// Small delay between closes to show the race
			time.Sleep(time.Duration(consumerID*50) * time.Millisecond)
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	// Check what got persisted
	client4, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client4.Close()

	shard4, _ := client4.getOrCreateShard(0)
	shard4.mu.RLock()
	offset0Final := int64(0)
	offset1Final := int64(0)
	if shard4.offsetMmap != nil {
		offset0Final, _ = shard4.offsetMmap.Get("consumer-0")
		offset1Final, _ = shard4.offsetMmap.Get("consumer-1")
	}
	shard4.mu.RUnlock()

	t.Logf("After concurrent clients: consumer-0 offset=%d, consumer-1 offset=%d", offset0Final, offset1Final)

	// This demonstrates the race condition - one of the offsets is likely lost
	if offset0Final == 0 || offset1Final == 0 {
		t.Logf("⚠️  Race condition demonstrated: One consumer's offset was lost due to concurrent index writes")
		t.Logf("This happens when multiple clients with same/no process ID write to the same shard")
	}

	// Now demonstrate CORRECT multi-process usage
	t.Logf("\n=== Testing correct multi-process configuration ===")

	// Write messages to both shard 0 and shard 1
	client5, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	// Write to shard 0001 as well
	stream1 := "concurrent:v1:shard:0001"
	_, err = client5.Append(ctx, stream1, messages)
	if err != nil {
		t.Fatal(err)
	}
	client5.Sync(ctx)
	client5.Close()

	// Process 0 handles shard 0, Process 1 handles shard 1
	wg2 := sync.WaitGroup{}
	for processID := 0; processID < 2; processID++ {
		wg2.Add(1)
		go func(pid int) {
			defer wg2.Done()

			// Each process gets proper configuration
			cfg := DeprecatedMultiProcessConfig(pid, 2)
			c, err := NewClient(dir, cfg)
			if err != nil {
				t.Errorf("Process %d: failed to create client: %v", pid, err)
				return
			}
			defer cleanupClient(t, c)

			// Consumer for the shard this process owns
			shardID := uint32(pid)

			consumer := NewConsumer(c, ConsumerOptions{
				Group: fmt.Sprintf("mp-consumer-%d", pid),
			})
			defer consumer.Close()

			// Read and ACK from owned shard
			msgs, err := consumer.Read(ctx, []uint32{shardID}, 10)
			if err != nil {
				t.Errorf("Process %d: read error: %v", pid, err)
				return
			}

			for _, msg := range msgs {
				if err := consumer.Ack(ctx, msg.ID); err != nil {
					t.Errorf("Process %d: ACK error: %v", pid, err)
					return
				}
			}

			t.Logf("Process %d: ACKed %d messages from shard %d", pid, len(msgs), shardID)
		}(processID)
	}

	wg2.Wait()
	time.Sleep(100 * time.Millisecond)

	// Verify both consumer offsets are persisted correctly
	client6, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client6.Close()

	// Check shard 0
	shard0, _ := client6.getOrCreateShard(0)
	shard0.mu.RLock()
	offset0MP := int64(0)
	if shard0.offsetMmap != nil {
		offset0MP, _ = shard0.offsetMmap.Get("mp-consumer-0")
	}
	shard0.mu.RUnlock()

	// Check shard 1
	shard1, _ := client6.getOrCreateShard(1)
	shard1.mu.RLock()
	offset1MP := int64(0)
	if shard1.offsetMmap != nil {
		offset1MP, _ = shard1.offsetMmap.Get("mp-consumer-1")
	}
	shard1.mu.RUnlock()

	t.Logf("With proper multi-process config: mp-consumer-0 offset=%d, mp-consumer-1 offset=%d", offset0MP, offset1MP)

	if offset0MP == 10 && offset1MP == 10 {
		t.Logf("✓ Multi-process mode works correctly when each process owns its shards")
	}
}
