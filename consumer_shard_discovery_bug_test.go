//go:build integration

package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestConsumerShardDiscoveryBug reproduces the issue where a consumer
// can't read from existing shards created by a different client instance
func TestConsumerShardDiscoveryBug(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 * time.Millisecond

	ctx := context.Background()

	// Step 1: Writer creates shards and writes data
	t.Log("Step 1: Creating writer and writing data...")
	writer, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write to multiple shards
	shardsToTest := []uint32{0, 1, 2, 3}
	messagesPerShard := 10

	for _, shardID := range shardsToTest {
		stream := fmt.Sprintf("test:v1:shard:%04d", shardID)
		for i := 0; i < messagesPerShard; i++ {
			data := fmt.Sprintf("shard-%d-msg-%d", shardID, i)
			_, err := writer.Append(ctx, stream, [][]byte{[]byte(data)})
			if err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("  Wrote %d messages to shard %d", messagesPerShard, shardID)
	}

	// Force flush to ensure data is persisted
	writer.Sync(ctx)
	time.Sleep(100 * time.Millisecond)

	// Verify data was written
	for _, shardID := range shardsToTest {
		stream := fmt.Sprintf("test:v1:shard:%04d", shardID)
		length, err := writer.Len(ctx, stream)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("  Verified shard %d has %d entries", shardID, length)
	}

	writer.Close()

	// Step 2: Create a fresh reader client (simulating a restart)
	t.Log("\nStep 2: Creating fresh reader client...")
	reader, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Step 3: Create consumer and try to read
	t.Log("\nStep 3: Creating consumer and attempting to read...")
	consumer := NewConsumer(reader, ConsumerOptions{
		Group: "test-consumer",
	})
	defer consumer.Close()

	// First, let's see what shards the consumer discovers
	discoveredShards, err := consumer.findExistingShards()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("  Consumer discovered %d shards on disk: %v", len(discoveredShards), discoveredShards)

	// Now try to read from these shards
	totalRead := 0
	messages, err := consumer.Read(ctx, shardsToTest, 100)
	if err != nil {
		t.Logf("  ERROR reading: %v", err)
	} else {
		totalRead += len(messages)
		t.Logf("  Read %d messages", len(messages))
	}

	// Try a few more times in case there's a delay
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		messages, err = consumer.Read(ctx, shardsToTest, 100)
		if err != nil {
			t.Logf("  Retry %d - ERROR: %v", i+1, err)
		} else if len(messages) > 0 {
			totalRead += len(messages)
			t.Logf("  Retry %d - Read %d more messages", i+1, len(messages))
		}
	}

	// Step 4: Diagnose the issue
	t.Log("\nStep 4: Diagnosing the issue...")

	// Check what shards are loaded in the reader client
	reader.mu.RLock()
	loadedShards := make([]uint32, 0, len(reader.shards))
	for shardID := range reader.shards {
		loadedShards = append(loadedShards, shardID)
	}
	reader.mu.RUnlock()
	t.Logf("  Reader client has %d shards loaded in memory: %v", len(loadedShards), loadedShards)

	// Try to get each shard through the consumer
	for _, shardID := range shardsToTest {
		_, err := consumer.getExistingShard(shardID)
		if err != nil {
			t.Logf("  consumer.getExistingShard(%d) failed: %v", shardID, err)
		} else {
			t.Logf("  consumer.getExistingShard(%d) succeeded!", shardID)
		}
	}

	// Final verdict
	expectedMessages := len(shardsToTest) * messagesPerShard
	t.Logf("\nFinal result: Read %d/%d messages (%.1f%%)",
		totalRead, expectedMessages, float64(totalRead)/float64(expectedMessages)*100)

	if totalRead == 0 {
		t.Error("BUG REPRODUCED: Consumer couldn't read any messages from existing shards!")
		t.Log("\nRoot cause: Consumer discovers shard directories on disk but can't access them")
		t.Log("because they're not loaded in the reader client's memory.")
	} else if totalRead < expectedMessages {
		t.Errorf("Partial failure: Only read %d/%d messages", totalRead, expectedMessages)
	} else {
		t.Log("Test passed - all messages were read successfully")
	}
}

// TestConsumerShardDiscoveryWithLenWorkaround shows that calling Len()
// loads the shard, working around the bug
func TestConsumerShardDiscoveryWithLenWorkaround(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 * time.Millisecond

	ctx := context.Background()

	// Step 1: Writer creates shards and writes data
	t.Log("Step 1: Creating writer and writing data...")
	writer, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write to a shard
	stream := "test:v1:shard:0000"
	for i := 0; i < 10; i++ {
		data := fmt.Sprintf("msg-%d", i)
		_, err := writer.Append(ctx, stream, [][]byte{[]byte(data)})
		if err != nil {
			t.Fatal(err)
		}
	}

	writer.Sync(ctx)
	writer.Close()

	// Step 2: Create fresh reader
	t.Log("\nStep 2: Creating fresh reader client...")
	reader, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Step 3: Try consumer WITHOUT workaround
	t.Log("\nStep 3: Testing consumer WITHOUT workaround...")
	consumer1 := NewConsumer(reader, ConsumerOptions{
		Group: "test-consumer-1",
	})

	messages, _ := consumer1.Read(ctx, []uint32{0}, 100)
	t.Logf("  Without workaround: Read %d messages", len(messages))
	consumer1.Close()

	// Step 4: Apply workaround - call Len() to load the shard
	t.Log("\nStep 4: Applying workaround - calling Len() to load shard...")
	length, err := reader.Len(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("  Len() returned %d entries (this loads the shard into memory)", length)

	// Step 5: Try consumer WITH workaround applied
	t.Log("\nStep 5: Testing consumer AFTER workaround...")
	consumer2 := NewConsumer(reader, ConsumerOptions{
		Group: "test-consumer-2",
	})
	defer consumer2.Close()

	messages, _ = consumer2.Read(ctx, []uint32{0}, 100)
	t.Logf("  With workaround: Read %d messages", len(messages))

	if len(messages) > 0 {
		t.Log("\nWorkaround confirmed: Calling Len() loads the shard, allowing consumer to read!")
	}
}
