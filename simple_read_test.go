//go:build integration

package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSimpleRead tests basic read functionality
func TestSimpleRead(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)

	// Enable debug
	SetDebug(true)
	defer SetDebug(false)

	// Test with explicit shard ID
	shardID := uint32(1)
	stream := fmt.Sprintf("simple:v1:shard:%04d", shardID)

	t.Logf("=== Writing to stream: %s ===", stream)

	// Write 10 messages
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < 10; i++ {
		messages = append(messages, []byte(fmt.Sprintf("msg-%02d", i)))
	}

	ctx := context.Background()
	result, err := client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Append result: %+v", result)

	// Ensure data is flushed before closing
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Try to read with exact stream
	t.Logf("\n=== Reading with exact stream: %s ===", stream)
	client2, _ := NewClient(dir, config)
	consumer2 := NewConsumer(client2, ConsumerOptions{Group: "test"})

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	readCount := 0
	err = consumer2.Process(ctx2, func(ctx context.Context, msgs []StreamMessage) error {
		t.Logf("Process callback: received %d messages", len(msgs))
		for _, msg := range msgs {
			t.Logf("  Read: %s", string(msg.Data))
			readCount++
		}
		return nil
	}, WithStream(stream), WithBatchSize(5), WithAutoAck(true))

	consumer2.Close()
	client2.Close()

	t.Logf("Read %d messages with exact stream", readCount)

	// Try to read with wildcard
	t.Logf("\n=== Reading with wildcard stream: simple:v1:shard:* ===")
	client3, _ := NewClient(dir, config)
	consumer3 := NewConsumer(client3, ConsumerOptions{Group: "test2"})

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()

	readCount2 := 0
	err = consumer3.Process(ctx3, func(ctx context.Context, msgs []StreamMessage) error {
		t.Logf("Process callback: received %d messages", len(msgs))
		for _, msg := range msgs {
			t.Logf("  Read: %s", string(msg.Data))
			readCount2++
		}
		return nil
	}, WithStream("simple:v1:shard:*"), WithBatchSize(5), WithAutoAck(true))

	consumer3.Close()
	client3.Close()

	t.Logf("Read %d messages with wildcard stream", readCount2)

	// Check what shards exist
	t.Logf("\n=== Checking shard structure ===")
	client4, _ := NewClient(dir, config)

	// Try to get shard directly
	shard, err := client4.getOrCreateShard(shardID)
	if err != nil {
		t.Logf("Error getting shard %d: %v", shardID, err)
	} else {
		shard.mu.RLock()
		t.Logf("Shard %d index state:", shardID)
		t.Logf("  CurrentEntryNumber: %d", shard.index.CurrentEntryNumber)
		t.Logf("  Files: %d", len(shard.index.Files))
		t.Logf("  Consumer offsets: %+v", shard.index.ConsumerOffsets)
		shard.mu.RUnlock()
	}

	client4.Close()

	if readCount == 0 && readCount2 == 0 {
		t.Error("CRITICAL: Unable to read any messages!")
	}
}
