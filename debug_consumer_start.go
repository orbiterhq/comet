package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDebugConsumerStart(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Enable debug
	SetDebug(true)
	defer SetDebug(false)

	stream := "debug:v1:shard:0001"

	// Write some messages
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < 10; i++ {
		messages = append(messages, []byte(fmt.Sprintf("debug-msg-%02d", i)))
	}

	ctx := context.Background()
	result, err := client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Append result: %+v", result)
	client.Close()

	// Now try to read with a consumer
	t.Logf("\n=== Starting consumer ===")
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client2, ConsumerOptions{Group: "debug-group"})

	// Check shard state before reading
	t.Logf("\n=== Checking shard state ===")
	shard, err := client2.getOrCreateShard(1)
	if err != nil {
		t.Logf("Error getting shard: %v", err)
	} else {
		shard.mu.RLock()
		t.Logf("Shard state:")
		t.Logf("  CurrentEntryNumber: %d", shard.index.CurrentEntryNumber)
		t.Logf("  Files count: %d", len(shard.index.Files))
		for i, file := range shard.index.Files {
			t.Logf("    File %d: %+v", i, file)
		}
		t.Logf("  Consumer offsets: %+v", shard.index.ConsumerOffsets)
		shard.mu.RUnlock()
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	processedCount := 0
	err = consumer.Process(ctx2, func(ctx context.Context, msgs []StreamMessage) error {
		t.Logf("Process callback: received %d messages", len(msgs))
		for _, msg := range msgs {
			t.Logf("  Read: %s", string(msg.Data))
			processedCount++
		}
		return nil
	}, WithStream("debug:v1:shard:*"), WithBatchSize(3), WithAutoAck(true))

	t.Logf("Process returned: %v", err)
	t.Logf("Processed %d messages", processedCount)

	consumer.Close()
	client2.Close()
}
