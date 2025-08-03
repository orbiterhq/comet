package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestDebugProcessIssue focuses on why Process() gets 0 messages in multi-process mode
func TestDebugProcessIssue(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0166"

	// Write messages
	totalMessages := 10
	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("debug-msg-%d", i)))
	}
	
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	// Verify messages exist
	length, err := client.Len(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Stream length after write: %d entries", length)

	consumer := NewConsumer(client, ConsumerOptions{Group: "debug-group"})
	defer consumer.Close()

	// Test 1: Try ListRecent - does it work?
	t.Log("=== Testing ListRecent ===")
	recent, err := client.ListRecent(ctx, stream, 5)
	if err != nil {
		t.Errorf("ListRecent failed: %v", err)
	} else {
		t.Logf("ListRecent returned %d messages", len(recent))
		for i, msg := range recent {
			t.Logf("  [%d] %s", i, string(msg.Data))
		}
	}

	// Test 2: Try manual Read - does it work?
	t.Log("=== Testing Manual Read ===")
	readMessages, err := consumer.Read(ctx, []uint32{166}, 5)
	if err != nil {
		t.Errorf("Manual Read failed: %v", err)
	} else {
		t.Logf("Manual Read returned %d messages", len(readMessages))
		for i, msg := range readMessages {
			t.Logf("  [%d] ID=%s, Data=%s", i, msg.ID.String(), string(msg.Data))
		}
	}

	// Test 3: Try discoverShards - does it find the shard?
	t.Log("=== Testing Shard Discovery ===")
	shards, err := consumer.discoverShards("test:v1:shard:*", 0, 1)
	if err != nil {
		t.Errorf("Shard discovery failed: %v", err)
	} else {
		t.Logf("Discovered shards: %v", shards)
	}

	// Test 4: Get shard stats
	t.Log("=== Testing Shard Stats ===")
	stats, err := consumer.GetShardStats(ctx, 166)
	if err != nil {
		t.Errorf("GetShardStats failed: %v", err)
	} else {
		t.Logf("Shard stats: TotalEntries=%d, ConsumerOffsets=%v", stats.TotalEntries, stats.ConsumerOffsets)
	}

	// Test 5: Check consumer lag
	t.Log("=== Testing Consumer Lag ===")
	lag, err := consumer.GetLag(ctx, 166)
	if err != nil {
		t.Errorf("GetLag failed: %v", err)
	} else {
		t.Logf("Consumer lag: %d", lag)
	}

	// Test 6: Try Process with manual timeout and logging
	t.Log("=== Testing Process with Debugging ===")
	
	processCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	processCallCount := 0
	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		processCallCount++
		t.Logf("ProcessFunc called %d times with %d messages", processCallCount, len(msgs))
		for i, msg := range msgs {
			t.Logf("  Process[%d] ID=%s, Data=%s", i, msg.ID.String(), string(msg.Data))
		}
		
		// Cancel after first successful batch
		if len(msgs) > 0 {
			cancel()
		}
		
		return nil
	}

	t.Logf("Starting Process() with stream pattern 'test:v1:shard:*'")
	err = consumer.Process(processCtx, processFunc,
		WithStream("test:v1:shard:*"),
		WithBatchSize(3),
		WithAutoAck(true),
		WithPollInterval(100*time.Millisecond),
	)

	t.Logf("Process() completed with error: %v", err)
	t.Logf("ProcessFunc was called %d times", processCallCount)
}