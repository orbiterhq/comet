package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestConsumerWildcardDiscovery tests that wildcard stream patterns work correctly
func TestConsumerWildcardDiscovery(t *testing.T) {
	SetDebug(true)
	defer SetDebug(false)
	dataDir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	consumer := NewConsumer(client, ConsumerOptions{Group: "wildcard-test"})
	defer consumer.Close()

	// Write to multiple shards that match a pattern
	streams := []string{
		"events:v1:shard:0000",
		"events:v1:shard:0001",
		"events:v1:shard:0002",
	}

	totalWritten := 0
	for i, stream := range streams {
		data := [][]byte{[]byte(fmt.Sprintf("message-from-shard-%d", i))}
		_, err := client.Append(ctx, stream, data)
		if err != nil {
			t.Fatalf("Failed to write to %s: %v", stream, err)
		}
		totalWritten++
	}

	// Sync all data
	client.Sync(ctx)

	t.Logf("Written %d messages to %d shards", totalWritten, len(streams))

	// Use Process with wildcard pattern to discover and read from all shards
	messagesReceived := 0
	processDone := make(chan error, 1)

	processCtx, processCancel := context.WithCancel(ctx)

	go func() {
		err := consumer.Process(processCtx, func(ctx context.Context, messages []StreamMessage) error {
			for _, msg := range messages {
				t.Logf("DEBUG: Received message ID=%s, Stream=%s, Data=%s", msg.ID, msg.Stream, string(msg.Data))
			}
			messagesReceived += len(messages)
			t.Logf("Received %d messages in batch (total: %d)", len(messages), messagesReceived)

			// Stop after receiving all expected messages
			if messagesReceived >= totalWritten {
				t.Logf("Got expected %d messages, canceling context", totalWritten)
				processCancel() // Cancel the context instead of returning error
			}
			return nil // Always return success so messages get ACKed
		},
			WithStream("events:v1:shard:*"), // Wildcard pattern
			WithBatchSize(1),                // Small batches for easier debugging
			WithAutoAck(true),               // Ensure messages are ACKed automatically
		)
		processDone <- err
	}()

	// Wait for processing to complete or timeout
	select {
	case err := <-processDone:
		if err != nil && err != context.Canceled {
			t.Fatalf("Process failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Process timed out - wildcard discovery may be broken")
	}

	t.Logf("RESULT: Written=%d, Received=%d", totalWritten, messagesReceived)

	if messagesReceived != totalWritten {
		t.Errorf("Wildcard discovery failed: wrote %d messages but only received %d", totalWritten, messagesReceived)
	} else {
		t.Log("SUCCESS: Wildcard discovery works correctly")
	}
}
