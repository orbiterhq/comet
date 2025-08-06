package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConsumerWildcardContinuous tests that wildcard Consumer.Process()
// continues to receive new messages as they arrive (like in production)
func TestConsumerWildcardContinuous(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	consumer := NewConsumer(client, ConsumerOptions{Group: "continuous-test"})
	defer consumer.Close()

	streams := []string{
		"events:v1:shard:0000",
		"events:v1:shard:0001",
	}

	// Write initial messages
	t.Log("Writing initial messages...")
	for i, stream := range streams {
		data := [][]byte{[]byte(fmt.Sprintf("initial-message-from-shard-%d", i))}
		_, err := client.Append(ctx, stream, data)
		if err != nil {
			t.Fatalf("Failed to write initial message to %s: %v", stream, err)
		}
	}
	client.Sync(ctx)

	// Start processing in background
	var mu sync.Mutex
	messagesReceived := make(map[string]int)
	processingDone := make(chan error, 1)

	processCtx, processCancel := context.WithCancel(ctx)

	go func() {
		err := consumer.Process(processCtx, func(ctx context.Context, messages []StreamMessage) error {
			mu.Lock()
			for _, msg := range messages {
				t.Logf("DEBUG: Received message ID=%s, Stream=%s, Data=%s", msg.ID, msg.Stream, string(msg.Data))
				messagesReceived[string(msg.Data)]++
			}
			totalReceived := len(messagesReceived)
			mu.Unlock()

			t.Logf("Received %d messages in batch (total unique: %d)", len(messages), totalReceived)

			// Cancel after receiving at least 4 messages (2 initial + 2 additional)
			if totalReceived >= 4 {
				t.Log("Received expected messages, canceling process context")
				processCancel()
			}
			return nil
		},
			WithStream("events:v1:shard:*"), // Wildcard pattern
			WithBatchSize(1),                // Small batches
			WithAutoAck(true),               // Ensure messages are ACKed
		)
		processingDone <- err
	}()

	// Wait a bit for processing to start
	time.Sleep(200 * time.Millisecond)

	// Write additional messages while processing is running
	t.Log("Writing additional messages while processing...")
	for i, stream := range streams {
		data := [][]byte{[]byte(fmt.Sprintf("additional-message-from-shard-%d", i))}
		_, err := client.Append(ctx, stream, data)
		if err != nil {
			t.Fatalf("Failed to write additional message to %s: %v", stream, err)
		}
	}
	client.Sync(ctx)

	// Wait for processing to complete
	select {
	case err := <-processingDone:
		if err != nil && err != context.Canceled {
			t.Fatalf("Process failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Test timed out")
	}

	mu.Lock()
	totalReceived := len(messagesReceived)
	mu.Unlock()

	t.Logf("RESULT: Total unique messages received: %d", totalReceived)

	// We should have received all 4 messages
	if totalReceived < 4 {
		t.Errorf("Expected at least 4 messages but only received %d", totalReceived)

		mu.Lock()
		t.Log("Messages received:")
		for msg, count := range messagesReceived {
			t.Logf("  %s: %d times", msg, count)
		}
		mu.Unlock()
	} else {
		t.Log("SUCCESS: Consumer.Process() with wildcards receives new messages continuously")
	}
}
