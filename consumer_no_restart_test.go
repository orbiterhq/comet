package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestConsumerReceivesEventsWithoutRestart specifically tests that consumers
// can receive new events without needing to restart after initial setup
func TestConsumerReceivesEventsWithoutRestart(t *testing.T) {
	dataDir := t.TempDir()

	// Create client with short flush interval for faster testing
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 * time.Millisecond // 50ms flush interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write initial batch of messages
	t.Log("Writing initial batch of 5 messages...")
	initialMessages := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		initialMessages[i] = []byte(fmt.Sprintf("initial-message-%d", i))
	}

	_, err = client.Append(ctx, stream, initialMessages)
	if err != nil {
		t.Fatalf("Failed to append initial messages: %v", err)
	}

	// Sync to make messages durable
	if err := client.Sync(ctx); err != nil {
		t.Fatalf("Failed to sync initial messages: %v", err)
	}

	// Create consumer AFTER initial messages exist
	consumer := NewConsumer(client, ConsumerOptions{Group: "no-restart-test"})
	defer consumer.Close()

	// Read initial messages
	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Failed to read initial messages: %v", err)
	}
	t.Logf("Read %d initial messages", len(messages))

	if len(messages) != 5 {
		t.Errorf("Expected 5 initial messages, got %d", len(messages))
	}

	// ACK them
	messageIDs := make([]MessageID, len(messages))
	for i, msg := range messages {
		messageIDs[i] = msg.ID
	}
	if err := consumer.Ack(ctx, messageIDs...); err != nil {
		t.Fatalf("Failed to ACK initial messages: %v", err)
	}

	// Now write MORE messages (this is the critical test)
	t.Log("Writing additional batch of 5 messages...")
	additionalMessages := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		additionalMessages[i] = []byte(fmt.Sprintf("additional-message-%d", i))
	}

	_, err = client.Append(ctx, stream, additionalMessages)
	if err != nil {
		t.Fatalf("Failed to append additional messages: %v", err)
	}

	// Sync to make messages durable and update index
	if err := client.Sync(ctx); err != nil {
		t.Fatalf("Failed to sync additional messages: %v", err)
	}

	// Give a small delay to ensure flush completes
	time.Sleep(100 * time.Millisecond)

	// Try to read the new messages with the SAME consumer (no restart)
	t.Log("Attempting to read additional messages with same consumer...")

	maxAttempts := 10
	var newMessages []StreamMessage

	for attempt := 0; attempt < maxAttempts; attempt++ {
		newMessages, err = consumer.Read(ctx, []uint32{0}, 10)
		if err != nil {
			t.Errorf("Failed to read additional messages on attempt %d: %v", attempt+1, err)
			continue
		}

		if len(newMessages) > 0 {
			t.Logf("Successfully read %d additional messages on attempt %d", len(newMessages), attempt+1)
			break
		}

		t.Logf("No new messages on attempt %d, waiting 100ms...", attempt+1)
		time.Sleep(100 * time.Millisecond)
	}

	// Verify we got the additional messages
	if len(newMessages) == 0 {
		t.Fatal("FAILED: Consumer did not receive any additional messages - still broken!")
	}

	if len(newMessages) != 5 {
		t.Errorf("Expected 5 additional messages, got %d", len(newMessages))
	}

	// Verify message content
	for i, msg := range newMessages {
		expected := fmt.Sprintf("additional-message-%d", i)
		if string(msg.Data) != expected {
			t.Errorf("Message %d content mismatch: got %q, want %q", i, string(msg.Data), expected)
		}
	}

	t.Log("SUCCESS: Consumer successfully received new events without restart!")
}
