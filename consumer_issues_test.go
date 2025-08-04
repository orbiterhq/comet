package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestConsumerGroupMemoryLeak verifies that consumer groups are leaked when consumers are closed
func TestConsumerGroupMemoryLeak(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Get initial count
	initialGroups := len(client.getActiveGroups())
	t.Logf("Initial active groups: %d", initialGroups)

	// Create and close many consumers
	for i := 0; i < 100; i++ {
		consumer := NewConsumer(client, ConsumerOptions{Group: fmt.Sprintf("group-%d", i)})
		consumer.Close()
	}

	// Check if groups are still registered
	finalGroups := len(client.getActiveGroups())
	t.Logf("Final active groups: %d", finalGroups)

	if finalGroups > initialGroups {
		t.Errorf("Consumer groups leaked: started with %d, ended with %d", initialGroups, finalGroups)
		t.Logf("This proves consumer groups are never deregistered!")
	}
}

// TestSingleProcessACKPersistence verifies ACKs might be lost in single-process mode
func TestSingleProcessACKPersistence(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Use single-process config
	config := DefaultCometConfig()

	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write messages
	_, err = client1.Append(ctx, "ack-test:v1:shard:0000", [][]byte{
		[]byte("msg-1"),
		[]byte("msg-2"),
		[]byte("msg-3"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create consumer and ACK messages
	consumer1 := NewConsumer(client1, ConsumerOptions{Group: "test-group"})
	msgs, err := consumer1.Read(ctx, []uint32{0}, 3)
	if err != nil {
		t.Fatal(err)
	}

	// ACK all messages
	for _, msg := range msgs {
		if err := consumer1.Ack(ctx, msg.ID); err != nil {
			t.Fatal(err)
		}
	}

	// Close WITHOUT calling Sync()
	consumer1.Close()
	client1.Close()

	// Simulate crash and restart - open new client
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	consumer2 := NewConsumer(client2, ConsumerOptions{Group: "test-group"})
	defer consumer2.Close()

	// Check if ACKs were persisted
	stats, err := consumer2.GetShardStats(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Consumer offset after restart: %v", stats.ConsumerOffsets)

	// Try to read - if we get messages, ACKs were lost
	msgs2, err := consumer2.Read(ctx, []uint32{0}, 3)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs2) > 0 {
		t.Errorf("ACKs were lost! Consumer re-read %d messages after restart", len(msgs2))
		t.Logf("This proves ACKs aren't immediately persisted in single-process mode")
	}
}

// TestAckRangeValidation verifies AckRange allows ACKing unread entries
func TestAckRangeValidation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write 10 messages
	_, err = client.Append(ctx, "range-test:v1:shard:0000", [][]byte{
		[]byte("msg-0"), []byte("msg-1"), []byte("msg-2"),
		[]byte("msg-3"), []byte("msg-4"), []byte("msg-5"),
		[]byte("msg-6"), []byte("msg-7"), []byte("msg-8"),
		[]byte("msg-9"),
	})
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "range-group"})
	defer consumer.Close()

	// Read only first 3 messages
	msgs, err := consumer.Read(ctx, []uint32{0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Read %d messages", len(msgs))

	// Try to ACK range 0-9 (including unread messages 3-9)
	err = consumer.AckRange(ctx, 1, 0, 9)
	if err == nil {
		t.Fatal("Expected error when ACKing unread messages, but got none")
	}
	t.Logf("✅ AckRange correctly rejected: %v", err)

	// Now ACK only the messages we've read (0-2)
	err = consumer.AckRange(ctx, 1, 0, 2)
	if err != nil {
		t.Fatalf("Failed to ACK read messages: %v", err)
	}
	t.Logf("✅ AckRange accepted for read messages 0-2")

	// Force a sync to ensure it's persisted
	client.Sync(ctx)

	// Check consumer offset
	stats, err := consumer.GetShardStats(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	offset, exists := stats.ConsumerOffsets["range-group"]
	t.Logf("Consumer offset after AckRange: %d (exists: %v)", offset, exists)

	// Should be 3 (next message to read)
	if offset != 3 {
		t.Errorf("Expected consumer offset 3 after ACKing messages 0-2, got %d", offset)
	}

	// Check lag - should be 7 (messages 3-9 remaining)
	lag, err := consumer.GetLag(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Consumer lag: %d", lag)

	if lag != 7 {
		t.Errorf("Expected lag of 7 (messages 3-9), got %d", lag)
	}

	// Verify we can read the remaining messages
	msgs2, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("After AckRange, consumer can read %d messages", len(msgs2))

	if len(msgs2) != 7 {
		t.Errorf("Expected to read 7 remaining messages, got %d", len(msgs2))
	}

	// Verify first unread message is msg-3
	if len(msgs2) > 0 && string(msgs2[0].Data) != "msg-3" {
		t.Errorf("Expected first unread message to be 'msg-3', got '%s'", string(msgs2[0].Data))
	}
}

// TestProcessAutoAckFailureHandling verifies what happens when auto-ACK fails
func TestProcessAutoAckFailureHandling(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	client, err := NewClient(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write messages
	_, err = client.Append(ctx, "process-test:v1:shard:0000", [][]byte{
		[]byte("msg-1"), []byte("msg-2"), []byte("msg-3"),
	})
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "process-group"})
	defer consumer.Close()

	processedCount := 0
	errorCount := 0

	// Use a short timeout to simulate ACK failures
	processCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	consumer.Process(processCtx, func(ctx context.Context, msgs []StreamMessage) error {
		processedCount += len(msgs)
		t.Logf("Processed batch of %d messages", len(msgs))

		// Cancel context to cause ACK failures
		if processedCount >= 3 {
			cancel()
		}
		return nil
	},
		WithBatchSize(1),
		WithAutoAck(true),
		WithErrorHandler(func(err error, retry int) {
			t.Logf("Error handler called: %v", err)
			errorCount++
		}),
	)

	// Check if messages might be reprocessed due to ACK failures
	consumer2 := NewConsumer(client, ConsumerOptions{Group: "process-group"})
	defer consumer2.Close()

	msgs2, _ := consumer2.Read(ctx, []uint32{0}, 10)
	t.Logf("Messages available for reprocessing: %d", len(msgs2))
	if len(msgs2) > 0 {
		t.Logf("WARNING: %d messages available for reprocessing due to ACK failures", len(msgs2))
	} else {
		t.Logf("All messages were successfully ACK'd despite context cancellation")
	}
}
