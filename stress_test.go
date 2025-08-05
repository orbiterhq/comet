//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQuickACKStress is a faster version for regular testing
func TestQuickACKStress(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	stream := "quick:v1:shard:0000"
	totalMessages := 200

	// Write messages
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("quick-msg-%04d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Track processed messages to detect duplicates
	processedMessages := make(map[string]bool)
	var mu sync.Mutex
	var totalProcessed int64
	var duplicates int64

	// Aggressive restart pattern - restart after every 2-3 batches
	for restart := 0; restart < 20 && atomic.LoadInt64(&totalProcessed) < int64(totalMessages); restart++ {
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}

		consumer := NewConsumer(client, ConsumerOptions{Group: "quick-test"})

		sessionProcessed := 0
		processCtx, cancel := context.WithTimeout(ctx, 3*time.Second)

		processFunc := func(ctx context.Context, msgs []StreamMessage) error {
			mu.Lock()
			for _, msg := range msgs {
				msgKey := string(msg.Data)
				if processedMessages[msgKey] {
					atomic.AddInt64(&duplicates, 1)
					t.Logf("DUPLICATE in restart %d: %s", restart, msgKey)
				} else {
					processedMessages[msgKey] = true
				}
			}
			mu.Unlock()

			atomic.AddInt64(&totalProcessed, int64(len(msgs)))
			sessionProcessed += len(msgs)

			// Stop after 2-3 batches to force frequent restarts
			if sessionProcessed >= 15 {
				cancel()
			}

			return nil
		}

		err = consumer.Process(processCtx, processFunc,
			WithStream("quick:v1:shard:*"),
			WithBatchSize(5),
			WithAutoAck(true),
		)

		consumer.Close()
		client.Close()
		cancel()

		if sessionProcessed == 0 {
			break
		}

		t.Logf("Restart %d: processed %d (session), %d (total)",
			restart, sessionProcessed, atomic.LoadInt64(&totalProcessed))
	}

	finalProcessed := atomic.LoadInt64(&totalProcessed)
	finalDuplicates := atomic.LoadInt64(&duplicates)

	t.Logf("Quick stress results: processed=%d/%d, duplicates=%d",
		finalProcessed, totalMessages, finalDuplicates)

	if finalDuplicates > 0 {
		t.Errorf("CRITICAL: Found %d duplicates - ACK persistence failed!", finalDuplicates)
	}

	if finalProcessed < int64(totalMessages) {
		t.Errorf("Only processed %d/%d messages", finalProcessed, totalMessages)
	}

	t.Logf("✅ Quick ACK stress test passed")
}

// TestACKEdgeCases tests edge cases that could break ACK persistence
func TestACKEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{"EmptyBatchACK", testEmptyBatchACK},
		{"DuplicateACK", testDuplicateACK},
		{"OutOfOrderACK", testOutOfOrderACK},
		{"RapidACKUnACK", testRapidACKUnACK},
		{"ACKDuringRotation", testACKDuringRotation},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func testEmptyBatchACK(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer := NewConsumer(client, ConsumerOptions{Group: "empty-test"})
	defer consumer.Close()

	// Try to ACK empty batch - should not crash
	err = consumer.Ack(context.Background())
	if err != nil {
		t.Errorf("Empty ACK failed: %v", err)
	}
}

func testDuplicateACK(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	stream := "dup:v1:shard:0000"

	// Write message
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Append(context.Background(), stream, [][]byte{[]byte("test-msg")})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Read and ACK same message multiple times
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	consumer := NewConsumer(client2, ConsumerOptions{Group: "dup-test"})
	defer consumer.Close()

	msgs, err := consumer.Read(context.Background(), []uint32{1}, 1)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatal("Expected 1 message")
	}

	// ACK same message multiple times
	for i := 0; i < 5; i++ {
		err = consumer.Ack(context.Background(), msgs[0].ID)
		if err != nil {
			t.Errorf("Duplicate ACK %d failed: %v", i, err)
		}
	}

	// Verify message is still ACKed after restart
	consumer.Close()
	client2.Close()

	client3, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client3.Close()

	consumer2 := NewConsumer(client3, ConsumerOptions{Group: "dup-test"})
	defer consumer2.Close()

	msgs2, err := consumer2.Read(context.Background(), []uint32{1}, 1)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs2) > 0 {
		t.Error("Message was not properly ACKed - duplicate ACK handling failed")
	}
}

func testOutOfOrderACK(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	stream := "order:v1:shard:0000"

	// Write messages
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < 10; i++ {
		messages = append(messages, []byte(fmt.Sprintf("order-msg-%d", i)))
	}

	_, err = client.Append(context.Background(), stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Read all messages
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	consumer := NewConsumer(client2, ConsumerOptions{Group: "order-test"})
	defer consumer.Close()

	msgs, err := consumer.Read(context.Background(), []uint32{1}, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 10 {
		t.Fatalf("Expected 10 messages, got %d", len(msgs))
	}

	// ACK in reverse order: 9, 8, 7, ..., 0
	for i := len(msgs) - 1; i >= 0; i-- {
		err = consumer.Ack(context.Background(), msgs[i].ID)
		if err != nil {
			t.Errorf("Out-of-order ACK %d failed: %v", i, err)
		}
	}

	// Verify all messages were ACKed
	lag, err := consumer.GetLag(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	if lag != 0 {
		t.Errorf("Expected lag 0 after out-of-order ACKs, got %d", lag)
	}
}

func testRapidACKUnACK(t *testing.T) {
	// Test rapid ACK operations that could cause race conditions
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	stream := "rapid:v1:shard:0000"

	// Write messages
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < 50; i++ {
		messages = append(messages, []byte(fmt.Sprintf("rapid-msg-%d", i)))
	}

	_, err = client.Append(context.Background(), stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Rapid ACK operations
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	consumer := NewConsumer(client2, ConsumerOptions{Group: "rapid-test"})
	defer consumer.Close()

	// Read and ACK in rapid succession
	for batch := 0; batch < 10; batch++ {
		msgs, err := consumer.Read(context.Background(), []uint32{1}, 5)
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) == 0 {
			break
		}

		// ACK all messages in batch rapidly
		for _, msg := range msgs {
			err = consumer.Ack(context.Background(), msg.ID)
			if err != nil {
				t.Errorf("Rapid ACK failed: %v", err)
			}
		}
	}

	// Verify no messages left
	msgs, err := consumer.Read(context.Background(), []uint32{1}, 1)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) > 0 {
		t.Errorf("Expected no messages after rapid ACKing, got %d", len(msgs))
	}
}

func testACKDuringRotation(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	config.Storage.MaxFileSize = 1024 // Very small files to force rotation
	stream := "rotation:v1:shard:0000"

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer := NewConsumer(client, ConsumerOptions{Group: "rotation-test"})
	defer consumer.Close()

	ctx := context.Background()

	// Write and ACK messages while triggering file rotations
	for i := 0; i < 100; i++ {
		// Write message
		msg := []byte(fmt.Sprintf("rotation-msg-%03d-%s", i,
			string(make([]byte, 100)))) // Large message to trigger rotation

		_, err = client.Append(ctx, stream, [][]byte{msg})
		if err != nil {
			t.Fatal(err)
		}

		// Immediately read and ACK
		msgs, err := consumer.Read(ctx, []uint32{0}, 1)
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) == 1 {
			err = consumer.Ack(ctx, msgs[0].ID)
			if err != nil {
				t.Errorf("ACK during rotation failed at msg %d: %v", i, err)
			}
		}
	}

	// Verify final state
	lag, err := consumer.GetLag(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	if lag > 0 {
		t.Errorf("Expected lag 0 after ACKing during rotations, got %d", lag)
	}
}
