package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestACKPersistenceBug reproduces the critical ACK persistence issue
func TestACKPersistenceBug(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Step 1: Write messages
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	
	ctx := context.Background()
	stream := "ack-test:v1:shard:0001"
	
	// Write 10 messages
	var messages [][]byte
	for i := 0; i < 10; i++ {
		messages = append(messages, []byte(fmt.Sprintf("msg-%d", i)))
	}
	
	_, err = client1.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client1.Close()

	// Step 2: Process and ACK some messages
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test-group"})
	
	// Read and ACK first 5 messages manually
	t.Log("=== Reading and ACKing first 5 messages ===")
	batch1, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Read batch 1: %d messages", len(batch1))
	
	for _, msg := range batch1 {
		err = consumer.Ack(ctx, msg.ID)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ACKed message: %s", msg.ID.String())
	}
	
	// Check lag after ACK
	lag1, err := consumer.GetLag(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Lag after ACKing 5 messages: %d (should be 5)", lag1)
	
	consumer.Close()
	client2.Close()

	// Step 3: Create new client/consumer (simulates restart)
	t.Log("=== Creating new client (simulate restart) ===")
	client3, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client3.Close()
	
	consumer2 := NewConsumer(client3, ConsumerOptions{Group: "test-group"})
	defer consumer2.Close()
	
	// Check lag with new consumer - should still be 5 if ACKs were persisted
	lag2, err := consumer2.GetLag(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Lag after restart: %d (should be 5 if ACKs persisted)", lag2)
	
	// Read remaining messages - should only get 5, not 10
	batch2, err := consumer2.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Read batch 2 after restart: %d messages (should be 5)", len(batch2))
	
	// BUG CHECK: If ACK persistence is broken, we'll get 10 messages instead of 5
	if len(batch2) > 5 {
		t.Errorf("CRITICAL ACK PERSISTENCE BUG: Expected ≤5 messages after ACKing 5, got %d", len(batch2))
		t.Error("This means ACKed messages were not properly persisted and will be reprocessed!")
		
		// Show which messages we got
		for i, msg := range batch2 {
			t.Logf("  Reprocessed msg[%d]: %s - %s", i, msg.ID.String(), string(msg.Data))
		}
	} else {
		t.Logf("✅ ACK persistence working: got %d messages as expected", len(batch2))
	}
	
	// Additional check: lag should be consistent
	if lag2 != 5 {
		t.Errorf("CRITICAL ACK PERSISTENCE BUG: Lag inconsistent - expected 5, got %d", lag2)
	}
}

// TestProcessACKPersistence tests ACK persistence through Process() method
func TestProcessACKPersistence(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	// Write messages
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	
	ctx := context.Background()
	stream := "process-ack:v1:shard:0001"
	
	var messages [][]byte
	for i := 0; i < 20; i++ {
		messages = append(messages, []byte(fmt.Sprintf("process-msg-%d", i)))
	}
	
	_, err = client1.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client1.Close()

	// Process first 10 messages
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	
	consumer1 := NewConsumer(client2, ConsumerOptions{Group: "process-group"})
	
	processedCount := 0
	processCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	
	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		processedCount += len(msgs)
		t.Logf("Process() batch: %d messages (total: %d)", len(msgs), processedCount)
		
		// Stop after processing 10 messages
		if processedCount >= 10 {
			cancel()
		}
		return nil
	}
	
	err = consumer1.Process(processCtx, processFunc,
		WithStream("process-ack:v1:shard:*"),
		WithBatchSize(5),
		WithAutoAck(true), // ACKs should be automatic
	)
	
	t.Logf("First Process() completed: processed %d messages", processedCount)
	consumer1.Close()
	client2.Close()

	// Start new client/consumer (simulate restart)
	client3, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client3.Close()
	
	consumer2 := NewConsumer(client3, ConsumerOptions{Group: "process-group"})
	defer consumer2.Close()
	
	// Check lag - should be 10 remaining
	lag, err := consumer2.GetLag(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Lag after first Process(): %d (should be 10)", lag)
	
	// Process remaining messages
	processedCount2 := 0
	processCtx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	
	processFunc2 := func(ctx context.Context, msgs []StreamMessage) error {
		processedCount2 += len(msgs)
		t.Logf("Second Process() batch: %d messages (total: %d)", len(msgs), processedCount2)
		
		// Stop after getting all remaining
		if processedCount2 >= 15 { // Allow some buffer for reprocessing
			cancel2()
		}
		return nil
	}
	
	err = consumer2.Process(processCtx2, processFunc2,
		WithStream("process-ack:v1:shard:*"),
		WithBatchSize(5),
		WithAutoAck(true),
	)
	
	t.Logf("Second Process() completed: processed %d messages", processedCount2)
	
	// BUG CHECK: If ACK persistence is broken, second Process() will reprocess messages
	if processedCount2 > 10 {
		t.Errorf("CRITICAL ACK PERSISTENCE BUG: Second Process() got %d messages, expected ≤10", processedCount2)
		t.Error("This indicates ACKed messages from first Process() were not persisted!")
	} else {
		t.Logf("✅ Process() ACK persistence working: second run processed %d messages", processedCount2)
	}
}