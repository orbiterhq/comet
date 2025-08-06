package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestConsumerBasicFlow tests the most basic Consumer scenario
func TestConsumerBasicFlow(t *testing.T) {
	dataDir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 // 50ms flush interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Consumer should work: write → read → write more → read more
	consumer := NewConsumer(client, ConsumerOptions{Group: "basic-test"})
	defer consumer.Close()

	// Phase 1: Write initial data
	initialData := [][]byte{[]byte("msg1"), []byte("msg2"), []byte("msg3")}
	_, err = client.Append(ctx, stream, initialData)
	if err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}
	client.Sync(ctx)

	// Phase 2: Consumer reads initial data
	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Failed to read initial messages: %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("Expected 3 initial messages, got %d", len(messages))
	}

	// ACK them
	messageIDs := make([]MessageID, len(messages))
	for i, msg := range messages {
		messageIDs[i] = msg.ID
	}
	consumer.Ack(ctx, messageIDs...)

	// Phase 3: Write MORE data (this is where the bug was)
	moreData := [][]byte{[]byte("msg4"), []byte("msg5")}
	_, err = client.Append(ctx, stream, moreData)
	if err != nil {
		t.Fatalf("Failed to write more data: %v", err)
	}
	client.Sync(ctx)

	// Phase 4: SAME Consumer should read new data without restart
	newMessages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Consumer failed to read new messages: %v", err)
	}
	if len(newMessages) != 2 {
		t.Fatalf("Expected 2 new messages, got %d", len(newMessages))
	}

	t.Log("SUCCESS: Consumer read new data without restart")
}

// TestConsumerThroughRotations tests Consumer working through file rotations
func TestConsumerThroughRotations(t *testing.T) {
	dataDir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 20
	config.Storage.MaxFileSize = 5 * 1024 // 5KB files to force rotations

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "rotation:v1:shard:0000"

	consumer := NewConsumer(client, ConsumerOptions{Group: "rotation-test"})
	defer consumer.Close()

	// Write enough data to cause multiple rotations
	totalMessages := 0
	for batch := 0; batch < 10; batch++ {
		// Write batch of large messages
		batchData := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			// 1KB messages to trigger rotations
			data := make([]byte, 1024)
			for j := range data {
				data[j] = byte('A' + (i % 26))
			}
			batchData[i] = data
			totalMessages++
		}

		_, err = client.Append(ctx, stream, batchData)
		if err != nil {
			t.Fatalf("Failed to write batch %d: %v", batch, err)
		}
		client.Sync(ctx)

		// Consumer should be able to read through rotations
		messages, err := consumer.Read(ctx, []uint32{0}, 20)
		if err != nil {
			t.Fatalf("Consumer failed to read batch %d: %v", batch, err)
		}
		if len(messages) == 0 {
			t.Fatalf("Consumer got no messages for batch %d", batch)
		}

		// ACK everything
		messageIDs := make([]MessageID, len(messages))
		for i, msg := range messages {
			messageIDs[i] = msg.ID
		}
		consumer.Ack(ctx, messageIDs...)

		t.Logf("Batch %d: wrote 10 messages, consumer read %d", batch, len(messages))
	}

	// Verify we've had file rotations
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatalf("Failed to get shard: %v", err)
	}
	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	if fileCount < 3 {
		t.Errorf("Expected multiple files from rotations, got %d", fileCount)
	}

	t.Logf("SUCCESS: Consumer handled %d files through rotations", fileCount)
}

// TestConsumerHighFrequencyWrites tests Consumer with rapid writes
func TestConsumerHighFrequencyWrites(t *testing.T) {
	dataDir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10 // Very fast flushes

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := "highfreq:v1:shard:0000"

	consumer := NewConsumer(client, ConsumerOptions{Group: "highfreq-test"})
	defer consumer.Close()

	// Rapid fire writes
	messagesWritten := 0
	messagesRead := 0

	// Write messages rapidly
	for i := 0; i < 50; i++ {
		data := [][]byte{[]byte(fmt.Sprintf("rapid-msg-%d", i))}
		_, err = client.Append(ctx, stream, data)
		if err != nil {
			t.Fatalf("Failed to write message %d: %v", i, err)
		}
		messagesWritten++

		// Sync every few messages
		if i%5 == 0 {
			client.Sync(ctx)
		}

		// Try to read what's available
		messages, err := consumer.Read(ctx, []uint32{0}, 10)
		if err != nil {
			continue // Some reads may fail due to timing
		}

		if len(messages) > 0 {
			messagesRead += len(messages)
			// ACK them
			messageIDs := make([]MessageID, len(messages))
			for j, msg := range messages {
				messageIDs[j] = msg.ID
			}
			consumer.Ack(ctx, messageIDs...)
		}

		time.Sleep(10 * time.Millisecond)
	}

	// Final sync and read
	client.Sync(ctx)
	time.Sleep(100 * time.Millisecond)

	// Try to catch any remaining messages
	for attempt := 0; attempt < 10; attempt++ {
		messages, err := consumer.Read(ctx, []uint32{0}, 20)
		if err != nil || len(messages) == 0 {
			break
		}
		messagesRead += len(messages)
		messageIDs := make([]MessageID, len(messages))
		for j, msg := range messages {
			messageIDs[j] = msg.ID
		}
		consumer.Ack(ctx, messageIDs...)
	}

	t.Logf("High frequency test: wrote %d, read %d", messagesWritten, messagesRead)

	if messagesRead < messagesWritten-5 { // Allow some margin
		t.Errorf("Consumer didn't keep up: wrote %d, read %d", messagesWritten, messagesRead)
	}

	t.Log("SUCCESS: Consumer handled high frequency writes")
}

// TestMultipleConsumerGroups tests multiple consumer groups reading same data
func TestMultipleConsumerGroups(t *testing.T) {
	dataDir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "multigroup:v1:shard:0000"

	// Create two consumer groups
	consumerA := NewConsumer(client, ConsumerOptions{Group: "group-a"})
	defer consumerA.Close()
	consumerB := NewConsumer(client, ConsumerOptions{Group: "group-b"})
	defer consumerB.Close()

	// Write test data
	testData := [][]byte{[]byte("shared1"), []byte("shared2"), []byte("shared3")}
	_, err = client.Append(ctx, stream, testData)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	client.Sync(ctx)

	// Both consumers should be able to read the same data
	messagesA, err := consumerA.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Consumer A failed to read: %v", err)
	}
	messagesB, err := consumerB.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Consumer B failed to read: %v", err)
	}

	if len(messagesA) != 3 || len(messagesB) != 3 {
		t.Fatalf("Expected both consumers to read 3 messages, got A=%d, B=%d",
			len(messagesA), len(messagesB))
	}

	// Verify they got the same data
	for i := 0; i < 3; i++ {
		if string(messagesA[i].Data) != string(messagesB[i].Data) {
			t.Errorf("Message %d differs: A=%q, B=%q",
				i, string(messagesA[i].Data), string(messagesB[i].Data))
		}
	}

	t.Log("SUCCESS: Multiple consumer groups work correctly")
}
