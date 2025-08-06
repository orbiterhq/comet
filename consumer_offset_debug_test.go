package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestConsumerOffsetDebug focuses specifically on consumer offset tracking
func TestConsumerOffsetDebug(t *testing.T) {
	dataDir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stream := "offset-debug:v1:shard:0000"
	shardID := uint32(0)
	ctx := context.Background()

	t.Log("📝 === PHASE 1: Write Test Messages ===")

	// Write 20 messages
	batch := make([][]byte, 0)
	for i := 0; i < 20; i++ {
		msg := fmt.Sprintf("test-message-%03d", i)
		batch = append(batch, []byte(msg))
	}

	_, err = client.Append(ctx, stream, batch)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("Failed to sync messages: %v", err)
	}

	// Get shard for offset inspection
	shard, err := client.getOrCreateShard(shardID)
	if err != nil {
		t.Fatalf("Failed to get shard: %v", err)
	}

	logOffsets := func(phase string, consumer *Consumer) {
		// Check persisted offsets in shard index
		shard.mu.RLock()
		persistedOffset, exists := shard.index.ConsumerOffsets[consumer.group]
		totalEntries := shard.index.CurrentEntryNumber
		shard.mu.RUnlock()

		// Check in-memory offsets in consumer
		consumer.memOffsetsMu.RLock()
		memOffset, hasMemOffset := consumer.memOffsets[shardID]
		consumer.memOffsetsMu.RUnlock()

		t.Logf("🔍 [%s] Offsets: persisted=%d (exists=%v), memory=%d (has=%v), totalEntries=%d",
			phase, persistedOffset, exists, memOffset, hasMemOffset, totalEntries)
	}

	t.Log("📖 === PHASE 2: Consumer Read/ACK Cycle ===")

	consumer := NewConsumer(client, ConsumerOptions{Group: "offset-test"})
	defer consumer.Close()

	logOffsets("INITIAL", consumer)

	// Read first 5 messages
	t.Log("📖 Reading first 5 messages...")
	msgs1, err := consumer.Read(ctx, []uint32{shardID}, 5)
	if err != nil {
		t.Fatalf("Failed to read first batch: %v", err)
	}
	t.Logf("✅ Read batch 1: %d messages (entries %d-%d)",
		len(msgs1), msgs1[0].ID.EntryNumber, msgs1[len(msgs1)-1].ID.EntryNumber)

	logOffsets("AFTER READ 1", consumer)

	// ACK first 5 messages
	t.Log("📝 ACKing first 5 messages...")
	messageIDs1 := make([]MessageID, len(msgs1))
	for i, msg := range msgs1 {
		messageIDs1[i] = msg.ID
	}
	err = consumer.Ack(ctx, messageIDs1...)
	if err != nil {
		t.Fatalf("Failed to ACK first batch: %v", err)
	}

	// Give time for background flush
	time.Sleep(100 * time.Millisecond)

	logOffsets("AFTER ACK 1", consumer)

	// Read next 5 messages
	t.Log("📖 Reading next 5 messages...")
	msgs2, err := consumer.Read(ctx, []uint32{shardID}, 5)
	if err != nil {
		t.Fatalf("Failed to read second batch: %v", err)
	}
	t.Logf("✅ Read batch 2: %d messages (entries %d-%d)",
		len(msgs2), msgs2[0].ID.EntryNumber, msgs2[len(msgs2)-1].ID.EntryNumber)

	logOffsets("AFTER READ 2", consumer)

	// Check if we got the right messages (should be 5-9, not 0-4 again)
	if len(msgs2) > 0 {
		firstEntry := msgs2[0].ID.EntryNumber
		if firstEntry != 5 {
			t.Errorf("❌ CONSUMER OFFSET BUG: Expected first message to be entry 5, got %d", firstEntry)
			t.Log("🔍 This indicates the consumer didn't advance its offset after ACK")
		} else {
			t.Log("✅ Consumer correctly advanced offset after ACK")
		}
	}

	// ACK second batch
	t.Log("📝 ACKing next 5 messages...")
	messageIDs2 := make([]MessageID, len(msgs2))
	for i, msg := range msgs2 {
		messageIDs2[i] = msg.ID
	}
	err = consumer.Ack(ctx, messageIDs2...)
	if err != nil {
		t.Fatalf("Failed to ACK second batch: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	logOffsets("AFTER ACK 2", consumer)

	t.Log("🔄 === PHASE 3: Consumer Restart Test ===")

	// Close first consumer
	consumer.Close()

	// Create new consumer (simulates restart)
	consumer2 := NewConsumer(client, ConsumerOptions{Group: "offset-test"})
	defer consumer2.Close()

	logOffsets("AFTER RESTART", consumer2)

	// Read with new consumer - should start from where we left off
	t.Log("📖 Reading with restarted consumer...")
	msgs3, err := consumer2.Read(ctx, []uint32{shardID}, 5)
	if err != nil {
		t.Fatalf("Failed to read after restart: %v", err)
	}

	if len(msgs3) > 0 {
		firstEntry := msgs3[0].ID.EntryNumber
		t.Logf("✅ Read after restart: %d messages (entries %d-%d)",
			len(msgs3), msgs3[0].ID.EntryNumber, msgs3[len(msgs3)-1].ID.EntryNumber)

		if firstEntry != 10 {
			t.Errorf("❌ RESTART OFFSET BUG: Expected first message after restart to be entry 10, got %d", firstEntry)
			t.Log("🔍 This indicates consumer restart didn't load the correct offset")
		} else {
			t.Log("✅ Consumer correctly loaded offset after restart")
		}
	}

	logOffsets("FINAL", consumer2)

	t.Log("🏁 === TEST COMPLETE ===")
}
