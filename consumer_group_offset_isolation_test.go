package comet

import (
	"context"
	"testing"
)

// TestConsumerGroupOffsetIsolation tests that different consumer groups
// maintain isolated offsets and don't pollute each other
func TestConsumerGroupOffsetIsolation(t *testing.T) {
	dir := t.TempDir()
	config := DeprecatedMultiProcessConfig(0, 2)
	SetDebug(true)
	defer SetDebug(false)

	// Write messages
	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	stream := "offset-isolation:v1:shard:0000"
	messages := [][]byte{
		[]byte("msg-00"), []byte("msg-01"), []byte("msg-02"),
		[]byte("msg-03"), []byte("msg-04"), []byte("msg-05"),
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to ensure data is written before closing
	err = client.Sync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// STEP 1: Consumer group-A processes first 3 messages
	client1, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()

	consumer1 := NewConsumer(client1, ConsumerOptions{Group: "group-A"})
	defer consumer1.Close()

	msgs1, err := consumer1.Read(ctx, []uint32{0}, 3)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Group-A read %d messages", len(msgs1))
	for _, msg := range msgs1 {
		t.Logf("  Group-A: %s (ID: %s)", string(msg.Data), msg.ID.String())
		err = consumer1.Ack(ctx, msg.ID)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check group-A offset
	stats1, err := consumer1.GetShardStats(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Group-A offsets after ACK: %v", stats1.ConsumerOffsets)

	consumer1.Close()
	client1.Close()

	// STEP 2: Consumer group-B should start from beginning, not from group-A's position
	client2, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	consumer2 := NewConsumer(client2, ConsumerOptions{Group: "group-B"})
	defer consumer2.Close()

	// Check group-B's view of offsets before reading
	stats2, err := consumer2.GetShardStats(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Group-B offsets BEFORE reading: %v", stats2.ConsumerOffsets)

	msgs2, err := consumer2.Read(ctx, []uint32{0}, 3)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Group-B read %d messages (should start from beginning)", len(msgs2))
	for _, msg := range msgs2 {
		t.Logf("  Group-B: %s (ID: %s)", string(msg.Data), msg.ID.String())
	}

	// CRITICAL TEST: Group-B should read the first message (msg-00), not from group-A's position
	if len(msgs2) > 0 {
		firstMsg := string(msgs2[0].Data)
		if firstMsg != "msg-00" {
			t.Fatalf("OFFSET POLLUTION BUG: Group-B started from message %s, expected msg-00", firstMsg)
		}
		t.Logf("✅ Group-B correctly started from beginning")
	} else {
		t.Fatal("Group-B read no messages, expected to read from beginning")
	}
}
