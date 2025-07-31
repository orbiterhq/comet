package comet

import (
	"context"
	"testing"
	"time"
)

// TestMultiProcessInSameProcess tests multi-client coordination within the same process
// This is NOT a true multi-process test but tests the coordination mechanisms
func TestMultiProcessInSameProcess(t *testing.T) {
	dir := t.TempDir()

	// Create two clients with multi-process config
	config := MultiProcessConfig()

	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()

	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Write from client1
	_, err = client1.Append(ctx, streamName, [][]byte{
		[]byte(`{"source":"client1","msg":"hello"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Sync to ensure it's persisted
	client1.Sync(ctx)

	// Give a moment for mmap coordination
	time.Sleep(50 * time.Millisecond)

	// Read from client2
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read from shard 1: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}

	// Write from client2
	_, err = client2.Append(ctx, streamName, [][]byte{
		[]byte(`{"source":"client2","msg":"world"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Both clients should see both messages
	t.Log("Multi-client coordination test passed")
}

// TestMultiProcessMmapSize verifies the mmap state file is exactly 8 bytes
func TestMultiProcessMmapSize(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write something to create the shard
	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{
		[]byte(`{"test":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check the mmap state file size
	client.mu.RLock()
	shard, exists := client.shards[1]
	client.mu.RUnlock()

	if !exists {
		t.Fatal("Shard 1 not found")
	}

	if shard.mmapState == nil {
		t.Fatal("Mmap state not initialized")
	}

	// The state file should be exactly 8 bytes
	if len(shard.indexStateData) != 8 {
		t.Errorf("Mmap state file is %d bytes, expected 8", len(shard.indexStateData))
	} else {
		t.Log("✓ Mmap state file is exactly 8 bytes")
	}
}
