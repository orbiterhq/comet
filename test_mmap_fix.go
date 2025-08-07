package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestMmapFix(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write a few entries rapidly
	for i := 0; i < 5; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for periodic flush
	time.Sleep(30 * time.Millisecond)

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	// Try to read - this should handle the mmap coherence issue
	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	t.Logf("Successfully read %d messages", len(messages))
}
