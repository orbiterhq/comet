package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDebugRaceCondition(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 5 // Very fast flushes

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	var wg sync.WaitGroup

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
			if err != nil {
				t.Logf("Write error: %v", err)
				return
			}
			t.Logf("Wrote message %d", i)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			messages, err := consumer.Read(ctx, []uint32{0}, 3)
			if err != nil {
				t.Logf("Read error: %v", err)
				return
			}
			t.Logf("Read %d messages", len(messages))
			if len(messages) > 0 {
				// ACK them
				for _, msg := range messages {
					consumer.Ack(ctx, msg.ID)
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
}
