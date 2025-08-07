package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOffsetBeyondFileSize(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10 // Very fast flushes to trigger race

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
	errorCh := make(chan error, 10)

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
			if err != nil {
				errorCh <- fmt.Errorf("write error: %w", err)
				return
			}
			// Sync periodically to make data durable
			if i%5 == 4 {
				if err := client.Sync(ctx); err != nil {
					errorCh <- fmt.Errorf("sync error: %w", err)
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		// Final sync
		if err := client.Sync(ctx); err != nil {
			errorCh <- fmt.Errorf("final sync error: %w", err)
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			messages, err := consumer.Read(ctx, []uint32{0}, 5)
			if err != nil {
				errorCh <- fmt.Errorf("read error: %w", err)
				return
			}
			t.Logf("Read %d messages", len(messages))

			// ACK the messages
			for _, msg := range messages {
				if err := consumer.Ack(ctx, msg.ID); err != nil {
					errorCh <- fmt.Errorf("ack error: %w", err)
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errorCh)

	// Check for errors
	for err := range errorCh {
		t.Error(err)
	}
}
