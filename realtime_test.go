package comet

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRealtimeVisibilityDebug tests realtime visibility with extensive logging
func TestRealtimeVisibilityDebug(t *testing.T) {
	dir := t.TempDir()

	// Enable debug mode
	os.Setenv("COMET_DEBUG", "true")
	defer os.Unsetenv("COMET_DEBUG")

	cfg := DefaultCometConfig()
	cfg.Storage.FlushInterval = 20 // 20ms - very aggressive flushing

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Counters for tracking
	var writtenCount int64
	var readCount int64
	var wg sync.WaitGroup

	// Start writer in goroutine with its own client
	wg.Add(1)
	go func() {
		defer wg.Done()

		writerClient, err := NewClient(dir, cfg)
		if err != nil {
			t.Errorf("Failed to create writer client: %v", err)
			return
		}
		defer writerClient.Close()

		// Write continuously for 5 seconds
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				fmt.Printf("[WRITER] Finished writing. Total messages written: %d\n", atomic.LoadInt64(&writtenCount))
				return
			case <-ticker.C:
				count := atomic.AddInt64(&writtenCount, 1)
				msg := fmt.Sprintf("msg-%d", count)

				_, err := writerClient.Append(ctx, stream, [][]byte{[]byte(msg)})
				if err != nil {
					t.Errorf("Write failed: %v", err)
					continue
				}

				if count%10 == 0 {
					fmt.Printf("[WRITER] Written %d messages\n", count)
				}
			}
		}
	}()

	// Give writer time to start and write some messages
	time.Sleep(500 * time.Millisecond)

	// Start consumer in goroutine with its own client
	wg.Add(1)
	go func() {
		defer wg.Done()

		consumerClient, err := NewClient(dir, cfg)
		if err != nil {
			t.Errorf("Failed to create consumer client: %v", err)
			return
		}
		defer consumerClient.Close()

		consumer := NewConsumer(consumerClient, ConsumerOptions{
			Group: "test",
		})

		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		err = consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
			for _, msg := range msgs {
				count := atomic.AddInt64(&readCount, 1)
				if count%10 == 0 || len(msgs) > 0 {
					fmt.Printf("[CONSUMER] Read %d messages (batch size: %d, latest: %s)\n",
						count, len(msgs), string(msg.Data))
				}
			}
			return nil
		}, WithStream("test:v1:shard:*"), WithBatchSize(1), WithPollInterval(100*time.Millisecond))

		if err != nil && err != context.DeadlineExceeded {
			t.Errorf("Consumer process error: %v", err)
		}

		fmt.Printf("[CONSUMER] Finished reading. Total messages read: %d\n", atomic.LoadInt64(&readCount))
	}()

	// Wait for both to complete
	wg.Wait()

	// Check results
	written := atomic.LoadInt64(&writtenCount)
	read := atomic.LoadInt64(&readCount)

	fmt.Printf("\n=== FINAL RESULTS ===\n")
	fmt.Printf("Messages written: %d\n", written)
	fmt.Printf("Messages read: %d\n", read)
	fmt.Printf("Lag: %d messages\n", written-read)

	// Allow some lag due to timing, but it shouldn't be too much
	maxAcceptableLag := int64(10)
	if written-read > maxAcceptableLag {
		t.Errorf("Consumer is lagging too far behind! Written: %d, Read: %d, Lag: %d (max acceptable: %d)",
			written, read, written-read, maxAcceptableLag)
	}
}
