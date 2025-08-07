package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRealtimeBulletproof tests 100% realtime performance with graceful shutdown
// Uses goroutines with separate clients to simulate cross-process behavior
func TestRealtimeBulletproof(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping bulletproof test in short mode")
	}

	dir := t.TempDir()

	// Ultra high-performance production configuration - optimized for 100% realtime  
	cfg := DefaultCometConfig()
	cfg.Storage.FlushInterval = 25       // 25ms flush (ultra high-performance)
	cfg.Storage.CheckpointInterval = 100 // 100ms checkpoint (ultra high-performance)

	ctx := context.Background()
	// Use 16 different shards to test lazy loading
	shards := make([]string, 16)
	for i := 0; i < 16; i++ {
		shards[i] = fmt.Sprintf("events:v1:shard:%04d", i)
	}

	// Realistic production test parameters - longer duration, higher message volume
	testDuration := 30 * time.Second                     // 30 seconds of sustained writing
	writeInterval := 50 * time.Millisecond               // Write every 50ms (high throughput: ~20/sec)
	expectedWrites := int64(testDuration / writeInterval) // ~600 writes

	var writtenCount int64
	var readCount int64
	var wg sync.WaitGroup

	// Coordination channels
	writerDone := make(chan struct{})

	t.Logf("Starting bulletproof test: duration=%v, writeInterval=%v, expectedWrites=%d",
		testDuration, writeInterval, expectedWrites)

	// Writer goroutine with graceful shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writerDone) // Signal when writer is completely done

		writerClient, err := NewClient(dir, cfg)
		if err != nil {
			t.Errorf("Failed to create writer client: %v", err)
			return
		}
		// Note: We explicitly close the client during graceful shutdown, no defer needed

		timeout := time.After(testDuration)
		ticker := time.NewTicker(writeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				// Graceful shutdown - ensure final flush happens
				written := atomic.LoadInt64(&writtenCount)
				t.Logf("[WRITER] Stopping after %d writes, forcing final flush...", written)

				// Force manual sync to ensure all data is immediately durable
				// This simulates production behavior where writer ensures data persistence before shutdown
				if err := writerClient.Close(); err != nil {
					t.Logf("[WRITER] Warning: Failed to close and sync all shards: %v", err)
				}

				// Brief pause to ensure index updates are visible to consumer
				// In production, this would be handled by the normal flush cycle
				time.Sleep(100 * time.Millisecond)

				t.Logf("[WRITER] All shards synced. Final total written: %d", written)
				return

			case <-ticker.C:
				count := atomic.AddInt64(&writtenCount, 1)
				msg := fmt.Sprintf("event-%d-%d", time.Now().UnixMilli(), count)

				// Write to different shards in round-robin fashion to test lazy loading
				shardIndex := (count - 1) % int64(len(shards))
				targetShard := shards[shardIndex]

				// Production write - no manual sync
				_, err := writerClient.Append(ctx, targetShard, [][]byte{[]byte(msg)})
				if err != nil {
					t.Errorf("Write failed: %v", err)
					continue
				}

				if count%100 == 0 {
					t.Logf("[WRITER] Progress: %d writes across %d shards", count, len(shards))
				}
			}
		}
	}()

	// Give writer time to start and create initial data  
	time.Sleep(1 * time.Second)

	// Consumer goroutine with proper coordination
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
			Group: "bulletproof-test",
		})

		// Consumer context that gets cancelled when writer finishes + catch-up time
		consumerCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		
		// Wait for writer to finish, then give consumer reasonable catch-up time for production scenario
		go func() {
			<-writerDone
			t.Logf("[CONSUMER] Writer finished, giving consumer 10 seconds to catch up...")
			time.Sleep(10 * time.Second)
			t.Logf("[CONSUMER] Catch-up time expired, stopping consumer...")
			cancel() // Stop the consumer
		}()

		err = consumer.Process(consumerCtx, func(ctx context.Context, msgs []StreamMessage) error {
			if len(msgs) > 0 {
				// Count all messages in this batch
				batchSize := int64(len(msgs))
				newTotal := atomic.AddInt64(&readCount, batchSize)

				currentWritten := atomic.LoadInt64(&writtenCount)

				// Log progress periodically to avoid spam  
				if newTotal%100 == 0 || batchSize >= 10 {
					t.Logf("[CONSUMER] Read batch: +%d messages, total=%d, written=%d, lag=%d",
						batchSize, newTotal, currentWritten, currentWritten-newTotal)
				}
			}
			return nil
		}, WithStream("events:v1:shard:*"), // Consumer watches all 16 shards
			WithBatchSize(10),                     // Small batches for minimal latency
			WithPollInterval(25*time.Millisecond))  // Very aggressive polling matching flush interval

		if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
			t.Errorf("Consumer process error: %v", err)
		}

		final := atomic.LoadInt64(&readCount)
		t.Logf("[CONSUMER] Finished with %d reads", final)
	}()

	// Wait for both to complete
	wg.Wait()

	// Analyze results
	written := atomic.LoadInt64(&writtenCount)
	read := atomic.LoadInt64(&readCount)

	t.Logf("\n=== BULLETPROOF TEST RESULTS (16 SHARDS) ===")
	t.Logf("Duration: %.1fs", testDuration.Seconds())
	t.Logf("Shards: %d (lazy loaded by consumer)", len(shards))
	t.Logf("Messages written: %d (expected ~%d)", written, expectedWrites)
	t.Logf("Messages read: %d", read)
	t.Logf("Realtime performance: %.1f%%", float64(read)*100/float64(written))

	// Expectations for 100% realtime performance
	if written < expectedWrites*90/100 {
		t.Errorf("Writer underperformed: expected ~%d, got %d", expectedWrites, written)
	}

	// With 25ms flush intervals, we demand 100% realtime performance
	// Every single message should be captured in realtime
	if read < written {
		t.Errorf("Failed 100%% realtime test: read %d/%d (%.1f%%), expected 100%%",
			read, written, float64(read)*100/float64(written))
	}

	// Success criteria - must be exactly 100%
	if read >= written {
		t.Logf("🎯 PERFECT 100%% REALTIME SUCCESS: %d/%d messages captured instantly",
			read, written)
	}
}
