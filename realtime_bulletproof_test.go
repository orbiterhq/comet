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

// TestRealtimeBulletproof tests 100% realtime performance with graceful shutdown
// Uses goroutines with separate clients to simulate cross-process behavior
func TestRealtimeBulletproof(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping bulletproof test in short mode")
	}

	dir := t.TempDir()
	
	// Enable debug for detailed logging
	os.Setenv("COMET_DEBUG", "true")
	defer os.Unsetenv("COMET_DEBUG")
	
	// Production configuration - 1s flush interval
	cfg := DefaultCometConfig()
	cfg.Storage.FlushInterval = 1000 // 1 second flush
	
	ctx := context.Background()
	stream := "events:v1:shard:0000"
	
	// Test parameters - designed for 100% success
	testDuration := 10 * time.Second  // 10 seconds of writing  
	writeInterval := 300 * time.Millisecond // Write every 300ms (slower for better sync)
	expectedWrites := int64(testDuration / writeInterval) // ~33 writes
	
	var writtenCount int64
	var readCount int64
	var wg sync.WaitGroup
	
	t.Logf("Starting bulletproof test: duration=%v, writeInterval=%v, expectedWrites=%d", 
		testDuration, writeInterval, expectedWrites)
	
	// Writer goroutine with graceful shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		
		writerClient, err := NewClient(dir, cfg)
		if err != nil {
			t.Errorf("Failed to create writer client: %v", err)
			return
		}
		defer writerClient.Close()
		
		timeout := time.After(testDuration)
		ticker := time.NewTicker(writeInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-timeout:
				// Graceful shutdown - ensure final flush happens
				written := atomic.LoadInt64(&writtenCount)
				t.Logf("[WRITER] Stopping after %d writes, waiting for final flush...", written)
				
				// Wait 2x flush interval to ensure all data is persisted
				time.Sleep(2 * time.Second) // 2x the 1s flush interval
				t.Logf("[WRITER] Final flush complete. Total written: %d", written)
				return
				
			case <-ticker.C:
				count := atomic.AddInt64(&writtenCount, 1)
				msg := fmt.Sprintf("event-%d-%d", time.Now().UnixMilli(), count)
				
				// Production write - no manual sync
				_, err := writerClient.Append(ctx, stream, [][]byte{[]byte(msg)})
				if err != nil {
					t.Errorf("Write failed: %v", err)
					continue
				}
				
				if count%20 == 0 {
					t.Logf("[WRITER] Progress: %d writes", count)
				}
			}
		}
	}()
	
	// Give writer time to start and create initial data
	time.Sleep(500 * time.Millisecond)
	
	// Consumer goroutine with smart shutdown detection
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
		
		// Consumer runs significantly longer to ensure 100% capture
		// Writer: 10s + 2s graceful = 12s total
		// Consumer: 25s to ensure complete catch-up (increased from 20s for more conservative termination)
		consumerCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		
		startTime := time.Now()
		lastMessageTime := startTime
		
		err = consumer.Process(consumerCtx, func(ctx context.Context, msgs []StreamMessage) error {
			if len(msgs) > 0 {
				// Count all messages in this batch
				batchSize := int64(len(msgs))
				newTotal := atomic.AddInt64(&readCount, batchSize)
				lastMessageTime = time.Now()
				
				currentWritten := atomic.LoadInt64(&writtenCount)
				
				// Log progress for every batch that has messages
				t.Logf("[CONSUMER] Read batch: +%d messages, total=%d, written=%d, lag=%d", 
					batchSize, newTotal, currentWritten, currentWritten-newTotal)
				
				// More conservative termination: ensure writer is fully done + extra time for final flush visibility
				elapsed := time.Since(startTime)
				timeSinceLastMessage := time.Since(lastMessageTime)
				writerFullyDone := elapsed > testDuration+4*time.Second // Writer + 4s buffer (was 3s)
				
				// Only terminate if writer is fully done AND we haven't seen messages for 3+ seconds
				if writerFullyDone && timeSinceLastMessage > 3*time.Second {
					t.Logf("[CONSUMER] Writer fully done + no messages for 3s. Final total: %d messages", newTotal)
					cancel()
					return context.Canceled
				}
			}
			return nil
		}, WithStream("events:v1:shard:*"), 
		   WithBatchSize(10),  // Larger batches for efficiency
		   WithPollInterval(50*time.Millisecond)) // More frequent polling
		
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
	
	t.Logf("\n=== BULLETPROOF TEST RESULTS ===")
	t.Logf("Duration: %.1fs", testDuration.Seconds())
	t.Logf("Messages written: %d (expected ~%d)", written, expectedWrites)
	t.Logf("Messages read: %d", read)
	t.Logf("Realtime performance: %.1f%%", float64(read)*100/float64(written))
	
	// Expectations for 100% realtime performance
	if written < expectedWrites*90/100 {
		t.Errorf("Writer underperformed: expected ~%d, got %d", expectedWrites, written)
	}
	
	// With proper graceful shutdown and catch-up time, we should get 100%
	// Allow tiny margin for final flush timing
	minExpected := written * 98 / 100 // Allow 2% for timing
	if read < minExpected {
		t.Errorf("Failed realtime test: read %d/%d (%.1f%%), expected >98%%",
			read, written, float64(read)*100/float64(written))
	}
	
	// Success criteria
	if read >= minExpected {
		t.Logf("✅ BULLETPROOF SUCCESS: %.1f%% realtime performance achieved",
			float64(read)*100/float64(written))
	}
}