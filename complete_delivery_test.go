package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCompleteDeliveryVerification ensures every single message is delivered
func TestCompleteDeliveryVerification(t *testing.T) {
	dataDir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10      // 10ms flush
	config.Storage.MaxFileSize = 10 * 1024 // 10KB files to force rotations

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stream := "complete-delivery:v1:shard:0000"
	shardID := uint32(0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	consumer := NewConsumer(client, ConsumerOptions{Group: "delivery-test"})
	defer consumer.Close()

	const totalMessages = 200
	var writtenMessages int64
	var readMessages int64

	// Track exactly which messages were written and read
	writtenIDs := make(map[int64]bool)
	readIDs := make(map[int64]bool)
	var writtenMutex, readMutex sync.RWMutex

	t.Logf("🚀 Starting complete delivery test with %d messages", totalMessages)

	// Writer goroutine - write exactly totalMessages
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)

		for i := int64(0); i < totalMessages; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg := fmt.Sprintf("message-%06d", i)
			_, err := client.Append(ctx, stream, [][]byte{[]byte(msg)})
			if err != nil {
				if ctx.Err() == nil {
					t.Logf("❌ Write error at message %d: %v", i, err)
				}
				return
			}

			writtenMutex.Lock()
			writtenIDs[i] = true
			writtenMutex.Unlock()

			atomic.AddInt64(&writtenMessages, 1)

			if i%50 == 0 {
				t.Logf("📝 Writer: %d/%d messages written", i+1, totalMessages)
			}

			// Small delay to simulate realistic write pattern
			time.Sleep(5 * time.Millisecond)
		}

		// Ensure all data is flushed
		if err := client.Sync(ctx); err != nil {
			t.Logf("❌ Sync error: %v", err)
		}

		t.Logf("✅ Writer: Completed writing all %d messages", totalMessages)
	}()

	// Consumer goroutine - read until we get all messages
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			messages, err := consumer.Read(ctx, []uint32{shardID}, 25)
			if err != nil {
				if ctx.Err() == nil {
					t.Logf("❌ Read error after %d messages: %v", atomic.LoadInt64(&readMessages), err)
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if len(messages) == 0 {
				// Check if we've read everything that's been written
				currentWritten := atomic.LoadInt64(&writtenMessages)
				currentRead := atomic.LoadInt64(&readMessages)

				if currentWritten >= totalMessages && currentRead >= totalMessages {
					t.Logf("🎉 Consumer: Read all %d messages!", totalMessages)
					return
				}

				// If writer is done but we haven't read everything, wait a bit more
				select {
				case <-writerDone:
					if currentRead < totalMessages {
						t.Logf("⏳ Writer done, but only read %d/%d messages, waiting...", currentRead, totalMessages)
						time.Sleep(100 * time.Millisecond)
						continue
					}
				default:
				}

				time.Sleep(50 * time.Millisecond)
				continue
			}

			// Process messages
			messageIDs := make([]MessageID, len(messages))
			for i, msg := range messages {
				messageIDs[i] = msg.ID

				// Extract message number from content
				var msgNum int64
				if n, err := fmt.Sscanf(string(msg.Data), "message-%06d", &msgNum); n == 1 && err == nil {
					readMutex.Lock()
					readIDs[msgNum] = true
					readMutex.Unlock()
				} else {
					t.Logf("⚠️ Could not parse message: %s", string(msg.Data))
				}
			}

			// ACK all messages
			if err := consumer.Ack(ctx, messageIDs...); err != nil {
				t.Logf("❌ ACK error: %v", err)
				continue
			}

			newReadCount := atomic.AddInt64(&readMessages, int64(len(messages)))

			if newReadCount%50 == 0 || newReadCount >= totalMessages {
				t.Logf("📖 Consumer: %d/%d messages read", newReadCount, totalMessages)
			}

			// Exit if we've read all messages
			if newReadCount >= totalMessages {
				t.Logf("🎉 Consumer: Reached target of %d messages!", totalMessages)
				return
			}
		}
	}()

	// Wait for writer to finish
	<-writerDone
	t.Logf("✅ Writer finished")

	// Give consumer extra time to catch up (up to 10 seconds)
	consumerTimeout := time.NewTimer(10 * time.Second)
	select {
	case <-consumerDone:
		consumerTimeout.Stop()
		t.Logf("✅ Consumer finished")
	case <-consumerTimeout.C:
		t.Logf("⏰ Consumer timeout after 10 seconds")
	}

	// Final verification
	finalWritten := atomic.LoadInt64(&writtenMessages)
	finalRead := atomic.LoadInt64(&readMessages)

	t.Logf("🏁 FINAL RESULTS:")
	t.Logf("   Written: %d", finalWritten)
	t.Logf("   Read: %d", finalRead)
	t.Logf("   Success Rate: %.2f%%", float64(finalRead)/float64(finalWritten)*100)

	// Detailed verification - check exactly which messages were missed
	writtenMutex.RLock()
	readMutex.RLock()

	missing := make([]int64, 0)
	duplicates := make([]int64, 0)

	// Check for missing messages
	for msgID := range writtenIDs {
		if !readIDs[msgID] {
			missing = append(missing, msgID)
		}
	}

	// Check for duplicates (shouldn't happen with proper ACK)
	readCount := make(map[int64]int)
	for msgID := range readIDs {
		readCount[msgID]++
		if readCount[msgID] > 1 {
			duplicates = append(duplicates, msgID)
		}
	}

	writtenMutex.RUnlock()
	readMutex.RUnlock()

	// Report detailed results
	if len(missing) > 0 {
		t.Logf("❌ MISSING MESSAGES (%d): %v", len(missing), missing[:minInt(10, len(missing))])
		if len(missing) > 10 {
			t.Logf("   ... and %d more", len(missing)-10)
		}
	}

	if len(duplicates) > 0 {
		t.Logf("⚠️ DUPLICATE MESSAGES (%d): %v", len(duplicates), duplicates[:minInt(10, len(duplicates))])
	}

	// Test passes only if we got every single message exactly once
	if finalRead != totalMessages {
		t.Errorf("❌ DELIVERY INCOMPLETE: Expected %d messages, got %d", totalMessages, finalRead)
	}

	if len(missing) > 0 {
		t.Errorf("❌ MISSING MESSAGES: %d messages were not delivered", len(missing))
	}

	if len(duplicates) > 0 {
		t.Errorf("❌ DUPLICATE MESSAGES: %d messages were delivered multiple times", len(duplicates))
	}

	if finalRead == totalMessages && len(missing) == 0 && len(duplicates) == 0 {
		t.Logf("🎉 SUCCESS: Perfect delivery - all %d messages delivered exactly once!", totalMessages)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
