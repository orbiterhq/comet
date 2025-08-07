package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestConsumerContinuousStream tests that consumers continuously receive messages
// from an active writer without needing restarts
func TestConsumerContinuousStream(t *testing.T) {
	dataDir := t.TempDir()

	// Create client with short flush interval
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10 * time.Millisecond // 10ms flush interval for faster testing

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create consumer
	consumer := NewConsumer(client, ConsumerOptions{Group: "continuous-test"})
	defer consumer.Close()

	// Create a context that will cancel after 10 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream := "test:v1:shard:0000"

	// Counters for tracking
	var messagesWritten int64
	var messagesRead int64
	writerErrors := make(chan error, 1)
	readerErrors := make(chan error, 1)

	// Start continuous writer
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond) // Write every 50ms
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				t.Logf("Writer: Stopped after writing %d messages", atomic.LoadInt64(&messagesWritten))
				return
			case <-ticker.C:
				count := atomic.LoadInt64(&messagesWritten)
				msg := fmt.Sprintf("continuous-message-%d", count)
				entries := [][]byte{[]byte(msg)}

				if _, err := client.Append(ctx, stream, entries); err != nil {
					select {
					case writerErrors <- fmt.Errorf("write error at message %d: %w", count, err):
					default:
					}
					return
				}

				atomic.AddInt64(&messagesWritten, 1)

				// Sync every 10 messages to make data available to consumers
				if (count+1)%10 == 0 {
					if err := client.Sync(ctx); err != nil {
						select {
						case writerErrors <- fmt.Errorf("sync error at message %d: %w", count, err):
						default:
						}
						return
					}
				}

				if count%100 == 0 {
					t.Logf("Writer: Sent %d messages", count)
				}
			}
		}
	}()

	// Start continuous reader
	go func() {
		consecutiveEmptyReads := 0
		maxEmptyReads := 50 // Allow up to 50 empty reads (5 seconds at 100ms interval)

		for {
			select {
			case <-ctx.Done():
				t.Logf("Reader: Stopped after reading %d messages", atomic.LoadInt64(&messagesRead))
				return
			default:
				messages, err := consumer.Read(ctx, []uint32{0}, 100) // Read up to 100 at a time
				if err != nil {
					select {
					case readerErrors <- fmt.Errorf("read error after %d messages: %w", atomic.LoadInt64(&messagesRead), err):
					default:
					}
					return
				}

				if len(messages) > 0 {
					// Got messages, reset empty read counter
					consecutiveEmptyReads = 0

					// Verify message content
					for _, msg := range messages {
						expectedContent := fmt.Sprintf("continuous-message-%d", atomic.LoadInt64(&messagesRead))
						if string(msg.Data) != expectedContent {
							select {
							case readerErrors <- fmt.Errorf("unexpected message content at position %d: got %q, want %q",
								atomic.LoadInt64(&messagesRead), string(msg.Data), expectedContent):
							default:
							}
							return
						}
						atomic.AddInt64(&messagesRead, 1)
					}

					// ACK the messages
					messageIDs := make([]MessageID, len(messages))
					for i, msg := range messages {
						messageIDs[i] = msg.ID
					}
					if err := consumer.Ack(ctx, messageIDs...); err != nil {
						select {
						case readerErrors <- fmt.Errorf("ACK error: %w", err):
						default:
						}
						return
					}

					count := atomic.LoadInt64(&messagesRead)
					if count%100 == 0 {
						t.Logf("Reader: Read %d messages", count)
					}
				} else {
					// No messages available
					consecutiveEmptyReads++
					if consecutiveEmptyReads > maxEmptyReads {
						// Check if writer is still active
						written := atomic.LoadInt64(&messagesWritten)
						read := atomic.LoadInt64(&messagesRead)
						if written > read+10 { // If writer is more than 10 messages ahead
							select {
							case readerErrors <- fmt.Errorf("reader stuck: written=%d, read=%d, %d consecutive empty reads",
								written, read, consecutiveEmptyReads):
							default:
							}
							return
						}
					}
				}

				// Small delay between reads
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Wait for context to expire or error
	select {
	case <-ctx.Done():
		// Normal completion
	case err := <-writerErrors:
		t.Fatalf("Writer error: %v", err)
	case err := <-readerErrors:
		t.Fatalf("Reader error: %v", err)
	}

	// Give a moment for final counts
	time.Sleep(500 * time.Millisecond)

	// Check final statistics
	finalWritten := atomic.LoadInt64(&messagesWritten)
	finalRead := atomic.LoadInt64(&messagesRead)

	t.Logf("Final statistics: Written=%d, Read=%d", finalWritten, finalRead)

	// Allow for some lag, but reader should have caught up to most messages
	if finalRead < finalWritten-10 {
		t.Errorf("Reader fell too far behind: written=%d, read=%d (diff=%d)",
			finalWritten, finalRead, finalWritten-finalRead)
	}

	// Ensure we actually processed a meaningful number of messages
	// At 50ms intervals over 10 seconds, we should write ~200 messages
	if finalWritten < 150 {
		t.Errorf("Expected to write at least 150 messages in 10 seconds, but only wrote %d", finalWritten)
	}

	if finalRead < 140 {
		t.Errorf("Expected to read at least 140 messages in 10 seconds, but only read %d", finalRead)
	}
}

// TestConsumerContinuousStreamWithRestart tests continuous streaming with consumer restart
func TestConsumerContinuousStreamWithRestart(t *testing.T) {
	dataDir := t.TempDir()

	// Create client with short flush interval
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10 * time.Millisecond // 10ms flush interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream := "test:v1:shard:0000"

	// Start continuous writer
	var messagesWritten int64
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count := atomic.LoadInt64(&messagesWritten)
				msg := fmt.Sprintf("restart-test-%d", count)
				entries := [][]byte{[]byte(msg)}

				if _, err := client.Append(ctx, stream, entries); err != nil {
					if ctx.Err() == nil {
						t.Logf("Write error: %v", err)
					}
					return
				}

				atomic.AddInt64(&messagesWritten, 1)

				// Sync every 10 messages to make data available to consumers
				if (count+1)%10 == 0 {
					client.Sync(ctx)
				}
			}
		}
	}()

	// Phase 1: Read with first consumer for 5 seconds
	t.Log("Phase 1: Starting first consumer")
	consumer1 := NewConsumer(client, ConsumerOptions{Group: "restart-test"})

	var phase1Read int64
	phase1Ctx, phase1Cancel := context.WithTimeout(ctx, 5*time.Second)
	go func() {
		defer phase1Cancel()
		for {
			select {
			case <-phase1Ctx.Done():
				return
			default:
				messages, err := consumer1.Read(phase1Ctx, []uint32{0}, 50)
				if err != nil && phase1Ctx.Err() == nil {
					t.Logf("Phase 1 read error: %v", err)
					return
				}

				atomic.AddInt64(&phase1Read, int64(len(messages)))

				// ACK messages
				if len(messages) > 0 {
					messageIDs := make([]MessageID, len(messages))
					for i, msg := range messages {
						messageIDs[i] = msg.ID
					}
					consumer1.Ack(phase1Ctx, messageIDs...)
				}

				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	<-phase1Ctx.Done()
	consumer1.Close()

	phase1Written := atomic.LoadInt64(&messagesWritten)
	phase1ReadFinal := atomic.LoadInt64(&phase1Read)
	t.Logf("Phase 1 complete: Written=%d, Read=%d", phase1Written, phase1ReadFinal)

	// Phase 2: Create new consumer and continue reading
	t.Log("Phase 2: Starting new consumer after restart")
	consumer2 := NewConsumer(client, ConsumerOptions{Group: "restart-test"})
	defer consumer2.Close()

	var phase2Read int64
	phase2Start := time.Now()

	for {
		select {
		case <-ctx.Done():
			goto done
		default:
			messages, err := consumer2.Read(ctx, []uint32{0}, 50)
			if err != nil && ctx.Err() == nil {
				t.Fatalf("Phase 2 read error: %v", err)
			}

			if len(messages) > 0 {
				atomic.AddInt64(&phase2Read, int64(len(messages)))

				// Verify we're continuing from where we left off
				if len(messages) > 0 {
					// Just check first message to see if we continued properly
					firstMsg := messages[0]
					t.Logf("Post-restart first message: %s", string(firstMsg.Data))
				}

				// ACK messages
				messageIDs := make([]MessageID, len(messages))
				for i, msg := range messages {
					messageIDs[i] = msg.ID
				}
				consumer2.Ack(ctx, messageIDs...)
			}

			// If we've been reading for 5+ seconds in phase 2, we're done
			if time.Since(phase2Start) > 5*time.Second {
				goto done
			}

			time.Sleep(100 * time.Millisecond)
		}
	}

done:
	cancel() // Stop the writer
	time.Sleep(200 * time.Millisecond)

	// Final statistics
	finalWritten := atomic.LoadInt64(&messagesWritten)
	phase1ReadFinal = atomic.LoadInt64(&phase1Read)
	totalRead := phase1ReadFinal + phase2Read

	t.Logf("Final: Written=%d, Phase1Read=%d, Phase2Read=%d, TotalRead=%d",
		finalWritten, phase1ReadFinal, phase2Read, totalRead)

	// Verify continuity - total read should be close to total written
	if totalRead < finalWritten-10 {
		t.Errorf("Reader fell behind after restart: written=%d, totalRead=%d (diff=%d)",
			finalWritten, totalRead, finalWritten-totalRead)
	}

	// Verify phase 2 picked up where phase 1 left off (no duplicates, no gaps)
	if phase2Read > 0 && totalRead > finalWritten {
		t.Errorf("Read more messages than written (duplicates?): written=%d, read=%d",
			finalWritten, totalRead)
	}
}
