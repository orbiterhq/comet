package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestConsumerDataIntegrity ensures NO messages are ever lost
func TestConsumerDataIntegrity(t *testing.T) {
	dataDir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 20
	config.Storage.MaxFileSize = 5 * 1024 // Force rotations

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "integrity:v1:shard:0000"

	consumer := NewConsumer(client, ConsumerOptions{Group: "integrity-test"})
	defer consumer.Close()

	// Write EXACTLY 500 messages with known content to force multiple rotations
	expectedMessages := make(map[string]bool)
	totalWritten := 500

	t.Logf("Writing exactly %d messages...", totalWritten)
	for i := 0; i < totalWritten; i++ {
		messageContent := fmt.Sprintf("message-%06d", i) // 6-digit padded
		data := [][]byte{[]byte(messageContent)}

		_, err = client.Append(ctx, stream, data)
		if err != nil {
			t.Fatalf("Failed to write message %d: %v", i, err)
		}

		expectedMessages[messageContent] = false // false = not yet read

		// Sync every 10 messages
		if (i+1)%10 == 0 {
			client.Sync(ctx)
		}
	}

	// Final sync to ensure ALL data is durable
	client.Sync(ctx)
	t.Logf("All %d messages written and synced", totalWritten)

	// Give time for any background operations
	time.Sleep(200 * time.Millisecond)

	// Now read ALL messages - no time limits, keep reading until no more
	totalRead := 0
	readMessages := make(map[string]bool)
	maxAttempts := 50 // Reasonable limit to prevent infinite loop

	for attempt := 0; attempt < maxAttempts; attempt++ {
		messages, err := consumer.Read(ctx, []uint32{0}, 50) // Large batch size
		if err != nil {
			t.Logf("Read error on attempt %d: %v", attempt, err)
			continue
		}

		if len(messages) == 0 {
			t.Logf("No more messages after attempt %d", attempt)
			break
		}

		// Process each message
		for _, msg := range messages {
			messageContent := string(msg.Data)

			// Check if this is an expected message
			if _, exists := expectedMessages[messageContent]; !exists {
				t.Errorf("Got unexpected message: %q", messageContent)
				continue
			}

			// Check for duplicates
			if readMessages[messageContent] {
				t.Errorf("Duplicate message: %q", messageContent)
				continue
			}

			readMessages[messageContent] = true
			totalRead++
		}

		// ACK all messages
		messageIDs := make([]MessageID, len(messages))
		for i, msg := range messages {
			messageIDs[i] = msg.ID
		}
		consumer.Ack(ctx, messageIDs...)

		t.Logf("Attempt %d: read %d messages (total so far: %d)", attempt, len(messages), totalRead)
	}

	t.Logf("FINAL COUNT: Written=%d, Read=%d", totalWritten, totalRead)

	// Check for data loss
	if totalRead != totalWritten {
		t.Errorf("DATA LOSS DETECTED: wrote %d messages but only read %d", totalWritten, totalRead)

		// Show which messages were lost
		missingCount := 0
		for expectedMsg := range expectedMessages {
			if !readMessages[expectedMsg] {
				if missingCount < 10 { // Show first 10 missing messages
					t.Errorf("MISSING MESSAGE: %q", expectedMsg)
				}
				missingCount++
			}
		}
		if missingCount > 10 {
			t.Errorf("... and %d more missing messages", missingCount-10)
		}
	}

	// Check file rotations occurred (to ensure we tested the hard case)
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatalf("Failed to get shard: %v", err)
	}
	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	t.Logf("File rotations: %d files created", fileCount)
	if fileCount < 3 {
		t.Logf("Warning: Expected multiple files from rotations, only got %d", fileCount)
	}

	// SUCCESS - all messages accounted for
	if totalRead == totalWritten {
		t.Logf("SUCCESS: Perfect data integrity - no messages lost through %d file rotations", fileCount)
	}
}
