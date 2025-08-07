package comet

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestOffsetCalculationRace tests if the offset calculation itself has issues
func TestOffsetCalculationRace(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 5 // Very fast flush
	// Disable the retry logic temporarily

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	errorCh := make(chan string, 100)
	var wg sync.WaitGroup

	// Writer that writes continuously
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, err := client.Append(ctx, stream, [][]byte{[]byte("x")}) // Small entries
			if err != nil {
				errorCh <- "write error: " + err.Error()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Reader that reads continuously
	wg.Add(1)
	go func() {
		defer wg.Done()
		totalRead := 0
		for attempt := 0; attempt < 20 && totalRead < 20; attempt++ {
			messages, err := consumer.Read(ctx, []uint32{0}, 5)
			if err != nil {
				errStr := err.Error()
				errorCh <- errStr
				// Check if it's the specific error we're looking for
				if strings.Contains(errStr, "beyond file size") {
					// Extract the offset and file size
					t.Logf("FOUND THE RACE: %s", errStr)
				}
				return
			}
			totalRead += len(messages)
			for _, msg := range messages {
				consumer.Ack(ctx, msg.ID)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errorCh)

	// Check errors
	for err := range errorCh {
		t.Logf("Error: %s", err)
		if strings.Contains(err, "beyond file size") {
			// This is the bug we're looking for
			t.Fatalf("Got offset beyond file size error: %s", err)
		}
	}
}
