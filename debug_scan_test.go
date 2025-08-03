// +build integration

package comet

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

// TestDataScanning - debug why written data isn't being scanned back
func TestDataScanning(t *testing.T) {
	dir := t.TempDir()
	
	config := DefaultCometConfig()
	config.Log.EnableDebug = true
	
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Write test data exactly like the failing test
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("Wrote 20 entries")

	// Try consumer read like the failing test
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Consumer.Read returned %d messages", len(messages))

	// Also check ScanAll
	var found int
	err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		found++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	
	t.Logf("ScanAll found %d entries", found)
	
	// Always check file info for debugging
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	t.Logf("Shard has %d files", len(shard.index.Files))
	for i, f := range shard.index.Files {
		t.Logf("  File %d: %s (start=%d, entries=%d)", i, f.Path, f.StartEntry, f.Entries)
	}
	t.Logf("Index CurrentEntryNumber: %d", shard.index.CurrentEntryNumber)
	if shard.state != nil {
		t.Logf("State LastEntryNumber: %d", shard.state.GetLastEntryNumber())
	}
	shard.mu.RUnlock()
	
	if found == 0 {
		// Check what the data file actually contains
		if len(shard.index.Files) > 0 {
			filePath := shard.index.Files[0].Path
			t.Logf("Data file: %s", filePath)
			
			// Try to read file directly
			data, err := os.ReadFile(filePath)
			if err != nil {
				t.Logf("Error reading file: %v", err)
			} else {
				t.Logf("File size: %d bytes", len(data))
				if len(data) >= 12 {
					length := binary.LittleEndian.Uint32(data[0:4])
					timestamp := binary.LittleEndian.Uint64(data[4:12])
					t.Logf("First entry header: length=%d, timestamp=%d", length, timestamp)
					
					if len(data) >= int(12+length) {
						entryData := data[12 : 12+length]
						t.Logf("First entry data: %q", string(entryData))
					}
				}
			}
		}
	}
}