// +build integration

package comet

import (
	"context"
	"fmt"
	"testing"
)

func init() {
	Debug = true
}

// TestThreshold - find exact breaking point
func TestThreshold(t *testing.T) {
	for _, count := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20} {
		t.Run(fmt.Sprintf("Count_%d", count), func(t *testing.T) {
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

			// Write entries
			for i := 0; i < count; i++ {
				data := []byte(fmt.Sprintf(`{"id": %d}`, i))
				_, err := client.Append(ctx, streamName, [][]byte{data})
				if err != nil {
					t.Fatalf("Write %d failed: %v", i, err)
				}
			}

			// Check results
			shard, _ := client.getOrCreateShard(1)
			shard.mu.RLock()
			fileCount := len(shard.index.Files)
			indexCurrent := shard.index.CurrentEntryNumber
			stateLast := int64(-999)
			if shard.state != nil {
				stateLast = shard.state.GetLastEntryNumber()
			}
			shard.mu.RUnlock()

			// ScanAll test
			var scanCount int
			err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
				scanCount++
				return true
			})
			if err != nil {
				t.Fatal(err)
			}

			// Log results in standardized format
			success := scanCount == count
			t.Logf("COUNT=%d FILES=%d INDEX=%d STATE=%d SCAN=%d SUCCESS=%v", 
				count, fileCount, indexCurrent, stateLast, scanCount, success)

			if !success {
				t.Errorf("Expected %d entries, got %d", count, scanCount)
			}
		})
	}
}