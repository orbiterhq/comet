//go:build integration
// +build integration

package comet

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestBrowseMultiProcessMinimal is a minimal test to debug the issue
func TestBrowseMultiProcessMinimal(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	// Check if we're a subprocess
	if role := os.Getenv("COMET_BROWSE_MINIMAL_ROLE"); role != "" {
		runMinimalWorker(t, role)
		return
	}

	// Create a completely fresh directory with timestamp to ensure uniqueness
	dir := filepath.Join(t.TempDir(), fmt.Sprintf("minimal_test_%d", time.Now().UnixNano()))
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	// Verify it's empty
	entries, _ := os.ReadDir(dir)
	if len(entries) > 0 {
		t.Fatalf("Directory not empty: %d entries", len(entries))
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Step 1: Write 5 entries
	cmd1 := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessMinimal$", "-test.v")
	cmd1.Env = append(os.Environ(),
		"COMET_BROWSE_MINIMAL_ROLE=write",
		fmt.Sprintf("COMET_BROWSE_MINIMAL_DIR=%s", dir),
	)
	output1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("Write failed: %v\nOutput: %s", err, output1)
	}
	t.Logf("Write output: %s", output1)

	// Give time for checkpoint
	time.Sleep(300 * time.Millisecond)

	// Step 2: List and scan
	cmd2 := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessMinimal$", "-test.v")
	cmd2.Env = append(os.Environ(),
		"COMET_BROWSE_MINIMAL_ROLE=browse",
		fmt.Sprintf("COMET_BROWSE_MINIMAL_DIR=%s", dir),
	)
	output2, err := cmd2.CombinedOutput()
	if err != nil {
		t.Fatalf("Browse failed: %v\nOutput: %s", err, output2)
	}
	t.Logf("Browse output: %s", output2)
}

func runMinimalWorker(t *testing.T, role string) {
	dir := os.Getenv("COMET_BROWSE_MINIMAL_DIR")
	if dir == "" {
		t.Fatal("COMET_BROWSE_MINIMAL_DIR not set")
	}

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	switch role {
	case "write":
		// Write exactly 5 entries
		for i := 0; i < 5; i++ {
			data, _ := json.Marshal(map[string]interface{}{
				"id":   i,
				"data": fmt.Sprintf("entry-%d", i),
			})
			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err != nil {
				t.Fatalf("Failed to append: %v", err)
			}
		}

		// Force sync
		if err := client.Sync(ctx); err != nil {
			t.Logf("Warning: Sync failed: %v", err)
		}

		fmt.Printf("Wrote 5 entries\n")

		// Debug: check what the client thinks
		stats := client.GetStats()
		fmt.Printf("Client stats after write: TotalEntries=%d\n", stats.TotalEntries)

		// Debug: check index file
		indexPath := filepath.Join(dir, "shard-0001", "index.bin")
		if info, err := os.Stat(indexPath); err == nil {
			fmt.Printf("Index file exists, size=%d\n", info.Size())
			// Read the raw content
			if data, err := os.ReadFile(indexPath); err == nil && len(data) >= 16 {
				entryNum := binary.LittleEndian.Uint64(data[8:16])
				fmt.Printf("Index file after write shows CurrentEntryNumber=%d\n", entryNum)
			}
		} else {
			fmt.Printf("Index file error: %v\n", err)
		}

	case "browse":
		// Debug: check index file before browse
		indexPath := filepath.Join(dir, "shard-0001", "index.bin")
		if info, err := os.Stat(indexPath); err == nil {
			fmt.Printf("Index file exists before browse, size=%d\n", info.Size())
		} else {
			fmt.Printf("Index file error before browse: %v\n", err)
		}

		// Try ListRecent
		messages, err := client.ListRecent(ctx, streamName, 10)
		if err != nil {
			t.Fatalf("ListRecent failed: %v", err)
		}
		fmt.Printf("ListRecent returned %d messages\n", len(messages))

		// Try ScanAll
		count := 0
		err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
			count++
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err == nil {
				fmt.Printf("Scanned entry %d: %v\n", msg.ID.EntryNumber, data)
			} else {
				fmt.Printf("Scanned entry %d: unmarshal error: %v\n", msg.ID.EntryNumber, err)
			}
			return count < 20 // Stop after 20 to prevent infinite loop
		})
		if err != nil {
			t.Fatalf("ScanAll failed: %v", err)
		}
		fmt.Printf("ScanAll found %d entries\n", count)

		// Debug: check stats
		stats := client.GetStats()
		fmt.Printf("Client stats after browse: TotalEntries=%d\n", stats.TotalEntries)

		// Debug: manually read index file to see what's in it
		if data, err := os.ReadFile(indexPath); err == nil && len(data) >= 16 {
			magic := binary.LittleEndian.Uint32(data[0:4])
			version := binary.LittleEndian.Uint32(data[4:8])
			entryNum := binary.LittleEndian.Uint64(data[8:16])
			fmt.Printf("Index file raw: magic=0x%x version=%d CurrentEntryNumber=%d\n", magic, version, entryNum)
		}
	}
}
