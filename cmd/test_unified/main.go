package main

import (
	"context"
	"fmt"
	"log"

	comet "github.com/orbiterhq/comet"
)

func main() {
	// Create a config with multi-process mode to enable CometState
	config := comet.DeprecatedMultiProcessConfig(0, 1)

	// Create client with temp directory
	client, err := comet.NewClientWithConfig("/tmp/test_unified_state", config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write some test data
	entries := [][]byte{
		[]byte("test entry 1"),
		[]byte("test entry 2"),
		[]byte("test entry 3"),
	}

	ids, err := client.Append(ctx, streamName, entries)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Added %d entries\n", len(ids))

	// Check if metrics are being tracked (this verifies CometState is working)
	stats := client.GetStats()
	fmt.Printf("Stats - TotalEntries: %d, TotalBytes: %d, FileRotations: %d\n",
		stats.TotalEntries, stats.TotalBytes, stats.FileRotations)

	if stats.TotalEntries > 0 && stats.TotalBytes > 0 {
		fmt.Println("✅ CometState integration test PASSED - metrics are being tracked!")
	} else {
		fmt.Println("❌ CometState integration test FAILED - no metrics tracked")
	}
}
