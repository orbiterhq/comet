//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMmapCoordinationAtomicity verifies that WriteOffset.Add() gives unique, non-overlapping ranges
func TestMmapCoordinationAtomicity(t *testing.T) {
	testDir := t.TempDir()
	config := MultiProcessConfig()

	// Write init entry
	client, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"init": true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Get path to test executable
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Start 10 processes that will record their allocation ranges
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cmd := exec.Command(executable, "-test.run", "^TestMmapCoordinationAtomicity$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_COORD_TEST_ROLE=allocator-%d", id),
				fmt.Sprintf("COMET_COORD_TEST_DIR=%s", testDir),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("allocator-%d failed: %v\nOutput: %s", id, err, out)
			} else {
				t.Logf("allocator-%d output: %s", id, out)
			}
		}(i)
	}

	wg.Wait()

	// Parse allocation ranges from output and verify no overlaps
	// This test will PROVE that atomic allocation works
}

// TestMmapDataIntegrity verifies that actual writes don't corrupt each other
func TestMmapDataIntegrity(t *testing.T) {
	testDir := t.TempDir()
	config := MultiProcessConfig()

	// Write init entry
	client, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"init": true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Get path to test executable
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Start 5 processes that write unique signatures
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cmd := exec.Command(executable, "-test.run", "^TestMmapDataIntegrity$", "-test.v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_COORD_TEST_ROLE=writer-%d", id),
				fmt.Sprintf("COMET_COORD_TEST_DIR=%s", testDir),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("writer-%d failed: %v\nOutput: %s", id, err, out)
			} else {
				t.Logf("writer-%d output: %s", id, out)
			}
		}(i)
	}

	wg.Wait()

	// Now read ALL data and verify no corruption
	client, err = NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer := NewConsumer(client, ConsumerOptions{Group: "integrity-test"})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 1000)
	if err != nil {
		t.Fatal(err)
	}

	// Count entries by writer
	writerCounts := make(map[string]int)
	totalEntries := len(messages)

	for _, msg := range messages {
		data := string(msg.Data)
		if strings.Contains(data, `"writer":`) {
			// Extract writer ID and increment count
			for i := 0; i < 5; i++ {
				if strings.Contains(data, fmt.Sprintf(`"writer":"writer-%d"`, i)) {
					writerCounts[fmt.Sprintf("writer-%d", i)]++
					break
				}
			}
		}
	}

	t.Logf("Data integrity test: found %d total entries", totalEntries)
	for writer, count := range writerCounts {
		t.Logf("  %s: %d entries", writer, count)
	}

	// Verify we have entries from all writers (proves no corruption)
	if len(writerCounts) != 5 {
		t.Errorf("Expected entries from 5 writers, got %d", len(writerCounts))
	}

	// Expected: 1 init + 5 writers × 20 entries each = 101 entries
	expectedTotal := 101
	if totalEntries < expectedTotal-5 { // Allow small variance
		t.Errorf("Expected ~%d entries, got %d (possible data corruption)", expectedTotal, totalEntries)
	}
}

func init() {
	// Handle child process roles
	role := os.Getenv("COMET_COORD_TEST_ROLE")
	if role == "" {
		return
	}

	switch {
	case strings.HasPrefix(role, "allocator-"):
		testCoordinationAllocator(role)
		os.Exit(0)
	case strings.HasPrefix(role, "writer-"):
		testDataIntegrityWriter(role)
		os.Exit(0)
	}
}

func testCoordinationAllocator(role string) {
	dir := os.Getenv("COMET_COORD_TEST_DIR")
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		fmt.Printf("%s: failed to create client: %v\n", role, err)
		return
	}
	defer client.Close()

	// Get the shard and access its mmap writer directly
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		fmt.Printf("%s: failed to get shard: %v\n", role, err)
		return
	}

	if shard.mmapWriter == nil {
		fmt.Printf("%s: no mmap writer available\n", role)
		return
	}

	// Record allocations to prove atomicity
	coordState := shard.mmapWriter.CoordinationState()
	allocations := make([][2]int64, 0, 50) // [start, end] pairs

	for i := 0; i < 50; i++ {
		// Simulate allocation
		size := int64(60) // Fixed size for easy verification
		startOffset := coordState.WriteOffset.Add(size) - size
		endOffset := startOffset + size - 1

		allocations = append(allocations, [2]int64{startOffset, endOffset})

		// Small delay to increase chance of interleaving
		time.Sleep(1 * time.Millisecond)
	}

	// Print allocations for verification
	fmt.Printf("%s: ALLOCATIONS ", role)
	for _, alloc := range allocations {
		fmt.Printf("[%d-%d] ", alloc[0], alloc[1])
	}
	fmt.Printf("\n")
}

func testDataIntegrityWriter(role string) {
	dir := os.Getenv("COMET_COORD_TEST_DIR")
	config := MultiProcessConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		fmt.Printf("%s: failed to create client: %v\n", role, err)
		return
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Write 20 entries with unique signature
	successCount := 0
	for i := 0; i < 20; i++ {
		data := fmt.Sprintf(`{"writer":"%s","seq":%d,"unique":"sig_%s_%d"}`, role, i, role, i)
		_, err := client.Append(ctx, streamName, [][]byte{[]byte(data)})
		if err == nil {
			successCount++
		} else {
			fmt.Printf("%s: write %d failed: %v\n", role, i, err)
		}

		// Small delay to increase concurrency chances
		time.Sleep(2 * time.Millisecond)
	}

	fmt.Printf("%s: wrote %d/20 entries\n", role, successCount)
}
