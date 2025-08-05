package comet

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSequenceCounterFileNaming verifies that .comet files use sequence counters, not timestamps
func TestSequenceCounterFileNaming(t *testing.T) {
	dir := t.TempDir()

	// Test both single-process and multi-process modes
	testCases := []struct {
		name   string
		config CometConfig
	}{
		{
			name:   "SingleProcess",
			config: DefaultCometConfig(),
		},
		{
			name:   "MultiProcess",
			config: MultiProcessConfig(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Small file size to force rotation
			tc.config.Storage.MaxFileSize = 100

			client, err := NewClient(dir, tc.config)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			ctx := context.Background()
			shardName := "test:v1:shard:0000"

			// Write data to create first file
			_, err = client.Append(ctx, shardName, [][]byte{[]byte("first entry")})
			if err != nil {
				t.Fatal(err)
			}

			// Get the first file name
			shard, err := client.getOrCreateShard(0)
			if err != nil {
				t.Fatal(err)
			}

			shard.mu.RLock()
			firstFile := shard.index.CurrentFile
			shard.mu.RUnlock()

			// Extract the sequence number from the filename
			// Format: log-NNNNNNNNNNNNNNNN.comet
			baseName := filepath.Base(firstFile)
			if !strings.HasPrefix(baseName, "log-") || !strings.HasSuffix(baseName, ".comet") {
				t.Errorf("Invalid file name format: %s", baseName)
			}

			// For multi-process mode, verify it's using sequence counter
			if tc.config.Concurrency.IsMultiProcess() {
				// Extract the number part
				numStr := strings.TrimPrefix(baseName, "log-")
				numStr = strings.TrimSuffix(numStr, ".comet")

				// Should be 16 digits (zero-padded)
				if len(numStr) != 16 {
					t.Errorf("Expected 16-digit sequence number, got %d digits: %s", len(numStr), numStr)
				}

				t.Logf("First file name: %s", baseName)

				// Check if it's a sequence number (should be small) vs timestamp (large)
				var seqNum uint64
				seqNum, _ = parseUint64(numStr)
				if seqNum > 1000 {
					t.Errorf("Appears to be using timestamp instead of sequence counter: %d", seqNum)
				}
			}

			// Write more data to force rotation
			largeData := make([]byte, 150)
			for i := range largeData {
				largeData[i] = 'A'
			}
			_, err = client.Append(ctx, shardName, [][]byte{largeData})
			if err != nil {
				t.Fatal(err)
			}

			// Check second file name
			shard.mu.RLock()
			secondFile := shard.index.CurrentFile
			shard.mu.RUnlock()

			if secondFile == firstFile {
				t.Error("File should have rotated but didn't")
			}

			// For multi-process mode, verify sequence incremented
			if tc.config.Concurrency.IsMultiProcess() {
				baseName2 := filepath.Base(secondFile)
				numStr2 := strings.TrimPrefix(baseName2, "log-")
				numStr2 = strings.TrimSuffix(numStr2, ".comet")

				t.Logf("Second file name: %s", baseName2)

				// Parse both sequence numbers
				seq1, _ := parseUint64(strings.TrimSuffix(strings.TrimPrefix(baseName, "log-"), ".comet"))
				seq2, _ := parseUint64(numStr2)

				// Second should be greater than first
				if seq2 <= seq1 {
					t.Errorf("Second file sequence (%d) should be greater than first (%d)", seq2, seq1)
				}

				// Should be sequential
				if seq2 != seq1+1 {
					t.Errorf("Files should have sequential numbers: first=%d, second=%d", seq1, seq2)
				}

				// Verify state tracking
				if shard.state != nil {
					lastSeq := atomic.LoadUint64(&shard.state.LastFileSequence)
					t.Logf("LastFileSequence in state: %d", lastSeq)
					if lastSeq < 2 {
						t.Errorf("LastFileSequence should be at least 2, got %d", lastSeq)
					}
				}
			}
		})
	}
}

// TestSequenceCounterVsTimestamp verifies files don't use timestamps
func TestSequenceCounterVsTimestamp(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	config.Storage.MaxFileSize = 100

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	shardName := "test:v1:shard:0000"

	// Record time before creating files
	timeBefore := time.Now().UnixNano()

	// Create multiple files quickly
	for i := 0; i < 3; i++ {
		data := make([]byte, 150)
		for j := range data {
			data[j] = byte('A' + i)
		}
		_, err = client.Append(ctx, shardName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	timeAfter := time.Now().UnixNano()

	// List all files created
	files, err := filepath.Glob(filepath.Join(dir, "shard-0000", "log-*.comet"))
	if err != nil {
		t.Fatal(err)
	}

	if len(files) < 3 {
		t.Errorf("Expected at least 3 files, got %d", len(files))
	}

	// Check that files use sequential numbers, not timestamps
	for i, file := range files {
		baseName := filepath.Base(file)
		numStr := strings.TrimPrefix(baseName, "log-")
		numStr = strings.TrimSuffix(numStr, ".comet")

		// Parse as number
		seqNum, err := parseUint64(numStr)
		if err != nil {
			t.Errorf("Failed to parse sequence number from %s: %v", baseName, err)
			continue
		}

		// If it were a timestamp, it would be between timeBefore and timeAfter
		// But sequence numbers should be 1, 2, 3, etc. (much smaller)
		if int64(seqNum) >= timeBefore && int64(seqNum) <= timeAfter {
			t.Errorf("File %s appears to use timestamp (%d) instead of sequence counter", baseName, seqNum)
		}

		t.Logf("File %d: %s (sequence: %d)", i, baseName, seqNum)
	}
}

// parseUint64 is a helper to parse uint64 from string
func parseUint64(s string) (uint64, error) {
	var result uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, nil
		}
		result = result*10 + uint64(c-'0')
	}
	return result, nil
}
