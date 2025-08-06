package comet

import (
	"context"
	"testing"
)

func TestMixedBatchCompression(t *testing.T) {
	dir := t.TempDir()

	// Create config with compression enabled
	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 100 // Compress entries >= 100 bytes

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Create a mixed batch
	smallEntry := []byte("small")   // 5 bytes - won't be compressed
	largeEntry := make([]byte, 200) // 200 bytes - will be compressed
	for i := range largeEntry {
		largeEntry[i] = byte('A' + i%26) // Fill with pattern ABCD...
	}

	// Create mixed batch: alternating small and large
	entries := [][]byte{
		smallEntry,
		largeEntry,
		smallEntry,
		largeEntry,
		smallEntry,
		largeEntry,
		smallEntry,
		largeEntry,
		smallEntry,
		largeEntry,
	}

	// Append the mixed batch
	ids, err := client.Append(ctx, streamName, entries)
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != len(entries) {
		t.Errorf("Expected %d IDs, got %d", len(entries), len(ids))
	}

	// Sync to make entries durable and trackable in index
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Get stats to verify compression happened
	stats := client.GetStats()
	t.Logf("Stats after mixed batch:")
	t.Logf("  Total entries: %d", stats.TotalEntries)
	t.Logf("  Compressed entries: %d", stats.CompressedEntries)
	t.Logf("  Skipped compression: %d", stats.SkippedCompression)
	t.Logf("  Total bytes: %d", stats.TotalBytes)
	t.Logf("  Total compressed: %d", stats.TotalCompressed)

	// Verify we have both compressed and uncompressed entries
	if stats.CompressedEntries == 0 {
		t.Error("Expected some entries to be compressed")
	}
	if stats.SkippedCompression == 0 {
		t.Error("Expected some entries to skip compression")
	}

	// For a mixed batch, we should have:
	// - 5 small entries (not compressed but still written)
	// - 5 large entries (compressed and written)
	expectedCompressed := int64(5)
	expectedSkipped := int64(5)

	if stats.CompressedEntries < expectedCompressed {
		t.Errorf("Expected at least %d compressed entries, got %d", expectedCompressed, stats.CompressedEntries)
	}
	if stats.SkippedCompression < expectedSkipped {
		t.Errorf("Expected at least %d entries to skip compression, got %d", expectedSkipped, stats.SkippedCompression)
	}

	// Verify all entries were written (compressed + skipped should be <= total)
	// Note: These are cumulative stats, so we check that our batch contributed correctly
	if stats.TotalEntries < int64(len(entries)) {
		t.Errorf("Expected at least %d total entries written, got %d", len(entries), stats.TotalEntries)
	}

	// Verify the shard has the correct number of entries
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	if shard.index.CurrentEntryNumber != int64(len(entries)) {
		t.Errorf("Expected shard to have %d entries, but CurrentEntryNumber is %d",
			len(entries), shard.index.CurrentEntryNumber)
	}

	t.Logf("All %d entries were written successfully:", len(entries))
	t.Logf("  - %d were compressed (large entries >= 100 bytes)", expectedCompressed)
	t.Logf("  - %d were written uncompressed (small entries < 100 bytes)", expectedSkipped)
}

func TestMixedBatchAllocations(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 100

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Test allocations for different scenarios
	scenarios := []struct {
		name    string
		entries [][]byte
	}{
		{
			name: "AllSmall",
			entries: [][]byte{
				[]byte("small1"),
				[]byte("small2"),
				[]byte("small3"),
				[]byte("small4"),
				[]byte("small5"),
			},
		},
		{
			name: "AllLarge",
			entries: [][]byte{
				make([]byte, 200),
				make([]byte, 200),
				make([]byte, 200),
				make([]byte, 200),
				make([]byte, 200),
			},
		},
		{
			name: "Mixed",
			entries: [][]byte{
				[]byte("small1"),
				make([]byte, 200),
				[]byte("small2"),
				make([]byte, 200),
				[]byte("small3"),
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Count allocations
			allocs := testing.AllocsPerRun(100, func() {
				_, err := client.Append(ctx, streamName, scenario.entries)
				if err != nil {
					t.Fatal(err)
				}
			})

			t.Logf("%s: %.1f allocations", scenario.name, allocs)
		})
	}
}

func BenchmarkMixedBatch(b *testing.B) {
	dir := b.TempDir()

	config := DefaultCometConfig()
	config.Compression.MinCompressSize = 100

	client, err := NewClient(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Create a realistic mixed batch
	entries := make([][]byte, 100)
	for i := range entries {
		if i%3 == 0 {
			// 1/3 are large, compressible
			entries[i] = make([]byte, 500)
			for j := range entries[i] {
				entries[i][j] = byte('A' + j%26)
			}
		} else {
			// 2/3 are small, not compressed
			entries[i] = []byte("log entry " + string(rune('0'+i%10)))
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, entries)
		if err != nil {
			b.Fatal(err)
		}
	}

	stats := client.GetStats()
	b.ReportMetric(float64(stats.CompressedEntries), "compressed")
	b.ReportMetric(float64(stats.SkippedCompression), "skipped")
}
