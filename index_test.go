package comet

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestBinaryIndexFormat(t *testing.T) {
	dir := t.TempDir()

	// Create test index with lots of data
	index := &ShardIndex{
		CurrentEntryNumber: 1000000,
		CurrentWriteOffset: 1024 * 1024 * 1024,
		ConsumerOffsets: map[string]int64{
			"consumer-group-1": 500000,
			"consumer-group-2": 600000,
			"consumer-group-3": 700000,
		},
		BinaryIndex: BinarySearchableIndex{
			IndexInterval: 100,
			MaxNodes:      10000,
			Nodes:         make([]EntryIndexNode, 1000),
		},
	}

	// Fill with test data
	for i := range index.BinaryIndex.Nodes {
		index.BinaryIndex.Nodes[i] = EntryIndexNode{
			EntryNumber: int64(i * 100),
			Position: EntryPosition{
				FileIndex:  i / 100,
				ByteOffset: int64(i * 1024),
			},
		}
	}

	// Create a mock shard for testing
	shard := &Shard{
		indexPath: filepath.Join(dir, "index.json"),
		index:     index,
	}

	// Save in binary format
	err := shard.saveBinaryIndex(index)
	if err != nil {
		t.Fatalf("failed to save binary index: %v", err)
	}

	// Load it back
	loaded, err := shard.loadBinaryIndex()
	if err != nil {
		t.Fatalf("failed to load binary index: %v", err)
	}

	// Verify data integrity
	if loaded.CurrentEntryNumber != index.CurrentEntryNumber {
		t.Errorf("CurrentEntryNumber mismatch: got %d, want %d",
			loaded.CurrentEntryNumber, index.CurrentEntryNumber)
	}

	if loaded.CurrentWriteOffset != index.CurrentWriteOffset {
		t.Errorf("CurrentWriteOffset mismatch: got %d, want %d",
			loaded.CurrentWriteOffset, index.CurrentWriteOffset)
	}

	// Check consumer offsets
	for group, offset := range index.ConsumerOffsets {
		if loaded.ConsumerOffsets[group] != offset {
			t.Errorf("ConsumerOffset[%s] mismatch: got %d, want %d",
				group, loaded.ConsumerOffsets[group], offset)
		}
	}

	// Check binary index nodes
	if len(loaded.BinaryIndex.Nodes) != len(index.BinaryIndex.Nodes) {
		t.Errorf("Node count mismatch: got %d, want %d",
			len(loaded.BinaryIndex.Nodes), len(index.BinaryIndex.Nodes))
	}

	for i, node := range loaded.BinaryIndex.Nodes {
		if node.EntryNumber != index.BinaryIndex.Nodes[i].EntryNumber {
			t.Errorf("Node[%d].EntryNumber mismatch: got %d, want %d",
				i, node.EntryNumber, index.BinaryIndex.Nodes[i].EntryNumber)
		}
	}

	// Check file size
	binaryStat, _ := os.Stat(shard.indexPath)
	t.Logf("Binary index size: %d bytes for %d entries and %d consumers",
		binaryStat.Size(), index.CurrentEntryNumber, len(index.ConsumerOffsets))

	// Verify reasonable size (should be compact)
	expectedMinSize := 24 + // header
		len(index.ConsumerOffsets)*16 + // consumer offsets (estimate)
		len(index.BinaryIndex.Nodes)*20 // index nodes (20 bytes each)

	if binaryStat.Size() > int64(expectedMinSize*2) {
		t.Errorf("Binary format larger than expected: %d bytes (expected ~%d bytes)",
			binaryStat.Size(), expectedMinSize)
	}
}

func TestVerifyBinaryFormat(t *testing.T) {
	dir := t.TempDir()
	client, err := NewClient(dir)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Add some data
	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte(`{"test": "data"}`),
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Force sync to persist index
	client.Sync(ctx)

	// Create consumer and ACK to force index write
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	messages, _ := consumer.Read(ctx, []uint32{0}, 10)
	if len(messages) > 0 {
		consumer.Ack(ctx, messages[0].ID)
	}

	// Close to ensure final persist
	client.Close()

	// Now check the actual file format
	indexPath := filepath.Join(dir, "shard-0000", "index.bin")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("failed to read index file: %v", err)
	}

	t.Logf("Index file size: %d bytes", len(data))
	t.Logf("First 32 bytes (hex): %s", hex.EncodeToString(data[:32]))
	t.Logf("First 32 bytes (ascii): %q", string(data[:32]))

	// Check for binary magic number (0x434F4D54 = "COMT")
	if len(data) >= 4 && data[0] == 0x54 && data[1] == 0x4D && data[2] == 0x4F && data[3] == 0x43 {
		t.Log("✓ File is in BINARY format (found COMT magic)")
	} else if len(data) > 0 && data[0] == '{' {
		t.Error("✗ File is still in JSON format")
	} else {
		t.Error("✗ Unknown format")
	}
}
