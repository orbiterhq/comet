package comet

import (
	"context"
	"testing"
)

// TestMmapIndexFilesIssue isolates the issue where Files array is empty in multi-process mode
func TestMmapIndexFilesIssue(t *testing.T) {
	dir := t.TempDir()

	t.Run("SingleProcess", func(t *testing.T) {
		config := DefaultCometConfig()
		client, err := NewClientWithConfig(dir+"/single", config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()
		_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
		if err != nil {
			t.Fatal(err)
		}

		shard, _ := client.getOrCreateShard(0)
		shard.mu.RLock()
		fileCount := len(shard.index.Files)
		shard.mu.RUnlock()

		t.Logf("Single-process mode: Files count = %d", fileCount)
		if fileCount == 0 {
			t.Error("Expected at least 1 file in single-process mode")
		}
	})

	t.Run("MultiProcess", func(t *testing.T) {
		config := MultiProcessConfig(0, 2)
		config.Concurrency.ProcessID = 0
		config.Concurrency.ProcessCount = 2

		client, err := NewClientWithConfig(dir+"/multi", config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()

		// Get the shard before writing to see initial state
		shard, _ := client.getOrCreateShard(0)
		shard.mu.RLock()
		t.Logf("Before write - Files: %d, CurrentEntryNumber: %d",
			len(shard.index.Files), shard.index.CurrentEntryNumber)
		hasMmapWriter := shard.mmapWriter != nil
		shard.mu.RUnlock()
		t.Logf("Has mmap writer: %v", hasMmapWriter)

		// Write data
		_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
		if err != nil {
			t.Fatal(err)
		}

		// Check state after write
		shard.mu.RLock()
		t.Logf("After write - Files: %d, CurrentEntryNumber: %d",
			len(shard.index.Files), shard.index.CurrentEntryNumber)
		if shard.mmapWriter != nil {
			shard.mmapWriter.mu.Lock()
			t.Logf("MmapWriter index Files: %d", len(shard.mmapWriter.index.Files))
			shard.mmapWriter.mu.Unlock()
		}
		fileCount := len(shard.index.Files)
		shard.mu.RUnlock()

		if fileCount == 0 {
			t.Error("Expected at least 1 file in multi-process mode")
		}
	})
}

// TestMmapWriterFileAddition tests that mmap writer correctly adds files to index
func TestMmapWriterFileAddition(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Create shard which should create mmap writer
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Log initial state
	shard.mu.RLock()
	t.Logf("Initial state:")
	t.Logf("  Shard index Files: %d", len(shard.index.Files))
	t.Logf("  Shard index CurrentFile: %s", shard.index.CurrentFile)

	if shard.mmapWriter != nil {
		shard.mmapWriter.mu.Lock()
		t.Logf("  MmapWriter index Files: %d", len(shard.mmapWriter.index.Files))
		t.Logf("  MmapWriter dataPath: %s", shard.mmapWriter.dataPath)
		t.Logf("  MmapWriter index pointer: %p", shard.mmapWriter.index)
		t.Logf("  Shard index pointer: %p", shard.index)
		shard.mmapWriter.mu.Unlock()
	}
	shard.mu.RUnlock()

	// The mmap writer should have added a file during initialization
	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	if fileCount == 0 {
		t.Error("MmapWriter should have added a file during initialization")
	}
}

// TestLoadIndexBehavior tests what happens when loadIndex is called
func TestLoadIndexBehavior(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Create shard
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Manually add a file to the index
	shard.mu.Lock()
	shard.index.Files = append(shard.index.Files, FileInfo{
		Path:    "/test/file.comet",
		Entries: 5,
	})
	t.Logf("Before loadIndex - Files: %d", len(shard.index.Files))
	origIndexPtr := shard.index
	t.Logf("Original index pointer: %p", origIndexPtr)

	// Call loadIndex
	if err = shard.loadIndex(); err != nil {
		t.Fatal(err)
	}
	newIndexPtr := shard.index
	t.Logf("After loadIndex - Files: %d", len(shard.index.Files))
	t.Logf("New index pointer: %p", newIndexPtr)
	shard.mu.Unlock()

	if origIndexPtr != newIndexPtr {
		t.Error("loadIndex replaced the index pointer!")
	}
}
