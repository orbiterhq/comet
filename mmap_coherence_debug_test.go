package comet

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestMmapCoherenceDebug tests the exact mmap coherence issue with detailed logging
func TestMmapCoherenceDebug(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10 // Fast flush

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write a few entries
	for i := 0; i < 5; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for periodic flush
	time.Sleep(30 * time.Millisecond)

	// Get the shard and file info
	shard, _ := client.getOrCreateShard(0)

	shard.mu.RLock()
	var dataFilePath string
	if shard.dataFile != nil {
		dataFilePath = shard.dataFile.Name()
	}
	currentEntryNum := shard.index.CurrentEntryNumber
	fileEntries := shard.index.Files[0].Entries
	fileEndOffset := shard.index.Files[0].EndOffset
	shard.mu.RUnlock()

	t.Logf("After flush: CurrentEntryNumber=%d, FileEntries=%d, FileEndOffset=%d",
		currentEntryNum, fileEntries, fileEndOffset)

	// Now let's manually check the file size
	stat, err := os.Stat(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Actual file size on disk: %d bytes", stat.Size())

	// Create a reader and try to read
	reader, err := NewReader(0, shard.cloneIndex())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Try to read entry 4 (the last one)
	t.Logf("Attempting to read entry 4...")
	data, err := reader.ReadEntryByNumber(4)
	if err != nil {
		t.Logf("Initial read failed: %v", err)

		// Let's manually check what the mmap sees
		file, err := os.Open(dataFilePath)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()

		// Get file size again
		stat2, _ := file.Stat()
		t.Logf("File size when opened by reader: %d bytes", stat2.Size())

		// Try manual mmap
		if stat2.Size() > 0 {
			mmapData, err := syscall.Mmap(int(file.Fd()), 0, int(stat2.Size()),
				syscall.PROT_READ, syscall.MAP_PRIVATE)
			if err != nil {
				t.Logf("Manual mmap failed: %v", err)
			} else {
				t.Logf("Manual mmap succeeded, got %d bytes", len(mmapData))
				syscall.Munmap(mmapData)
			}
		}

		// Now write one more entry to trigger growth
		t.Logf("\nWriting one more entry...")
		_, err = client.Append(ctx, stream, [][]byte{[]byte("msg-5")})
		if err != nil {
			t.Fatal(err)
		}

		// Force sync manually
		shard.writeMu.Lock()
		if shard.writer != nil {
			shard.writer.Flush()
		}
		shard.writeMu.Unlock()

		if shard.dataFile != nil {
			shard.dataFile.Sync()
		}

		// Check file size after sync
		stat3, _ := os.Stat(dataFilePath)
		t.Logf("File size after sync: %d bytes", stat3.Size())

		// Try to read again with a fresh reader
		shard.mu.RLock()
		newIndex := shard.cloneIndex()
		shard.mu.RUnlock()

		reader2, err := NewReader(0, newIndex)
		if err != nil {
			t.Fatal(err)
		}
		defer reader2.Close()

		// Read the new entry
		data2, err := reader2.ReadEntryByNumber(5)
		if err != nil {
			t.Fatalf("Failed to read after sync: %v", err)
		}
		t.Logf("Successfully read entry 5: %s", data2)

	} else {
		t.Logf("Successfully read entry 4: %s", data)
	}
}

// TestMmapCoherenceRootCause isolates the exact issue
func TestMmapCoherenceRootCause(t *testing.T) {
	// Create a test file
	file, err := os.CreateTemp(t.TempDir(), "mmap-test-*.dat")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write initial data
	initialData := []byte("Hello, World!")
	if _, err := file.Write(initialData); err != nil {
		t.Fatal(err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	// Get initial size
	stat1, _ := file.Stat()
	initialSize := stat1.Size()
	t.Logf("Initial file size: %d", initialSize)

	// Memory map the file
	mmapData, err := syscall.Mmap(int(file.Fd()), 0, int(initialSize),
		syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(mmapData)

	t.Logf("Mapped %d bytes: %s", len(mmapData), string(mmapData))

	// Now append more data
	moreData := []byte(" More data!")
	if _, err := file.Write(moreData); err != nil {
		t.Fatal(err)
	}

	// Sync again
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	// Check new size
	stat2, _ := file.Stat()
	newSize := stat2.Size()
	t.Logf("New file size after append: %d", newSize)

	// The mmap still sees old data!
	t.Logf("Mmap still sees %d bytes: %s", len(mmapData), string(mmapData))

	// Even if we re-stat through a new file handle...
	file2, err := os.Open(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer file2.Close()

	stat3, _ := file2.Stat()
	t.Logf("New file handle sees size: %d", stat3.Size())

	// Create a new mmap from the new handle
	mmapData2, err := syscall.Mmap(int(file2.Fd()), 0, int(stat3.Size()),
		syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(mmapData2)

	t.Logf("New mmap sees %d bytes: %s", len(mmapData2), string(mmapData2))

	// This demonstrates the issue: even with fsync, the old mmap doesn't update!
}
