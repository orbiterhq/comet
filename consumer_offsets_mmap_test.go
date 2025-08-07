package comet

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestConsumerOffsetMmap(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset-mmap-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a new mmap offset store
	store, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Test initial state
	offset, exists := store.Get("test-group")
	if exists || offset != 0 {
		t.Errorf("Expected no offset for new group, got %d (exists=%v)", offset, exists)
	}

	// Set an offset
	if err := store.Set("test-group", 100); err != nil {
		t.Fatalf("Failed to set offset: %v", err)
	}

	// Verify it's immediately visible
	offset, exists = store.Get("test-group")
	if !exists || offset != 100 {
		t.Errorf("Expected offset 100, got %d (exists=%v)", offset, exists)
	}

	// Close and reopen to verify persistence
	store.Close()

	store2, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	// Verify loaded offset
	offset, exists = store2.Get("test-group")
	if !exists || offset != 100 {
		t.Errorf("Expected loaded offset 100, got %d (exists=%v)", offset, exists)
	}

	// Test multiple groups
	if err := store2.Set("group-1", 200); err != nil {
		t.Fatal(err)
	}
	if err := store2.Set("group-2", 300); err != nil {
		t.Fatal(err)
	}

	allOffsets := store2.GetAll()
	if len(allOffsets) != 3 {
		t.Errorf("Expected 3 offsets, got %d", len(allOffsets))
	}

	if allOffsets["test-group"] != 100 {
		t.Errorf("Expected test-group=100, got %d", allOffsets["test-group"])
	}
	if allOffsets["group-1"] != 200 {
		t.Errorf("Expected group-1=200, got %d", allOffsets["group-1"])
	}
	if allOffsets["group-2"] != 300 {
		t.Errorf("Expected group-2=300, got %d", allOffsets["group-2"])
	}
}

func TestConsumerOffsetMmapConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset-mmap-concurrent")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Test concurrent writes to different groups
	var wg sync.WaitGroup
	numGroups := 100
	numUpdates := 1000

	for i := 0; i < numGroups; i++ {
		wg.Add(1)
		go func(groupID int) {
			defer wg.Done()
			group := fmt.Sprintf("group-%d", groupID)

			for j := 0; j < numUpdates; j++ {
				if err := store.Set(group, int64(j)); err != nil {
					t.Errorf("Failed to set offset for %s: %v", group, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all groups have the correct final offset
	for i := 0; i < numGroups; i++ {
		group := fmt.Sprintf("group-%d", i)
		offset, exists := store.Get(group)
		if !exists {
			t.Errorf("Group %s missing", group)
		} else if offset != int64(numUpdates-1) {
			t.Errorf("Group %s has offset %d, expected %d", group, offset, numUpdates-1)
		}
	}
}

func TestConsumerOffsetMmapMultiProcess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset-mmap-multiproc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create first "process" (store instance)
	store1, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store1.Close()

	// Create second "process" (another store instance)
	store2, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	// Write from process 1
	if err := store1.Set("shared-group", 100); err != nil {
		t.Fatal(err)
	}

	// Should be immediately visible in process 2
	offset, exists := store2.Get("shared-group")
	if !exists || offset != 100 {
		t.Errorf("Process 2 should see offset 100, got %d (exists=%v)", offset, exists)
	}

	// Update from process 2
	if err := store2.Set("shared-group", 200); err != nil {
		t.Fatal(err)
	}

	// Should be immediately visible in process 1
	offset, exists = store1.Get("shared-group")
	if !exists || offset != 200 {
		t.Errorf("Process 1 should see offset 200, got %d (exists=%v)", offset, exists)
	}
}

func TestConsumerOffsetMmapMigration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset-mmap-migration")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create old-style offset file manually (simulating the old format)
	// The old format was: version(1 byte), count(4 bytes), then entries
	oldPath := filepath.Join(tmpDir, "offsets.bin")
	oldData := []byte{
		1,          // version
		2, 0, 0, 0, // count = 2
		14, 'm', 'i', 'g', 'r', 'a', 't', 'e', 'd', '-', 'g', 'r', 'o', 'u', 'p', // group name length + name
		244, 1, 0, 0, 0, 0, 0, 0, // offset = 500 (little endian)
		13, 'a', 'n', 'o', 't', 'h', 'e', 'r', '-', 'g', 'r', 'o', 'u', 'p', // group name length + name
		88, 2, 0, 0, 0, 0, 0, 0, // offset = 600 (little endian)
	}
	if err := os.WriteFile(oldPath, oldData, 0644); err != nil {
		t.Fatal(err)
	}

	// Verify old file exists
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatal("Old offset file should exist")
	}

	// Create mmap store - should migrate
	mmapStore, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer mmapStore.Close()

	// Verify migrated offsets
	offset, exists := mmapStore.Get("migrated-group")
	if !exists || offset != 500 {
		t.Errorf("Expected migrated offset 500, got %d (exists=%v)", offset, exists)
	}

	offset, exists = mmapStore.Get("another-group")
	if !exists || offset != 600 {
		t.Errorf("Expected migrated offset 600, got %d (exists=%v)", offset, exists)
	}

	// Old file should be deleted after migration
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Error("Old offset file should be deleted after migration")
	}
}

func TestConsumerOffsetMmapTableFull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset-mmap-full")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Try to fill the table
	for i := 0; i < MaxConsumerGroups+10; i++ {
		group := fmt.Sprintf("group-%d", i)
		err := store.Set(group, int64(i))

		if i < MaxConsumerGroups {
			if err != nil {
				t.Errorf("Should be able to set offset for group %d: %v", i, err)
			}
		} else {
			if err == nil {
				t.Errorf("Should fail when table is full at group %d", i)
			}
		}
	}

	// Verify we have exactly MaxConsumerGroups entries
	used, total := store.GetStats()
	if used != MaxConsumerGroups {
		t.Errorf("Expected %d used slots, got %d", MaxConsumerGroups, used)
	}
	if total != MaxConsumerGroups {
		t.Errorf("Expected %d total slots, got %d", MaxConsumerGroups, total)
	}
}

func BenchmarkConsumerOffsetMmapGet(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "offset-mmap-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Pre-populate some groups
	for i := 0; i < 100; i++ {
		group := fmt.Sprintf("group-%d", i)
		store.Set(group, int64(i*1000))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			group := fmt.Sprintf("group-%d", i%100)
			offset, _ := store.Get(group)
			if offset != int64((i%100)*1000) {
				b.Errorf("Wrong offset for %s: %d", group, offset)
			}
			i++
		}
	})
}

func BenchmarkConsumerOffsetMmapSet(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "offset-mmap-bench-set")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewConsumerOffsetMmap(tmpDir, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Pre-populate groups to avoid allocation during benchmark
	for i := 0; i < 100; i++ {
		group := fmt.Sprintf("group-%d", i)
		store.Set(group, 0)
	}

	var counter int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			offset := atomic.AddInt64(&counter, 1)
			group := fmt.Sprintf("group-%d", offset%100)
			if err := store.Set(group, offset); err != nil {
				b.Error(err)
			}
		}
	})
}
