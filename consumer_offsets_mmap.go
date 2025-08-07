package comet

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	// ConsumerOffsetMagic identifies a valid consumer offset file
	ConsumerOffsetMagic = 0xC0FE0FF5

	// ConsumerOffsetVersion for format changes
	ConsumerOffsetVersion = 1

	// MaxConsumerGroups supported per shard (must be power of 2 for efficient hashing)
	MaxConsumerGroups = 512

	// ConsumerGroupNameSize maximum bytes for group name
	ConsumerGroupNameSize = 48

	// ConsumerOffsetFileSize total size of mmap file
	ConsumerOffsetFileSize = 64 + (MaxConsumerGroups * 128) // 64KB + 64 bytes header
)

// ConsumerOffsetHeader is the header of the memory-mapped file
type ConsumerOffsetHeader struct {
	Version  uint32    // 4 bytes
	Magic    uint32    // 4 bytes
	Reserved [56]uint8 // 56 bytes padding to 64 bytes
}

// ConsumerEntry represents a single consumer group's offset
// Sized to exactly 128 bytes (2 cache lines) for alignment
type ConsumerEntry struct {
	// Cache line 1 (64 bytes)
	GroupName  [ConsumerGroupNameSize]byte // 48 bytes - null-terminated string
	Offset     int64                       // 8 bytes - atomic access
	LastUpdate int64                       // 8 bytes - unix nano timestamp

	// Cache line 2 (64 bytes)
	AckCount uint64    // 8 bytes - total ACKs for metrics
	Reserved [56]uint8 // 56 bytes - future use
}

// ConsumerOffsetMmap provides lock-free access to consumer offsets
type ConsumerOffsetMmap struct {
	path    string
	file    *os.File
	data    []byte
	header  *ConsumerOffsetHeader
	entries *[MaxConsumerGroups]ConsumerEntry
	shardID uint32
}

// NewConsumerOffsetMmap creates or opens a memory-mapped consumer offset file
func NewConsumerOffsetMmap(shardPath string, shardID uint32) (*ConsumerOffsetMmap, error) {
	offsetPath := filepath.Join(shardPath, "offsets.state")

	// Check if we need to migrate from old format
	oldPath := filepath.Join(shardPath, "offsets.bin")
	oldOffsets := make(map[string]int64)
	if data, err := os.ReadFile(oldPath); err == nil && len(data) >= 5 {
		// Parse old format: version(1), count(4), then entries
		if data[0] == 1 { // version 1
			count := binary.LittleEndian.Uint32(data[1:5])
			offset := 5

			for i := uint32(0); i < count && offset < len(data); i++ {
				// Read group name length
				groupLen := int(data[offset])
				offset++

				if offset+groupLen+8 > len(data) {
					break
				}

				// Read group name
				group := string(data[offset : offset+groupLen])
				offset += groupLen

				// Read offset
				consumerOffset := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
				offset += 8

				oldOffsets[group] = consumerOffset
			}
		}
		// We'll delete the old file after successful migration
	}

	// Open or create the mmap file
	file, err := os.OpenFile(offsetPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open consumer offset file: %w", err)
	}

	// Check if this is a new file
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat consumer offset file: %w", err)
	}

	isNew := stat.Size() == 0

	// Ensure file is the correct size
	if stat.Size() != 0 && stat.Size() != ConsumerOffsetFileSize {
		file.Close()
		return nil, fmt.Errorf("consumer offset file has wrong size: %d (expected %d)",
			stat.Size(), ConsumerOffsetFileSize)
	}

	if isNew {
		// Initialize new file
		if err := file.Truncate(ConsumerOffsetFileSize); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to resize consumer offset file: %w", err)
		}
	}

	// Memory map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, ConsumerOffsetFileSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap consumer offset file: %w", err)
	}

	com := &ConsumerOffsetMmap{
		path:    offsetPath,
		file:    file,
		data:    data,
		shardID: shardID,
	}

	// Cast the header
	com.header = (*ConsumerOffsetHeader)(unsafe.Pointer(&data[0]))

	// Cast the entries array (starts after 64-byte header)
	com.entries = (*[MaxConsumerGroups]ConsumerEntry)(unsafe.Pointer(&data[64]))

	if isNew {
		// Initialize header using binary encoding for proper byte order
		binary.LittleEndian.PutUint32(data[0:4], ConsumerOffsetVersion)
		binary.LittleEndian.PutUint32(data[4:8], ConsumerOffsetMagic)

		// Sync header to disk
		if err := com.msync(0, 64); err != nil {
			com.Close()
			return nil, fmt.Errorf("failed to sync header: %w", err)
		}
	} else {
		// Validate existing file
		version := binary.LittleEndian.Uint32(data[0:4])
		magic := binary.LittleEndian.Uint32(data[4:8])

		if magic != ConsumerOffsetMagic {
			com.Close()
			return nil, fmt.Errorf("invalid consumer offset file magic: %x", magic)
		}
		if version != ConsumerOffsetVersion {
			com.Close()
			return nil, fmt.Errorf("unsupported consumer offset version: %d", version)
		}
	}

	// Migrate old offsets if needed
	if len(oldOffsets) > 0 {
		for group, offset := range oldOffsets {
			com.Set(group, offset)
		}
		// Delete old file after successful migration
		os.Remove(oldPath)
	}

	return com, nil
}

// findSlot finds or allocates a slot for the consumer group
func (c *ConsumerOffsetMmap) findSlot(group string) int {
	// Hash the group name
	h := fnv.New32a()
	h.Write([]byte(group))
	hash := h.Sum32()

	// Start at hash position
	start := int(hash % MaxConsumerGroups)

	// Linear probe to find the group or an empty slot
	for i := 0; i < MaxConsumerGroups; i++ {
		slot := (start + i) % MaxConsumerGroups
		entry := &c.entries[slot]

		// Check if this is our group
		groupBytes := entry.GroupName[:]
		if groupBytes[0] != 0 {
			// Slot is occupied, check if it's our group
			existingGroup := string(groupBytes[:cStringLen(groupBytes)])
			if existingGroup == group {
				return slot
			}
		} else {
			// Empty slot, try to claim it
			groupNameBytes := []byte(group)
			if len(groupNameBytes) > ConsumerGroupNameSize-1 {
				groupNameBytes = groupNameBytes[:ConsumerGroupNameSize-1]
			}

			// Try to atomically claim the slot by checking if we can set the group name
			// First check if still empty
			if entry.GroupName[0] == 0 {
				// Copy the group name
				copy(entry.GroupName[:], groupNameBytes)
				// Memory barrier to ensure the write is visible
				atomic.StoreInt64(&entry.LastUpdate, 1) // Mark as claimed

				// Double-check it's still our slot
				existingGroup := string(entry.GroupName[:cStringLen(entry.GroupName[:])])
				if existingGroup == group {
					return slot
				}
			}
			// Someone else claimed it, continue searching
		}
	}

	return -1 // Table full
}

// Get returns the offset for a consumer group
func (c *ConsumerOffsetMmap) Get(group string) (int64, bool) {
	slot := c.findSlot(group)
	if slot < 0 {
		return 0, false
	}

	entry := &c.entries[slot]
	offset := atomic.LoadInt64(&entry.Offset)
	lastUpdate := atomic.LoadInt64(&entry.LastUpdate)

	// Check if this group has ever set a real offset (not just claimed the slot)
	// LastUpdate > 1 means an actual offset was set
	if lastUpdate <= 1 {
		return 0, false
	}

	return offset, true
}

// Set updates the offset for a consumer group
func (c *ConsumerOffsetMmap) Set(group string, offset int64) error {
	slot := c.findSlot(group)
	if slot < 0 {
		return fmt.Errorf("consumer offset table full (max %d groups)", MaxConsumerGroups)
	}

	entry := &c.entries[slot]

	// Update offset atomically
	atomic.StoreInt64(&entry.Offset, offset)
	atomic.StoreInt64(&entry.LastUpdate, time.Now().UnixNano())
	atomic.AddUint64(&entry.AckCount, 1)

	return nil
}

// GetAll returns all consumer offsets
func (c *ConsumerOffsetMmap) GetAll() map[string]int64 {
	result := make(map[string]int64)

	for i := 0; i < MaxConsumerGroups; i++ {
		entry := &c.entries[i]

		// Skip empty slots
		if entry.GroupName[0] == 0 {
			continue
		}

		// Skip groups that never set an offset (only claimed the slot)
		if atomic.LoadInt64(&entry.LastUpdate) <= 1 {
			continue
		}

		group := string(entry.GroupName[:cStringLen(entry.GroupName[:])])
		offset := atomic.LoadInt64(&entry.Offset)
		result[group] = offset
	}

	return result
}

// Remove removes a consumer group (for testing/admin)
func (c *ConsumerOffsetMmap) Remove(group string) {
	slot := c.findSlot(group)
	if slot < 0 {
		return
	}

	entry := &c.entries[slot]

	// Clear the entry
	atomic.StoreInt64(&entry.Offset, 0)
	atomic.StoreInt64(&entry.LastUpdate, 0)
	atomic.StoreUint64(&entry.AckCount, 0)

	// Clear group name last to avoid race
	for i := range entry.GroupName {
		entry.GroupName[i] = 0
	}
}

// msync syncs a range of the memory map to disk
func (c *ConsumerOffsetMmap) msync(offset, length int) error {
	// Calculate page-aligned boundaries
	pageSize := syscall.Getpagesize()
	start := (offset / pageSize) * pageSize
	end := offset + length
	if end%pageSize != 0 {
		end = ((end / pageSize) + 1) * pageSize
	}

	// Ensure we don't go past the mmap size
	if end > len(c.data) {
		end = len(c.data)
	}

	// Use msync to flush changes
	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&c.data[start])),
		uintptr(end-start),
		uintptr(syscall.MS_SYNC),
	)
	if errno != 0 {
		return errno
	}

	return nil
}

// Close unmaps the file and closes it
func (c *ConsumerOffsetMmap) Close() error {
	if c.data != nil {
		// Sync any pending changes
		c.msync(0, len(c.data))

		// Unmap
		if err := syscall.Munmap(c.data); err != nil {
			return fmt.Errorf("failed to munmap consumer offsets: %w", err)
		}
		c.data = nil
	}

	if c.file != nil {
		if err := c.file.Close(); err != nil {
			return fmt.Errorf("failed to close consumer offset file: %w", err)
		}
		c.file = nil
	}

	return nil
}

// cStringLen returns the length of a null-terminated C string
func cStringLen(b []byte) int {
	for i, v := range b {
		if v == 0 {
			return i
		}
	}
	return len(b)
}

// GetStats returns statistics about consumer offset usage
func (c *ConsumerOffsetMmap) GetStats() (used, total int) {
	total = MaxConsumerGroups
	for i := 0; i < MaxConsumerGroups; i++ {
		if c.entries[i].GroupName[0] != 0 {
			used++
		}
	}
	return used, total
}
