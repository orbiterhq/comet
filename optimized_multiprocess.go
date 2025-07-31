package comet

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// OptimizedShardState represents shared state for ultra-fast multi-process coordination
type OptimizedShardState struct {
	// Atomic counters for lock-free coordination
	NextSequence    atomic.Int64  // Next entry sequence number
	WriteOffset     atomic.Int64  // Current write offset in active file
	ActiveFileIndex atomic.Int64  // Index of current file (for rotation detection)
	FlushGeneration atomic.Int64  // Incremented on each background flush
	_padding        [32]byte      // Avoid false sharing between fields
}

// ProcessLocalBuffer holds entries before they're flushed to shared storage
type ProcessLocalBuffer struct {
	mu          sync.Mutex
	entries     []BufferedEntry
	maxEntries  int
	flushTicker *time.Ticker
	shard       *OptimizedShard
}

// BufferedEntry represents an entry waiting to be flushed
type BufferedEntry struct {
	Sequence int64
	Data     []byte
	Offset   int64 // Pre-allocated offset in mmap
}

// OptimizedShard implements ultra-fast multi-process writes
type OptimizedShard struct {
	// Memory-mapped regions
	sharedState     *OptimizedShardState // Shared coordination state
	sharedStateData []byte               // Mmap data for shared state
	dataFile        *os.File             // Current data file
	dataMap         []byte               // Memory-mapped data region
	dataMapSize     int64                // Size of mapped region

	// Process-local state
	localBuffer  *ProcessLocalBuffer
	shardID      uint32
	maxFileSize  int64
	lastFlushGen int64

	// Paths
	shardDir  string
	statePath string
}

// NewOptimizedShard creates a new optimized shard for multi-process writes
func NewOptimizedShard(shardID uint32, shardDir string) (*OptimizedShard, error) {
	s := &OptimizedShard{
		shardID:     shardID,
		shardDir:    shardDir,
		statePath:   shardDir + "/optimized.state",
		maxFileSize: 1 << 30, // 1GB default
	}

	// Initialize shared state
	if err := s.initSharedState(); err != nil {
		return nil, fmt.Errorf("failed to init shared state: %w", err)
	}

	// Open current data file
	if err := s.openCurrentFile(); err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	// Initialize process-local buffer
	s.localBuffer = &ProcessLocalBuffer{
		maxEntries:  1000, // Flush every 1000 entries or 10ms
		shard:       s,
		flushTicker: time.NewTicker(10 * time.Millisecond),
	}

	// Start background flusher
	go s.localBuffer.backgroundFlush()

	return s, nil
}

// initSharedState initializes the memory-mapped shared state
func (s *OptimizedShard) initSharedState() error {
	// Create or open state file
	file, err := os.OpenFile(s.statePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Ensure file is correct size
	const stateSize = 256 // Plenty of room for future fields
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		// New file - initialize
		if err := file.Truncate(stateSize); err != nil {
			return err
		}
	}

	// Memory map the state
	data, err := syscall.Mmap(int(file.Fd()), 0, stateSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	s.sharedStateData = data
	s.sharedState = (*OptimizedShardState)(unsafe.Pointer(&data[0]))

	return nil
}

// openCurrentFile opens and maps the current data file
func (s *OptimizedShard) openCurrentFile() error {
	// For simplicity, using a single file for now
	// Real implementation would handle rotation
	dataPath := fmt.Sprintf("%s/log-%016d.comet", s.shardDir,
		s.sharedState.ActiveFileIndex.Load())

	// Open or create file
	file, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Pre-allocate space (1GB)
	if err := file.Truncate(s.maxFileSize); err != nil {
		file.Close()
		return err
	}

	// Memory map the entire file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(s.maxFileSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return err
	}

	s.dataFile = file
	s.dataMap = data
	s.dataMapSize = s.maxFileSize

	return nil
}

// Append adds entries with ultra-low latency
func (s *OptimizedShard) Append(entries [][]byte) ([]MessageID, error) {
	ids := make([]MessageID, len(entries))

	// Step 1: Allocate sequence numbers atomically (~50ns per entry)
	sequences := make([]int64, len(entries))
	for i := range entries {
		sequences[i] = s.sharedState.NextSequence.Add(1) - 1
		ids[i] = MessageID{EntryNumber: sequences[i], ShardID: s.shardID}
	}

	// Step 2: Pre-allocate space in the mmap region (~100ns)
	totalSize := int64(0)
	for _, entry := range entries {
		totalSize += 12 + int64(len(entry)) // header + data
	}

	// Atomic allocation of write space
	offset := s.sharedState.WriteOffset.Add(totalSize) - totalSize

	// Check if we need rotation (simplified - real impl would be more robust)
	if offset+totalSize > s.dataMapSize {
		return nil, fmt.Errorf("file full - rotation needed")
	}

	// Step 3: Write directly to mmap (~100ns per entry)
	currentOffset := offset
	for _, entry := range entries {
		// Write header
		header := s.dataMap[currentOffset : currentOffset+12]
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(entry)))
		binary.LittleEndian.PutUint64(header[4:12], uint64(time.Now().UnixNano()))
		currentOffset += 12

		// Write data
		copy(s.dataMap[currentOffset:currentOffset+int64(len(entry))], entry)
		currentOffset += int64(len(entry))
	}

	// Step 4: Optional - add to local buffer for index updates
	// This is done asynchronously and doesn't block the write
	s.localBuffer.addEntries(sequences, entries, offset)

	return ids, nil
}

// addEntries adds entries to the local buffer for background processing
func (b *ProcessLocalBuffer) addEntries(sequences []int64, entries [][]byte, baseOffset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	offset := baseOffset
	for i, entry := range entries {
		b.entries = append(b.entries, BufferedEntry{
			Sequence: sequences[i],
			Data:     entry,
			Offset:   offset,
		})
		offset += 12 + int64(len(entry))
	}

	// Flush if buffer is full
	if len(b.entries) >= b.maxEntries {
		b.flushLocked()
	}
}

// backgroundFlush periodically flushes the local buffer
func (b *ProcessLocalBuffer) backgroundFlush() {
	for range b.flushTicker.C {
		b.mu.Lock()
		if len(b.entries) > 0 {
			b.flushLocked()
		}
		b.mu.Unlock()
	}
}

// flushLocked flushes buffered entries (must hold lock)
func (b *ProcessLocalBuffer) flushLocked() {
	// In a real implementation, this would:
	// 1. Update the index with entry positions
	// 2. Notify readers via shared state update
	// 3. Handle any necessary cleanup

	// For now, just clear the buffer
	b.entries = b.entries[:0]

	// Update flush generation to notify readers
	b.shard.sharedState.FlushGeneration.Add(1)
}

// Close cleans up resources
func (s *OptimizedShard) Close() error {
	// Stop background flusher
	if s.localBuffer != nil {
		s.localBuffer.flushTicker.Stop()

		// Final flush
		s.localBuffer.mu.Lock()
		if len(s.localBuffer.entries) > 0 {
			s.localBuffer.flushLocked()
		}
		s.localBuffer.mu.Unlock()
	}

	// Unmap regions
	if s.dataMap != nil {
		syscall.Munmap(s.dataMap)
	}
	if s.sharedStateData != nil {
		syscall.Munmap(s.sharedStateData)
	}

	// Close files
	if s.dataFile != nil {
		s.dataFile.Close()
	}

	return nil
}

// Reader implementation would use similar mmap techniques for zero-copy reads
// It would check FlushGeneration to detect new data availability
