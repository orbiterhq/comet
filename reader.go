package comet

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/klauspost/compress/zstd"
)

// AtomicSlice provides atomic access to a byte slice using atomic.Value
type AtomicSlice struct {
	value atomic.Value // Stores []byte
}

// Load atomically loads the slice
func (a *AtomicSlice) Load() []byte {
	if v := a.value.Load(); v != nil {
		return v.([]byte)
	}
	return nil
}

// Store atomically stores a new slice
func (a *AtomicSlice) Store(data []byte) {
	a.value.Store(data)
}

// ReaderConfig configures the bounded reader behavior
type ReaderConfig struct {
	MaxMappedFiles    int   // Maximum number of files to keep mapped (default: 10)
	MaxMemoryBytes    int64 // Maximum memory to use for mapping (default: 1GB)
	CleanupIntervalMs int   // How often to run cleanup in milliseconds (default: 5000)
}

// DefaultReaderConfig returns the default configuration for Reader
func DefaultReaderConfig() ReaderConfig {
	return ReaderConfig{
		MaxMappedFiles:    10,                 // Reasonable default
		MaxMemoryBytes:    1024 * 1024 * 1024, // 1GB default
		CleanupIntervalMs: 5000,               // 5 second cleanup
	}
}

// MappedFile represents a memory-mapped file with atomic data access
type MappedFile struct {
	FileInfo
	file     *os.File
	data     atomic.Value // Stores []byte, accessed atomically
	lastSize int64        // Last known file size for remapping
}

// recentFileCache implements an LRU cache for recently accessed files
type recentFileCache struct {
	mu       sync.RWMutex
	items    map[int]*list.Element
	order    *list.List
	capacity int
}

type cacheItem struct {
	fileIndex int
}

func newRecentFileCache(capacity int) *recentFileCache {
	if capacity <= 0 {
		capacity = 5 // Reasonable minimum
	}
	return &recentFileCache{
		items:    make(map[int]*list.Element),
		order:    list.New(),
		capacity: capacity,
	}
}

func (c *recentFileCache) access(fileIndex int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[fileIndex]; exists {
		// Move to front
		c.order.MoveToFront(elem)
		return
	}

	// Add new item
	if c.order.Len() >= c.capacity {
		// Remove oldest
		oldest := c.order.Back()
		if oldest != nil {
			oldItem := oldest.Value.(*cacheItem)
			delete(c.items, oldItem.fileIndex)
			c.order.Remove(oldest)
		}
	}

	item := &cacheItem{fileIndex: fileIndex}
	elem := c.order.PushFront(item)
	c.items[fileIndex] = elem
}

// contains checks if a file index is in the recent cache (protected from eviction)
func (c *recentFileCache) contains(fileIndex int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.items[fileIndex]
	return exists
}

// Reader provides bounded memory-mapped read access to a shard with intelligent file management
type Reader struct {
	shardID      uint32
	index        *ShardIndex
	config       ReaderConfig
	fileInfos    []FileInfo          // All file metadata (not necessarily mapped)
	mappedFiles  map[int]*MappedFile // Currently mapped files
	mappingMu    sync.RWMutex        // Protects mappedFiles
	localMemory  int64               // Atomic counter for local memory usage
	recentCache  *recentFileCache
	decompressor *zstd.Decoder
	bufferPool   *sync.Pool
	state        *CometState         // Shared state for metrics (optional)
}

// NewReader creates a new bounded reader for a shard with smart file mapping
func NewReader(shardID uint32, index *ShardIndex) (*Reader, error) {
	if index == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	config := DefaultReaderConfig()

	r := &Reader{
		shardID:     shardID,
		index:       index,
		config:      config,
		fileInfos:   make([]FileInfo, len(index.Files)),
		mappedFiles: make(map[int]*MappedFile),
		recentCache: newRecentFileCache(config.MaxMappedFiles / 2), // Half capacity for recent files
		bufferPool: &sync.Pool{
			New: func() any {
				return make([]byte, 0, 64*1024)
			},
		},
	}

	// Copy file infos
	copy(r.fileInfos, index.Files)

	// Create decompressor
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create decompressor: %w", err)
	}
	r.decompressor = dec

	// Don't map files here - wait until state is set so metrics are tracked
	return r, nil
}

// SetState sets the shared state for metrics tracking
func (r *Reader) SetState(state *CometState) {
	r.state = state
	
	// Map the most recent file now that we have state for metrics
	if state != nil && len(r.fileInfos) > 0 {
		r.mappingMu.Lock()
		// Only map if we haven't already mapped any files
		if len(r.mappedFiles) == 0 {
			lastIndex := len(r.fileInfos) - 1
			mapped, err := r.mapFile(lastIndex, r.fileInfos[lastIndex])
			if err == nil {
				r.mappedFiles[lastIndex] = mapped
			}
		}
		mappedCount := len(r.mappedFiles)
		r.mappingMu.Unlock()
		
		// Update metrics with current state
		atomic.StoreUint64(&state.ReaderMappedFiles, uint64(mappedCount))
		atomic.StoreUint64(&state.ReaderCacheBytes, uint64(atomic.LoadInt64(&r.localMemory)))
	}
}

// mapFile maps a single file into memory
func (r *Reader) mapFile(fileIndex int, info FileInfo) (*MappedFile, error) {
	file, err := os.Open(info.Path)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	size := stat.Size()
	mappedFile := &MappedFile{
		FileInfo: info,
		file:     file,
		lastSize: size,
	}

	if size == 0 {
		// For empty files, store an empty byte slice
		mappedFile.data.Store([]byte{})
		return mappedFile, nil
	}

	// Check memory limits before mapping
	if atomic.LoadInt64(&r.localMemory)+size > r.config.MaxMemoryBytes {
		file.Close()
		return nil, fmt.Errorf("mapping would exceed memory limit: current=%d + new=%d > limit=%d",
			atomic.LoadInt64(&r.localMemory), size, r.config.MaxMemoryBytes)
	}

	// Memory map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	// Store data atomically
	mappedFile.data.Store(data)

	// Update memory tracking
	atomic.AddInt64(&r.localMemory, size)

	// Update metrics
	if r.state != nil {
		atomic.AddUint64(&r.state.ReaderFileMaps, 1)
		atomic.StoreUint64(&r.state.ReaderCacheBytes, uint64(atomic.LoadInt64(&r.localMemory)))
		// Note: ReaderMappedFiles is updated by the caller after adding to mappedFiles map
	}

	return mappedFile, nil
}

// ReadEntryAtPosition reads a single entry at the given position
func (r *Reader) ReadEntryAtPosition(pos EntryPosition) ([]byte, error) {
	// Check if closed
	r.mappingMu.RLock()
	closed := r.decompressor == nil
	mapped, isMapped := r.mappedFiles[pos.FileIndex]
	validIndex := pos.FileIndex >= 0 && pos.FileIndex < len(r.fileInfos)
	r.mappingMu.RUnlock()

	if closed {
		return nil, fmt.Errorf("reader is closed")
	}

	if !validIndex {
		return nil, fmt.Errorf("file index %d out of range", pos.FileIndex)
	}

	if !isMapped {
		// In smart mapping mode, temporarily map the file if we have capacity
		if err := r.ensureFileMapped(pos.FileIndex); err != nil {
			return nil, fmt.Errorf("failed to map file for read: %w", err)
		}

		// Get the mapped file again
		r.mappingMu.RLock()
		mapped = r.mappedFiles[pos.FileIndex]
		r.mappingMu.RUnlock()
	}

	if mapped == nil {
		return nil, fmt.Errorf("file %d could not be mapped", pos.FileIndex)
	}

	// Check if file has grown since last mapping
	if err := r.checkAndRemapIfGrown(pos.FileIndex, mapped); err != nil {
		return nil, fmt.Errorf("failed to check file growth: %w", err)
	}

	// Update recent cache
	r.recentCache.access(pos.FileIndex)

	// Read from the mapped data
	data := mapped.data.Load().([]byte)
	return r.readEntryFromFileData(data, pos.ByteOffset)
}

// checkAndRemapIfGrown checks if a file has grown and remaps it if necessary
func (r *Reader) checkAndRemapIfGrown(fileIndex int, mapped *MappedFile) error {
	if mapped.file == nil {
		return nil // Can't check growth without file handle
	}

	// Get current file size
	stat, err := mapped.file.Stat()
	if err != nil {
		return err
	}

	currentSize := stat.Size()

	// If file has grown, we need to remap it
	if currentSize > mapped.lastSize {
		r.mappingMu.Lock()
		defer r.mappingMu.Unlock()

		// Double-check under lock
		if currentSize > mapped.lastSize {
			// Unmap old mapping
			if data := mapped.data.Load(); data != nil {
				if dataBytes, ok := data.([]byte); ok && len(dataBytes) > 0 {
					if err := syscall.Munmap(dataBytes); err != nil {
						return fmt.Errorf("failed to unmap old data: %w", err)
					}
					// Update memory tracking
					atomic.AddInt64(&r.localMemory, -int64(len(dataBytes)))
				}
			}

			// Check memory limits for new mapping
			newSize := currentSize
			if atomic.LoadInt64(&r.localMemory)+newSize > r.config.MaxMemoryBytes {
				// Try to evict something first
				if err := r.evictOldestFile(); err != nil {
					return fmt.Errorf("cannot remap file: would exceed memory limit and cannot evict: %w", err)
				}
			}

			// Create new mapping
			if currentSize == 0 {
				// For empty files, store an empty byte slice
				mapped.data.Store([]byte{})
				mapped.lastSize = 0
				return nil
			}

			// Memory map the grown file
			data, err := syscall.Mmap(int(mapped.file.Fd()), 0, int(currentSize), syscall.PROT_READ, syscall.MAP_PRIVATE)
			if err != nil {
				return fmt.Errorf("failed to remap grown file: %w", err)
			}

			// Store new data atomically
			mapped.data.Store(data)
			mapped.lastSize = currentSize

			// Update memory tracking
			atomic.AddInt64(&r.localMemory, newSize)

			// Update metrics
			if r.state != nil {
				atomic.AddUint64(&r.state.ReaderFileRemaps, 1)
				atomic.StoreUint64(&r.state.ReaderCacheBytes, uint64(atomic.LoadInt64(&r.localMemory)))
			}
		}
	}

	return nil
}

// ensureFileMapped ensures a file is mapped, respecting memory limits
func (r *Reader) ensureFileMapped(fileIndex int) error {
	if fileIndex < 0 || fileIndex >= len(r.fileInfos) {
		return fmt.Errorf("file index %d out of range", fileIndex)
	}

	r.mappingMu.Lock()
	defer r.mappingMu.Unlock()

	// Check if already mapped
	if _, exists := r.mappedFiles[fileIndex]; exists {
		return nil
	}

	// Check if we have room for another file
	if len(r.mappedFiles) >= r.config.MaxMappedFiles {
		// Need to evict something
		if err := r.evictOldestFile(); err != nil {
			return fmt.Errorf("failed to evict file for mapping: %w", err)
		}
	}

	// Check memory limits
	if r.config.MaxMemoryBytes <= 0 {
		return fmt.Errorf("memory limit too restrictive: %d bytes", r.config.MaxMemoryBytes)
	}

	if atomic.LoadInt64(&r.localMemory) >= r.config.MaxMemoryBytes {
		// Try to free some memory
		if err := r.evictOldestFile(); err != nil {
			return fmt.Errorf("at memory limit and cannot evict: %w", err)
		}
	}

	// Map the file
	if fileIndex >= len(r.fileInfos) {
		return fmt.Errorf("file index %d out of range", fileIndex)
	}

	mapped, err := r.mapFile(fileIndex, r.fileInfos[fileIndex])
	if err != nil {
		return err
	}

	r.mappedFiles[fileIndex] = mapped

	// Update mapped files count metric
	if r.state != nil {
		atomic.StoreUint64(&r.state.ReaderMappedFiles, uint64(len(r.mappedFiles)))
	}

	return nil
}

// evictOldestFile evicts the oldest non-recent file
func (r *Reader) evictOldestFile() error {
	// Find a file to evict (not in recent cache)
	for fileIndex, mapped := range r.mappedFiles {
		if !r.recentCache.contains(fileIndex) {
			// Evict this file
			delete(r.mappedFiles, fileIndex)

			// Unmap the file
			if data := mapped.data.Load(); data != nil {
				if dataBytes, ok := data.([]byte); ok && len(dataBytes) > 0 {
					if err := syscall.Munmap(dataBytes); err != nil {
						return fmt.Errorf("failed to munmap file %d: %w", fileIndex, err)
					}
					// Update memory tracking
					atomic.AddInt64(&r.localMemory, -int64(len(dataBytes)))
				}
			}

			// Close file descriptor
			if mapped.file != nil {
				mapped.file.Close()
			}

			// Update metrics
			if r.state != nil {
				atomic.AddUint64(&r.state.ReaderFileUnmaps, 1)
				atomic.AddUint64(&r.state.ReaderCacheEvicts, 1)
				atomic.StoreUint64(&r.state.ReaderCacheBytes, uint64(atomic.LoadInt64(&r.localMemory)))
				atomic.StoreUint64(&r.state.ReaderMappedFiles, uint64(len(r.mappedFiles)))
			}

			return nil
		}
	}

	return fmt.Errorf("no files to evict")
}

// readEntryFromFileData reads a single entry from memory-mapped data at a byte offset
func (r *Reader) readEntryFromFileData(data []byte, byteOffset int64) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("file not memory mapped")
	}

	if byteOffset < 0 {
		return nil, fmt.Errorf("invalid byte offset: %d", byteOffset)
	}

	headerSize := int64(12)
	fileSize := int64(len(data))

	// Check for overflow and bounds
	if byteOffset >= fileSize || byteOffset > fileSize-headerSize {
		return nil, fmt.Errorf("invalid offset: header extends beyond file")
	}

	// Read header
	header := data[byteOffset : byteOffset+headerSize]
	length := binary.LittleEndian.Uint32(header[0:4])

	// Check bounds
	dataStart := byteOffset + headerSize
	dataEnd := dataStart + int64(length)
	if dataEnd > int64(len(data)) {
		return nil, fmt.Errorf("invalid entry: data extends beyond file")
	}

	// Extract entry data
	entryData := data[dataStart:dataEnd]

	// Check if data is compressed by looking for zstd magic bytes
	// zstd magic number is 0xFD2FB528 (little-endian: 28 B5 2F FD)
	if len(entryData) >= 4 &&
		entryData[0] == 0x28 && entryData[1] == 0xB5 &&
		entryData[2] == 0x2F && entryData[3] == 0xFD {
		// Data is compressed with zstd, decompress it

		// Get buffer from pool
		buf := r.bufferPool.Get().([]byte)
		//nolint:staticcheck // SA6002: Buffer pool pattern - resetting slice length is intentional
		defer r.bufferPool.Put(buf[:0])

		// Decompress the entire data (no compression flag prefix)
		decompressed, err := r.decompressor.DecodeAll(entryData, buf)
		if err != nil {
			return nil, fmt.Errorf("decompression failed: %w", err)
		}

		// Return a copy since we're returning the buffer to the pool
		result := make([]byte, len(decompressed))
		copy(result, decompressed)
		return result, nil
	}

	// Data is not compressed - return as is
	return entryData, nil
}

// UpdateFiles updates the reader with new file information
func (r *Reader) UpdateFiles(newFiles *[]FileInfo) error {
	r.mappingMu.Lock()
	defer r.mappingMu.Unlock()

	// Update file infos
	r.fileInfos = make([]FileInfo, len(*newFiles))
	copy(r.fileInfos, *newFiles)

	// Optionally re-map recent files if they still exist
	if len(*newFiles) > 0 {
		// Try to map the most recent file
		lastIndex := len(*newFiles) - 1
		if _, exists := r.mappedFiles[lastIndex]; !exists {
			if mapped, err := r.mapFile(lastIndex, (*newFiles)[lastIndex]); err == nil {
				r.mappedFiles[lastIndex] = mapped
			}
		}
	}

	return nil
}

// Close unmaps all files and cleans up resources
func (r *Reader) Close() error {
	r.mappingMu.Lock()
	defer r.mappingMu.Unlock()

	// Track how many files we're unmapping
	unmapCount := len(r.mappedFiles)

	var firstErr error
	for _, mapped := range r.mappedFiles {
		// Unmap if mapped
		if data := mapped.data.Load(); data != nil {
			if dataBytes, ok := data.([]byte); ok && len(dataBytes) > 0 {
				if err := syscall.Munmap(dataBytes); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}

		// Close file
		if mapped.file != nil {
			if err := mapped.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	r.mappedFiles = make(map[int]*MappedFile)

	// Reset memory tracking
	atomic.StoreInt64(&r.localMemory, 0)

	// Update metrics for files unmapped during close
	if r.state != nil && unmapCount > 0 {
		atomic.AddUint64(&r.state.ReaderFileUnmaps, uint64(unmapCount))
		atomic.StoreUint64(&r.state.ReaderCacheBytes, 0)
		atomic.StoreUint64(&r.state.ReaderMappedFiles, 0)
	}

	// Close decompressor
	if r.decompressor != nil {
		r.decompressor.Close()
		r.decompressor = nil
	}

	return firstErr
}

// GetMemoryUsage returns current memory usage statistics
func (r *Reader) GetMemoryUsage() (int64, int) {
	r.mappingMu.RLock()
	defer r.mappingMu.RUnlock()

	return atomic.LoadInt64(&r.localMemory), len(r.mappedFiles)
}

// ReadEntryByNumber reads an entry by its sequential entry number, using the file metadata to locate it
func (r *Reader) ReadEntryByNumber(entryNumber int64) ([]byte, error) {
	// Find which file contains this entry
	for fileIndex, fileInfo := range r.fileInfos {
		if fileInfo.StartEntry <= entryNumber && entryNumber < fileInfo.StartEntry+fileInfo.Entries {
			// Found the file - now we need to find the exact position within the file
			relativeEntryNum := entryNumber - fileInfo.StartEntry

			if relativeEntryNum == 0 {
				// First entry in file - starts at offset 0
				pos := EntryPosition{FileIndex: fileIndex, ByteOffset: 0}
				return r.ReadEntryAtPosition(pos)
			}

			// For non-first entries, we need to scan from the beginning
			// This is inefficient but correct for now
			return r.readEntryByScanning(fileIndex, relativeEntryNum)
		}
	}

	return nil, fmt.Errorf("entry %d not found in any file", entryNumber)
}

// readEntryByScanning scans through a file to find the Nth entry (0-indexed within file)
func (r *Reader) readEntryByScanning(fileIndex int, relativeEntryNum int64) ([]byte, error) {
	// Ensure file is mapped
	if err := r.ensureFileMapped(fileIndex); err != nil {
		return nil, err
	}

	r.mappingMu.RLock()
	mapped, exists := r.mappedFiles[fileIndex]
	r.mappingMu.RUnlock()

	if !exists || mapped == nil {
		return nil, fmt.Errorf("file %d not mapped", fileIndex)
	}

	// Check if file has grown since last mapping
	if err := r.checkAndRemapIfGrown(fileIndex, mapped); err != nil {
		return nil, fmt.Errorf("failed to check file growth: %w", err)
	}

	data := mapped.data.Load().([]byte)
	if len(data) == 0 {
		return nil, fmt.Errorf("file %d is empty", fileIndex)
	}

	// Scan through entries to find the target
	currentOffset := int64(0)
	for entryIdx := int64(0); entryIdx < relativeEntryNum; entryIdx++ {
		// Read entry header to get length
		if currentOffset+12 > int64(len(data)) {
			return nil, fmt.Errorf("insufficient data for entry %d header at offset %d", entryIdx, currentOffset)
		}

		// Extract length from header (first 4 bytes)
		length := binary.LittleEndian.Uint32(data[currentOffset : currentOffset+4])

		// Skip to next entry: header (12 bytes) + data length
		entrySize := int64(12 + length)
		currentOffset += entrySize

		// Safety check
		if currentOffset > int64(len(data)) {
			return nil, fmt.Errorf("entry %d extends beyond file", entryIdx)
		}
	}

	// Now read the target entry at currentOffset
	return r.readEntryFromFileData(data, currentOffset)
}
