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

// Removed ErrFileAwaitingData - no longer needed with immutable cloned indexes

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
	MaxMappedFiles  int   // Maximum number of files to keep mapped (default: 10)
	MaxMemoryBytes  int64 // Maximum memory to use for mapping (default: 1GB)
	CleanupInterval int   // How often to run cleanup in milliseconds (default: 5000)
}

// DefaultReaderConfig returns the default configuration for Reader
func DefaultReaderConfig() ReaderConfig {
	return ReaderConfig{
		MaxMappedFiles:  10,                     // Reasonable default
		MaxMemoryBytes:  2 * 1024 * 1024 * 1024, // 2GB default (fixed: was 2MB)
		CleanupInterval: 5000,                   // 5 second cleanup
	}
}

// ReaderConfigForStorage returns a reader configuration optimized for the given storage settings
func ReaderConfigForStorage(maxFileSize int64) ReaderConfig {
	// Start with defaults
	cfg := DefaultReaderConfig()

	// Validate maxFileSize
	if maxFileSize <= 0 {
		return cfg // Return defaults for invalid input
	}

	// Calculate optimal max mapped files based on memory and file size
	// Assume files are typically 80% full on average
	avgFileSize := (maxFileSize * 4) / 5
	if avgFileSize <= 0 {
		avgFileSize = maxFileSize // Prevent division issues
	}

	// How many average-sized files fit in our memory limit?
	filesInMemory := cfg.MaxMemoryBytes / avgFileSize

	// Set max mapped files to 2x what fits in memory (allows for variation in file sizes)
	// But keep it within reasonable bounds
	optimalMaxFiles := filesInMemory * 2
	if optimalMaxFiles < 10 {
		cfg.MaxMappedFiles = 10 // Minimum for good performance
	} else {
		cfg.MaxMappedFiles = int(optimalMaxFiles)
	}

	return cfg
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
	shardID              uint32
	index                *ShardIndex
	config               ReaderConfig
	fileInfos            []FileInfo          // All file metadata (not necessarily mapped)
	mappedFiles          map[int]*MappedFile // Currently mapped files
	mappingMu            sync.RWMutex        // Protects mappedFiles
	localMemory          int64               // Atomic counter for local memory usage
	recentCache          *recentFileCache
	decompressor         *zstd.Decoder
	bufferPool           *sync.Pool
	state                *CometState // Shared state for metrics (optional)
	lastKnownIndexUpdate int64       // Last known index update timestamp for cache invalidation
	client               *Client     // Reference to client for index refreshing (optional)
}

// NewReader creates a new bounded reader for a shard with smart file mapping
func NewReader(shardID uint32, index *ShardIndex, config ...ReaderConfig) (*Reader, error) {
	if index == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	// Use provided config or default
	var cfg ReaderConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultReaderConfig()
	}

	r := &Reader{
		shardID:     shardID,
		index:       index,
		config:      cfg,
		fileInfos:   make([]FileInfo, len(index.Files)),
		mappedFiles: make(map[int]*MappedFile),
		recentCache: newRecentFileCache(cfg.MaxMappedFiles / 2), // Half capacity for recent files
		bufferPool: &sync.Pool{
			New: func() any {
				return make([]byte, 0, 64*1024)
			},
		},
	}

	// Deep copy file infos to avoid data races
	copy(r.fileInfos, index.Files)

	// Create decompressor
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create decompressor: %w", err)
	}
	r.decompressor = dec

	// In smart mapping mode, just map the most recent file to get started
	if len(r.fileInfos) > 0 {
		lastIndex := len(r.fileInfos) - 1
		mapped, err := r.mapFile(r.fileInfos[lastIndex])
		if err == nil {
			r.mappedFiles[lastIndex] = mapped
			// Note: Can't update ReaderMappedFiles here as state isn't set yet
		}
	}

	return r, nil
}

// SetState sets the shared state for metrics tracking
func (r *Reader) SetState(state *CometState) {
	r.state = state

	// Update current metrics now that we have state
	if state != nil {
		r.mappingMu.RLock()
		mappedCount := len(r.mappedFiles)
		r.mappingMu.RUnlock()

		atomic.StoreUint64(&state.ReaderMappedFiles, uint64(mappedCount))
		atomic.StoreUint64(&state.ReaderCacheBytes, uint64(atomic.LoadInt64(&r.localMemory)))
		// If we already have mapped files, update the file maps counter
		if mappedCount > 0 {
			atomic.StoreUint64(&state.ReaderFileMaps, uint64(mappedCount))
		}

		// Initialize the last known index update timestamp
		atomic.StoreInt64(&r.lastKnownIndexUpdate, state.GetLastIndexUpdate())
	}
}

// SetClient sets the client reference for index refreshing
func (r *Reader) SetClient(client *Client) {
	r.client = client
}

// refreshFromLiveIndex refreshes the reader's file info from the live shard index
func (r *Reader) refreshFromLiveIndex() error {
	if r.client == nil {
		return fmt.Errorf("no client reference available for index refresh")
	}

	// Get the current shard
	shard := r.client.getShard(r.shardID)
	if shard == nil {
		return fmt.Errorf("shard %d not found", r.shardID)
	}

	// Get a copy of the current files under read lock
	shard.mu.RLock()
	currentFiles := make([]FileInfo, len(shard.index.Files))
	copy(currentFiles, shard.index.Files)
	shard.mu.RUnlock()

	// Update our file info with the fresh data
	return r.UpdateFiles(&currentFiles)
}

// mapFile maps a single file into memory
func (r *Reader) mapFile(info FileInfo) (*MappedFile, error) {
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
		// For empty files, we should check if this file is expected to have data
		// If it's in the index with entries > 0, this is likely a race condition
		// where the file was created but data hasn't been written yet
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

	// Handle empty files gracefully - this is a transient condition
	if len(data) == 0 {
		// Check if file has grown since mapping
		if mapped.file != nil {
			stat, err := mapped.file.Stat()
			if err == nil && stat.Size() > 0 {
				// File has grown - try remapping
				if err := r.checkAndRemapIfGrown(pos.FileIndex, mapped); err != nil {
					return nil, fmt.Errorf("failed to remap grown file: %w", err)
				}
				// Try again with the new mapping
				data = mapped.data.Load().([]byte)
			}
		}

		// If still empty, this is likely a new file awaiting its first flush
		if len(data) == 0 {
			return nil, fmt.Errorf("file %d is empty or not yet mapped", pos.FileIndex)
		}
	}

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

	// Check if we need to remap:
	// 1. File has grown
	// 2. Current mapping is empty but file now has data
	currentData := mapped.data.Load().([]byte)
	needsRemap := currentSize > mapped.lastSize || (len(currentData) == 0 && currentSize > 0)

	// If file has grown or was empty but now has data, we need to remap it
	if needsRemap {
		r.mappingMu.Lock()
		defer r.mappingMu.Unlock()

		// Double-check under lock
		currentData = mapped.data.Load().([]byte)
		needsRemap = currentSize > mapped.lastSize || (len(currentData) == 0 && currentSize > 0)
		if needsRemap {
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

	mapped, err := r.mapFile(r.fileInfos[fileIndex])
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
		// This can happen when a file has been created but not yet written to
		// It's a temporary condition that should resolve once the writer flushes
		return nil, fmt.Errorf("file is empty (likely awaiting flush)")
	}

	if byteOffset < 0 {
		return nil, fmt.Errorf("invalid byte offset: %d", byteOffset)
	}

	headerSize := int64(12)
	fileSize := int64(len(data))

	// Check for overflow and bounds
	if byteOffset >= fileSize || byteOffset > fileSize-headerSize {
		return nil, fmt.Errorf("offset %d beyond file size %d", byteOffset, fileSize)
	}

	// Read header
	header := data[byteOffset : byteOffset+headerSize]
	length := binary.LittleEndian.Uint32(header[0:4])

	// Check bounds
	dataStart := byteOffset + headerSize
	dataEnd := dataStart + int64(length)
	if dataEnd > int64(len(data)) {
		return nil, fmt.Errorf("entry data extends beyond file: dataEnd=%d, fileSize=%d", dataEnd, len(data))
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
		//lint:ignore SA6002 Buffer pool pattern - resetting slice length is intentional
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

	// Data is not compressed - return a copy to avoid segfaults
	// when the memory-mapped region is unmapped
	result := make([]byte, len(entryData))
	copy(result, entryData)
	return result, nil
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
			if mapped, err := r.mapFile((*newFiles)[lastIndex]); err == nil {
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
	// First try with cached file infos
	for fileIndex, fileInfo := range r.fileInfos {
		// Check if this entry falls within this file's range
		if fileInfo.StartEntry <= entryNumber && entryNumber < fileInfo.StartEntry+fileInfo.Entries {
			// Found the file - now we need to find the exact position within the file
			relativeEntryNum := entryNumber - fileInfo.StartEntry

			if relativeEntryNum == 0 {
				// First entry in file - starts at offset 0
				pos := EntryPosition{FileIndex: fileIndex, ByteOffset: 0}
				return r.ReadEntryAtPosition(pos)
			}

			// For non-first entries, we need to scan from the beginning
			// This should rarely happen if the binary index is properly maintained
			return r.readEntryByScanning(fileIndex, relativeEntryNum)
		}
	}

	// Entry not found in cached files - check if index has been updated using LastIndexUpdate
	if r.state != nil {
		currentIndexUpdate := r.state.GetLastIndexUpdate()
		lastKnown := atomic.LoadInt64(&r.lastKnownIndexUpdate)

		if IsDebug() {
			fmt.Printf("[DEBUG] Reader.ReadEntryByNumber: entry=%d, currentIndexUpdate=%d, lastKnown=%d, stale=%v\n",
				entryNumber, currentIndexUpdate, lastKnown, currentIndexUpdate > lastKnown)
		}

		if currentIndexUpdate > lastKnown {
			// Reader index is stale - try to refresh from the live index
			if IsDebug() {
				fmt.Printf("[DEBUG] Reader index is stale: entry=%d, currentUpdate=%d > lastKnown=%d, attempting refresh\n",
					entryNumber, currentIndexUpdate, lastKnown)
			}

			if err := r.refreshFromLiveIndex(); err != nil {
				if IsDebug() {
					fmt.Printf("[DEBUG] Failed to refresh reader index: %v\n", err)
				}
				return nil, fmt.Errorf("reader index is stale and refresh failed: %w", err)
			}

			// Update our known index timestamp
			atomic.StoreInt64(&r.lastKnownIndexUpdate, currentIndexUpdate)

			// Try reading again with refreshed index
			return r.ReadEntryByNumber(entryNumber)
		}
	}

	// Entry not found in any file - this could be because:
	// 1. The entry doesn't exist yet (reading ahead of writes)
	// 2. The entry was in a file that got deleted by retention

	// If we're looking for entry 0 and have files, but none contain it,
	// it might be in a newly created file that hasn't been written to yet
	if entryNumber == 0 && len(r.fileInfos) > 0 {
		// Try the first file even if it claims not to have entry 0
		firstFile := r.fileInfos[0]
		if firstFile.Entries == 0 && firstFile.StartEntry > 0 {
			// This is likely a new file that was just created
			// Try reading from it anyway
			pos := EntryPosition{FileIndex: 0, ByteOffset: 0}
			data, err := r.ReadEntryAtPosition(pos)
			if err == nil {
				return data, nil
			}
			// Log read errors for debugging
			if IsDebug() {
				fmt.Printf("[DEBUG] Error reading entry 0 from file 0: %v\n", err)
			}
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
			// The index claims there are more entries than we have data for
			return nil, fmt.Errorf("file %d: insufficient data for entry %d (file too short)", fileIndex, relativeEntryNum)
		}

		// Extract length from header (first 4 bytes)
		length := binary.LittleEndian.Uint32(data[currentOffset : currentOffset+4])

		// Sanity check the length
		if length > 10*1024*1024 { // 10MB max entry size
			return nil, fmt.Errorf("file %d: invalid entry length %d at offset %d", fileIndex, length, currentOffset)
		}

		// Skip to next entry: header (12 bytes) + data length
		entrySize := int64(12 + length)
		nextOffset := currentOffset + entrySize

		// Safety check
		if nextOffset > int64(len(data)) {
			return nil, fmt.Errorf("file %d: entry at offset %d extends beyond file (need %d, have %d)", fileIndex, currentOffset, nextOffset, len(data))
		}

		currentOffset = nextOffset
	}

	// Now read the target entry at currentOffset
	return r.readEntryFromFileData(data, currentOffset)
}
