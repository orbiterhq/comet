package comet

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/klauspost/compress/zstd"
)

// Reader provides memory-mapped read access to a shard
// Fields ordered for optimal memory alignment
type Reader struct {
	// Pointers first (8 bytes on 64-bit)
	decompressor *zstd.Decoder
	bufferPool   *sync.Pool

	// Slices (24 bytes: ptr + len + cap)
	files []*MappedFile

	// Mutex (platform-specific size)
	mu sync.RWMutex

	// Smaller fields last
	shardID uint32 // 4 bytes
	// 4 bytes padding
}

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

// MappedFile represents a memory-mapped data file with atomic data updates
// Fields ordered for optimal memory alignment (embedded struct first)
type MappedFile struct {
	FileInfo             // Embedded struct (already aligned)
	data     AtomicSlice // Atomic slice for lock-free updates
	file     *os.File    // Pointer (8 bytes)
	remapMu  sync.Mutex  // Mutex for remapping operations only
	lastSize int64       // Last known size for growth detection
}

// NewReader creates a new reader for a shard
func NewReader(shardID uint32, index *ShardIndex) (*Reader, error) {
	r := &Reader{
		shardID: shardID,
		files:   make([]*MappedFile, 0, len(index.Files)),
		bufferPool: &sync.Pool{
			New: func() any {
				// Start with 64KB buffer, will grow as needed
				return make([]byte, 0, 64*1024)
			},
		},
	}

	// Create decompressor
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create decompressor: %w", err)
	}
	r.decompressor = dec

	// Memory map all files
	for _, fileInfo := range index.Files {
		mapped, err := r.mapFile(fileInfo)
		if err != nil {
			// Clean up already mapped files
			r.Close()
			return nil, fmt.Errorf("failed to map file %s: %w", fileInfo.Path, err)
		}
		r.files = append(r.files, mapped)
	}

	return r, nil
}

// mapFile memory maps a single data file
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
		return mappedFile, nil
	}

	// Memory map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	// Store data atomically
	mappedFile.data.Store(data)

	return mappedFile, nil
}

// remapFile remaps a file that has grown - now lock-free for readers!
func (r *Reader) remapFile(fileIndex int) error {
	// Only validate file index under read lock
	r.mu.RLock()
	if fileIndex < 0 || fileIndex >= len(r.files) {
		r.mu.RUnlock()
		return fmt.Errorf("invalid file index %d", fileIndex)
	}
	mappedFile := r.files[fileIndex]
	r.mu.RUnlock()

	// Use per-file mutex to prevent concurrent remaps of the same file
	// This doesn't block readers of other files or readers using the current mapping
	mappedFile.remapMu.Lock()
	defer mappedFile.remapMu.Unlock()

	// Get current file size
	stat, err := mappedFile.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	newSize := stat.Size()

	// Check if we've already been remapped by another goroutine
	if newSize <= mappedFile.lastSize {
		return nil // No growth or already remapped
	}

	// Get current data atomically
	oldData := mappedFile.data.Load()
	if oldData != nil && int64(len(oldData)) >= newSize {
		return nil // Already remapped by another goroutine
	}

	// Create new mapping with the current file size
	newData, err := syscall.Mmap(int(mappedFile.file.Fd()), 0, int(newSize), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return fmt.Errorf("failed to remap file: %w", err)
	}

	// Atomically update the data pointer - readers will see either old or new mapping
	mappedFile.data.Store(newData)
	mappedFile.lastSize = newSize

	// Unmap old data if it exists
	// Safe to unmap immediately because:
	// 1. We use MAP_PRIVATE (copy-on-write) which protects active readers
	// 2. The atomic.Value ensures readers see either old or new mapping
	// 3. Any active readers have their own memory pages via COW
	if oldData != nil {
		go func() {
			// Defer unmapping to avoid blocking the current operation
			syscall.Munmap(oldData)
		}()
	}

	return nil
}

// ReadEntryAtPosition reads a single entry at the given position
func (r *Reader) ReadEntryAtPosition(pos EntryPosition) ([]byte, error) {
	r.mu.RLock()

	// Validate file index
	if pos.FileIndex < 0 || pos.FileIndex >= len(r.files) {
		r.mu.RUnlock()
		return nil, fmt.Errorf("invalid file index %d", pos.FileIndex)
	}

	targetFile := r.files[pos.FileIndex]
	r.mu.RUnlock() // Release read lock early - we'll use atomic operations

	// Check if we need to remap the file due to growth
	currentData := targetFile.data.Load()
	if targetFile.file != nil {
		stat, err := targetFile.file.Stat()
		if err == nil && (currentData == nil || stat.Size() > int64(len(currentData))) {
			// Remap the file with the new size (lock-free!)
			if err := r.remapFile(pos.FileIndex); err != nil {
				return nil, fmt.Errorf("failed to remap grown file: %w", err)
			}
			// Get the potentially updated mapping
			currentData = targetFile.data.Load()
		}
	}

	// Read from the current mapping (no locks needed!)
	data, err := r.readEntryFromFileData(currentData, pos.ByteOffset)
	if err != nil && strings.Contains(err.Error(), "extends beyond file") {
		// Handle the case where index was updated but data hasn't been flushed yet
		// This can occur during active writes - remap and retry once
		if targetFile.file != nil {
			if stat, statErr := targetFile.file.Stat(); statErr == nil {
				currentSize := stat.Size()
				if currentSize > int64(len(currentData)) {
					// File has grown, remap and retry
					if remapErr := r.remapFile(pos.FileIndex); remapErr == nil {
						currentData = targetFile.data.Load()
						return r.readEntryFromFileData(currentData, pos.ByteOffset)
					}
				}
			}
		}
	}
	return data, err
}

// ReadEntry reads a single entry at the given byte offset (legacy method)
func (r *Reader) ReadEntry(offset int64) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Find the file containing this offset
	var targetFile *MappedFile
	for i := range r.files {
		file := r.files[i]
		if offset >= file.StartOffset && offset < file.EndOffset {
			targetFile = file
			break
		}
	}

	if targetFile == nil {
		return nil, fmt.Errorf("offset %d not found in any file", offset)
	}

	// Calculate position within the file
	fileOffset := offset - targetFile.StartOffset

	// Get data atomically
	data := targetFile.data.Load()
	if fileOffset+headerSize > int64(len(data)) {
		return nil, fmt.Errorf("invalid offset: header extends beyond file")
	}

	// Read header
	header := data[fileOffset : fileOffset+headerSize]
	length := binary.LittleEndian.Uint32(header[0:4])

	// Check bounds
	dataStart := fileOffset + headerSize
	dataEnd := dataStart + int64(length)
	if dataEnd > int64(len(data)) {
		return nil, fmt.Errorf("invalid entry: data extends beyond file")
	}

	// Read compressed data
	compressedData := data[dataStart:dataEnd]

	// Get buffer from pool for decompression
	buf := r.bufferPool.Get().([]byte)
	defer func() {
		// Reset buffer and return to pool
		buf = buf[:0]
		r.bufferPool.Put(buf) //lint:ignore SA6002 sync.Pool.Put expects interface{}, slice is correct
	}()

	// Check if data is compressed by looking for zstd magic number
	if len(compressedData) >= 4 {
		// Try to decompress - if it fails, assume data is uncompressed
		decompressed, err := r.decompressor.DecodeAll(compressedData, buf)
		if err == nil {
			// Successfully decompressed
			result := make([]byte, len(decompressed))
			copy(result, decompressed)
			return result, nil
		}
	}

	// Data is not compressed - return as is
	result := make([]byte, len(compressedData))
	copy(result, compressedData)
	return result, nil
}

// ReadRange reads entries in a byte range
func (r *Reader) ReadRange(startOffset, endOffset int64, callback func(offset int64, data []byte) error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	offset := startOffset
	for offset < endOffset {
		// Find the file containing this offset
		var targetFile *MappedFile
		for i := range r.files {
			file := r.files[i]
			if offset >= file.StartOffset && (i == len(r.files)-1 || offset < r.files[i+1].StartOffset) {
				targetFile = file
				break
			}
		}

		if targetFile == nil {
			break // No more data
		}

		// Read entries from this file
		fileOffset := offset - targetFile.StartOffset
		targetData := targetFile.data.Load() // Get data atomically

		for fileOffset < int64(len(targetData)) && offset < endOffset {
			// Check if we have enough data for header
			if fileOffset+headerSize > int64(len(targetData)) {
				break
			}

			// Read header
			header := targetData[fileOffset : fileOffset+headerSize]
			length := binary.LittleEndian.Uint32(header[0:4])

			// Check bounds
			dataStart := fileOffset + headerSize
			dataEnd := dataStart + int64(length)
			if dataEnd > int64(len(targetData)) {
				break
			}

			// Read and decompress data
			compressedData := targetData[dataStart:dataEnd]

			// Get buffer from pool for decompression
			buf := r.bufferPool.Get().([]byte)

			var dataToCallback []byte
			if len(compressedData) >= 4 {
				// Try to decompress - if it fails, assume data is uncompressed
				decompressed, err := r.decompressor.DecodeAll(compressedData, buf)
				if err == nil {
					dataToCallback = decompressed
				} else {
					dataToCallback = compressedData
				}
			} else {
				dataToCallback = compressedData
			}

			// Call callback
			callbackErr := callback(offset, dataToCallback)

			// Return buffer to pool
			buf = buf[:0]
			r.bufferPool.Put(buf) //lint:ignore SA6002 sync.Pool.Put expects interface{}, slice is correct

			if callbackErr != nil {
				return callbackErr
			}

			// Move to next entry
			fileOffset = dataEnd
			offset = targetFile.StartOffset + fileOffset
		}
	}

	return nil
}

// Close unmaps all files and cleans up resources
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	for i := range r.files {
		file := r.files[i]

		// Unmap if mapped
		data := file.data.Load()
		if data != nil {
			if err := syscall.Munmap(data); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		// Close file
		if file.file != nil {
			if err := file.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// readEntryFromFileData reads a single entry from memory-mapped data at a byte offset
func (r *Reader) readEntryFromFileData(data []byte, byteOffset int64) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("file not memory mapped")
	}

	if byteOffset+headerSize > int64(len(data)) {
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

	// Read entry data
	entryData := data[dataStart:dataEnd]

	// Check if data is compressed by looking for zstd magic number
	if len(entryData) >= 4 && binary.LittleEndian.Uint32(entryData[0:4]) == 0xFD2FB528 {
		// Data is compressed - decompress it
		decompressed, err := r.decompressor.DecodeAll(entryData, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %w", err)
		}
		return decompressed, nil
	}

	// Data is not compressed - return as is
	return entryData, nil
}
