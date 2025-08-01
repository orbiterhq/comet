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

// MmapCoordinationState represents shared state for ultra-fast multi-process coordination
type MmapCoordinationState struct {
	// Core coordination fields
	ActiveFileIndex atomic.Int64 // Current file being written to
	WriteOffset     atomic.Int64 // Current write position in active file
	FileSize        atomic.Int64 // Current size of active file

	// Performance tracking
	LastWriteNanos atomic.Int64 // Timestamp of last write
	TotalWrites    atomic.Int64 // Total number of writes

	// Reserved for future use
	_reserved [176]byte // Adjusted for atomic types (each atomic.Int64 is 8 bytes)
}

// CoordinationState returns the coordination state for external access
func (w *MmapWriter) CoordinationState() *MmapCoordinationState {
	return w.coordinationState
}

// MmapWriter implements ultra-fast memory-mapped writes for multi-process mode
type MmapWriter struct {
	mu sync.Mutex

	// Coordination state
	coordinationPath  string
	coordinationData  []byte
	coordinationState *MmapCoordinationState

	// Current mapped region
	dataFile     *os.File
	dataPath     string
	mappedData   []byte
	mappedOffset int64 // Where this mapping starts in the file
	mappedSize   int64 // Size of current mapping

	// Configuration
	shardDir        string
	initialSize     int64 // Initial file size (default: 128MB)
	growthIncrement int64 // How much to grow (default: 128MB)
	mappingWindow   int64 // Size of active mapping (default: 32MB)
	maxFileSize     int64 // Max file size before rotation (default: 1GB)

	// References for index updates
	index            *ShardIndex
	metrics          *ClientMetrics
	rotationLockFile *os.File // File lock for rotation coordination

	// Metrics
	remapCount    int64
	rotationCount int64
}

// NewMmapWriter creates a new memory-mapped writer for a shard
func NewMmapWriter(shardDir string, maxFileSize int64, index *ShardIndex, metrics *ClientMetrics, rotationLockFile *os.File) (*MmapWriter, error) {
	w := &MmapWriter{
		shardDir:         shardDir,
		coordinationPath: shardDir + "/coordination.state",
		initialSize:      4 * 1024,        // 4KB initial
		growthIncrement:  1 * 1024 * 1024, // 1MB growth
		mappingWindow:    1 * 1024 * 1024, // 1MB active window
		maxFileSize:      maxFileSize,
		index:            index,
		metrics:          metrics,
		rotationLockFile: rotationLockFile,
	}

	// Initialize coordination state
	if err := w.initCoordinationState(); err != nil {
		return nil, fmt.Errorf("failed to init coordination state: %w", err)
	}

	// Open or create current data file
	if err := w.openCurrentFile(); err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	return w, nil
}

// initCoordinationState initializes the memory-mapped coordination state
func (w *MmapWriter) initCoordinationState() error {
	// Create or open state file
	file, err := os.OpenFile(w.coordinationPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Ensure file is correct size
	const stateSize = 256
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

	w.coordinationData = data
	w.coordinationState = (*MmapCoordinationState)(unsafe.Pointer(&data[0]))

	return nil
}

// openCurrentFile opens the current data file and maps the active region
func (w *MmapWriter) openCurrentFile() error {
	// Get current file index
	fileIndex := w.coordinationState.ActiveFileIndex.Load()
	if fileIndex == 0 {
		fileIndex = 1
		w.coordinationState.ActiveFileIndex.Store(1)
	}

	// Construct file path using standard naming convention
	w.dataPath = fmt.Sprintf("%s/log-%016d.comet", w.shardDir, fileIndex)

	// Check if this is the current file in the index
	createNew := w.index.CurrentFile != w.dataPath

	// Open or create file
	file, err := os.OpenFile(w.dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	fileSize := stat.Size()

	// Always ensure we have at least a minimal size to map
	minSize := int64(4096) // 4KB minimum
	if fileSize < minSize {
		if err := file.Truncate(minSize); err != nil {
			file.Close()
			return err
		}
		fileSize = minSize
	}

	// Store the actual file size
	w.coordinationState.FileSize.Store(fileSize)

	// Update index if this is a new file or we're initializing
	if createNew || w.index.CurrentFile == "" {
		w.index.CurrentFile = w.dataPath

		// Check if we need to add this file to the index
		found := false
		for _, f := range w.index.Files {
			if f.Path == w.dataPath {
				found = true
				break
			}
		}

		if !found {
			w.index.Files = append(w.index.Files, FileInfo{
				Path:        w.dataPath,
				StartOffset: 0,
				StartEntry:  w.index.CurrentEntryNumber,
				StartTime:   time.Now(),
				Entries:     0,
			})
		}
	}

	w.dataFile = file

	// Map the active window (last portion of file)
	return w.remapActiveWindow()
}

// remapActiveWindow maps or remaps the active portion of the file
func (w *MmapWriter) remapActiveWindow() error {
	// Get current file size
	fileSize := w.coordinationState.FileSize.Load()

	// Calculate mapping window
	mappingStart := int64(0) // Always start from beginning for simplicity
	mappingSize := fileSize  // Map the entire file

	// Ensure we have something to map
	if mappingSize <= 0 {
		return fmt.Errorf("file size is 0, cannot map")
	}

	// Unmap previous mapping if exists
	if w.mappedData != nil {
		if err := syscall.Munmap(w.mappedData); err != nil {
			return fmt.Errorf("failed to unmap: %w", err)
		}
	}

	// Map new window
	data, err := syscall.Mmap(int(w.dataFile.Fd()), mappingStart, int(mappingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap window: %w", err)
	}

	w.mappedData = data
	w.mappedOffset = mappingStart
	w.mappedSize = mappingSize
	atomic.AddInt64(&w.remapCount, 1)

	return nil
}

// Write appends entries using memory-mapped I/O
func (w *MmapWriter) Write(entries [][]byte, entryNumbers []int64) error {
	if len(entries) == 0 {
		return nil
	}

	// Calculate total size needed
	totalSize := int64(0)
	for _, entry := range entries {
		totalSize += 12 + int64(len(entry)) // header + data
	}

	// Atomically allocate write space
	writeOffset := w.coordinationState.WriteOffset.Add(totalSize) - totalSize

	// Check if we need rotation - return special error to let shard handle it
	if writeOffset+totalSize > w.maxFileSize {
		// Roll back the allocation
		w.coordinationState.WriteOffset.Add(-totalSize)

		// Return special error to indicate rotation is needed
		// The shard will handle rotation and index updates properly
		return fmt.Errorf("rotation needed: current file would exceed size limit")
	}

	// Write entries - hold lock for entire write operation including growth check
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if we need to grow the file (inside lock to prevent races)
	currentFileSize := w.coordinationState.FileSize.Load()
	if writeOffset+totalSize > currentFileSize {
		if err := w.growFile(writeOffset + totalSize); err != nil {
			// Roll back the allocation on error
			w.coordinationState.WriteOffset.Add(-totalSize)
			return err
		}
	}

	// Check if we have a valid file
	if w.dataFile == nil {
		return fmt.Errorf("data file is nil")
	}

	// Since we're mapping the entire file, just check if we have enough space
	if writeOffset+totalSize > w.mappedSize {
		// Need to remap after growing
		if err := w.remapActiveWindow(); err != nil {
			return fmt.Errorf("failed to remap for write: %w", err)
		}
	}

	// Verify we have enough mapped space after potential remap
	if writeOffset+totalSize > w.mappedSize {
		return fmt.Errorf("insufficient mapped space: need %d bytes at offset %d, but only %d bytes mapped",
			totalSize, writeOffset, w.mappedSize)
	}

	// Write directly to mapped memory
	offset := writeOffset
	now := time.Now().UnixNano()
	for i, entry := range entries {
		// Since we map from beginning, offset is the position
		pos := offset

		// Double-check bounds before writing
		if pos+12+int64(len(entry)) > int64(len(w.mappedData)) {
			return fmt.Errorf("write would exceed mapped region: pos=%d, entry=%d bytes, mapped=%d bytes",
				pos, len(entry)+12, len(w.mappedData))
		}

		// Write header (12 bytes)
		binary.LittleEndian.PutUint32(w.mappedData[pos:pos+4], uint32(len(entry)))
		binary.LittleEndian.PutUint64(w.mappedData[pos+4:pos+12], uint64(now))

		// Write data
		copy(w.mappedData[pos+12:pos+12+int64(len(entry))], entry)

		offset += 12 + int64(len(entry))
		_ = entryNumbers[i] // Entry numbers already allocated by caller
	}

	// CRITICAL: Ensure file size reflects what we wrote
	// This is what makes the data visible to readers
	finalOffset := writeOffset + totalSize
	if stat, err := w.dataFile.Stat(); err == nil {
		if stat.Size() < finalOffset {
			// File is smaller than what we wrote - extend it
			if err := w.dataFile.Truncate(finalOffset); err != nil {
				return fmt.Errorf("failed to extend file to written size: %w", err)
			}
		}
	}

	// Update coordination state
	w.coordinationState.LastWriteNanos.Store(now)
	w.coordinationState.TotalWrites.Add(int64(len(entries)))

	// Update client metrics if available
	if w.metrics != nil {
		// Track total bytes written
		totalBytes := uint64(0)
		for _, entry := range entries {
			totalBytes += uint64(len(entry))
		}

		w.metrics.TotalEntries.Add(uint64(len(entries)))
		w.metrics.TotalBytes.Add(totalBytes)

		// Note: Compression is already handled by the caller, so we don't track it here
	}

	// Note: Index updates are handled by the caller (shard) which holds the appropriate locks
	// We only update the coordination state here

	return nil
}

// growFile grows the file to accommodate more data
func (w *MmapWriter) growFile(minSize int64) error {
	// Calculate new size (round up to growth increment)
	newSize := ((minSize + w.growthIncrement - 1) / w.growthIncrement) * w.growthIncrement

	// Grow the file (already holding lock)
	if w.dataFile == nil {
		return fmt.Errorf("data file is nil")
	}

	if err := w.dataFile.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to grow file: %w", err)
	}

	// Update coordination state
	w.coordinationState.FileSize.Store(newSize)

	// Remap if needed
	if w.mappedOffset+w.mappedSize < minSize {
		return w.remapActiveWindow()
	}

	return nil
}

// rotateFile handles file rotation when the current file is full
func (w *MmapWriter) rotateFile() error {
	// Use proper file locking for multi-process coordination
	if w.rotationLockFile != nil {
		// Use non-blocking try-lock to avoid hanging if another process is rotating
		if err := syscall.Flock(int(w.rotationLockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			// Another process is rotating - just return, they'll handle it
			return nil
		}
		defer syscall.Flock(int(w.rotationLockFile.Fd()), syscall.LOCK_UN)
	}
	// Note: For single-process mode (rotationLockFile == nil), rotation is already
	// protected by the shard mutex in the caller, so no additional coordination needed.

	// Close current file
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.mappedData != nil {
		syscall.Munmap(w.mappedData)
		w.mappedData = nil
	}

	if w.dataFile != nil {
		w.dataFile.Close()
		w.dataFile = nil
	}

	// Increment file index
	newIndex := w.coordinationState.ActiveFileIndex.Add(1)

	// Reset write offset
	w.coordinationState.WriteOffset.Store(0)
	w.coordinationState.FileSize.Store(0)

	// Open new file using standard naming convention
	w.dataPath = fmt.Sprintf("%s/log-%016d.comet", w.shardDir, newIndex)
	file, err := os.OpenFile(w.dataPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			// Another process created it, just open it
			file, err = os.OpenFile(w.dataPath, os.O_RDWR, 0644)
			if err != nil {
				return fmt.Errorf("failed to open existing file: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create new file: %w", err)
		}
	}

	// Initial size for new file - start small
	initialSize := int64(4096) // 4KB
	if err := file.Truncate(initialSize); err != nil {
		file.Close()
		return err
	}

	w.dataFile = file
	w.coordinationState.FileSize.Store(initialSize)
	atomic.AddInt64(&w.rotationCount, 1)

	// Update client metrics
	if w.metrics != nil {
		w.metrics.FileRotations.Add(1)
		w.metrics.TotalFiles.Add(1)
	}

	// Note: Index updates are handled by the caller (shard) which holds the appropriate locks
	// We don't directly modify the index here to avoid race conditions

	// Map the new file
	return w.remapActiveWindow()
}

// Sync ensures data is persisted to disk
func (w *MmapWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// On macOS, use file sync instead of msync
	if w.dataFile != nil {
		return w.dataFile.Sync()
	}

	return nil
}

// Close cleans up resources
func (w *MmapWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Unmap data
	if w.mappedData != nil {
		syscall.Munmap(w.mappedData)
		w.mappedData = nil
	}

	// Unmap coordination state
	if w.coordinationData != nil {
		syscall.Munmap(w.coordinationData)
		w.coordinationData = nil
	}

	// Close file
	if w.dataFile != nil {
		w.dataFile.Close()
		w.dataFile = nil
	}

	return nil
}
