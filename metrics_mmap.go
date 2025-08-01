package comet

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// mmapMetricsState represents the memory-mapped metrics structure
// CRITICAL: This must be a fixed-size struct with only primitive types
// All fields must be 8-byte aligned for atomic operations
type mmapMetricsState struct {
	// Write metrics
	TotalEntries     uint64
	TotalBytes       uint64
	TotalCompressed  uint64
	WriteLatencyNano uint64
	MinWriteLatency  uint64
	MaxWriteLatency  uint64

	// Compression metrics
	CompressionRatio    uint64
	CompressedEntries   uint64
	SkippedCompression  uint64
	CompressionWaitNano uint64

	// File management
	FilesCreated    uint64
	FilesDeleted    uint64
	FileRotations   uint64
	CheckpointCount uint64
	LastCheckpoint  uint64

	// Consumer metrics
	ActiveReaders uint64
	ConsumerLag   uint64

	// Error tracking
	ErrorCount         uint64
	LastErrorNano      uint64
	IndexPersistErrors uint64

	// Reserved for future expansion
	_reserved [8]uint64
}

// mmapMetrics implements MetricsProvider using memory-mapped file
type mmapMetrics struct {
	path  string
	file  *os.File
	data  []byte
	state *mmapMetricsState
	isNew bool
}

// newMmapMetrics creates a memory-mapped metrics provider
func newMmapMetrics(baseDir string) (MetricsProvider, error) {
	metricsPath := filepath.Join(baseDir, "metrics.state")

	// Check if file exists
	isNew := false
	if _, err := os.Stat(metricsPath); os.IsNotExist(err) {
		isNew = true
	}

	// Open or create file
	file, err := os.OpenFile(metricsPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open metrics file: %w", err)
	}

	// Ensure file is correct size
	const stateSize = unsafe.Sizeof(mmapMetricsState{})
	if err := file.Truncate(int64(stateSize)); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to resize metrics file: %w", err)
	}

	// Memory map the file
	data, err := syscall.Mmap(
		int(file.Fd()),
		0,
		int(stateSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap metrics file: %w", err)
	}

	// Cast to our state struct
	state := (*mmapMetricsState)(unsafe.Pointer(&data[0]))

	// Initialize if new
	if isNew {
		state.MinWriteLatency = ^uint64(0) // Max uint64
	}

	return &mmapMetrics{
		path:  metricsPath,
		file:  file,
		data:  data,
		state: state,
		isNew: isNew,
	}, nil
}

// Close unmaps and closes the metrics file
func (m *mmapMetrics) Close() error {
	if err := syscall.Munmap(m.data); err != nil {
		return fmt.Errorf("failed to unmap metrics: %w", err)
	}
	return m.file.Close()
}

// Implement all MetricsProvider methods using atomic operations on mmap

func (m *mmapMetrics) IncrementEntries(count uint64) {
	atomic.AddUint64(&m.state.TotalEntries, count)
}

func (m *mmapMetrics) AddBytes(bytes uint64) {
	atomic.AddUint64(&m.state.TotalBytes, bytes)
}

func (m *mmapMetrics) AddCompressedBytes(bytes uint64) {
	atomic.AddUint64(&m.state.TotalCompressed, bytes)
}

func (m *mmapMetrics) RecordWriteLatency(nanos uint64) {
	atomic.StoreUint64(&m.state.WriteLatencyNano, nanos)
}

func (m *mmapMetrics) RecordMinWriteLatency(nanos uint64) {
	for {
		current := atomic.LoadUint64(&m.state.MinWriteLatency)
		if current != 0 && current <= nanos {
			break
		}
		if atomic.CompareAndSwapUint64(&m.state.MinWriteLatency, current, nanos) {
			break
		}
	}
}

func (m *mmapMetrics) RecordMaxWriteLatency(nanos uint64) {
	for {
		current := atomic.LoadUint64(&m.state.MaxWriteLatency)
		if current >= nanos {
			break
		}
		if atomic.CompareAndSwapUint64(&m.state.MaxWriteLatency, current, nanos) {
			break
		}
	}
}

func (m *mmapMetrics) SetCompressionRatio(ratio uint64) {
	atomic.StoreUint64(&m.state.CompressionRatio, ratio)
}

func (m *mmapMetrics) IncrementCompressedEntries(count uint64) {
	atomic.AddUint64(&m.state.CompressedEntries, count)
}

func (m *mmapMetrics) IncrementSkippedCompression(count uint64) {
	atomic.AddUint64(&m.state.SkippedCompression, count)
}

func (m *mmapMetrics) AddCompressionWait(nanos uint64) {
	atomic.AddUint64(&m.state.CompressionWaitNano, nanos)
}

func (m *mmapMetrics) IncrementFilesCreated(count uint64) {
	atomic.AddUint64(&m.state.FilesCreated, count)
}

func (m *mmapMetrics) IncrementFilesDeleted(count uint64) {
	atomic.AddUint64(&m.state.FilesDeleted, count)
}

func (m *mmapMetrics) IncrementFileRotations(count uint64) {
	atomic.AddUint64(&m.state.FileRotations, count)
}

func (m *mmapMetrics) IncrementCheckpoints(count uint64) {
	atomic.AddUint64(&m.state.CheckpointCount, count)
}

func (m *mmapMetrics) SetLastCheckpoint(nanos uint64) {
	atomic.StoreUint64(&m.state.LastCheckpoint, nanos)
}

func (m *mmapMetrics) SetActiveReaders(count uint64) {
	atomic.StoreUint64(&m.state.ActiveReaders, count)
}

func (m *mmapMetrics) SetMaxConsumerLag(lag uint64) {
	atomic.StoreUint64(&m.state.ConsumerLag, lag)
}

func (m *mmapMetrics) IncrementErrors(count uint64) {
	atomic.AddUint64(&m.state.ErrorCount, count)
}

func (m *mmapMetrics) SetLastError(nanos uint64) {
	atomic.StoreUint64(&m.state.LastErrorNano, nanos)
}

func (m *mmapMetrics) IncrementIndexPersistErrors(count uint64) {
	atomic.AddUint64(&m.state.IndexPersistErrors, count)
}

func (m *mmapMetrics) GetStats() MetricsSnapshot {
	// Read all values atomically
	return MetricsSnapshot{
		TotalEntries:        atomic.LoadUint64(&m.state.TotalEntries),
		TotalBytes:          atomic.LoadUint64(&m.state.TotalBytes),
		TotalCompressed:     atomic.LoadUint64(&m.state.TotalCompressed),
		WriteLatencyNano:    atomic.LoadUint64(&m.state.WriteLatencyNano),
		MinWriteLatency:     atomic.LoadUint64(&m.state.MinWriteLatency),
		MaxWriteLatency:     atomic.LoadUint64(&m.state.MaxWriteLatency),
		CompressionRatio:    atomic.LoadUint64(&m.state.CompressionRatio),
		CompressedEntries:   atomic.LoadUint64(&m.state.CompressedEntries),
		SkippedCompression:  atomic.LoadUint64(&m.state.SkippedCompression),
		CompressionWaitNano: atomic.LoadUint64(&m.state.CompressionWaitNano),
		FilesCreated:        atomic.LoadUint64(&m.state.FilesCreated),
		FilesDeleted:        atomic.LoadUint64(&m.state.FilesDeleted),
		FileRotations:       atomic.LoadUint64(&m.state.FileRotations),
		CheckpointCount:     atomic.LoadUint64(&m.state.CheckpointCount),
		LastCheckpoint:      atomic.LoadUint64(&m.state.LastCheckpoint),
		ActiveReaders:       atomic.LoadUint64(&m.state.ActiveReaders),
		ConsumerLag:         atomic.LoadUint64(&m.state.ConsumerLag),
		ErrorCount:          atomic.LoadUint64(&m.state.ErrorCount),
		LastErrorNano:       atomic.LoadUint64(&m.state.LastErrorNano),
		IndexPersistErrors:  atomic.LoadUint64(&m.state.IndexPersistErrors),
	}
}
