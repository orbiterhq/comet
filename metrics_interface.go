package comet

import (
	"sync/atomic"
)

// MetricsProvider defines the interface for metrics tracking
// This allows us to swap implementations based on single vs multi-process mode
type MetricsProvider interface {
	// Write metrics
	IncrementEntries(count uint64)
	AddBytes(bytes uint64)
	AddCompressedBytes(bytes uint64)
	RecordWriteLatency(nanos uint64)
	RecordMinWriteLatency(nanos uint64)
	RecordMaxWriteLatency(nanos uint64)

	// Compression metrics
	SetCompressionRatio(ratio uint64)
	IncrementCompressedEntries(count uint64)
	IncrementSkippedCompression(count uint64)
	AddCompressionWait(nanos uint64)

	// File management metrics
	IncrementFilesCreated(count uint64)
	IncrementFilesDeleted(count uint64)
	IncrementFileRotations(count uint64)
	IncrementCheckpoints(count uint64)
	SetLastCheckpoint(nanos uint64)

	// Consumer metrics
	SetActiveReaders(count uint64)
	SetMaxConsumerLag(lag uint64)

	// Error tracking
	IncrementErrors(count uint64)
	SetLastError(nanos uint64)
	IncrementIndexPersistErrors(count uint64)

	// Get current values
	GetStats() MetricsSnapshot
}

// MetricsSnapshot represents a point-in-time view of metrics
type MetricsSnapshot struct {
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

	// File management - NEW: separate created/deleted counters
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
}

// atomicMetrics implements MetricsProvider using atomic operations
// This is the current implementation, good for single-process mode
type atomicMetrics struct {
	// Write metrics
	totalEntries     atomic.Uint64
	totalBytes       atomic.Uint64
	totalCompressed  atomic.Uint64
	writeLatencyNano atomic.Uint64
	minWriteLatency  atomic.Uint64
	maxWriteLatency  atomic.Uint64

	// Compression metrics
	compressionRatio    atomic.Uint64
	compressedEntries   atomic.Uint64
	skippedCompression  atomic.Uint64
	compressionWaitNano atomic.Uint64

	// File management
	filesCreated    atomic.Uint64
	filesDeleted    atomic.Uint64
	fileRotations   atomic.Uint64
	checkpointCount atomic.Uint64
	lastCheckpoint  atomic.Uint64

	// Consumer metrics
	activeReaders atomic.Uint64
	consumerLag   atomic.Uint64

	// Error tracking
	errorCount         atomic.Uint64
	lastErrorNano      atomic.Uint64
	indexPersistErrors atomic.Uint64
}

// Implement all MetricsProvider methods for atomicMetrics
func (m *atomicMetrics) IncrementEntries(count uint64) {
	m.totalEntries.Add(count)
}

func (m *atomicMetrics) AddBytes(bytes uint64) {
	m.totalBytes.Add(bytes)
}

func (m *atomicMetrics) AddCompressedBytes(bytes uint64) {
	m.totalCompressed.Add(bytes)
}

func (m *atomicMetrics) RecordWriteLatency(nanos uint64) {
	// For average, we'd need to track count and sum separately
	// For now, just store the latest
	m.writeLatencyNano.Store(nanos)
}

func (m *atomicMetrics) RecordMinWriteLatency(nanos uint64) {
	for {
		current := m.minWriteLatency.Load()
		if current != 0 && current <= nanos {
			break
		}
		if m.minWriteLatency.CompareAndSwap(current, nanos) {
			break
		}
	}
}

func (m *atomicMetrics) RecordMaxWriteLatency(nanos uint64) {
	for {
		current := m.maxWriteLatency.Load()
		if current >= nanos {
			break
		}
		if m.maxWriteLatency.CompareAndSwap(current, nanos) {
			break
		}
	}
}

func (m *atomicMetrics) SetCompressionRatio(ratio uint64) {
	m.compressionRatio.Store(ratio)
}

func (m *atomicMetrics) IncrementCompressedEntries(count uint64) {
	m.compressedEntries.Add(count)
}

func (m *atomicMetrics) IncrementSkippedCompression(count uint64) {
	m.skippedCompression.Add(count)
}

func (m *atomicMetrics) AddCompressionWait(nanos uint64) {
	m.compressionWaitNano.Add(nanos)
}

func (m *atomicMetrics) IncrementFilesCreated(count uint64) {
	m.filesCreated.Add(count)
}

func (m *atomicMetrics) IncrementFilesDeleted(count uint64) {
	m.filesDeleted.Add(count)
}

func (m *atomicMetrics) IncrementFileRotations(count uint64) {
	m.fileRotations.Add(count)
}

func (m *atomicMetrics) IncrementCheckpoints(count uint64) {
	m.checkpointCount.Add(count)
}

func (m *atomicMetrics) SetLastCheckpoint(nanos uint64) {
	m.lastCheckpoint.Store(nanos)
}

func (m *atomicMetrics) SetActiveReaders(count uint64) {
	m.activeReaders.Store(count)
}

func (m *atomicMetrics) SetMaxConsumerLag(lag uint64) {
	m.consumerLag.Store(lag)
}

func (m *atomicMetrics) IncrementErrors(count uint64) {
	m.errorCount.Add(count)
}

func (m *atomicMetrics) SetLastError(nanos uint64) {
	m.lastErrorNano.Store(nanos)
}

func (m *atomicMetrics) IncrementIndexPersistErrors(count uint64) {
	m.indexPersistErrors.Add(count)
}

func (m *atomicMetrics) GetStats() MetricsSnapshot {
	return MetricsSnapshot{
		TotalEntries:        m.totalEntries.Load(),
		TotalBytes:          m.totalBytes.Load(),
		TotalCompressed:     m.totalCompressed.Load(),
		WriteLatencyNano:    m.writeLatencyNano.Load(),
		MinWriteLatency:     m.minWriteLatency.Load(),
		MaxWriteLatency:     m.maxWriteLatency.Load(),
		CompressionRatio:    m.compressionRatio.Load(),
		CompressedEntries:   m.compressedEntries.Load(),
		SkippedCompression:  m.skippedCompression.Load(),
		CompressionWaitNano: m.compressionWaitNano.Load(),
		FilesCreated:        m.filesCreated.Load(),
		FilesDeleted:        m.filesDeleted.Load(),
		FileRotations:       m.fileRotations.Load(),
		CheckpointCount:     m.checkpointCount.Load(),
		LastCheckpoint:      m.lastCheckpoint.Load(),
		ActiveReaders:       m.activeReaders.Load(),
		ConsumerLag:         m.consumerLag.Load(),
		ErrorCount:          m.errorCount.Load(),
		LastErrorNano:       m.lastErrorNano.Load(),
		IndexPersistErrors:  m.indexPersistErrors.Load(),
	}
}

// newAtomicMetrics creates a metrics provider for single-process mode
func newAtomicMetrics() MetricsProvider {
	m := &atomicMetrics{}
	// Initialize min latency to max value so first write updates it
	m.minWriteLatency.Store(^uint64(0))
	return m
}
