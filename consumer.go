package comet

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MessageID represents a structured message ID
// Fields ordered for optimal memory alignment: int64 first, then uint32
type MessageID struct {
	EntryNumber int64  `json:"entry_number"`
	ShardID     uint32 `json:"shard_id"`
}

// String returns the string representation of the ID (ShardID-EntryNumber format)
func (id MessageID) String() string {
	return fmt.Sprintf("%d-%d", id.ShardID, id.EntryNumber)
}

// ParseMessageID parses a string ID back to MessageID
func ParseMessageID(str string) (MessageID, error) {
	var shardID uint32
	var entryNumber int64
	if _, err := fmt.Sscanf(str, "%d-%d", &shardID, &entryNumber); err != nil {
		return MessageID{}, fmt.Errorf("invalid message ID format: %w", err)
	}
	return MessageID{EntryNumber: entryNumber, ShardID: shardID}, nil
}

// StreamMessage represents a message read from a stream
type StreamMessage struct {
	Stream string    // Stream name/identifier
	ID     MessageID // Unique message ID
	Data   []byte    // Raw message data
}

// Consumer reads from comet stream shards
// Fields ordered for optimal memory alignment
type Consumer struct {
	// Pointers first (8 bytes on 64-bit)
	client *Client

	// Composite types
	readers sync.Map // Cached readers per shard (optimized for read-heavy workload)

	// Strings last
	group string
}

// ConsumerOptions configures a consumer
type ConsumerOptions struct {
	Group string
}

// ProcessFunc handles a batch of messages, returning error to trigger retry
type ProcessFunc func(ctx context.Context, messages []StreamMessage) error

// ProcessOption configures the Process method
type ProcessOption func(*processConfig)

// processConfig holds internal configuration built from options
type processConfig struct {
	// Core processing
	handler ProcessFunc
	autoAck bool

	// Callbacks
	onError func(err error, retryCount int)
	onBatch func(size int, duration time.Duration)

	// Behavior
	batchSize    int
	maxRetries   int
	pollInterval time.Duration
	retryDelay   time.Duration

	// Sharding
	stream        string
	shards        []uint32
	consumerID    int
	consumerCount int
}

// WithStream specifies a stream pattern for shard discovery
func WithStream(pattern string) ProcessOption {
	return func(cfg *processConfig) {
		cfg.stream = pattern
	}
}

// WithShards specifies explicit shards to process
func WithShards(shards ...uint32) ProcessOption {
	return func(cfg *processConfig) {
		cfg.shards = shards
	}
}

// WithBatchSize sets the number of messages to read at once
func WithBatchSize(size int) ProcessOption {
	return func(cfg *processConfig) {
		cfg.batchSize = size
	}
}

// WithMaxRetries sets the number of retry attempts for failed batches
func WithMaxRetries(retries int) ProcessOption {
	return func(cfg *processConfig) {
		cfg.maxRetries = retries
	}
}

// WithPollInterval sets how long to wait when no messages are available
func WithPollInterval(interval time.Duration) ProcessOption {
	return func(cfg *processConfig) {
		cfg.pollInterval = interval
	}
}

// WithRetryDelay sets the base delay between retries
func WithRetryDelay(delay time.Duration) ProcessOption {
	return func(cfg *processConfig) {
		cfg.retryDelay = delay
	}
}

// WithAutoAck controls automatic acknowledgment (default: true)
func WithAutoAck(enabled bool) ProcessOption {
	return func(cfg *processConfig) {
		cfg.autoAck = enabled
	}
}

// WithErrorHandler sets a callback for processing errors
func WithErrorHandler(handler func(err error, retryCount int)) ProcessOption {
	return func(cfg *processConfig) {
		cfg.onError = handler
	}
}

// WithBatchCallback sets a callback after each batch completes
func WithBatchCallback(callback func(size int, duration time.Duration)) ProcessOption {
	return func(cfg *processConfig) {
		cfg.onBatch = callback
	}
}

// WithConsumerAssignment configures distributed processing
func WithConsumerAssignment(id, total int) ProcessOption {
	return func(cfg *processConfig) {
		cfg.consumerID = id
		cfg.consumerCount = total
	}
}

// buildProcessConfig applies options and defaults
func buildProcessConfig(handler ProcessFunc, opts []ProcessOption) *processConfig {
	cfg := &processConfig{
		handler:       handler,
		autoAck:       true,
		batchSize:     100,
		maxRetries:    3,
		pollInterval:  100 * time.Millisecond,
		retryDelay:    time.Second,
		consumerCount: 1,
		consumerID:    0,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// If no stream or shards specified, discover all shards
	if cfg.stream == "" && len(cfg.shards) == 0 {
		cfg.stream = "*:*:*:*" // Match any stream pattern
	}

	return cfg
}

// NewConsumer creates a new consumer for comet streams
func NewConsumer(client *Client, opts ConsumerOptions) *Consumer {
	return &Consumer{
		client: client,
		group:  opts.Group,
		// readers sync.Map is zero-initialized and ready to use
	}
}

// Close closes the consumer and releases all resources
func (c *Consumer) Close() error {
	// Close all cached readers
	c.readers.Range(func(key, value any) bool {
		if reader, ok := value.(*Reader); ok {
			reader.Close()
			// Decrement active readers count
			shardID := key.(uint32)
			if shard, err := c.client.getOrCreateShard(shardID); err == nil && shard.state != nil {
				atomic.AddUint64(&shard.state.ActiveReaders, ^uint64(0)) // Decrement by 1
			}
		}
		return true
	})
	return nil
}

// Read reads up to count entries from the specified shards
func (c *Consumer) Read(ctx context.Context, shards []uint32, count int) ([]StreamMessage, error) {
	var messages []StreamMessage
	remaining := count

	for _, shardID := range shards {
		if remaining <= 0 {
			break
		}

		shard, err := c.client.getOrCreateShard(shardID)
		if err != nil {
			return nil, fmt.Errorf("failed to get shard %d: %w", shardID, err)
		}

		shardMessages, err := c.readFromShard(ctx, shard, remaining)
		if err != nil {
			return nil, fmt.Errorf("failed to read from shard %d: %w", shardID, err)
		}

		messages = append(messages, shardMessages...)
		remaining -= len(shardMessages)
	}

	return messages, nil
}

// Process continuously reads and processes messages from shards.
// The simplest usage processes all discoverable shards:
//
//	err := consumer.Process(ctx, handleMessages)
//
// With options:
//
//	err := consumer.Process(ctx, handleMessages,
//	    comet.WithStream("events:v1:shard:*"),
//	    comet.WithBatchSize(1000),
//	    comet.WithErrorHandler(logError),
//	)
//
// For distributed processing:
//
//	err := consumer.Process(ctx, handleMessages,
//	    comet.WithStream("events:v1:shard:*"),
//	    comet.WithConsumerAssignment(workerID, totalWorkers),
//	)
func (c *Consumer) Process(ctx context.Context, handler ProcessFunc, opts ...ProcessOption) error {
	// Build config from options
	cfg := buildProcessConfig(handler, opts)

	// Validate required fields
	if cfg.handler == nil {
		return fmt.Errorf("handler is required")
	}

	// Determine shards to process
	var shards []uint32
	if len(cfg.shards) > 0 {
		shards = cfg.shards
	} else if cfg.stream != "" {
		// Auto-discover shards from stream pattern
		discoveredShards, err := c.discoverShards(cfg.stream, cfg.consumerID, cfg.consumerCount)
		if err != nil {
			return fmt.Errorf("failed to discover shards: %w", err)
		}
		shards = discoveredShards
	} else {
		return fmt.Errorf("either Shards or Stream must be specified")
	}

	// Apply defaults (already set in buildProcessConfig)
	autoAck := cfg.autoAck

	// Main processing loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		start := time.Now()
		messages, err := c.Read(ctx, shards, cfg.batchSize)
		if err != nil {
			if cfg.onError != nil {
				cfg.onError(err, 0)
			}
			// For read errors, sleep and retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.pollInterval):
				continue
			}
		}

		if len(messages) == 0 {
			// No messages, wait before polling again
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.pollInterval):
				continue
			}
		}

		// Process batch with retries
		var processErr error
		for retry := 0; retry <= cfg.maxRetries; retry++ {
			processErr = cfg.handler(ctx, messages)
			if processErr == nil {
				break
			}

			if cfg.onError != nil {
				cfg.onError(processErr, retry)
			}

			if retry < cfg.maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(cfg.retryDelay * time.Duration(retry+1)):
					// Exponential backoff
				}
			}
		}

		// Auto-ack if enabled and processing succeeded
		if autoAck && processErr == nil {
			for _, msg := range messages {
				if err := c.Ack(ctx, msg.ID); err != nil && cfg.onError != nil {
					cfg.onError(fmt.Errorf("ack failed for message %s: %w", msg.ID, err), 0)
				}
			}
		}

		// Call batch callback if provided
		if cfg.onBatch != nil {
			cfg.onBatch(len(messages), time.Since(start))
		}
	}
}

// getOrCreateReader gets or creates a reader for a shard
func (c *Consumer) getOrCreateReader(shard *Shard) (*Reader, error) {
	// Fast path: check if reader exists using sync.Map
	if value, ok := c.readers.Load(shard.shardID); ok {
		reader := value.(*Reader)

		// Update the reader with current files - the new Reader handles this efficiently
		shard.mu.RLock()
		// Make a copy to avoid race conditions
		currentFiles := make([]FileInfo, len(shard.index.Files))
		copy(currentFiles, shard.index.Files)
		shard.mu.RUnlock()

		if err := reader.UpdateFiles(&currentFiles); err != nil {
			// If update fails, close the reader and create a new one
			c.readers.Delete(shard.shardID)
			reader.Close()
			// Decrement active readers count
			if shard.state != nil {
				atomic.AddUint64(&shard.state.ActiveReaders, ^uint64(0)) // Decrement by 1
			}
		} else {
			// Track reader cache hit
			if shard.state != nil {
				atomic.AddUint64(&shard.state.ReaderCacheHits, 1)
			}
			return reader, nil
		}
	}

	// Create new reader with a snapshot of the current index
	shard.mu.RLock()
	// Make a copy of the index to avoid race conditions
	indexCopy := &ShardIndex{
		Files:              make([]FileInfo, len(shard.index.Files)),
		CurrentFile:        shard.index.CurrentFile,
		CurrentWriteOffset: shard.index.CurrentWriteOffset,
		CurrentEntryNumber: shard.index.CurrentEntryNumber,
		ConsumerOffsets:    make(map[string]int64),
		BinaryIndex: BinarySearchableIndex{
			IndexInterval: shard.index.BinaryIndex.IndexInterval,
			MaxNodes:      shard.index.BinaryIndex.MaxNodes,
			Nodes:         make([]EntryIndexNode, len(shard.index.BinaryIndex.Nodes)),
		},
	}
	copy(indexCopy.Files, shard.index.Files)
	copy(indexCopy.BinaryIndex.Nodes, shard.index.BinaryIndex.Nodes)
	maps.Copy(indexCopy.ConsumerOffsets, shard.index.ConsumerOffsets)
	shard.mu.RUnlock()

	newReader, err := NewReader(shard.shardID, indexCopy)
	if err != nil {
		return nil, err
	}

	// Set the state for metrics tracking
	if shard.state != nil {
		newReader.SetState(shard.state)
	}

	// Use LoadOrStore to handle race where multiple goroutines try to create
	if actual, loaded := c.readers.LoadOrStore(shard.shardID, newReader); loaded {
		// Another goroutine created a reader, close ours and use theirs
		newReader.Close()
		return actual.(*Reader), nil
	}

	// Track new reader creation
	if shard.state != nil {
		atomic.AddUint64(&shard.state.TotalReaders, 1)
		atomic.AddUint64(&shard.state.ActiveReaders, 1)
	}

	return newReader, nil
}

// readFromShard reads entries from a single shard using entry-based positioning
func (c *Consumer) readFromShard(ctx context.Context, shard *Shard, maxCount int) ([]StreamMessage, error) {
	// Lock-free reader tracking
	atomic.AddInt64(&shard.readerCount, 1)
	defer atomic.AddInt64(&shard.readerCount, -1)

	// Check unified state for instant change detection (multi-process mode only)
	// In single-process mode, skip this entirely as there's no external coordination needed
	if c.client.config.Concurrency.EnableMultiProcessMode && shard.state != nil {
		currentTimestamp := shard.state.GetLastIndexUpdate()
		if currentTimestamp != atomic.LoadInt64(&shard.lastMmapCheck) {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != atomic.LoadInt64(&shard.lastMmapCheck) {
				if err := shard.loadIndexWithRecovery(); err != nil {
					shard.mu.Unlock()
					return nil, fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
				}
				atomic.StoreInt64(&shard.lastMmapCheck, currentTimestamp)

				// In multi-process mode, check if we need to rebuild index from files
				shardDir := filepath.Join(c.client.dataDir, fmt.Sprintf("shard-%04d", shard.shardID))
				if Debug && shard.logger != nil {
					shard.logger.Debug("Consumer triggering index rebuild check",
						"shard", shard.shardID,
						"multiProcessMode", c.client.config.Concurrency.EnableMultiProcessMode,
						"shardDir", shardDir)
				}
				shard.lazyRebuildIndexIfNeeded(c.client.config, shardDir)

				// Also invalidate any cached readers since the index changed
				c.readers.Range(func(key, value any) bool {
					if key.(uint32) == shard.shardID {
						if reader, ok := value.(*Reader); ok {
							reader.Close()
							// Decrement active readers count
							if shard.state != nil {
								atomic.AddUint64(&shard.state.ActiveReaders, ^uint64(0)) // Decrement by 1
							}
						}
						c.readers.Delete(key)
						return false // Stop after finding this shard's reader
					}
					return true
				})
			}
			shard.mu.Unlock()
		}
	}

	shard.mu.RLock()
	// Get consumer entry offset (not byte offset!)
	startEntryNum, exists := shard.index.ConsumerOffsets[c.group]
	if !exists {
		startEntryNum = 0
	}

	// After retention, the requested start entry might no longer exist
	// Adjust to the earliest available entry if needed
	if len(shard.index.Files) > 0 {
		earliestEntry := shard.index.Files[0].StartEntry
		if startEntryNum < earliestEntry {
			startEntryNum = earliestEntry
		}
	}

	endEntryNum := shard.index.CurrentEntryNumber
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	// In multi-process mode, check if index might be stale by comparing with state
	if Debug && shard.logger != nil {
		shard.logger.Debug("Consumer read state check",
			"shard", shard.shardID,
			"stateExists", shard.state != nil,
			"mmapWriterExists", shard.mmapWriter != nil,
			"mmapWriterStateExists", shard.mmapWriter != nil && shard.mmapWriter.state != nil,
			"endEntryNum", endEntryNum)
	}
	if shard.state != nil && shard.mmapWriter != nil && shard.mmapWriter.state != nil {
		totalWrites := shard.mmapWriter.state.GetTotalWrites()
		if totalWrites > uint64(endEntryNum) {
			// Need write lock for rebuild
			shard.mu.Lock()
			shardDir := filepath.Join(c.client.dataDir, fmt.Sprintf("shard-%04d", shard.shardID))
			// Force a full rebuild by manually updating the rebuild trigger
			// Temporarily modify the file end offset to trigger a rebuild
			oldEndOffset := int64(0)
			if len(shard.index.Files) > 0 {
				oldEndOffset = shard.index.Files[len(shard.index.Files)-1].EndOffset
				// Set end offset to 0 to force rebuild detection
				shard.index.Files[len(shard.index.Files)-1].EndOffset = 0
			}

			shard.lazyRebuildIndexIfNeeded(c.client.config, shardDir)

			// Restore if rebuild didn't happen (shouldn't occur)
			if len(shard.index.Files) > 0 && shard.index.Files[len(shard.index.Files)-1].EndOffset == 0 {
				shard.index.Files[len(shard.index.Files)-1].EndOffset = oldEndOffset
			}
			// Reload the updated values
			startEntryNum, exists = shard.index.ConsumerOffsets[c.group]
			if !exists {
				startEntryNum = 0
			}

			// After retention, adjust to earliest available entry if needed
			if len(shard.index.Files) > 0 {
				earliestEntry := shard.index.Files[0].StartEntry
				if startEntryNum < earliestEntry {
					startEntryNum = earliestEntry
				}
			}

			endEntryNum = shard.index.CurrentEntryNumber
			fileCount = len(shard.index.Files)
			shard.mu.Unlock()
		}
	}

	if startEntryNum >= endEntryNum {
		return nil, nil // No new data
	}

	// Safety check: if we have entries but no files, something is wrong
	if endEntryNum > 0 && fileCount == 0 {
		return nil, fmt.Errorf("shard %d has %d entries but no data files", shard.shardID, endEntryNum)
	}

	// Preallocate slice capacity based on available entries and maxCount
	availableEntries := endEntryNum - startEntryNum
	expectedCount := availableEntries
	if maxCount > 0 && expectedCount > int64(maxCount) {
		expectedCount = int64(maxCount)
	}
	messages := make([]StreamMessage, 0, expectedCount)

	// Read entries by entry number, looking up positions from index
	for entryNum := startEntryNum; entryNum < endEntryNum && len(messages) < maxCount; entryNum++ {
		// Check context
		if ctx.Err() != nil {
			return messages, ctx.Err()
		}

		// Get reader for this shard
		reader, err := c.getOrCreateReader(shard)
		if err != nil {
			return nil, fmt.Errorf("failed to get reader for shard %d: %w", shard.shardID, err)
		}

		// Use the new ReadEntryByNumber method which handles position finding internally
		data, err := reader.ReadEntryByNumber(entryNum)
		if err != nil {
			// In multi-process mode, some entries might not exist due to gaps in the sequence
			// Skip these entries instead of failing the entire read
			if strings.Contains(err.Error(), "not found in any file") {
				continue
			}
			if err != nil {
				// Track read error in CometState
				if shard.state != nil {
					atomic.AddUint64(&shard.state.ReadErrors, 1)
				}
				return nil, fmt.Errorf("failed to read entry %d from shard %d: %w", entryNum, shard.shardID, err)
			}
		}

		message := StreamMessage{
			Stream: fmt.Sprintf("shard:%04d", shard.shardID),
			ID:     MessageID{EntryNumber: entryNum, ShardID: shard.shardID},
			Data:   data,
		}

		messages = append(messages, message)
	}

	// Track read metrics in CometState
	if shard.state != nil && len(messages) > 0 {
		atomic.AddUint64(&shard.state.TotalEntriesRead, uint64(len(messages)))
	}

	return messages, nil
}

// scanDataFilesForEntry directly scans data files to find an entry when index is incomplete
func (c *Consumer) scanDataFilesForEntry(shard *Shard, targetEntry int64) (EntryPosition, error) {
	// This is the fallback when index-based lookup fails in multi-process mode
	// TODO: Optimize this function - currently using a simple approach (todo item #13)

	shard.mu.RLock()
	files := make([]FileInfo, len(shard.index.Files))
	copy(files, shard.index.Files)
	shard.mu.RUnlock()

	if len(files) == 0 {
		return EntryPosition{}, fmt.Errorf("no files to scan")
	}

	// Find which file should contain the target entry based on index metadata
	for fileIdx, fileInfo := range files {
		if fileInfo.StartEntry <= targetEntry && targetEntry < fileInfo.StartEntry+fileInfo.Entries {
			// Found the file that should contain the entry
			return EntryPosition{
				FileIndex:  fileIdx,
				ByteOffset: 0, // Start at beginning - inefficient but works
			}, nil
		}
	}

	return EntryPosition{}, fmt.Errorf("entry %d not found in any file", targetEntry)
}

// Ack acknowledges one or more processed messages and updates consumer offset
func (c *Consumer) Ack(ctx context.Context, messageIDs ...MessageID) error {
	if len(messageIDs) == 0 {
		return nil
	}

	// Single message fast path
	if len(messageIDs) == 1 {
		messageID := messageIDs[0]
		shard, err := c.client.getOrCreateShard(messageID.ShardID)
		if err != nil {
			return err
		}

		shard.mu.Lock()
		// Check if this is a new consumer group
		_, groupExists := shard.index.ConsumerOffsets[c.group]
		if !groupExists && shard.state != nil {
			atomic.AddUint64(&shard.state.ConsumerGroups, 1)
		}
		// Update consumer offset to the next entry number
		// This ensures we won't re-read this entry
		nextEntry := messageID.EntryNumber + 1
		shard.index.ConsumerOffsets[c.group] = nextEntry
		// Mark that we need a checkpoint
		shard.writesSinceCheckpoint++
		// Track acked entries
		if shard.state != nil {
			atomic.AddUint64(&shard.state.AckedEntries, 1)
		}
		shard.mu.Unlock()

		// Don't persist immediately - let periodic checkpoint handle it
		return nil
	}

	// Multiple messages - use batch logic
	return c.ackBatch(messageIDs)
}

// ackBatch is a helper for batch acknowledgments
func (c *Consumer) ackBatch(messageIDs []MessageID) error {
	// Group messages by shard
	shardGroups := make(map[uint32][]MessageID)
	for _, id := range messageIDs {
		shardGroups[id.ShardID] = append(shardGroups[id.ShardID], id)
	}

	// Process each shard group
	for shardID, ids := range shardGroups {
		shard, err := c.client.getOrCreateShard(shardID)
		if err != nil {
			return err
		}

		shard.mu.Lock()

		// Find the highest entry number in this shard's batch
		var maxEntry int64 = -1
		for _, id := range ids {
			if id.EntryNumber > maxEntry {
				maxEntry = id.EntryNumber
			}
		}

		// Update to one past the highest ACK'd entry
		if maxEntry >= 0 {
			// Check if this is a new consumer group
			_, groupExists := shard.index.ConsumerOffsets[c.group]
			if !groupExists && shard.state != nil {
				atomic.AddUint64(&shard.state.ConsumerGroups, 1)
			}
			shard.index.ConsumerOffsets[c.group] = maxEntry + 1
			// Mark that we need a checkpoint
			shard.writesSinceCheckpoint++
			// Track acked entries
			if shard.state != nil {
				atomic.AddUint64(&shard.state.AckedEntries, uint64(len(ids)))
			}
		}

		shard.mu.Unlock()
	}

	return nil
}

// GetLag returns how many entries behind this consumer group is
func (c *Consumer) GetLag(ctx context.Context, shardID uint32) (int64, error) {
	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return 0, err
	}

	// Check unified state for instant change detection
	if shard.state != nil {
		currentTimestamp := shard.state.GetLastIndexUpdate()
		if currentTimestamp != atomic.LoadInt64(&shard.lastMmapCheck) {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != atomic.LoadInt64(&shard.lastMmapCheck) {
				if err := shard.loadIndexWithRecovery(); err != nil {
					shard.mu.Unlock()
					return 0, fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
				}
				atomic.StoreInt64(&shard.lastMmapCheck, currentTimestamp)
			}
			shard.mu.Unlock()
		}
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	consumerEntry, exists := shard.index.ConsumerOffsets[c.group]
	if !exists {
		consumerEntry = 0
	}

	// Entry-based lag calculation
	lag := shard.index.CurrentEntryNumber - consumerEntry

	// Track max consumer lag
	if shard.state != nil && lag > 0 {
		// Update max lag if this is higher
		for {
			currentMax := atomic.LoadUint64(&shard.state.MaxConsumerLag)
			if uint64(lag) <= currentMax {
				break
			}
			if atomic.CompareAndSwapUint64(&shard.state.MaxConsumerLag, currentMax, uint64(lag)) {
				break
			}
		}
	}

	return lag, nil
}

// ResetOffset sets the consumer offset to a specific entry number
func (c *Consumer) ResetOffset(ctx context.Context, shardID uint32, entryNumber int64) error {
	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	if entryNumber < 0 {
		// Negative means from end
		entryNumber = shard.index.CurrentEntryNumber + entryNumber
		if entryNumber < 0 {
			entryNumber = 0
		}
	}

	shard.index.ConsumerOffsets[c.group] = entryNumber
	// Mark that we need a checkpoint
	shard.writesSinceCheckpoint++
	shard.mu.Unlock()

	// Don't persist immediately - let periodic checkpoint handle it
	return nil
}

// AckRange acknowledges all messages in a contiguous range for a shard
// This is more efficient than individual acks for bulk processing
func (c *Consumer) AckRange(ctx context.Context, shardID uint32, fromEntry, toEntry int64) error {
	if fromEntry > toEntry {
		return fmt.Errorf("invalid range: from %d > to %d", fromEntry, toEntry)
	}

	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	// Update to one past the end of the range
	newOffset := toEntry + 1
	currentOffset, exists := shard.index.ConsumerOffsets[c.group]

	// Only update if this advances the offset (no going backwards)
	if !exists || newOffset > currentOffset {
		shard.index.ConsumerOffsets[c.group] = newOffset
		// Mark that we need a checkpoint
		shard.writesSinceCheckpoint++
	}
	shard.mu.Unlock()

	return nil
}

// StreamStats returns statistics about a shard
// Fields ordered for optimal memory alignment
type StreamStats struct {
	// 64-bit aligned fields first
	TotalEntries int64
	TotalBytes   int64
	OldestEntry  time.Time
	NewestEntry  time.Time

	// Composite types
	ConsumerOffsets map[string]int64

	// Smaller fields last
	FileCount int    // 8 bytes
	ShardID   uint32 // 4 bytes
	// 4 bytes padding
}

// GetShardStats returns statistics for a specific shard
func (c *Consumer) GetShardStats(ctx context.Context, shardID uint32) (*StreamStats, error) {
	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return nil, err
	}

	// Check unified state for instant change detection
	if shard.state != nil {
		currentTimestamp := shard.state.GetLastIndexUpdate()
		if currentTimestamp != atomic.LoadInt64(&shard.lastMmapCheck) {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != atomic.LoadInt64(&shard.lastMmapCheck) {
				if err := shard.loadIndexWithRecovery(); err != nil {
					shard.mu.Unlock()
					return nil, fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
				}
				atomic.StoreInt64(&shard.lastMmapCheck, currentTimestamp)
			}
			shard.mu.Unlock()
		}
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	stats := &StreamStats{
		ShardID:         shardID,
		FileCount:       len(shard.index.Files),
		ConsumerOffsets: make(map[string]int64),
	}

	// Copy consumer offsets
	for group, offset := range shard.index.ConsumerOffsets {
		stats.ConsumerOffsets[group] = offset
	}

	// Calculate totals from files
	for _, file := range shard.index.Files {
		stats.TotalEntries += file.Entries
		stats.TotalBytes += file.EndOffset - file.StartOffset

		if stats.OldestEntry.IsZero() || file.StartTime.Before(stats.OldestEntry) {
			stats.OldestEntry = file.StartTime
		}

		if file.EndTime.After(stats.NewestEntry) {
			stats.NewestEntry = file.EndTime
		}
	}

	return stats, nil
}

// discoverShards discovers available shards based on stream pattern and consumer assignment
func (c *Consumer) discoverShards(streamPattern string, consumerID, consumerCount int) ([]uint32, error) {
	// Apply defaults
	if consumerCount <= 0 {
		consumerCount = 1
	}
	if consumerID < 0 || consumerID >= consumerCount {
		return nil, fmt.Errorf("invalid consumerID %d for consumerCount %d", consumerID, consumerCount)
	}

	// Parse stream pattern (e.g., "events:v1:shard:*")
	if !strings.HasSuffix(streamPattern, ":*") {
		return nil, fmt.Errorf("stream pattern must end with :* (e.g., events:v1:shard:*)")
	}

	baseStream := strings.TrimSuffix(streamPattern, "*")

	// Discover all available shards
	// Check up to 32 shards by default (can be increased if needed)
	maxShards := uint32(32)
	var allShards []uint32

	// Try to discover shards in parallel for better performance
	type result struct {
		shardID uint32
		exists  bool
	}

	results := make(chan result, maxShards)
	var wg sync.WaitGroup

	for shardID := uint32(0); shardID < maxShards; shardID++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			streamName := fmt.Sprintf("%s%04d", baseStream, id)
			_, err := c.client.Len(context.Background(), streamName)
			results <- result{shardID: id, exists: err == nil}
		}(shardID)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		if res.exists {
			allShards = append(allShards, res.shardID)
		}
	}

	if len(allShards) == 0 {
		return nil, fmt.Errorf("no shards found for pattern %s", streamPattern)
	}

	// Sort shards for predictable assignment
	slices.Sort(allShards)

	// Assign shards to this consumer using modulo distribution
	var assignedShards []uint32
	for _, shardID := range allShards {
		if int(shardID)%consumerCount == consumerID {
			assignedShards = append(assignedShards, shardID)
		}
	}

	return assignedShards, nil
}
