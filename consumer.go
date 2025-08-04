package comet

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ErrStopProcessing is a sentinel error that can be returned from ProcessFunc
// to gracefully stop processing while still ACKing the current batch
var ErrStopProcessing = errors.New("stop processing")

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
	readers       sync.Map // Cached readers per shard (optimized for read-heavy workload)
	shards        sync.Map // Track shards that have been accessed (for Close persistence)
	claimedShards sync.Map // Track claimed shards for this consumer group (shardID -> *os.File)

	// Track highest read entry per shard for AckRange validation
	highestReadMu sync.RWMutex
	highestRead   map[uint32]int64 // shardID -> highest entry read

	// Strings last
	group string
}

// ConsumerOptions configures a consumer
type ConsumerOptions struct {
	Group string
}

// tryClaimShard attempts to claim exclusive access to a shard for this consumer group
func (c *Consumer) tryClaimShard(shardID uint32) bool {
	// Check if already claimed
	if _, exists := c.claimedShards.Load(shardID); exists {
		return true
	}

	// Create claim lock file
	claimPath := filepath.Join(c.client.dataDir,
		fmt.Sprintf("consumer-group-%s-shard-%d.claim", c.group, shardID))

	claimFile, err := os.OpenFile(claimPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return false
	}

	// Try to acquire exclusive lock (non-blocking)
	if err := syscall.Flock(int(claimFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		claimFile.Close()
		return false
	}

	// Successfully claimed - store the lock file
	c.claimedShards.Store(shardID, claimFile)
	return true
}

// releaseShardClaims releases all claimed shards
func (c *Consumer) releaseShardClaims() {
	c.claimedShards.Range(func(key, value interface{}) bool {
		if claimFile, ok := value.(*os.File); ok {
			syscall.Flock(int(claimFile.Fd()), syscall.LOCK_UN)
			claimFile.Close()
		}
		c.claimedShards.Delete(key)
		return true
	})
}

// getClaimedShards returns list of shards claimed by this consumer
func (c *Consumer) getClaimedShards(candidateShards []uint32) []uint32 {
	var claimed []uint32
	for _, shardID := range candidateShards {
		if c.tryClaimShard(shardID) {
			claimed = append(claimed, shardID)
		}
	}
	return claimed
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
	group := opts.Group
	// If no group specified, use a consistent default to avoid confusion
	if group == "" {
		group = "default"
	}

	// Register this consumer group as active
	client.registerConsumerGroup(group)

	return &Consumer{
		client:      client,
		group:       group,
		highestRead: make(map[uint32]int64),
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
			if shard, err := c.client.getOrCreateShard(shardID); err == nil {
				if state := shard.loadState(); state != nil {
					atomic.AddUint64(&state.ActiveReaders, ^uint64(0)) // Decrement by 1
				}
			}
		}
		return true
	})

	// Persist any pending consumer offsets before closing
	// This is critical to prevent ACK loss when consumer restarts
	// In multiprocess mode, we need to ensure all pending checkpoints complete
	if c.client.config.Concurrency.EnableMultiProcessMode {
		// Wait for any pending async checkpoints to complete
		c.shards.Range(func(key, value any) bool {
			shardID := key.(uint32)
			if shard, err := c.client.getOrCreateShard(shardID); err == nil {
				// Wait for the wait group which tracks async operations
				shard.wg.Wait()
			}
			return true
		})
	}

	// Now persist any remaining changes synchronously
	c.shards.Range(func(key, value any) bool {
		shardID := key.(uint32)
		if shard, err := c.client.getOrCreateShard(shardID); err == nil {
			shard.mu.Lock()

			// Always persist on close, regardless of writesSinceCheckpoint
			// This ensures any recent ACKs are saved
			if IsDebug() && c.client.logger != nil {
				c.client.logger.Debug("Consumer close: persisting index",
					"shardID", shardID,
					"group", c.group,
					"offset", shard.index.ConsumerOffsets[c.group],
					"writesSinceCheckpoint", shard.writesSinceCheckpoint)
			}

			if err := shard.persistIndex(); err != nil && c.client.logger != nil {
				c.client.logger.Warn("Failed to persist consumer offsets on close",
					"error", err, "shard", shardID, "group", c.group)
			}

			shard.mu.Unlock()
		}
		return true
	})

	// Release all shard claims for this consumer group
	c.releaseShardClaims()

	// Deregister this consumer group
	c.client.deregisterConsumerGroup(c.group)

	return nil
}

// Read reads up to count entries from the specified shards starting from the consumer group's current offset.
// This is a low-level method for manual message processing - you probably want Process() instead.
//
// Key differences from Process():
// - Read() is one-shot, Process() is continuous
// - Read() requires manual ACKing, Process() can auto-ACK
// - Read() uses explicit shard IDs, Process() can use wildcards
// - Read() has no retry logic, Process() has configurable retries
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
// This is the high-level API for stream processing - handles discovery, retries, and ACKing automatically.
//
// The simplest usage processes all discoverable shards:
//
//	err := consumer.Process(ctx, handleMessages)
//
// Recommended usage with explicit configuration:
//
//	err := consumer.Process(ctx, handleMessages,
//	    comet.WithStream("events:v1:shard:*"),     // Wildcard pattern for shard discovery
//	    comet.WithBatchSize(1000),                 // Process up to 1000 messages at once
//	    comet.WithErrorHandler(logError),          // Handle processing errors
//	    comet.WithAutoAck(true),                   // Automatically ACK successful batches
//	)
//
// For distributed processing across multiple consumer instances:
//
//	err := consumer.Process(ctx, handleMessages,
//	    comet.WithStream("events:v1:shard:*"),
//	    comet.WithConsumerAssignment(workerID, totalWorkers), // Distribute shards across workers
//	)
//
// Stream patterns must end with ":*" for wildcard matching, e.g.:
// - "events:v1:shard:*" matches events:v1:shard:0000, events:v1:shard:0001, etc.
// - "logs:*:*:*" matches any 4-part stream name starting with "logs"
func (c *Consumer) Process(ctx context.Context, handler ProcessFunc, opts ...ProcessOption) error {
	// Build config from options
	cfg := buildProcessConfig(handler, opts)

	// Validate required fields
	if cfg.handler == nil {
		return fmt.Errorf("handler is required")
	}

	// Determine candidate shards to process
	var candidateShards []uint32
	if len(cfg.shards) > 0 {
		candidateShards = cfg.shards
	} else if cfg.stream != "" {
		// Auto-discover shards from stream pattern
		discoveredShards, err := c.discoverShards(cfg.stream, cfg.consumerID, cfg.consumerCount)
		if err != nil {
			return fmt.Errorf("failed to discover shards: %w", err)
		}
		candidateShards = discoveredShards
	} else {
		return fmt.Errorf("either Shards or Stream must be specified")
	}

	// Main processing loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Consumer group coordination: claim shards we can exclusively access
		shards := c.getClaimedShards(candidateShards)
		if len(shards) == 0 {
			// No shards available for this consumer group - wait and retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.pollInterval):
				continue
			}
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
		if cfg.autoAck && processErr == nil {
			for _, msg := range messages {
				if err := c.Ack(ctx, msg.ID); err != nil {
					if cfg.onError != nil {
						cfg.onError(fmt.Errorf("ack failed for message %s: %w", msg.ID, err), 0)
					}
					// Also log for debugging
					if IsDebug() && c.client.logger != nil {
						c.client.logger.Debug("AutoAck failed",
							"messageID", msg.ID,
							"error", err)
					}
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
			if state := shard.loadState(); state != nil {
				atomic.AddUint64(&state.ActiveReaders, ^uint64(0)) // Decrement by 1
			}
		} else {
			// Track reader cache hit
			if state := shard.loadState(); state != nil {
				atomic.AddUint64(&state.ReaderCacheHits, 1)
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

	newReader, err := NewReader(shard.shardID, indexCopy, c.client.config.Reader)
	if err != nil {
		return nil, err
	}

	// Set the state for metrics tracking
	if state := shard.loadState(); state != nil {
		newReader.SetState(state)
	}

	// Use LoadOrStore to handle race where multiple goroutines try to create
	if actual, loaded := c.readers.LoadOrStore(shard.shardID, newReader); loaded {
		// Another goroutine created a reader, close ours and use theirs
		newReader.Close()
		return actual.(*Reader), nil
	}

	// Track new reader creation
	if state := shard.loadState(); state != nil {
		atomic.AddUint64(&state.TotalReaders, 1)
		atomic.AddUint64(&state.ActiveReaders, 1)
	}

	return newReader, nil
}

// readFromShard reads entries from a single shard using entry-based positioning
func (c *Consumer) readFromShard(ctx context.Context, shard *Shard, maxCount int) ([]StreamMessage, error) {
	// Track this shard for persistence on close
	c.shards.Store(shard.shardID, true)
	// Lock-free reader tracking
	atomic.AddInt64(&shard.readerCount, 1)
	defer atomic.AddInt64(&shard.readerCount, -1)

	// Check unified state for instant change detection (multi-process mode only)
	// In single-process mode, skip this entirely as there's no external coordination needed
	if c.client.config.Concurrency.EnableMultiProcessMode {
		if state := shard.loadState(); state != nil {
			currentTimestamp := state.GetLastIndexUpdate()
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
					if IsDebug() && shard.logger != nil {
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
								if state := shard.loadState(); state != nil {
									atomic.AddUint64(&state.ActiveReaders, ^uint64(0)) // Decrement by 1
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
	if IsDebug() && shard.logger != nil {
		shard.logger.Debug("Consumer read state check",
			"shard", shard.shardID,
			"stateExists", shard.loadState() != nil,
			"mmapWriterExists", shard.mmapWriter != nil,
			"mmapWriterStateExists", shard.mmapWriter != nil && shard.mmapWriter.state != nil,
			"endEntryNum", endEntryNum)
	}
	if shard.loadState() != nil && shard.mmapWriter != nil && shard.mmapWriter.state != nil {
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
				if state := shard.loadState(); state != nil {
					atomic.AddUint64(&state.ReadErrors, 1)
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
	if state := shard.loadState(); state != nil && len(messages) > 0 {
		atomic.AddUint64(&state.TotalEntriesRead, uint64(len(messages)))
	}

	// Track highest read entry for AckRange validation
	if len(messages) > 0 {
		lastRead := messages[len(messages)-1].ID.EntryNumber
		c.highestReadMu.Lock()
		if current, exists := c.highestRead[shard.shardID]; !exists || lastRead > current {
			c.highestRead[shard.shardID] = lastRead
		}
		c.highestReadMu.Unlock()
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

		// Track this shard for persistence on close
		c.shards.Store(shard.shardID, true)

		shard.mu.Lock()
		// Check if this is a new consumer group
		_, groupExists := shard.index.ConsumerOffsets[c.group]
		if !groupExists {
			if state := shard.loadState(); state != nil {
				atomic.AddUint64(&state.ConsumerGroups, 1)
			}
		}
		// Update consumer offset only if this is the next expected message
		// This ensures exactly-once semantics for consumer groups
		oldOffset := shard.index.ConsumerOffsets[c.group]
		expectedEntryNum := oldOffset
		var nextEntry int64

		if messageID.EntryNumber == expectedEntryNum {
			// This is the next expected message, we can advance the offset
			nextEntry = messageID.EntryNumber + 1
			shard.index.ConsumerOffsets[c.group] = nextEntry

			if IsDebug() && c.client.logger != nil {
				c.client.logger.Debug("Updating consumer offset",
					"group", c.group,
					"messageID", messageID.EntryNumber,
					"oldOffset", oldOffset,
					"newOffset", nextEntry)
			}

			// DEBUG: Log the ACK operation
			if IsDebug() {
				if logger := shard.logger; logger != nil {
					logger.Debug("Consumer ACK",
						"consumerGroup", c.group,
						"messageID", messageID.String(),
						"nextEntry", nextEntry,
						"allOffsets", shard.index.ConsumerOffsets)
				}
			}
		} else if messageID.EntryNumber < expectedEntryNum {
			// This message was already processed, ignore the ACK
			if IsDebug() && c.client.logger != nil {
				c.client.logger.Debug("Ignoring duplicate ACK",
					"group", c.group,
					"messageID", messageID.EntryNumber,
					"expectedEntryNum", expectedEntryNum)
			}
		} else {
			// This message is from the future, we can't ACK it yet
			if IsDebug() && c.client.logger != nil {
				c.client.logger.Debug("Cannot ACK future message",
					"group", c.group,
					"messageID", messageID.EntryNumber,
					"expectedEntryNum", expectedEntryNum)
			}
			shard.mu.Unlock()
			return fmt.Errorf("cannot ACK message %d: expected %d", messageID.EntryNumber, expectedEntryNum)
		}
		// Mark that we need a checkpoint
		shard.writesSinceCheckpoint++
		// Track acked entries
		if state := shard.loadState(); state != nil {
			atomic.AddUint64(&state.AckedEntries, 1)
		}

		// Always persist consumer offsets immediately to prevent data loss
		// The index lock in persistIndex() prevents contention in multiprocess mode
		if err := shard.persistIndex(); err != nil {
			if c.client.logger != nil {
				c.client.logger.Warn("Failed to persist consumer offset after ACK", "error", err)
			}
		} else {
			if IsDebug() && c.client.logger != nil {
				c.client.logger.Debug("ACK persisted successfully",
					"consumerGroup", c.group,
					"shardID", shard.shardID,
					"offset", shard.index.ConsumerOffsets[c.group])
			}
		}

		shard.mu.Unlock()

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

		// Track this shard for persistence on close
		c.shards.Store(shardID, true)

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
			if !groupExists {
				if state := shard.loadState(); state != nil {
					atomic.AddUint64(&state.ConsumerGroups, 1)
				}
			}
			shard.index.ConsumerOffsets[c.group] = maxEntry + 1
			// Mark that we need a checkpoint
			shard.writesSinceCheckpoint++
			// Track acked entries
			if state := shard.loadState(); state != nil {
				atomic.AddUint64(&state.AckedEntries, uint64(len(ids)))
			}
		}

		// Always persist consumer offsets immediately to prevent data loss
		// The index lock in persistIndex() prevents contention in multiprocess mode
		if err := shard.persistIndex(); err != nil && c.client.logger != nil {
			c.client.logger.Warn("Failed to persist consumer offsets after batch ACK", "error", err)
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
	if state := shard.loadState(); state != nil {
		currentTimestamp := state.GetLastIndexUpdate()
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
	if state := shard.loadState(); state != nil && lag > 0 {
		// Update max lag if this is higher
		for {
			currentMax := atomic.LoadUint64(&state.MaxConsumerLag)
			if uint64(lag) <= currentMax {
				break
			}
			if atomic.CompareAndSwapUint64(&state.MaxConsumerLag, currentMax, uint64(lag)) {
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

	// Persist consumer offset change immediately to prevent data loss
	if err := shard.persistIndex(); err != nil && c.client.logger != nil {
		c.client.logger.Warn("Failed to persist consumer offset change", "error", err)
	}

	return nil
}

// AckRange acknowledges all messages in a contiguous range for a shard
// This is more efficient than individual acks for bulk processing
// IMPORTANT: This method only allows ACKing messages that have been read by this consumer
func (c *Consumer) AckRange(ctx context.Context, shardID uint32, fromEntry, toEntry int64) error {
	if fromEntry > toEntry {
		return fmt.Errorf("invalid range: from %d > to %d", fromEntry, toEntry)
	}

	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	// Get current consumer offset
	currentOffset, exists := shard.index.ConsumerOffsets[c.group]
	if !exists {
		currentOffset = 0
	}

	// Check if we're trying to ACK beyond what we've read
	c.highestReadMu.RLock()
	highestRead, hasRead := c.highestRead[shardID]
	c.highestReadMu.RUnlock()

	if hasRead && toEntry > highestRead {
		shard.mu.Unlock()
		return fmt.Errorf("cannot ACK range [%d,%d]: consumer has only read up to entry %d",
			fromEntry, toEntry, highestRead)
	}

	// Also warn if there's a gap from current offset
	if fromEntry > currentOffset && c.client.logger != nil {
		c.client.logger.Warn("AckRange might be skipping entries",
			"consumerGroup", c.group,
			"rangeStart", fromEntry,
			"currentOffset", currentOffset,
			"gap", fromEntry-currentOffset)
	}

	// This is actually moving the offset backwards, which might be intentional
	// for reprocessing scenarios, so we'll allow it with a warning
	newOffset := toEntry + 1
	if newOffset < currentOffset && c.client.logger != nil {
		c.client.logger.Warn("AckRange moving offset backwards",
			"consumerGroup", c.group,
			"currentOffset", currentOffset,
			"newOffset", newOffset)
	}

	shard.index.ConsumerOffsets[c.group] = newOffset
	// Mark that we need a checkpoint
	shard.writesSinceCheckpoint++
	shard.mu.Unlock()

	// Persist consumer offset change immediately to prevent data loss
	if err := shard.persistIndex(); err != nil && c.client.logger != nil {
		c.client.logger.Warn("Failed to persist consumer offset change", "error", err)
	}

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
	if state := shard.loadState(); state != nil {
		currentTimestamp := state.GetLastIndexUpdate()
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

	// Copy consumer offsets, but only include offsets for this consumer's group
	// This prevents consumer group offset leakage between different client instances
	activeGroups := c.client.getActiveGroups()
	for group, offset := range shard.index.ConsumerOffsets {
		if activeGroups[group] {
			stats.ConsumerOffsets[group] = offset
		}
	}

	if IsDebug() && c.client.logger != nil {
		c.client.logger.Debug("GetShardStats filtered consumer offsets",
			"consumerGroup", c.group,
			"shardID", shardID,
			"activeGroups", activeGroups,
			"allIndexOffsets", shard.index.ConsumerOffsets,
			"filteredOffsets", stats.ConsumerOffsets)
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
		return nil, fmt.Errorf("stream pattern must end with :* for wildcard matching\n"+
			"Examples:\n"+
			"  ✓ events:v1:shard:*  (matches events:v1:shard:0000, events:v1:shard:0001, etc.)\n"+
			"  ✓ logs:*:*:*         (matches any 4-part stream starting with 'logs')\n"+
			"  ✗ events:v1:shard    (missing :*)\n"+
			"  ✗ events:v1:*:shard  (wildcard not at end)\n"+
			"Got: %s", streamPattern)
	}

	baseStream := strings.TrimSuffix(streamPattern, "*")

	// Discover all available shards
	// Discover shards by scanning existing shard directories
	// This is much more efficient than checking every possible shard ID
	var allShards []uint32

	// Use filesystem-based discovery for efficiency - check for existing shard directories
	// This is much faster than calling Len() on every possible shard
	shardDirs, err := filepath.Glob(filepath.Join(c.client.dataDir, "shard-*"))
	if err != nil {
		return nil, fmt.Errorf("failed to scan for shard directories: %w", err)
	}

	for _, shardDir := range shardDirs {
		// Extract shard ID from directory name (e.g., "shard-0166" -> 166)
		dirName := filepath.Base(shardDir)
		if !strings.HasPrefix(dirName, "shard-") {
			continue
		}

		shardIDStr := strings.TrimPrefix(dirName, "shard-")
		var shardID uint64
		if _, parseErr := fmt.Sscanf(shardIDStr, "%04d", &shardID); parseErr == nil {
			// Verify this shard matches our stream pattern by checking if data exists
			streamName := fmt.Sprintf("%s%04d", baseStream, shardID)
			if length, lenErr := c.client.Len(context.Background(), streamName); lenErr == nil && length > 0 {
				allShards = append(allShards, uint32(shardID))
			}
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
