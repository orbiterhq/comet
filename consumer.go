package comet

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	readers sync.Map // Cached readers per shard (optimized for read-heavy workload)
	shards  sync.Map // Track shards that have been accessed (for Close persistence)

	// Track highest read entry per shard for AckRange validation
	highestReadMu sync.RWMutex
	highestRead   map[uint32]int64 // shardID -> highest entry read

	// In-memory consumer offsets for immediate read-after-ACK consistency
	memOffsetsMu sync.RWMutex
	memOffsets   map[uint32]int64 // shardID -> consumer offset (in-memory view)

	// Idempotent processing: track processed messages to prevent duplicates
	processedMsgsMu sync.RWMutex
	processedMsgs   map[MessageID]bool // messageID -> processed flag

	// Deterministic assignment
	consumerID    int
	consumerCount int

	// Background ACK flushing
	flushMu   sync.Mutex
	flushDone chan struct{}
	flushWg   sync.WaitGroup

	// Strings last
	group string
}

// ConsumerOptions configures a consumer
type ConsumerOptions struct {
	Group         string
	ConsumerID    int // 0, 1, 2, etc. for deterministic shard assignment
	ConsumerCount int // Total consumers in this group for deterministic shard assignment
}

// isShardAssigned checks if this shard is deterministically assigned to this consumer
func (c *Consumer) isShardAssigned(shardID uint32) bool {
	// If no consumer coordination is configured, default to claiming all shards
	if c.consumerCount <= 0 {
		return true
	}

	// Deterministic assignment: shard goes to consumer (shardID % consumerCount)
	return shardID%uint32(c.consumerCount) == uint32(c.consumerID)
}

// getAssignedShards returns shards deterministically assigned to this consumer
func (c *Consumer) getAssignedShards(candidateShards []uint32) []uint32 {
	var assigned []uint32
	for _, shardID := range candidateShards {
		if c.isShardAssigned(shardID) {
			assigned = append(assigned, shardID)
		}
	}
	return assigned
}

// findExistingShards scans filesystem for existing shard directories
func (c *Consumer) findExistingShards() ([]uint32, error) {
	var shards []uint32

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
			shards = append(shards, uint32(shardID))
		}
	}

	return shards, nil
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

	// Since processes own their shards exclusively, they own all consumer groups
	// No need to register/track active groups
	// client.registerConsumerGroup(group)

	consumer := &Consumer{
		client:        client,
		group:         group,
		consumerID:    opts.ConsumerID,
		consumerCount: opts.ConsumerCount,
		highestRead:   make(map[uint32]int64),
		memOffsets:    make(map[uint32]int64),
		processedMsgs: make(map[MessageID]bool),
		flushDone:     make(chan struct{}),
		// readers sync.Map is zero-initialized and ready to use
	}

	// Start background ACK flusher for durability
	consumer.flushWg.Add(1)
	go consumer.backgroundFlush()

	return consumer
}

// backgroundFlush periodically persists consumer offsets for durability
func (c *Consumer) backgroundFlush() {
	defer c.flushWg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		// Check if we should exit
		c.flushMu.Lock()
		done := c.flushDone
		c.flushMu.Unlock()

		if done == nil {
			return
		}

		select {
		case <-ticker.C:
			// Flush any pending ACKs
			ctx := context.Background()
			if err := c.FlushACKs(ctx); err != nil && c.client.logger != nil {
				// Only log if it's not a "directory not found" error (common in tests)
				if !os.IsNotExist(err) && !strings.Contains(err.Error(), "no such file or directory") {
					c.client.logger.Warn("Background ACK flush failed", "error", err)
				}
			}
		case <-done:
			return
		}
	}
}

// Close closes the consumer and releases all resources
func (c *Consumer) Close() error {
	// Stop background flusher first to prevent races
	c.flushMu.Lock()
	if c.flushDone != nil {
		// Only close channel if it hasn't been closed already
		select {
		case <-c.flushDone:
			// Already closed
		default:
			close(c.flushDone)
			c.flushDone = nil // Prevent double close
		}
	}
	c.flushMu.Unlock()

	// Wait for background goroutine to exit
	c.flushWg.Wait()

	// Close all cached readers
	c.readers.Range(func(key, value any) bool {
		if reader, ok := value.(*Reader); ok {
			reader.Close()
			// Decrement active readers count
			shardID := key.(uint32)
			if shard, err := c.client.getOrCreateShard(shardID); err == nil {
				if state := shard.state; state != nil {
					atomic.AddUint64(&state.ActiveReaders, ^uint64(0)) // Decrement by 1
				}
			}
		}
		return true
	})

	// Persist any pending consumer offsets before closing
	// This is critical to prevent ACK loss when consumer restarts
	// Note: We don't wait for shard background operations (like periodic flush)
	// since those are managed by the client, not the consumer

	// Now persist any remaining changes synchronously
	c.shards.Range(func(key, value any) bool {
		shardID := key.(uint32)
		if shard, err := c.client.getOrCreateShard(shardID); err == nil {
			// Clone index while holding lock
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

			// Clone the index to avoid holding lock during persist
			indexCopy := shard.cloneIndex()
			shard.mu.Unlock()

			// Return index to pool after use
			defer returnIndexToPool(indexCopy)

			// Persist without holding the lock
			shard.indexMu.Lock()
			err := shard.saveBinaryIndex(indexCopy)
			shard.indexMu.Unlock()

			if err != nil && c.client.logger != nil {
				c.client.logger.Warn("Failed to persist consumer offsets on close",
					"error", err, "shard", shardID, "group", c.group)
			}
		}
		return true
	})

	// Since processes own their shards exclusively, they own all consumer groups
	// No need to deregister
	// c.client.deregisterConsumerGroup(c.group)

	return nil
}

// isMessageProcessed checks if a message has already been processed by this consumer group
func (c *Consumer) isMessageProcessed(messageID MessageID) bool {
	c.processedMsgsMu.RLock()
	defer c.processedMsgsMu.RUnlock()
	return c.processedMsgs[messageID]
}

// markMessageProcessed marks a message as processed by this consumer group
func (c *Consumer) markMessageProcessed(messageID MessageID) {
	c.processedMsgsMu.Lock()
	defer c.processedMsgsMu.Unlock()
	c.processedMsgs[messageID] = true

	// Cleanup old entries periodically to prevent unbounded growth
	// Clean up every 1000 messages to amortize the cost
	if len(c.processedMsgs) > 1000 && len(c.processedMsgs)%1000 == 0 {
		c.cleanupProcessedMessages()
	}
}

// cleanupProcessedMessages removes entries for messages that are below the current consumer offset
// Must be called with processedMsgsMu held
func (c *Consumer) cleanupProcessedMessages() {
	// Get current consumer offsets for all shards
	shardOffsets := make(map[uint32]int64)

	c.client.mu.RLock()
	for shardID, shard := range c.client.shards {
		shard.mu.RLock()
		if offset, exists := shard.index.ConsumerOffsets[c.group]; exists {
			shardOffsets[shardID] = offset
		}
		shard.mu.RUnlock()
	}
	c.client.mu.RUnlock()

	// Remove messages that are below the consumer offset
	for msgID := range c.processedMsgs {
		if offset, exists := shardOffsets[msgID.ShardID]; exists {
			// Remove if this message is below the consumer offset
			// We keep a buffer of 100 to handle edge cases with concurrent processing
			if msgID.EntryNumber < offset-100 {
				delete(c.processedMsgs, msgID)
			}
		}
	}
}

// FilterDuplicates removes already-processed messages from a batch, returning only new messages
func (c *Consumer) FilterDuplicates(messages []StreamMessage) []StreamMessage {
	if len(messages) == 0 {
		return messages
	}

	var filteredMessages []StreamMessage
	for _, msg := range messages {
		if !c.isMessageProcessed(msg.ID) {
			filteredMessages = append(filteredMessages, msg)
		}
	}

	return filteredMessages
}

// MarkBatchProcessed marks all messages in a batch as processed
func (c *Consumer) MarkBatchProcessed(messages []StreamMessage) {
	for _, msg := range messages {
		c.markMessageProcessed(msg.ID)
	}
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

	// Pick random shards without replacement to ensure fair reading
	for i := 0; i < len(shards) && remaining > 0; i++ {
		// Pick a random shard from the remaining ones
		j := rand.Intn(len(shards) - i)
		shardID := shards[j]
		
		// Swap the used shard to the end so we don't pick it again
		shards[j], shards[len(shards)-1-i] = shards[len(shards)-1-i], shards[j]

		// Use getOrCreateShard to ensure we can read from existing shards after restart
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
		// Auto-discover shards from stream pattern using systematic polling
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

		// Consumer group coordination: get deterministically assigned shards
		shards := c.getAssignedShards(candidateShards)

		// Re-discover shards when we have no work - with systematic polling, catches new shards immediately
		if cfg.stream != "" && len(shards) == 0 {
			newShards, err := c.discoverShards(cfg.stream, cfg.consumerID, cfg.consumerCount)
			if err == nil && len(newShards) > len(candidateShards) {
				oldCount := len(candidateShards)
				candidateShards = newShards
				if IsDebug() && c.client.logger != nil {
					c.client.logger.Debug("Systematic rediscovery found new shards",
						"old_count", oldCount,
						"new_count", len(candidateShards),
						"shards", candidateShards)
				}
				// Re-run shard assignment with new candidates
				continue
			}
		}
		if len(shards) == 0 {
			// No shards available for this consumer group - wait and retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.pollInterval):
				continue
			}
		}

		// Proactively refresh indexes for all candidate shards to ensure real-time visibility
		// This catches updates from other processes even when we're not actively reading from all shards
		c.refreshShardIndexes(candidateShards)

		start := time.Now()
		messages, err := c.Read(ctx, shards, cfg.batchSize)
		if IsDebug() && c.client.logger != nil {
			c.client.logger.Debug("Consumer read attempt",
				"messages_count", len(messages),
				"batch_size", cfg.batchSize,
				"error", err)
		}
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

		// Reader exists - it will handle its own staleness detection and refresh
		// Track reader cache hit
		if shard.state != nil {
			atomic.AddUint64(&shard.state.ReaderCacheHits, 1)
		}
		return reader, nil
	}

	// Create new reader with current index - reader will refresh itself when stale
	shard.mu.RLock()
	indexSnapshot := shard.cloneIndex()
	shard.mu.RUnlock()

	newReader, err := NewReader(shard.shardID, indexSnapshot, c.client.config.Reader)
	if err != nil {
		return nil, err
	}

	// Set the state and client reference for metrics tracking and index refreshing
	if state := shard.state; state != nil {
		newReader.SetState(state)
	}
	newReader.SetClient(c.client)

	// Use LoadOrStore to handle race where multiple goroutines try to create
	if actual, loaded := c.readers.LoadOrStore(shard.shardID, newReader); loaded {
		// Another goroutine created a reader, close ours and use theirs
		newReader.Close()
		return actual.(*Reader), nil
	}

	// Track new reader creation
	if state := shard.state; state != nil {
		atomic.AddUint64(&state.TotalReaders, 1)
		atomic.AddUint64(&state.ActiveReaders, 1)
	}

	return newReader, nil
}

// readFromShard reads entries from a single shard using entry-based positioning
func (c *Consumer) readFromShard(ctx context.Context, shard *Shard, maxCount int) ([]StreamMessage, error) {
	if IsDebug() && c.client.logger != nil {
		c.client.logger.Debug("readFromShard called", "shardID", shard.shardID, "maxCount", maxCount)
	}

	// Track this shard for persistence on close
	c.shards.Store(shard.shardID, true)
	// Lock-free reader tracking
	atomic.AddInt64(&shard.readerCount, 1)
	defer atomic.AddInt64(&shard.readerCount, -1)

	// Check if index has been updated by another process using memory-mapped atomic state
	// This is much more efficient than time-based reloading since we only reload when actually needed
	var needsReload bool
	var lastKnownUpdate int64

	var currentIndexUpdate int64
	if shard.state != nil {
		currentIndexUpdate = shard.state.GetLastIndexUpdate()
		shard.mu.RLock()
		lastKnownUpdate = shard.lastIndexReload.UnixNano()
		shard.mu.RUnlock()
		needsReload = currentIndexUpdate > lastKnownUpdate

		if IsDebug() && c.client.logger != nil {
			c.client.logger.Debug("Index reload check",
				"shardID", shard.shardID,
				"currentIndexUpdate", currentIndexUpdate,
				"lastKnownUpdate", lastKnownUpdate,
				"needsReload", needsReload)
		}

		if needsReload && IsDebug() && c.client.logger != nil {
			c.client.logger.Debug("Index update detected via memory-mapped state",
				"shard", shard.shardID,
				"current_update", currentIndexUpdate,
				"last_known", lastKnownUpdate)
		}
	} else {
		if c.client.logger != nil && IsDebug() {
			c.client.logger.Debug("No shard state available for index update detection",
				"shard", shard.shardID)
		}
	}

	if needsReload {
		shard.mu.Lock()
		oldEntries := shard.index.CurrentEntryNumber

		if err := shard.loadIndex(); err != nil {
			// Log but don't fail - continue with current index
			if c.client.logger != nil {
				c.client.logger.Warn("Failed to reload index, using cached state",
					"shard", shard.shardID,
					"error", err)
			}
		} else {
			newEntries := shard.index.CurrentEntryNumber
			
			// Only update lastIndexReload if we actually loaded a non-empty index
			// This prevents marking an empty index as "up to date" when it's not
			if newEntries > 0 || len(shard.index.Files) > 0 {
				shard.lastIndexReload = time.Unix(0, currentIndexUpdate)
				
				if c.client.logger != nil && IsDebug() {
					c.client.logger.Debug("Reloaded index after state change",
						"shard", shard.shardID,
						"old_entries", oldEntries,
						"new_entries", newEntries,
						"entries_added", newEntries-oldEntries,
						"files", len(shard.index.Files),
						"updated_lastReload", true)
				}
			} else {
				if c.client.logger != nil && IsDebug() {
					c.client.logger.Debug("Reloaded empty index, NOT updating lastIndexReload",
						"shard", shard.shardID,
						"currentIndexUpdate", currentIndexUpdate,
						"keeping_lastReload", shard.lastIndexReload.UnixNano())
				}
			}
		}
		shard.mu.Unlock()
	}

	// First check in-memory offset for immediate consistency
	c.memOffsetsMu.RLock()
	memOffset, hasMemOffset := c.memOffsets[shard.shardID]
	c.memOffsetsMu.RUnlock()

	shard.mu.RLock()
	// Get consumer entry offset from persistent index
	persistedOffset, exists := shard.index.ConsumerOffsets[c.group]
	if !exists {
		persistedOffset = 0
	}

	// Use the higher of memory or persisted offset for consistency
	startEntryNum := persistedOffset
	if hasMemOffset && memOffset > startEntryNum {
		startEntryNum = memOffset
	}

	// After retention, the requested start entry might no longer exist
	// Adjust to the earliest available entry if needed
	if len(shard.index.Files) > 0 {
		earliestEntry := shard.index.Files[0].StartEntry
		if startEntryNum < earliestEntry {
			startEntryNum = earliestEntry
		}
	}

	endEntryNum := shard.index.CurrentEntryNumber // Only read durable data
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	// If we just reloaded the index, re-capture the updated endEntryNum
	if needsReload {
		shard.mu.RLock()
		endEntryNum = shard.index.CurrentEntryNumber
		shard.mu.RUnlock()

		if c.client.logger != nil && IsDebug() {
			c.client.logger.Debug("Updated endEntryNum after index reload",
				"shard", shard.shardID,
				"end_entry_num", endEntryNum)
		}
	}

	// The reader cache invalidation in getOrCreateReader now handles stale data detection
	// No need for the hacky workaround anymore

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
			// With immutable cloned indexes, errors indicate either:
			// 1. Entry doesn't exist yet (reading ahead)
			// 2. Reader index is stale (needs fresh Reader)
			// 3. Real file corruption issues

			// For "not found" errors, skip the entry in multi-process scenarios
			if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "stale") {
				// CRITICAL: Update memory offset even for skipped entries to prevent infinite loops
				c.memOffsetsMu.Lock()
				if currentMemOffset, exists := c.memOffsets[shard.shardID]; !exists || entryNum >= currentMemOffset {

					c.memOffsets[shard.shardID] = entryNum + 1 // Next entry to try
				}
				c.memOffsetsMu.Unlock()
				continue
			}
			// Track read error in CometState
			if state := shard.state; state != nil {
				atomic.AddUint64(&state.ReadErrors, 1)
			}
			return nil, fmt.Errorf("failed to read entry %d from shard %d: %w", entryNum, shard.shardID, err)

		}

		message := StreamMessage{
			Stream: fmt.Sprintf("shard:%04d", shard.shardID),
			ID:     MessageID{EntryNumber: entryNum, ShardID: shard.shardID},
			Data:   data,
		}

		messages = append(messages, message)
	}

	// Track read metrics in CometState
	if state := shard.state; state != nil && len(messages) > 0 {
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

	// Mark messages as processed immediately for deduplication
	for _, msgID := range messageIDs {
		c.markMessageProcessed(msgID)
	}

	// Group messages by shard
	shardGroups := make(map[uint32][]MessageID)
	for _, id := range messageIDs {
		shardGroups[id.ShardID] = append(shardGroups[id.ShardID], id)
	}

	// Process each shard group
	for shardID, ids := range shardGroups {
		// Find the highest entry number in this shard's batch
		var maxEntry int64 = -1
		for _, id := range ids {
			if id.EntryNumber > maxEntry {
				maxEntry = id.EntryNumber
			}
		}

		if maxEntry >= 0 {
			newOffset := maxEntry + 1

			// Update in-memory offset immediately for read consistency
			c.memOffsetsMu.Lock()
			if currentMemOffset, exists := c.memOffsets[shardID]; !exists || newOffset > currentMemOffset {
				c.memOffsets[shardID] = newOffset
			}
			c.memOffsetsMu.Unlock()

			// Get shard for persistence
			shard, err := c.client.getOrCreateShard(shardID)
			if err != nil {
				return err
			}

			// Track this shard for persistence on close
			c.shards.Store(shardID, true)

			shard.mu.Lock()
			// Check if this is a new consumer group
			currentOffset, groupExists := shard.index.ConsumerOffsets[c.group]
			if !groupExists {
				if state := shard.state; state != nil {
					atomic.AddUint64(&state.ConsumerGroups, 1)
				}
				shard.index.ConsumerOffsets[c.group] = newOffset
			} else if newOffset > currentOffset {
				// Only update if the new offset is higher
				shard.index.ConsumerOffsets[c.group] = newOffset
			}
			// Track acked entries
			if state := shard.state; state != nil {
				atomic.AddUint64(&state.AckedEntries, uint64(len(ids)))
			}
			shard.mu.Unlock()

			// Don't persist on every ACK - let the background flusher handle it
			// This preserves read-after-write consistency via in-memory offsets
			// while improving performance by batching disk writes
		}
	}

	return nil
}

// FlushACKs forces immediate persistence of all consumer offsets.
// This is useful for ensuring durability before shutdown or tests.
func (c *Consumer) FlushACKs(ctx context.Context) error {
	// Persist all shards that have been accessed
	var lastErr error
	c.shards.Range(func(key, value any) bool {
		shardID := key.(uint32)
		shard, err := c.client.getOrCreateShard(shardID)
		if err != nil {
			lastErr = err
			return true
		}

		// Force persist the current state
		if err := shard.persistIndex(); err != nil {
			lastErr = err
			if c.client.logger != nil {
				// Only log if it's not a "directory not found" error (common in tests)
				if !os.IsNotExist(err) && !strings.Contains(err.Error(), "no such file or directory") {
					c.client.logger.Warn("Failed to flush consumer offsets", "shard", shardID, "error", err)
				}
			}
		}
		return true
	})

	return lastErr
}

// Sync forces immediate persistence of any pending ACKs
func (c *Consumer) Sync(ctx context.Context) error {
	return c.FlushACKs(ctx)
}

// GetLag returns how many entries behind this consumer group is
func (c *Consumer) GetLag(ctx context.Context, shardID uint32) (int64, error) {
	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return 0, err
	}

	// Since processes own their shards exclusively, no need to check for changes

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	consumerEntry, exists := shard.index.ConsumerOffsets[c.group]
	if !exists {
		consumerEntry = 0
	}

	// Entry-based lag calculation
	lag := shard.index.CurrentEntryNumber - consumerEntry

	// Track max consumer lag
	if state := shard.state; state != nil && lag > 0 {
		// Since processes own their shards exclusively, we can use simple atomic operations
		currentMax := atomic.LoadUint64(&state.MaxConsumerLag)
		if uint64(lag) > currentMax {
			atomic.StoreUint64(&state.MaxConsumerLag, uint64(lag))
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
		entryNumber = max(shard.index.CurrentEntryNumber+entryNumber, 0)
	}

	shard.index.ConsumerOffsets[c.group] = entryNumber
	shard.mu.Unlock()

	// Update in-memory offset as well
	c.memOffsetsMu.Lock()
	c.memOffsets[shardID] = entryNumber
	c.memOffsetsMu.Unlock()

	// Clear processed messages cache when offset is reset
	// This allows messages to be re-processed after a reset
	c.processedMsgsMu.Lock()
	for msgID := range c.processedMsgs {
		// Only clear messages from this shard
		if msgID.ShardID == shardID {
			delete(c.processedMsgs, msgID)
		}
	}
	c.processedMsgsMu.Unlock()

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

	// Since processes own their shards exclusively, no need to check for changes

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	stats := &StreamStats{
		ShardID:         shardID,
		FileCount:       len(shard.index.Files),
		ConsumerOffsets: make(map[string]int64),
	}

	// Since processes own their shards exclusively, they own all consumer groups
	// Copy all consumer offsets
	maps.Copy(stats.ConsumerOffsets, shard.index.ConsumerOffsets)

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

	// Adaptive discovery: start with existing shards, expand range if needed
	var allShards []uint32
	// First, find existing shards via filesystem scan
	existingShards, err := c.findExistingShards()
	if err != nil {
		return nil, fmt.Errorf("failed to find existing shards: %w", err)
	}

	// If no existing shards, generate a small initial range for on-demand creation
	if len(existingShards) == 0 {
		// Start with a reasonable initial range (0-31) for on-demand shard creation
		for shardID := uint32(0); shardID < 32; shardID++ {
			allShards = append(allShards, shardID)
		}
		if IsDebug() && c.client.logger != nil {
			c.client.logger.Debug("No existing shards found, using initial range for on-demand creation",
				"pattern", streamPattern,
				"range", "0-31")
		}
	} else {
		// Use existing shards as base, but expand range to catch potential gaps
		allShards = existingShards

		// Find the max existing shard ID
		var maxShardID uint32 = 0
		for _, shardID := range existingShards {
			if shardID > maxShardID {
				maxShardID = shardID
			}
		}

		// Add some buffer beyond max to catch new shards
		bufferSize := uint32(16) // Add 16 more potential shards beyond the max
		for shardID := maxShardID + 1; shardID <= maxShardID+bufferSize; shardID++ {
			allShards = append(allShards, shardID)
		}

		if IsDebug() && c.client.logger != nil {
			c.client.logger.Debug("Found existing shards, expanded with buffer",
				"existing_count", len(existingShards),
				"max_existing", maxShardID,
				"total_range", len(allShards),
				"buffer", bufferSize)
		}
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

// refreshShardIndexes proactively checks and reloads indexes for all candidate shards
// This ensures real-time visibility of updates from other processes
func (c *Consumer) refreshShardIndexes(candidateShards []uint32) {
	for _, shardID := range candidateShards {
		// Get or create the shard (this may trigger on-demand creation)
		shard, err := c.client.getOrCreateShard(shardID)
		if err != nil {
			continue // Skip this shard on error
		}

		// Check if index needs reloading using the same logic as readFromShard
		var needsReload bool
		var currentIndexUpdate int64
		var lastKnownUpdate int64

		if shard.state != nil {
			currentIndexUpdate = shard.state.GetLastIndexUpdate()
			lastKnownUpdate = shard.lastIndexReload.UnixNano()
			needsReload = currentIndexUpdate > lastKnownUpdate
		}

		if needsReload {
			shard.mu.Lock()
			oldEntries := shard.index.CurrentEntryNumber

			if err := shard.loadIndex(); err != nil {
				// Log but don't fail - continue with current index
				if c.client.logger != nil && IsDebug() {
					c.client.logger.Debug("Failed to reload index during refresh, using cached state",
						"shard", shard.shardID,
						"error", err)
				}
			} else {
				newEntries := shard.index.CurrentEntryNumber
				
				// Only update lastIndexReload if we actually loaded a non-empty index
				// This prevents marking an empty index as "up to date" when it's not
				if newEntries > 0 || len(shard.index.Files) > 0 {
					shard.lastIndexReload = time.Unix(0, currentIndexUpdate)
				}

				if c.client.logger != nil && IsDebug() && newEntries > oldEntries {
					c.client.logger.Debug("Proactively reloaded index with new entries",
						"shard", shard.shardID,
						"old_entries", oldEntries,
						"new_entries", newEntries,
						"entries_added", newEntries-oldEntries)
				}
			}
			shard.mu.Unlock()
		}
	}
}
