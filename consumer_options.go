package comet

import "time"

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
