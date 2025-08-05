package comet

import (
	"testing"
	"time"
)

// TestHighCompressionConfig tests the HighCompressionConfig convenience function
func TestHighCompressionConfig(t *testing.T) {
	config := HighCompressionConfig()

	// Verify compression settings
	if config.Compression.MinCompressSize != 512 {
		t.Errorf("Expected MinCompressSize 512, got %d", config.Compression.MinCompressSize)
	}

	// The config inherits other settings from DefaultCometConfig
	if config.Storage.CheckpointTime != 2000 {
		t.Errorf("Expected checkpoint time 2000ms, got %d", config.Storage.CheckpointTime)
	}
}

// TestHighThroughputConfig tests the HighThroughputConfig convenience function
func TestHighThroughputConfig(t *testing.T) {
	config := HighThroughputConfig()

	// Verify throughput optimizations
	if config.Compression.MinCompressSize != 1024*1024 {
		t.Errorf("Expected MinCompressSize 1048576, got %d", config.Compression.MinCompressSize)
	}
	if config.Storage.CheckpointTime != 10000 {
		t.Errorf("Expected checkpoint time 10000ms, got %d", config.Storage.CheckpointTime)
	}
}

// TestParseMessageID tests the ParseMessageID function
func TestParseMessageID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    MessageID
		wantErr bool
	}{
		{
			name:  "valid ID",
			input: "123-456",
			want:  MessageID{ShardID: 123, EntryNumber: 456},
		},
		{
			name:    "invalid format - no dash",
			input:   "123456",
			wantErr: true,
		},
		{
			name:    "invalid format - empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid shard ID",
			input:   "abc-456",
			wantErr: true,
		},
		{
			name:    "invalid entry number",
			input:   "123-def",
			wantErr: true,
		},
		{
			name:  "valid ID with large numbers",
			input: "999999-888888",
			want:  MessageID{ShardID: 999999, EntryNumber: 888888},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMessageID(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMessageID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseMessageID() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestProcessOptions tests the various ProcessOption functions
func TestProcessOptions(t *testing.T) {
	// These functions return ProcessOption functions that modify processConfig
	// We can't test them directly without using Process, but we can verify they don't panic

	// Test WithPollInterval
	opt1 := WithPollInterval(100 * time.Millisecond)
	if opt1 == nil {
		t.Error("WithPollInterval returned nil")
	}

	// Test WithBatchCallback
	callback := func(size int, duration time.Duration) {}
	opt2 := WithBatchCallback(callback)
	if opt2 == nil {
		t.Error("WithBatchCallback returned nil")
	}

	// Test WithConsumerAssignment
	opt3 := WithConsumerAssignment(2, 5)
	if opt3 == nil {
		t.Error("WithConsumerAssignment returned nil")
	}
}
