package comet

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestLogger is a logger implementation for testing that captures output
type TestLogger struct {
	level  LogLevel
	buffer *bytes.Buffer
	t      *testing.T
}

// NewTestLogger creates a new test logger that captures output
func NewTestLogger(t *testing.T, level LogLevel) *TestLogger {
	return &TestLogger{
		level:  level,
		buffer: &bytes.Buffer{},
		t:      t,
	}
}

// Contains checks if the captured output contains the given string
func (tl *TestLogger) Contains(text string) bool {
	return strings.Contains(tl.buffer.String(), text)
}

// Reset clears the captured output
func (tl *TestLogger) Reset() {
	tl.buffer.Reset()
}

// String returns all captured output
func (tl *TestLogger) String() string {
	return tl.buffer.String()
}

// Logger interface implementation
func (tl *TestLogger) Debug(msg string, keysAndValues ...interface{}) {
	if tl.level <= LogLevelDebug {
		tl.log("DEBUG", msg, keysAndValues...)
	}
}

func (tl *TestLogger) Info(msg string, keysAndValues ...interface{}) {
	if tl.level <= LogLevelInfo {
		tl.log("INFO", msg, keysAndValues...)
	}
}

func (tl *TestLogger) Warn(msg string, keysAndValues ...interface{}) {
	if tl.level <= LogLevelWarn {
		tl.log("WARN", msg, keysAndValues...)
	}
}

func (tl *TestLogger) Error(msg string, keysAndValues ...interface{}) {
	if tl.level <= LogLevelError {
		tl.log("ERROR", msg, keysAndValues...)
	}
}

func (tl *TestLogger) WithContext(ctx context.Context) Logger {
	return tl // Simple implementation for tests
}

func (tl *TestLogger) WithFields(keysAndValues ...interface{}) Logger {
	// For simplicity, return same logger - could be enhanced if needed
	return tl
}

func (tl *TestLogger) log(level, msg string, keysAndValues ...interface{}) {
	fmt.Fprintf(tl.buffer, "[%s] %s", level, msg)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			fmt.Fprintf(tl.buffer, " %v=%v", keysAndValues[i], keysAndValues[i+1])
		}
	}
	fmt.Fprintln(tl.buffer)
}

func TestNoOpLogger(t *testing.T) {
	logger := NoOpLogger{}

	// These should not panic
	logger.Debug("debug message")
	logger.Info("info message", "key", "value")
	logger.Warn("warn message", "count", 42)
	logger.Error("error message", "error", "something went wrong")

	// Test context and fields
	ctxLogger := logger.WithContext(context.Background())
	if ctxLogger != logger {
		t.Error("WithContext should return same NoOpLogger")
	}

	fieldLogger := logger.WithFields("field1", "value1")
	if fieldLogger != logger {
		t.Error("WithFields should return same NoOpLogger")
	}
}

func TestStdLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := &StdLogger{
		level:  LogLevelInfo,
		writer: &buf,
	}

	// Debug should not output (below level)
	logger.Debug("debug message")
	if buf.Len() > 0 {
		t.Error("Debug message should not be logged at Info level")
	}

	// Info should output
	logger.Info("info message")
	output := buf.String()
	if !strings.Contains(output, "[INFO] info message") {
		t.Errorf("Expected info message, got: %s", output)
	}
	buf.Reset()

	// Test with fields
	logger.Warn("warning", "code", 404, "message", "not found")
	output = buf.String()
	if !strings.Contains(output, "[WARN] warning") {
		t.Errorf("Expected warning message, got: %s", output)
	}
	if !strings.Contains(output, "code=404") {
		t.Errorf("Expected code field, got: %s", output)
	}
	if !strings.Contains(output, "message=not found") {
		t.Errorf("Expected message field, got: %s", output)
	}
	buf.Reset()

	// Test WithFields
	fieldLogger := logger.WithFields("request_id", "123")
	fieldLogger.Error("request failed", "status", 500)
	output = buf.String()
	if !strings.Contains(output, "request_id=123") {
		t.Errorf("Expected request_id field, got: %s", output)
	}
	if !strings.Contains(output, "status=500") {
		t.Errorf("Expected status field, got: %s", output)
	}
}

func TestSlogAdapter(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slogger := slog.New(handler)
	adapter := NewSlogAdapter(slogger)

	// Test basic logging
	adapter.Info("slog test message", "key", "value")
	output := buf.String()
	if !strings.Contains(output, "slog test message") {
		t.Errorf("Expected message in output, got: %s", output)
	}
	if !strings.Contains(output, "key=value") {
		t.Errorf("Expected key=value in output, got: %s", output)
	}
	buf.Reset()

	// Test WithFields
	fieldAdapter := adapter.WithFields("service", "comet")
	fieldAdapter.Warn("warning from service", "code", 429)
	output = buf.String()
	if !strings.Contains(output, "service=comet") {
		t.Errorf("Expected service field, got: %s", output)
	}
	if !strings.Contains(output, "code=429") {
		t.Errorf("Expected code field, got: %s", output)
	}
}

func TestCreateLogger(t *testing.T) {
	tests := []struct {
		name       string
		config     LogConfig
		expectType string
	}{
		{
			name:       "none level returns NoOpLogger",
			config:     LogConfig{Level: "none"},
			expectType: "NoOpLogger",
		},
		{
			name:       "off level returns NoOpLogger",
			config:     LogConfig{Level: "off"},
			expectType: "NoOpLogger",
		},
		{
			name:       "debug level returns StdLogger",
			config:     LogConfig{Level: "debug"},
			expectType: "*StdLogger",
		},
		{
			name:       "custom logger",
			config:     LogConfig{Logger: NoOpLogger{}},
			expectType: "NoOpLogger",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := createLogger(tt.config)
			typeName := fmt.Sprintf("%T", logger)
			if !strings.Contains(typeName, strings.TrimPrefix(tt.expectType, "*")) {
				t.Errorf("Expected %s, got %s", tt.expectType, typeName)
			}
		})
	}
}

func TestLoggingIntegration(t *testing.T) {
	// This test would verify logging integration with Client
	// but requires the full package context to run
	t.Skip("Integration test - run with full package tests")
}

func TestDebugMode(t *testing.T) {
	// Save original state
	originalDebug := IsDebug()
	defer SetDebug(originalDebug)

	// Test SetDebug
	SetDebug(true)
	if !IsDebug() {
		t.Error("Expected debug mode to be enabled")
	}

	SetDebug(false)
	if IsDebug() {
		t.Error("Expected debug mode to be disabled")
	}

	// Test environment variable (tested in init, so we just verify current state)
	// Note: Can't easily test init() behavior in unit tests
}
