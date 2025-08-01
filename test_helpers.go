package comet

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestLogger captures log output for testing
type TestLogger struct {
	t      *testing.T
	buffer *bytes.Buffer
	level  LogLevel
	fields []interface{}
}

// NewTestLogger creates a logger that captures output for testing
func NewTestLogger(t *testing.T, level LogLevel) *TestLogger {
	return &TestLogger{
		t:      t,
		buffer: &bytes.Buffer{},
		level:  level,
	}
}

func (tl *TestLogger) log(level LogLevel, levelStr, msg string, keysAndValues ...interface{}) {
	if level < tl.level {
		return
	}

	// Combine fields with keysAndValues
	allFields := append(tl.fields, keysAndValues...)

	// Log to test output
	tl.t.Logf("[%s] %s %v", levelStr, msg, allFields)

	// Also capture in buffer for assertions
	output := "[" + levelStr + "] " + msg
	if len(allFields) > 0 {
		output += " " + formatFields(allFields...)
	}
	tl.buffer.WriteString(output + "\n")
}

func formatFields(keysAndValues ...interface{}) string {
	var parts []string
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			parts = append(parts, fmt.Sprintf("%v=%v", keysAndValues[i], keysAndValues[i+1]))
		} else {
			parts = append(parts, fmt.Sprintf("%v=<missing>", keysAndValues[i]))
		}
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (tl *TestLogger) Debug(msg string, keysAndValues ...interface{}) {
	tl.log(LogLevelDebug, "DEBUG", msg, keysAndValues...)
}

func (tl *TestLogger) Info(msg string, keysAndValues ...interface{}) {
	tl.log(LogLevelInfo, "INFO", msg, keysAndValues...)
}

func (tl *TestLogger) Warn(msg string, keysAndValues ...interface{}) {
	tl.log(LogLevelWarn, "WARN", msg, keysAndValues...)
}

func (tl *TestLogger) Error(msg string, keysAndValues ...interface{}) {
	tl.log(LogLevelError, "ERROR", msg, keysAndValues...)
}

func (tl *TestLogger) WithContext(ctx context.Context) Logger {
	return tl
}

func (tl *TestLogger) WithFields(keysAndValues ...interface{}) Logger {
	// Create a new logger with the additional fields
	newLogger := &TestLogger{
		t:      tl.t,
		buffer: tl.buffer, // Share the same buffer
		level:  tl.level,
		fields: append(tl.fields, keysAndValues...),
	}
	return newLogger
}

// GetOutput returns all captured log output
func (tl *TestLogger) GetOutput() string {
	return tl.buffer.String()
}

// Contains checks if the log output contains a string
func (tl *TestLogger) Contains(substr string) bool {
	return strings.Contains(tl.buffer.String(), substr)
}
