package comet

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// LogLevel represents the severity of a log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// Logger is the interface for Comet's logging needs.
// It's designed to be simple and easy to adapt to various logging libraries.
type Logger interface {
	// Debug logs a debug message with optional key-value pairs
	Debug(msg string, keysAndValues ...any)

	// Info logs an informational message with optional key-value pairs
	Info(msg string, keysAndValues ...any)

	// Warn logs a warning message with optional key-value pairs
	Warn(msg string, keysAndValues ...any)

	// Error logs an error message with optional key-value pairs
	Error(msg string, keysAndValues ...any)

	// WithContext returns a logger with the given context
	WithContext(ctx context.Context) Logger

	// WithFields returns a logger with the given fields attached
	WithFields(keysAndValues ...any) Logger
}

// NoOpLogger is a logger that discards all log messages
type NoOpLogger struct{}

var _ Logger = NoOpLogger{}

func (NoOpLogger) Debug(msg string, keysAndValues ...any)   {}
func (NoOpLogger) Info(msg string, keysAndValues ...any)    {}
func (NoOpLogger) Warn(msg string, keysAndValues ...any)    {}
func (NoOpLogger) Error(msg string, keysAndValues ...any)   {}
func (n NoOpLogger) WithContext(ctx context.Context) Logger { return n }
func (n NoOpLogger) WithFields(keysAndValues ...any) Logger { return n }

// StdLogger is a simple logger that writes to stdout/stderr
type StdLogger struct {
	level  LogLevel
	writer io.Writer
	fields []any
}

var _ Logger = (*StdLogger)(nil)

// NewStdLogger creates a new standard logger
func NewStdLogger(level LogLevel) *StdLogger {
	return &StdLogger{
		level:  level,
		writer: os.Stderr,
	}
}

func (s *StdLogger) log(level LogLevel, levelStr, msg string, keysAndValues ...any) {
	if level < s.level {
		return
	}

	// Combine fields with keysAndValues
	allFields := append(s.fields, keysAndValues...)

	// Format the message
	output := fmt.Sprintf("[%s] %s", levelStr, msg)

	// Add fields if any
	if len(allFields) > 0 {
		output += " {"
		for i := 0; i < len(allFields); i += 2 {
			if i > 0 {
				output += ", "
			}
			if i+1 < len(allFields) {
				output += fmt.Sprintf("%v=%v", allFields[i], allFields[i+1])
			} else {
				output += fmt.Sprintf("%v=<missing>", allFields[i])
			}
		}
		output += "}"
	}

	fmt.Fprintln(s.writer, output)
}

func (s *StdLogger) Debug(msg string, keysAndValues ...any) {
	s.log(LogLevelDebug, "DEBUG", msg, keysAndValues...)
}

func (s *StdLogger) Info(msg string, keysAndValues ...any) {
	s.log(LogLevelInfo, "INFO", msg, keysAndValues...)
}

func (s *StdLogger) Warn(msg string, keysAndValues ...any) {
	s.log(LogLevelWarn, "WARN", msg, keysAndValues...)
}

func (s *StdLogger) Error(msg string, keysAndValues ...any) {
	s.log(LogLevelError, "ERROR", msg, keysAndValues...)
}

func (s *StdLogger) WithContext(ctx context.Context) Logger {
	// For simplicity, we don't use context in StdLogger
	return s
}

func (s *StdLogger) WithFields(keysAndValues ...any) Logger {
	newLogger := &StdLogger{
		level:  s.level,
		writer: s.writer,
		fields: append(s.fields, keysAndValues...),
	}
	return newLogger
}

// SlogAdapter adapts slog.Logger to the Comet Logger interface
type SlogAdapter struct {
	logger *slog.Logger
}

var _ Logger = (*SlogAdapter)(nil)

// NewSlogAdapter creates a new adapter for slog.Logger
func NewSlogAdapter(logger *slog.Logger) *SlogAdapter {
	return &SlogAdapter{logger: logger}
}

func (s *SlogAdapter) Debug(msg string, keysAndValues ...any) {
	s.logger.Debug(msg, keysAndValues...)
}

func (s *SlogAdapter) Info(msg string, keysAndValues ...any) {
	s.logger.Info(msg, keysAndValues...)
}

func (s *SlogAdapter) Warn(msg string, keysAndValues ...any) {
	s.logger.Warn(msg, keysAndValues...)
}

func (s *SlogAdapter) Error(msg string, keysAndValues ...any) {
	s.logger.Error(msg, keysAndValues...)
}

func (s *SlogAdapter) WithContext(ctx context.Context) Logger {
	// slog doesn't have built-in context support in the same way
	// You could extract values from context and add as fields if needed
	return s
}

func (s *SlogAdapter) WithFields(keysAndValues ...any) Logger {
	// Create a new logger with additional fields
	args := make([]any, 0, len(keysAndValues))
	args = append(args, keysAndValues...)
	newLogger := s.logger.With(args...)
	return &SlogAdapter{logger: newLogger}
}

// Helper function to create appropriate logger based on config
func createLogger(config LogConfig) Logger {
	if config.Logger != nil {
		return config.Logger
	}

	switch config.Level {
	case "none", "off":
		return NoOpLogger{}
	default:
		level := LogLevelInfo
		switch config.Level {
		case "debug":
			level = LogLevelDebug
		case "warn", "warning":
			level = LogLevelWarn
		case "error":
			level = LogLevelError
		}
		return NewStdLogger(level)
	}
}
