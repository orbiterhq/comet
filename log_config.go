package comet

// LogConfig controls logging behavior
type LogConfig struct {
	// Logger allows injecting a custom logger
	// If nil, a default logger will be created based on Level
	Logger Logger `json:"-"`

	// Level controls log level when using default logger
	// Options: "debug", "info", "warn", "error", "none"
	Level string `json:"level"`

	// EnableDebug turns on debug mode (can also be set via COMET_DEBUG env var)
	EnableDebug bool `json:"enable_debug"`
}
