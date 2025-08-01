package comet

import (
	"os"
	"strings"
)

// Debug controls whether debug logging is enabled
var Debug bool

func init() {
	// Check environment variable for debug mode
	debugEnv := os.Getenv("COMET_DEBUG")
	Debug = debugEnv != "" && debugEnv != "0" && strings.ToLower(debugEnv) != "false"
}

// SetDebug allows runtime control of debug mode
func SetDebug(enabled bool) {
	Debug = enabled
}

// IsDebug returns whether debug mode is enabled
func IsDebug() bool {
	return Debug
}
