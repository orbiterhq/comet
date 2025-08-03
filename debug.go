package comet

import (
	"os"
	"strings"
	"sync/atomic"
)

// debugEnabled controls whether debug logging is enabled (using atomic for thread safety)
var debugEnabled int32

func init() {
	// Check environment variable for debug mode
	debugEnv := os.Getenv("COMET_DEBUG")
	if debugEnv != "" && debugEnv != "0" && strings.ToLower(debugEnv) != "false" {
		atomic.StoreInt32(&debugEnabled, 1)
	}
}

// Debug is a variable that should be accessed via IsDebug() for thread safety
// Deprecated: Use IsDebug() instead for thread-safe access
var Debug = IsDebug()

// SetDebug allows runtime control of debug mode
func SetDebug(enabled bool) {
	if enabled {
		atomic.StoreInt32(&debugEnabled, 1)
	} else {
		atomic.StoreInt32(&debugEnabled, 0)
	}
}

// IsDebug returns whether debug mode is enabled (thread-safe)
func IsDebug() bool {
	return atomic.LoadInt32(&debugEnabled) == 1
}
