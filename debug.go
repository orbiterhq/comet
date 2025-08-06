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
	// Check atomic flag first (fastest path)
	if atomic.LoadInt32(&debugEnabled) == 1 {
		return true
	}
	
	// Also check environment variable dynamically (slower but handles runtime changes)
	debugEnv := os.Getenv("COMET_DEBUG")
	return debugEnv != "" && debugEnv != "0" && strings.ToLower(debugEnv) != "false"
}
