# Process ID Helper Functions

Comet provides helper functions for automatic process ID assignment in multi-process deployments. This is useful when you don't have explicit control over process startup order.

## Overview

The `GetProcessID()` function uses a shared memory file to coordinate process ID assignment across multiple processes that may start in any order. This is particularly useful for:

- Process managers (PM2, Supervisor)
- Manual process spawning
- Go Fiber's prefork mode

## Usage

### Simple API (Recommended)

The easiest way to use multi-process coordination is with `NewMultiProcessClient`:

```go
package main

import (
    "log"
    "github.com/orbiterhq/comet"
)

func main() {
    // Create a multi-process client - automatically handles process ID coordination
    client, err := comet.NewMultiProcessClient("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close() // Automatically releases process ID

    // Use client normally - it's already configured for multi-process mode
    // Process ID assignment and cleanup is handled automatically
}
```

### With Custom Shared Memory File

```go
// For multiple independent deployments on the same machine
client, err := comet.NewMultiProcessClientWithFile("./data", "/tmp/myapp-slots")
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Using Config Directly

```go
// For advanced customization, create config first
config, err := comet.MultiProcessCometConfig("/tmp/myapp-slots")
if err != nil {
    log.Fatal(err)
}

// Customize other config options
config.Storage.MaxFileSize = 512 << 20  // 512MB files
config.Retention.MaxAge = time.Hour     // 1 hour retention

client, err := comet.NewClient("./data", config)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Manual API (Advanced)

For more control, you can manually handle process ID coordination:

```go
package main

import (
    "log"
    "runtime"
    "github.com/orbiterhq/comet"
)

func main() {
    // Get a unique process ID (0 to NumCPU-1)
    processID := comet.GetProcessID()
    if processID < 0 {
        log.Fatal("Failed to acquire process ID")
    }

    // Create multi-process configuration
    config := comet.MultiProcessConfig(processID, runtime.NumCPU())

    // Initialize Comet client
    client, err := comet.NewClient("./data", config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Use client normally...

    // Optional: Release process ID on graceful shutdown
    defer comet.ReleaseProcessID()
}
```

### Custom Shared Memory File

For multiple independent deployments on the same machine:

```go
// Use a deployment-specific shared memory file
processID := comet.GetProcessID("/tmp/my-app-worker-slots")
if processID < 0 {
    log.Fatal("Failed to acquire process ID")
}

// Remember to release with the same file
defer comet.ReleaseProcessID("/tmp/my-app-worker-slots")
```

### With Go Fiber

The new API makes Fiber integration dead simple:

```go
package main

import (
    "log"
    "github.com/gofiber/fiber/v2"
    "github.com/orbiterhq/comet"
)

func main() {
    // Dead simple - just create the client and let it handle everything
    client, err := comet.NewMultiProcessClient("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close() // Automatically releases process ID

    app := fiber.New(fiber.Config{
        Prefork: true,
    })

    // Your routes here...
    app.Get("/", func(c *fiber.Ctx) error {
        // Use client normally - process coordination is automatic
        return c.SendString("Hello from process!")
    })

    app.Listen(":3000")
}
```

For custom deployments:

```go
// Custom shared memory file to avoid conflicts
client, err := comet.NewMultiProcessClientWithFile("./data", "/tmp/myapp-slots")
```

## How It Works

1. **Shared Memory File**: Creates a memory-mapped file at `/tmp/comet-worker-slots-worker-slots` (or custom path)
2. **Slot Size**: Each slot is 8 bytes containing the process PID
3. **Atomic Operations**: Uses atomic compare-and-swap for thread-safe slot acquisition
4. **Dead Process Detection**: Checks if existing PIDs are still alive using `kill(pid, 0)`
5. **Automatic Cleanup**: Dead processes' slots are automatically reclaimed

## File Layout

```
Slot 0: [PID (4 bytes)] [Padding (4 bytes)]
Slot 1: [PID (4 bytes)] [Padding (4 bytes)]
...
Slot N: [PID (4 bytes)] [Padding (4 bytes)]
```

Where N = `runtime.NumCPU() - 1`

## Return Values

- `0` to `NumCPU-1`: Successfully acquired process ID
- `-1`: Failed to acquire a process ID (all slots taken or error)

## Error Scenarios

The function returns `-1` in these cases:

1. **All slots occupied**: More processes than CPU cores are running
2. **File system errors**: Cannot create or access the shared memory file
3. **Memory mapping errors**: System doesn't support mmap operations

## Best Practices

### 1. Graceful Shutdown

Always release the process ID on graceful shutdown:

```go
// Set up signal handling
c := make(chan os.Signal, 1)
signal.Notify(c, os.Interrupt, syscall.SIGTERM)

go func() {
    <-c
    log.Println("Releasing process ID...")
    comet.ReleaseProcessID()
    os.Exit(0)
}()
```

### 2. Error Handling

Handle the case where process ID acquisition fails:

```go
processID := comet.GetProcessID()
if processID < 0 {
    // Fallback strategies:
    // 1. Use single-process mode
    config := comet.DefaultCometConfig()

    // 2. Or retry with backoff
    // time.Sleep(time.Second)
    // processID = comet.GetProcessID()
}
```

### 3. Deployment-Specific Files

Use deployment-specific shared memory files to avoid conflicts:

```go
shmFile := fmt.Sprintf("/tmp/myapp-%s-worker-slots", deploymentName)
processID := comet.GetProcessID(shmFile)
```

## Testing

### Unit Tests

```bash
go test -run=TestGetProcessID -v
```

### Integration Tests (Multi-Process)

```bash
go test -tags=integration -run=TestGetProcessID -v
```

The integration tests spawn actual OS processes to verify:

- Unique process ID assignment
- Process restart scenarios
- Failure recovery (dead process slot reclamation)
- Concurrent process startup

## Troubleshooting

### Permission Errors

If you get permission errors, ensure `/tmp` is writable or use a custom path:

```go
processID := comet.GetProcessID("/var/run/myapp/worker-slots")
```

### Container Environments

In containerized environments, ensure the shared memory file is on a shared volume if processes run in separate containers.

### High Process Count

If you need more processes than CPU cores, consider using a custom process count:

```go
processCount := 16  // More than NumCPU
config := comet.MultiProcessConfig(processID, processCount)
```

But ensure your shared memory file path supports this:

```go
// This helper assumes NumCPU slots, so use custom implementation
// for higher process counts
```

## Performance

- **Process ID acquisition**: ~1-5μs per call
- **Memory usage**: 8 bytes × NumCPU (typically 64-512 bytes)
- **File system overhead**: One small file per deployment

The operation is fast enough to be called during application startup without noticeable delay.
