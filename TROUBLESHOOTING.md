# Comet Troubleshooting Guide

This guide helps diagnose and resolve common issues with Comet.

## Common Issues

### High Write Latency

**Symptoms**: Write operations taking longer than expected (>2μs for single-process, >35μs for multi-process)

**Possible Causes**:

1. **Disk I/O bottleneck** - Check disk performance with `iostat`
2. **Compression overhead** - Entries over 4KB being compressed by default
3. **Lock contention** - Multiple writers on same shard
4. **Not using batch writes** - Single entries have higher per-entry overhead
5. **Multi-process mode overhead** - Memory-mapped coordination adds ~30μs

**Solutions**:

```go
// 1. Increase batch size to amortize syscall overhead
ids, err := client.Append(ctx, stream, largeBatch)

// 2. Adjust compression threshold (default: 4KB)
config := comet.DefaultCometConfig()
config.Compression.MinCompressSize = 8192  // Only compress entries > 8KB
// Or effectively disable compression by setting very high threshold
config.Compression.MinCompressSize = 1 << 30  // 1GB threshold

// 3. Use more shards to reduce contention
stream := comet.PickShardStream(key, "events", "v1", 32) // 32 shards instead of 16
```

### Consumer Falling Behind

**Symptoms**: Increasing lag between producer and consumer

**Diagnosis**:

```go
// Check lag for specific shards
shards := []uint32{0, 1, 2, 3} // Your shard IDs
for _, shardID := range shards {
    lag, err := consumer.GetLag(ctx, shardID)
    if err == nil {
        fmt.Printf("Shard %d: lag=%d entries\n", shardID, lag)
    }
}

// Get detailed stats per shard
stats, err := consumer.GetShardStats(ctx, shardID)
if err == nil {
    fmt.Printf("Shard %d stats:\n", shardID)
    fmt.Printf("  Total entries: %d\n", stats.TotalEntries)
    fmt.Printf("  Total bytes: %d\n", stats.TotalBytes)
    fmt.Printf("  Oldest entry: %v\n", stats.OldestEntry)
    fmt.Printf("  Newest entry: %v\n", stats.NewestEntry)

    // Check consumer group offsets
    for group, offset := range stats.ConsumerOffsets {
        fmt.Printf("  Consumer group '%s' offset: %d\n", group, offset)
        fmt.Printf("  Lag: %d entries\n", stats.TotalEntries - offset)
    }
}
```

**Solutions**:

1. **Increase batch size** - Process more entries per iteration
2. **Add more workers** - Scale horizontally with consumer assignments
3. **Optimize processing** - Profile your handler function

### Disk Space Issues

**Symptoms**: "No space left on device" errors

**Check current usage**:

```go
stats := client.GetStats()
fmt.Printf("Total size: %d MB\n", stats.TotalBytes/(1024*1024))
fmt.Printf("Total files: %d\n", stats.TotalFiles)
```

**Solutions**:

```go
// Reduce retention time
config.Retention.MaxAge = 2 * time.Hour  // Keep only 2 hours

// Reduce per-shard size limit
config.Retention.MaxShardSize = 500 * 1024 * 1024  // 500MB per shard

// Check retention stats
retentionStats := client.GetRetentionStats()
fmt.Printf("Files deleted: %d, Bytes freed: %d MB\n",
    retentionStats.FilesDeleted, retentionStats.BytesDeleted/(1024*1024))

// Note: Retention runs automatically every minute
```

### Multi-Process Coordination Issues

**Symptoms**: Corrupted data, missing entries, or "resource temporarily unavailable" errors

**Diagnosis**:

```bash
# Check for multiple processes accessing same directory
lsof | grep /var/lib/comet

# Look for mmap state files (8-byte files)
ls -la /var/lib/comet/shard-*/index.state

# Check file locks
ls -la /var/lib/comet/shard-*/*.lock
```

**Common Issues**:

1. **Not enabling multi-process mode** - Must be explicitly enabled
2. **Index file conflicts** - Delete index.bin and restart to rebuild
3. **Stale mmap state** - Delete `index.state` files if corrupted

**Solution**:

```go
// Enable multi-process mode with optimized settings
config := comet.MultiProcessConfig()
client, err := comet.NewClient("/data", config)

// For Fiber prefork deployments
app := fiber.New(fiber.Config{
    Prefork: true,
})

// Each process gets its own client instance
var client *comet.Client
app.Hooks().OnFork(func() error {
    client, err = comet.NewClient("/data", comet.MultiProcessConfig())
    return err
})
```

### File Descriptor Limits

**Symptoms**: "too many open files" errors or "cannot open file" errors

**Diagnosis**:

```bash
# Check current limits
ulimit -n

# Check how many files Comet has open
lsof -p $(pgrep your-app) | grep comet | wc -l

# See system-wide limits
cat /proc/sys/fs/file-max
```

**Why this happens**:

- Each shard requires multiple file descriptors (data files, index, mmap state)
- Each reader keeps files open for memory-mapped access
- With 16 shards and multiple consumers, you can easily need 100+ file descriptors

**Solutions**:

```bash
# Temporary fix for current session
ulimit -n 65536

# Permanent fix - add to /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536

# Or in systemd service file
[Service]
LimitNOFILE=65536
```

**Application-level fixes**:

```go
// 1. Reduce number of shards if possible
stream := comet.PickShardStream(key, "events", "v1", 8) // 8 instead of 16

// 2. Close consumers when done
consumer := comet.NewConsumer(client, opts)
defer consumer.Close() // Important!

// 3. Reuse consumers instead of creating new ones
// Bad: Creating new consumer for each request
// Good: Create once, reuse for all requests
```

### Recovery After Crash

**Symptoms**: Consumer restarts from beginning or skips entries

**Check consumer state**:

```go
// Consumer offsets are stored in the index, not .offset files
// Use GetShardStats to check current offset
stats, err := consumer.GetShardStats(ctx, shardID)
if err == nil {
    // Show all consumer group offsets
    for group, offset := range stats.ConsumerOffsets {
        fmt.Printf("Consumer group '%s' at offset: %d\n", group, offset)
    }
}
```

**Manual recovery**:

```go
// Reset consumer to specific entry number
err := consumer.ResetOffset(ctx, shardID, entryNumber)

// Reset to beginning (entry 0)
err := consumer.ResetOffset(ctx, shardID, 0)

// Or acknowledge a range to skip forward
err := consumer.AckRange(ctx, shardID, 0, 1000) // Skip first 1000 entries
```

## Performance Debugging

### CPU Profile

```go
import _ "net/http/pprof"
go http.ListenAndServe("localhost:6060", nil)
// Visit http://localhost:6060/debug/pprof/
```

### Memory Profile

```go
stats := client.GetStats()
fmt.Printf("Total entries: %d\n", stats.TotalEntries)
fmt.Printf("Total bytes: %d MB\n", stats.TotalBytes/(1024*1024))
fmt.Printf("Total files: %d\n", stats.TotalFiles)
fmt.Printf("Compression ratio: %.2f\n", float64(stats.CompressionRatio)/100)

// Check memory usage via system tools
// Use pprof for detailed memory analysis
```

### Lock Contention

```bash
# Run with mutex profiling
GODEBUG=mutexprofile=1 ./your-app
```

## Monitoring & Metrics

### Built-in Stats

```go
stats := client.GetStats()
fmt.Printf("Total entries: %d\n", stats.TotalEntries)
fmt.Printf("Total bytes: %d MB\n", stats.TotalBytes/(1024*1024))
fmt.Printf("Total files: %d\n", stats.TotalFiles)
fmt.Printf("File rotations: %d\n", stats.FileRotations)
fmt.Printf("Active readers: %d\n", stats.ActiveReaders)
fmt.Printf("Error count: %d\n", stats.ErrorCount)

// Write performance
if stats.TotalEntries > 0 {
    avgLatency := time.Duration(stats.WriteLatencyNano)
    fmt.Printf("Avg write latency: %v\n", avgLatency)
}

// Compression stats
if stats.CompressedEntries > 0 {
    fmt.Printf("Compressed entries: %d\n", stats.CompressedEntries)
    fmt.Printf("Compression ratio: %.2fx\n", float64(stats.CompressionRatio)/100)
}
```

### Consumer Lag Monitoring

```go
// Monitor consumer lag in production
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    shards := comet.AllShardsRange(16) // Monitor all 16 shards

    for range ticker.C {
        for _, shardID := range shards {
            lag, err := consumer.GetLag(ctx, shardID)
            if err == nil && lag > 10000 {
                log.Printf("WARNING: Consumer lag on shard %d: %d entries",
                    shardID, lag)
            }
        }
    }
}()
```

## Data Recovery

### Verify segment integrity

```go
// Check if shard is healthy by trying to read length
length, err := client.Len(ctx, "events:v1:shard:0001")
if err != nil {
    log.Printf("Shard may be corrupted: %v", err)
    // May need to:
    // 1. Stop all processes
    // 2. Delete index.bin and index.state files
    // 3. Restart - index will be rebuilt from data files
}

// Check overall health
stats := client.GetStats()
fmt.Printf("Total shards active: %d\n", stats.TotalFiles)
fmt.Printf("Total entries: %d\n", stats.TotalEntries)
```

### Export data for analysis

```go
// Method 1: Use consumer for easy reading
consumer := comet.NewConsumer(client, comet.ConsumerOptions{
    Group: "debug-export",
})

// Read from specific shard
messages, err := consumer.Read(ctx, []uint32{1}, 10000)
for _, msg := range messages {
    fmt.Printf("Entry %d: %s\n", msg.ID.EntryNumber, msg.Data)
}

// Method 2: Read all entries from beginning
consumer.ResetOffset(ctx, 1, 0) // Reset to start
messages, _ = consumer.Read(ctx, []uint32{1}, 1000)

// Method 3: Process with handler for streaming
err = consumer.Process(ctx, func(batch []comet.StreamMessage) error {
    for _, msg := range batch {
        // Export/process each message
        fmt.Printf("%d,%d,%s\n", msg.ID.ShardID, msg.ID.EntryNumber, msg.Data)
    }
    return nil
}, comet.WithStream("events:v1:shard:0001"))
```

## Performance Tuning

### Optimization Tips

1. **Use batching** - Dramatic performance improvement with larger batches
2. **Disable compression** - Set `MinCompressSize` very high (e.g., 1GB)
3. **Pre-allocate buffers** - Reuse byte slices when possible
4. **Use appropriate shard count** - More shards = less contention
5. **Consider async writes** - Buffer writes in your application

## Getting Help

1. Check error messages - Comet provides detailed error context
2. Enable debug logging - Set environment variable `COMET_DEBUG=1`
3. Review metrics - Use `client.GetStats()` for internal state
4. Check system resources - `ulimit -n` for file descriptors, disk space
5. File an issue - Include Comet version, config, error messages, and benchmark results

## Common Error Messages

### "failed to acquire file lock"

Another process holds the lock. Either:

- Enable multi-process mode with `MultiProcessConfig()`
- Ensure previous process released the lock properly
- Check for zombie processes holding locks

### "index entry limit exceeded"

Too many entries without rotation. Reduce `MaxFileSize` or increase `MaxIndexEntries`.

### "compression failed: data too large"

Entry exceeds maximum size. Split large entries or increase limits.

### "consumer group not found"

Consumer group doesn't exist. It's created on first use.

### "checksum mismatch"

Data corruption detected. Check disk health and file system errors.

### "mmap: cannot allocate memory"

Too many memory-mapped files. Solutions:

- Increase system limits: `sysctl -w vm.max_map_count=262144`
- Reduce number of shards
- Close unused readers

### "sequence number mismatch"

Multi-process coordination issue. Delete `index.state` files and restart all processes.

### "write offset exceeds file size"

File rotation needed. This usually happens automatically, but can indicate:

- Disk full
- Permission issues
- Extremely large entries
