# Comet Troubleshooting Guide

This guide helps diagnose and resolve common issues with Comet.

## Common Issues

### High Write Latency

**Symptoms**: Write operations taking longer than expected (>10μs)

**Possible Causes**:

1. **Disk I/O bottleneck** - Check disk performance with `iostat`
2. **Compression overhead** - Large entries being compressed
3. **Lock contention** - Multiple writers on same shard

**Solutions**:

```go
// 1. Increase batch size to amortize syscall overhead
ids, err := client.Append(ctx, stream, largeBatch)

// 2. Adjust compression threshold for your data
config := comet.DefaultCometConfig()
config.Compression.MinSize = 2048  // Only compress entries > 2KB

// 3. Use more shards to reduce contention
stream := comet.PickShardStream(key, "events", "v1", 32) // 32 shards instead of 16
```

### Consumer Falling Behind

**Symptoms**: Increasing lag between producer and consumer

**Diagnosis**:

```go
stats := consumer.GetStats()
for stream, stat := range stats {
    fmt.Printf("Stream %s: lag=%d entries, oldest=%v\n",
        stream, stat.Lag, stat.OldestUnprocessed)
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
fmt.Printf("Total size: %d MB\n", stats.TotalBytes/1024/1024)
fmt.Printf("Shard sizes: %+v\n", stats.ShardSizes)
```

**Solutions**:

```go
// Reduce retention time
config.Retention.MaxAge = 2 * time.Hour  // Keep only 2 hours

// Reduce per-shard size limit
config.Retention.MaxShardSize = 500 * 1024 * 1024  // 500MB per shard

// Force immediate cleanup
client.RunRetention(ctx)
```

### Multi-Process Coordination Issues

**Symptoms**: Corrupted data, missing entries, or panics in prefork mode

**Diagnosis**:

```bash
# Check for multiple processes accessing same directory
lsof | grep /var/lib/comet

# Verify file locking is enabled
# Should see .lock files in data directory
ls -la /var/lib/comet/*.lock
```

**Solution**:

```go
// Enable multi-process mode
config := comet.MultiProcessConfig()
client, err := comet.NewClientWithConfig("/data", config)
```

### Recovery After Crash

**Symptoms**: Consumer restarts from beginning or skips entries

**Check consumer state**:

```bash
# Consumer offsets are stored in .offset files
cat /var/lib/comet/*.offset
```

**Manual recovery**:

```go
// Reset consumer to specific position
consumer.SeekToTimestamp(ctx, time.Now().Add(-1*time.Hour))

// Or start from beginning
consumer.SeekToBeginning(ctx)
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
fmt.Printf("Index entries: %d\n", stats.IndexEntries)
fmt.Printf("Readers cached: %d\n", stats.ReadersCached)
fmt.Printf("Memory mapped: %d MB\n", stats.MappedMemory/1024/1024)
```

### Lock Contention

```bash
# Run with mutex profiling
GODEBUG=mutexprofile=1 ./your-app
```

## Data Recovery

### Verify segment integrity

```go
// Coming soon: comet verify command
// For now, check segment headers manually
```

### Export data for analysis

```go
reader := client.NewReader("events:v1:shard:0")
entries, err := reader.ReadRange(0, 1000)
for _, entry := range entries {
    fmt.Printf("%d: %s\n", entry.ID, entry.Data)
}
```

## Getting Help

1. Check error messages - Comet provides detailed error context
2. Enable debug logging - Set environment variable `COMET_DEBUG=1`
3. Review metrics - Use `client.GetStats()` for internal state
4. File an issue - Include Comet version, config, and error messages

## Common Error Messages

### "failed to acquire file lock"

Multi-process mode not enabled. See Multi-Process Coordination section.

### "index entry limit exceeded"

Too many entries without rotation. Reduce `MaxFileSize` or increase `MaxIndexEntries`.

### "compression failed: data too large"

Entry exceeds maximum size. Split large entries or increase limits.

### "consumer group not found"

Consumer group doesn't exist. It's created on first use.

### "checksum mismatch"

Data corruption detected. Check disk health and file system errors.
