# Comet Security Considerations

This document outlines security considerations when deploying Comet in production environments.

## File System Security

### Directory Permissions

Comet creates files with standard Unix permissions. Ensure proper directory ownership:

```bash
# Recommended permissions
mkdir -p /var/lib/comet
chown app:app /var/lib/comet
chmod 750 /var/lib/comet
```

### File Permissions

By default, Comet creates files with 0644 permissions. For sensitive data:

```go
config := comet.DefaultCometConfig()
config.Storage.FilePermissions = 0600  // Read/write for owner only
client, err := comet.NewClientWithConfig("/secure/data", config)
```

### Multi-Process Isolation

In multi-process mode, all processes must run as the same user to access shared files. Consider:

- Using separate data directories per service for isolation
- Running each service with its own system user
- Using filesystem ACLs for fine-grained access control

## Data Security

### Encryption at Rest

Comet does not provide built-in encryption. For sensitive data:

1. **Filesystem encryption** - Use encrypted filesystems (LUKS, FileVault)
2. **Application-level encryption** - Encrypt data before writing:

```go
encrypted := encrypt(data)
client.Append(ctx, stream, [][]byte{encrypted})
```

### Data Validation

Comet accepts arbitrary byte arrays. Always validate data:

```go
// Size limits
if len(data) > maxEntrySize {
    return errors.New("entry too large")
}

// Content validation
if !isValidJSON(data) {
    return errors.New("invalid data format")
}
```

## Resource Limits

### Memory Exhaustion

Protect against memory exhaustion:

```go
config := comet.DefaultCometConfig()
config.Indexing.MaxIndexEntries = 10_000  // Limit index size
config.Storage.MaxFileSize = 100_000_000  // 100MB segments
```

### Disk Exhaustion

Prevent disk filling:

```go
config.Retention.MaxAge = 4 * time.Hour
config.Retention.MaxShardSize = 1_000_000_000  // 1GB per shard
config.Retention.MaxTotalSize = 10_000_000_000 // 10GB total
```

### File Descriptor Limits

Monitor file descriptor usage:

```bash
# Check current limits
ulimit -n

# Increase if needed (in systemd service)
LimitNOFILE=65535
```

## Input Validation

### Stream Names

Validate stream names to prevent path traversal:

```go
func validateStreamName(name string) error {
    if strings.Contains(name, "..") || strings.Contains(name, "/") {
        return errors.New("invalid stream name")
    }
    if len(name) > 255 {
        return errors.New("stream name too long")
    }
    return nil
}
```

### Entry Size

Enforce size limits:

```go
const maxEntrySize = 10 * 1024 * 1024  // 10MB

func validateEntry(data []byte) error {
    if len(data) > maxEntrySize {
        return errors.New("entry exceeds size limit")
    }
    return nil
}
```

## Operational Security

### Monitoring

Monitor for suspicious activity:

```go
stats := client.GetStats()
if stats.ErrorRate > 0.01 {  // >1% errors
    alert("High error rate detected")
}
if stats.WritesPerSecond > expectedMax {
    alert("Unusual write volume")
}
```

### Audit Logging

Log security-relevant operations:

```go
func auditLog(user, action, stream string) {
    log.Printf("AUDIT: user=%s action=%s stream=%s time=%s",
        user, action, stream, time.Now().Format(time.RFC3339))
}
```

### Backup Security

When backing up Comet data:

1. Encrypt backups in transit and at rest
2. Limit backup retention to comply with data policies
3. Test restore procedures regularly
4. Audit backup access

## Compliance Considerations

### Data Retention

Configure retention to comply with regulations:

```go
// GDPR - Delete data after 30 days
config.Retention.MaxAge = 30 * 24 * time.Hour

// Custom retention per stream
if isUserData(stream) {
    config.Retention.MaxAge = 7 * 24 * time.Hour
}
```

### Data Deletion

Comet deletes entire segment files. For selective deletion:

1. Use shorter retention periods
2. Implement application-level tombstones
3. Consider separate streams for different data types

### Access Logging

Track data access for compliance:

```go
type AccessLog struct {
    User      string
    Stream    string
    Operation string
    Timestamp time.Time
    Success   bool
}
```

## Security Checklist

- [ ] Set appropriate file permissions on data directory
- [ ] Configure retention policies to limit data exposure
- [ ] Implement authentication/authorization in your service
- [ ] Validate all input data and stream names
- [ ] Monitor resource usage and error rates
- [ ] Encrypt sensitive data before storage
- [ ] Regular backups with encryption
- [ ] Audit logging for compliance
- [ ] File descriptor limits configured
- [ ] Disk space monitoring in place

## Reporting Security Issues

If you discover a security vulnerability in Comet, please report it responsibly by emailing security@yourcompany.com with:

1. Description of the vulnerability
2. Steps to reproduce
3. Potential impact
4. Suggested fix (if any)

We aim to respond within 48 hours and provide fixes for confirmed vulnerabilities promptly.
