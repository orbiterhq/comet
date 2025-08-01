# Comet Security Considerations

**Important**: Comet is a high-performance storage engine with **no built-in security features**. This document provides guidance on securing Comet deployments at the infrastructure and application layers.

## Do You Need This Document?

**For most users: NO**. If you're using Comet for:

- Local development
- Non-sensitive observability data (logs, metrics)
- Data that's already public or non-confidential
- Temporary buffering before shipping to secure storage

Then the standard deployment is fine. Comet's lack of security features is by design - it optimizes purely for performance.

**You SHOULD read this if:**

- Storing sensitive data (PII, credentials, financial data)
- Deploying in shared/multi-tenant environments
- Subject to compliance requirements (GDPR, HIPAA, etc.)
- Running in production with untrusted access

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

Comet creates all files with hardcoded 0644 permissions. This cannot be configured. For sensitive data, you must:

1. Set restrictive permissions on the parent directory
2. Use filesystem-level encryption
3. Run Comet with a dedicated user account

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
config.Indexing.MaxIndexEntries = 10000   // Limit index size
config.Storage.MaxFileSize = 100 << 20    // 100MB segments
```

### Disk Exhaustion

Prevent disk filling:

```go
config.Retention.MaxAge = 4 * time.Hour
config.Retention.MaxShardSize = 1 << 30        // 1GB per shard
config.Retention.MaxTotalSize = 10 << 30       // 10GB total
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

**Note**: Comet does not validate stream names for path traversal. However, this is not a security risk because Comet only extracts the numeric shard ID from the stream name and constructs file paths using that ID. The stream name itself is never used in file paths.

For consistency, you may want to validate stream names in your application:

```go
// Example validation (not enforced by Comet)
func validateStreamName(name string) error {
    // Ensure it matches expected format
    if !strings.HasPrefix(name, "namespace:version:shard:") {
        return errors.New("invalid stream format")
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

// Calculate rates manually
errorRate := float64(stats.ErrorCount) / float64(stats.TotalEntries)
if errorRate > 0.01 {  // >1% errors
    alert("High error rate detected")
}

// Track writes over time
writesPerSecond := float64(stats.TotalEntries) / time.Since(startTime).Seconds()
if writesPerSecond > expectedMax {
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

## Data at Rest

**Comet provides no encryption at rest**. All data is stored in plaintext. For sensitive data:

1. **Use encrypted filesystems** (LUKS on Linux, FileVault on macOS)
2. **Encrypt before writing** - Implement encryption in your application
3. **Use cloud provider encryption** (EBS encryption on AWS)

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

**Infrastructure Level:**

- [ ] Restrict data directory permissions (750 or more restrictive)
- [ ] Use dedicated user account for Comet process
- [ ] Enable filesystem encryption for sensitive data
- [ ] Configure file descriptor limits (`ulimit -n`)
- [ ] Set up disk space monitoring and alerts

**Application Level:**

- [ ] Implement authentication/authorization in your service
- [ ] Encrypt sensitive data before calling `Append()`
- [ ] Validate entry sizes to prevent resource exhaustion
- [ ] Configure appropriate retention policies
- [ ] Implement audit logging for compliance

**Operational:**

- [ ] Regular encrypted backups
- [ ] Monitor error rates and unusual patterns
- [ ] Document data classification and retention policies
- [ ] Test recovery procedures

## Limitations and Honest Disclosure

Comet is designed for performance, not security. It lacks:

- **No encryption** - All data stored in plaintext
- **No authentication** - Any process with file access can read/write
- **No authorization** - No access control mechanisms
- **No audit logging** - Must be implemented at application layer
- **Fixed file permissions** - Always 0644, not configurable
- **No network security** - Comet is embedded, not networked

## Reporting Security Issues

If you discover a security vulnerability in Comet, please open an issue on GitHub. Since Comet has no network interface and requires filesystem access, most security issues would be in the deployment configuration rather than Comet itself.

1. Description of the vulnerability
2. Steps to reproduce
3. Potential impact
4. Suggested fix (if any)
