## Unreleased

### Improvements
- expirable: add `AddWithTTL` for per-entry TTLs, replacing fixed
  expiration buckets with an expiry min-heap so entries with different
  lifetimes share one cache and are reaped individually on time

### Changes

### Fixed

### Security
