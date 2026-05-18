# `partly-proxy-lib`

A programmable HTTP/HTTPS proxy library for integration testing.

See [`SPECIFICATION.md`](../../SPECIFICATION.md) in the workspace root for
the full design.

## Storage backends

The recorder writes through a pluggable [`SnapshotStorage`] trait so a
single proxy instance can persist to NDJSON, SQLite, or an S3-compatible
object store. Each backend lives in its own workspace crate and is
enabled by an additive Cargo feature:

| Feature           | Default | Backend crate                       |
| ----------------- | ------- | ----------------------------------- |
| `storage-jsonl`   | yes     | `partly-proxy-storage-jsonl`        |
| `storage-sqlite`  | no      | `partly-proxy-storage-sqlite`       |
| `storage-object`  | no      | `partly-proxy-storage-object`       |

Disable the default to ship without any backend:

```toml
partly-proxy-lib = { version = "0.1", default-features = false }
```
