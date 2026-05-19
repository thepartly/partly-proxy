//! Bytes-per-second throughput for POST bodies of increasing size.
//!
//! Stresses the proxy's body buffering (`SPECIFICATION.md` §6.1 —
//! middleware sees fully materialised `Bytes`) and, when recording is on,
//! the storage backend's append path: JSONL base64-encodes the body,
//! `SQLite` stores the JSON-serialised exchange as a BLOB.
//!
//! Run with `cargo bench -p partly-proxy-lib --bench large_body`.

mod common;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use crate::common::{Recording, do_post, http_client, multi_thread_runtime, spawn_proxy};

/// Body sizes swept. Picked to span "small JSON" through "biggest body
/// you'd reasonably push through a test proxy" — the spec calls 10 MiB
/// out as the soft upper bound (§6.1 trailing note on out-of-band bodies).
const SIZES: &[usize] = &[
    1024,             // 1 KiB
    64 * 1024,        // 64 KiB
    1024 * 1024,      // 1 MiB
    10 * 1024 * 1024, // 10 MiB
];

fn large_body(c: &mut Criterion) {
    let runtime = multi_thread_runtime();

    for &recording in Recording::all() {
        let handle = runtime.block_on(spawn_proxy(recording));
        let url: hyper::Uri = format!("http://{}/large", handle.proxy_addr)
            .parse()
            .expect("uri");
        let client = http_client();

        let mut group = c.benchmark_group(format!("large_body/{}", recording.label()));

        for &size in SIZES {
            // Allocate the body once; cloning `Bytes` is a refcount bump.
            let body = Bytes::from(vec![0xab; size]);
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
                let client = client.clone();
                let url = url.clone();
                let body = body.clone();
                b.to_async(&runtime).iter(|| {
                    let client = client.clone();
                    let url = url.clone();
                    let body = body.clone();
                    async move {
                        do_post(&client, &url, body).await;
                    }
                });
            });
        }

        group.finish();
        runtime.block_on(handle.shutdown());
    }
}

criterion_group!(benches, large_body);
criterion_main!(benches);
