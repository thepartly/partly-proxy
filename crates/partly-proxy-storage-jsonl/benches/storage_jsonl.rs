//! Direct micro-benchmarks for the NDJSON `SnapshotStorage` backend.
//!
//! Bypasses the proxy entirely — these benches measure the cost of the
//! trait surface (`append` + `flush` + `load`) on its own so that
//! regressions in the backend can be attributed without the HTTP pipeline
//! in the way.
//!
//! Run with `cargo bench -p partly-proxy-storage-jsonl`.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::TryStreamExt;
use partly_proxy_storage_jsonl::JsonlStorage;
use partly_proxy_types::{storage::SharedStorage, testing::make_exchange};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Body sizes for append benches.
const APPEND_SIZES: &[usize] = &[256, 4 * 1024, 1024 * 1024];

/// How many exchanges to write per "batch" iteration.
const BATCH: usize = 1_000;

/// How many exchanges to pre-populate for the `load` bench.
const LOAD_N: usize = 10_000;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

async fn fresh(dir: &TempDir, name: &str) -> SharedStorage {
    let path = dir.path().join(name);
    Arc::new(JsonlStorage::open(&path).await.expect("open jsonl"))
}

fn append_single(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("append_single");

    for &size in APPEND_SIZES {
        let body = vec![0xab; size];
        let exchange = make_exchange("/append", &body, 200);

        // Storage lives for the whole bench_function — many iterations
        // append into the same file, which is the realistic steady-state
        // cost we want to measure.
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = runtime.block_on(fresh(&dir, &format!("single-{size}.ndjson")));

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(&runtime).iter(|| {
                let storage = storage.clone();
                let exchange = exchange.clone();
                async move {
                    storage.append(&exchange).await.expect("append");
                }
            });
        });
    }

    group.finish();
}

fn append_batch_1000(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("append_batch_1000");

    for &size in APPEND_SIZES {
        let body = vec![0xab; size];
        let exchange = make_exchange("/append-batch", &body, 200);

        let dir = tempfile::tempdir().expect("tempdir");
        let storage = runtime.block_on(fresh(&dir, &format!("batch-{size}.ndjson")));

        // One element per appended exchange — Criterion divides per-iter
        // time by `BATCH` to give per-row throughput.
        group.throughput(Throughput::Elements(BATCH as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(&runtime).iter(|| {
                let storage = storage.clone();
                let exchange = exchange.clone();
                async move {
                    for _ in 0..BATCH {
                        storage.append(&exchange).await.expect("append");
                    }
                }
            });
        });
    }

    group.finish();
}

fn load_10k(c: &mut Criterion) {
    let runtime = runtime();

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = runtime.block_on(fresh(&dir, "load.ndjson"));

    // Populate once. Use a small body so the cost we measure is dominated
    // by the per-line parse, not by the body copy.
    runtime.block_on(async {
        for i in 0..LOAD_N {
            let ex = make_exchange(&format!("/load/{i}"), b"x", 200);
            storage.append(&ex).await.expect("append");
        }
        storage.flush().await.expect("flush");
    });

    let mut group = c.benchmark_group("load_10k");
    group.throughput(Throughput::Elements(LOAD_N as u64));
    group.sample_size(10); // each iteration parses 10k lines
    group.bench_function("jsonl", |b| {
        b.to_async(&runtime).iter(|| {
            let storage = storage.clone();
            async move {
                let loaded: Vec<_> = storage
                    .load()
                    .try_collect()
                    .await
                    .expect("load stream");
                assert_eq!(loaded.len(), LOAD_N);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, append_single, append_batch_1000, load_10k);
criterion_main!(benches);
