//! End-to-end throughput across (concurrency × recording-flavour).
//!
//! Each iteration completes a fixed batch of `BATCH` requests against
//! a real proxy listener sitting in front of the in-process echo
//! upstream. Concurrency is bounded by a `Semaphore` so that the
//! reported number reflects "this many in-flight requests at once",
//! not "fire and forget all `BATCH` and let tokio sort it out".
//!
//! Run with `cargo bench -p partly-proxy-lib --bench throughput`.

mod common;

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::{sync::Semaphore, task::JoinSet};

use crate::common::{ProxyHandle, Recording, do_get, http_client, multi_thread_runtime, spawn_proxy};

/// Requests per iteration. Big enough to amortise per-iteration tokio
/// scheduling cost; small enough that Criterion can take many samples.
const BATCH: u64 = 64;

/// Concurrency levels we sweep across. Keep this list short — every
/// entry multiplies the matrix.
const CONCURRENCY: &[usize] = &[1, 16, 128];

fn throughput(c: &mut Criterion) {
    let runtime = multi_thread_runtime();

    for &recording in Recording::all() {
        let handle: ProxyHandle = runtime.block_on(spawn_proxy(recording));
        let url: hyper::Uri = format!("http://{}/throughput", handle.proxy_addr)
            .parse()
            .expect("uri");
        let client = http_client();

        let mut group = c.benchmark_group(format!("throughput/{}", recording.label()));
        group.throughput(Throughput::Elements(BATCH));

        for &conc in CONCURRENCY {
            group.bench_with_input(BenchmarkId::from_parameter(conc), &conc, |b, &conc| {
                let client = client.clone();
                let url = url.clone();
                b.to_async(&runtime).iter(|| {
                    let client = client.clone();
                    let url = url.clone();
                    async move {
                        let sem = Arc::new(Semaphore::new(conc));
                        let mut set = JoinSet::new();
                        for _ in 0..BATCH {
                            let permit = sem.clone().acquire_owned().await.expect("permit");
                            let client = client.clone();
                            let url = url.clone();
                            set.spawn(async move {
                                do_get(&client, &url).await;
                                drop(permit);
                            });
                        }
                        while set.join_next().await.is_some() {}
                    }
                });
            });
        }

        group.finish();
        runtime.block_on(handle.shutdown());
    }
}

criterion_group!(benches, throughput);
criterion_main!(benches);
