//! Per-request added latency across recording flavours.
//!
//! One in-flight request per iteration on a current-thread runtime, so the
//! Criterion sample is the wall-clock cost of a single round trip through
//! the proxy. The recording-flavour matrix highlights how much each
//! storage backend contributes to that latency.
//!
//! Run with `cargo bench -p partly-proxy-lib --bench latency`.

mod common;

use criterion::{Criterion, criterion_group, criterion_main};

use crate::common::{Recording, current_thread_runtime, do_get, http_client, spawn_proxy};

fn latency(c: &mut Criterion) {
    // Current-thread so the reported time is end-to-end on one OS thread,
    // not smeared across the multi-thread scheduler.
    let runtime = current_thread_runtime();

    let mut group = c.benchmark_group("latency");
    // One element per iteration → reported rate is requests/sec, and the
    // per-iteration time IS the latency.
    group.throughput(criterion::Throughput::Elements(1));

    for &recording in Recording::all() {
        let handle = runtime.block_on(spawn_proxy(recording));
        let url: hyper::Uri = format!("http://{}/latency", handle.proxy_addr)
            .parse()
            .expect("uri");
        let client = http_client();

        group.bench_function(recording.label(), |b| {
            let client = client.clone();
            let url = url.clone();
            b.to_async(&runtime).iter(|| {
                let client = client.clone();
                let url = url.clone();
                async move {
                    do_get(&client, &url).await;
                }
            });
        });

        runtime.block_on(handle.shutdown());
    }

    group.finish();
}

criterion_group!(benches, latency);
criterion_main!(benches);
