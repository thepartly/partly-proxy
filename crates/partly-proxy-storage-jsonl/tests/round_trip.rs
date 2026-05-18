//! Round-trip tests for the NDJSON snapshot backend.
//!
//! These tests stand alone — they don't use the shared
//! `partly_proxy_types::testing::run_conformance` suite (that lives in
//! `tests/conformance.rs`). They prove the basic write → reopen → read
//! cycle plus the per-line durability behaviour the JSONL backend is
//! contracted to provide.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Method};
use partly_proxy_storage_jsonl::{JsonlStorage, parse_ndjson_line};
use partly_proxy_types::{
    recorded::{ExchangeOutcome, RecordedExchange, RecordedRequest, RecordedResponse},
    storage::SnapshotStorage,
};
use tokio::io::AsyncWriteExt;

fn make_exchange(path: &str, body: &[u8], status: u16) -> RecordedExchange {
    let req = RecordedRequest::from_parts(
        &Method::POST,
        &path.parse().unwrap(),
        &HeaderMap::new(),
        Bytes::copy_from_slice(body),
    );
    let resp = RecordedResponse {
        status,
        headers: vec![("x-replay".to_owned(), "true".to_owned())],
        body: Bytes::from(format!("body-for-{path}").into_bytes()),
    };
    RecordedExchange::new(
        Some("api".to_owned()),
        req,
        ExchangeOutcome::Response(resp),
        Duration::from_millis(7),
    )
}

#[tokio::test]
async fn append_then_load_preserves_insertion_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    let storage = JsonlStorage::open(&path).await.unwrap();
    for n in 0u8..5 {
        storage
            .append(&make_exchange(&format!("/n/{n}"), &[n], 200))
            .await
            .unwrap();
    }
    storage.flush().await.unwrap();

    let exchanges: Vec<_> = storage
        .load()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(exchanges.len(), 5);
    for (i, ex) in exchanges.iter().enumerate() {
        assert_eq!(ex.request.uri, format!("/n/{i}"));
        let i_byte = u8::try_from(i).expect("0..5 fits in u8");
        assert_eq!(ex.request.body, Bytes::copy_from_slice(&[i_byte]));
    }
}

#[tokio::test]
async fn append_flushes_per_line_so_tail_f_sees_each_write() {
    // The contract: a second reader opening the same path between
    // appends sees every previously appended exchange, with no
    // additional flush() call needed.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    let storage = JsonlStorage::open(&path).await.unwrap();

    storage
        .append(&make_exchange("/a", b"a", 200))
        .await
        .unwrap();

    let raw = tokio::fs::read_to_string(&path).await.unwrap();
    assert!(
        !raw.is_empty(),
        "first append should be visible without explicit flush"
    );
    assert_eq!(raw.lines().count(), 1);
}

#[tokio::test]
async fn reopen_and_load_sees_everything_after_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    {
        let storage = JsonlStorage::open(&path).await.unwrap();
        for n in 0..3 {
            storage
                .append(&make_exchange(&format!("/x/{n}"), b"", 200))
                .await
                .unwrap();
        }
        storage.flush().await.unwrap();
    } // drop the writer

    let reader = JsonlStorage::open(&path).await.unwrap();
    let exchanges: Vec<_> = reader
        .load()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(exchanges.len(), 3);
}

#[tokio::test]
async fn load_skips_blank_lines_and_reports_lineno_on_parse_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");

    // Hand-craft a file with two valid lines, a blank line, and a
    // garbage line — we want blanks ignored and the garbage line to
    // produce a 1-indexed error message.
    let storage = JsonlStorage::open(&path).await.unwrap();
    storage
        .append(&make_exchange("/a", b"", 200))
        .await
        .unwrap();
    storage
        .append(&make_exchange("/b", b"", 200))
        .await
        .unwrap();
    drop(storage);
    let mut f = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .unwrap();
    f.write_all(b"\nnot-json\n").await.unwrap();
    f.sync_data().await.unwrap();
    drop(f);

    let reader = JsonlStorage::open(&path).await.unwrap();
    let mut stream = reader.load();
    // First two yields are Ok.
    let one = stream.next().await.unwrap().unwrap();
    assert_eq!(one.request.uri, "/a");
    let two = stream.next().await.unwrap().unwrap();
    assert_eq!(two.request.uri, "/b");
    // Blank line is skipped; the next yield is the parse error.
    let err = stream.next().await.unwrap().unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("line 4"), "expected `line 4` in error: {msg}");
}

#[tokio::test]
async fn shared_storage_arc_is_send_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.ndjson");
    let storage: Arc<dyn SnapshotStorage> = Arc::new(JsonlStorage::open(&path).await.unwrap());
    let cloned = storage.clone();
    let h = tokio::spawn(async move {
        cloned
            .append(&make_exchange("/a", b"a", 200))
            .await
            .unwrap();
    });
    h.await.unwrap();
}

#[test]
fn parse_ndjson_line_round_trips() {
    let ex = make_exchange("/p", b"payload", 201);
    let serialised = serde_json::to_string(&ex).unwrap();
    let parsed = parse_ndjson_line(&serialised, 0).unwrap();
    assert_eq!(parsed.request.uri, "/p");
    assert_eq!(parsed.request.body, Bytes::from_static(b"payload"));
}

#[test]
fn parse_ndjson_line_reports_lineno_in_error_message() {
    let err = parse_ndjson_line("{ not json }", 41).unwrap_err();
    assert!(err.to_string().contains("line 42"));
}
