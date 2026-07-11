//! Tests for the `trace-capture` feature: the BTreeEngine page-access
//! trace that feeds the eviction-policy research fixtures.
//!
//! Traced: every logical node access (reads in descents/scans, writes on
//! mutation and splits). Not traced: `find_parent` sweeps and
//! `persist_pages` serialization — see the `trace` module docs.

#![cfg(feature = "trace-capture")]

use rust_dst_db::config::DatabaseConfig;
use rust_dst_db::storage::btree::BTreeEngine;
use rust_dst_db::storage::btree::trace::TraceOp;
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::traits::state_machine::StateMachine;

const CLIENT: ActorId = ActorId(9);

fn engine() -> BTreeEngine {
    let mut cfg = DatabaseConfig::default();
    cfg.btree_max_leaf_entries = 4;
    cfg.btree_max_internal_keys = 4;
    BTreeEngine::new(ActorId(1), ActorId(2), ActorId(3), &cfg)
}

fn put(e: &mut BTreeEngine, key: u32) {
    e.receive(
        CLIENT,
        Message::BTreePut {
            key: key.to_be_bytes().to_vec(),
            value: vec![0xAB; 8],
        },
    );
}

fn get(e: &mut BTreeEngine, key: u32) {
    e.receive(
        CLIENT,
        Message::BTreeGet {
            key: key.to_be_bytes().to_vec(),
        },
    );
}

#[test]
fn first_put_traces_root_creation_then_rwr() {
    let mut e = engine();
    put(&mut e, 1);
    let trace = e.take_trace();

    // ensure_root W, find_leaf R, leaf-insert W, maybe_split check R —
    // and nothing from persist_pages (which would add extra events).
    let ops: Vec<TraceOp> = trace.iter().map(|ev| ev.op).collect();
    assert_eq!(
        ops,
        vec![TraceOp::Write, TraceOp::Read, TraceOp::Write, TraceOp::Read]
    );
    assert!(trace.iter().all(|ev| ev.page_id == trace[0].page_id));
    assert!(trace.iter().all(|ev| ev.page_type == 0));
}

#[test]
fn split_put_excludes_find_parent_and_persist() {
    let mut e = engine();
    for k in 0..4 {
        put(&mut e, k);
    }
    e.take_trace(); // discard pre-split noise

    // 5th entry exceeds max_leaf=4 and forces a root split.
    put(&mut e, 4);
    let trace = e.take_trace();

    // find_leaf R, leaf-insert W, split-check R, W left, W right,
    // W new root. find_parent's full-map sweep must contribute nothing.
    let ops: Vec<TraceOp> = trace.iter().map(|ev| ev.op).collect();
    assert_eq!(
        ops,
        vec![
            TraceOp::Read,
            TraceOp::Write,
            TraceOp::Read,
            TraceOp::Write,
            TraceOp::Write,
            TraceOp::Write,
        ]
    );
    // The final write is the new internal root; everything before is leaves.
    assert_eq!(trace.last().unwrap().page_type, 1);
    assert!(trace[..trace.len() - 1].iter().all(|ev| ev.page_type == 0));
}

#[test]
fn get_descends_root_to_leaf() {
    let mut e = engine();
    for k in 0..10 {
        put(&mut e, k);
    }
    assert!(e.node_count() > 2, "expected a split tree");
    e.take_trace();

    get(&mut e, 7);
    let trace = e.take_trace();

    // Two-level tree: R(internal root), R(leaf), R(leaf again in get()).
    assert_eq!(trace.len(), 3);
    assert!(trace.iter().all(|ev| ev.op == TraceOp::Read));
    assert_eq!(trace[0].page_type, 1);
    assert_eq!(trace[1].page_type, 0);
    assert_eq!(trace[2].page_id, trace[1].page_id);
}

#[test]
fn full_scan_reads_root_and_every_leaf() {
    let mut e = engine();
    for k in 0..10 {
        put(&mut e, k);
    }
    let leaves = e.node_count() - 1; // two-level tree: one internal root
    e.take_trace();

    e.receive(
        CLIENT,
        Message::BTreeScan {
            start: None,
            end: None,
        },
    );
    let trace = e.take_trace();

    assert!(trace.iter().all(|ev| ev.op == TraceOp::Read));
    assert_eq!(trace[0].page_type, 1);
    let leaf_reads: Vec<_> = trace.iter().filter(|ev| ev.page_type == 0).collect();
    assert_eq!(leaf_reads.len(), leaves);
    let distinct: std::collections::BTreeSet<u64> =
        leaf_reads.iter().map(|ev| ev.page_id).collect();
    assert_eq!(distinct.len(), leaves, "each leaf read exactly once");
}

#[test]
fn take_trace_drains() {
    let mut e = engine();
    put(&mut e, 1);
    assert!(!e.take_trace().is_empty());
    assert!(e.take_trace().is_empty());
}

#[test]
fn identical_workloads_yield_identical_traces() {
    let run = || {
        let mut e = engine();
        for k in [5u32, 3, 9, 1, 7, 2, 8, 0, 6, 4] {
            put(&mut e, k);
        }
        get(&mut e, 9);
        e.receive(
            CLIENT,
            Message::BTreeDelete {
                key: 3u32.to_be_bytes().to_vec(),
            },
        );
        e.receive(
            CLIENT,
            Message::BTreeScan {
                start: None,
                end: None,
            },
        );
        e.take_trace()
    };
    assert_eq!(run(), run());
}
